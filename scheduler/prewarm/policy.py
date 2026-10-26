"""Bounded forecast-to-capacity policy (CORE-09). Pure: no I/O, no clock.

Per function, each tick:
  1. Reject missing, invalid, stale, future-dated or mismatched forecasts -> reactive
     fallback (0 proactive replicas; Knative's own autoscaler still serves traffic).
  2. Look at predicted buckets that overlap [now, now + lead_time + tick). Capacity
     asked for now must be Ready before the next decision could ask again.
  3. Turn the peak predicted count into a warm replica target (binary or capacity mode),
     capped per function.
  4. Zero demand releases capacity only after a cooldown (avoids flapping).
Then the total proactive budget is shared out in a stable order:
highest predicted peak first, ties by function_id. Reductions are recorded.
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta

from .contracts import Forecast, decision_record, format_ts

MODES = ("binary", "capacity")


@dataclass
class PolicyConfig:
    interval_seconds: int = 30
    # Measured p95 patch -> Ready capacity was 2032 ms (cached image, kind on a laptop),
    # plus a ~3 s margin. Source: docs/decisions/0001-knative-actuation.md.
    lead_time_ms: int = 5000
    tick_seconds: float = 5.0
    per_function_cap: int = 2          # experiment setting (docs/03-architecture.md)
    total_budget: int = 6              # experiment setting (docs/03-architecture.md)
    cooldown_seconds: float = 60.0
    demand_threshold: float = 0.5      # predicted count needed to justify a warm replica
    mode: str = "binary"
    service_ms: float = 50.0           # capacity mode: benchmarked service time
    target_concurrency: int = 1        # capacity mode: matches containerConcurrency
    target_utilisation: float = 0.7    # capacity mode: headroom for bursts inside a bucket
    max_forecast_age_seconds: float = 90.0
    clock_skew_seconds: float = 2.0
    dry_run: bool = True

    def validate(self) -> "PolicyConfig":
        if self.mode not in MODES:
            raise ValueError("mode must be one of %s" % (MODES,))
        if self.per_function_cap < 0 or self.total_budget < 0:
            raise ValueError("caps must be non-negative")
        if self.interval_seconds <= 0 or self.tick_seconds <= 0 or self.lead_time_ms < 0:
            raise ValueError("interval, tick and lead time must be positive")
        if not (0 < self.target_utilisation <= 1) or self.target_concurrency <= 0:
            raise ValueError("bad capacity-mode settings")
        return self

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class _FunctionState:
    held_target: int = 0
    last_positive_at: datetime = None


@dataclass
class _Plan:
    function_id: str
    forecast: Forecast = None
    peak: float = 0.0
    requested: int = 0
    desired: int = 0
    outcome: str = "forecast"          # forecast | hold | fallback
    notes: list = field(default_factory=list)
    window: tuple = None


class Policy:
    def __init__(self, config: PolicyConfig, functions):
        self.config = config.validate()
        self.functions = sorted(set(functions))
        self._state = {fn: _FunctionState() for fn in self.functions}

    # forecasts: function_id -> Forecast | str (reason it was rejected on input) | None
    def decide(self, now: datetime, forecasts: dict):
        plans = [self._plan(fn, now, forecasts.get(fn)) for fn in self.functions]
        remaining = self.config.total_budget
        granted = {}
        for plan in sorted(plans, key=lambda p: (-p.peak, p.function_id)):
            grant = min(plan.desired, remaining)
            if grant < plan.desired:
                plan.notes.append("total budget %d: requested %d, granted %d"
                                  % (self.config.total_budget, plan.desired, grant))
            remaining -= grant
            granted[plan.function_id] = (grant, remaining)
        decisions, audits = [], []
        for plan in plans:
            grant, left = granted[plan.function_id]
            state = self._state[plan.function_id]
            state.held_target = grant if plan.outcome != "fallback" else 0
            decisions.append(decision_record(
                plan.function_id, now,
                plan.forecast.generated_at if plan.forecast else None,
                grant, self.config.lead_time_ms, "; ".join(plan.notes), self.config.dry_run))
            audits.append({
                "function_id": plan.function_id, "decided_at": format_ts(now),
                "outcome": plan.outcome, "model": plan.forecast.model if plan.forecast else None,
                "window_start": format_ts(plan.window[0]) if plan.window else None,
                "window_end": format_ts(plan.window[1]) if plan.window else None,
                "predicted_peak": plan.peak, "requested": plan.requested,
                "desired_after_cap_and_cooldown": plan.desired, "granted": grant,
                "budget_left": left, "budget_limited": grant < plan.desired,
            })
        return decisions, audits

    def _fallback(self, plan: _Plan, reason: str) -> _Plan:
        plan.outcome = "fallback"
        plan.desired = 0
        plan.notes.append(reason + "; reactive fallback")
        self._state[plan.function_id].last_positive_at = None
        return plan

    def _plan(self, fn: str, now: datetime, forecast) -> _Plan:
        cfg = self.config
        plan = _Plan(fn)
        if forecast is None:
            return self._fallback(plan, "no forecast")
        if isinstance(forecast, str):
            return self._fallback(plan, "invalid forecast (%s)" % forecast)
        if forecast.function_id != fn:
            return self._fallback(plan, "forecast is for %s" % forecast.function_id)
        if forecast.generated_at > now + timedelta(seconds=cfg.clock_skew_seconds):
            return self._fallback(plan, "forecast generated in the future (%s)"
                                  % format_ts(forecast.generated_at))
        if now >= forecast.valid_until:
            return self._fallback(plan, "stale forecast (expired %s)" % format_ts(forecast.valid_until))
        if (now - forecast.generated_at).total_seconds() > cfg.max_forecast_age_seconds:
            return self._fallback(plan, "stale forecast (older than %gs)" % cfg.max_forecast_age_seconds)
        if forecast.interval_seconds != cfg.interval_seconds:
            return self._fallback(plan, "forecast interval %ds != %ds"
                                  % (forecast.interval_seconds, cfg.interval_seconds))
        plan.forecast = forecast

        width = timedelta(seconds=forecast.interval_seconds)
        window_end = now + timedelta(milliseconds=cfg.lead_time_ms, seconds=cfg.tick_seconds)
        plan.window = (now, window_end)
        relevant = [count for start, count in forecast.predictions
                    if start < window_end and start + width > now]
        if not relevant:
            return self._fallback(plan, "forecast does not cover %s to %s"
                                  % (format_ts(now), format_ts(window_end)))
        plan.peak = max(relevant)
        plan.requested = self._replicas_for(plan.peak)
        desired = min(plan.requested, cfg.per_function_cap)
        if plan.requested > 0:
            plan.notes.append("predicted peak %.2f in next %.0fs -> %d warm"
                              % (plan.peak, (window_end - now).total_seconds(), plan.requested))
        if desired < plan.requested:
            plan.notes.append("per-function cap %d" % cfg.per_function_cap)

        state = self._state[fn]
        if desired > 0:
            state.last_positive_at = now
        elif state.held_target > 0 and state.last_positive_at is not None:
            release_at = state.last_positive_at + timedelta(seconds=cfg.cooldown_seconds)
            if now < release_at:
                desired = min(state.held_target, cfg.per_function_cap)
                plan.outcome = "hold"
                plan.notes.append("predicted peak %.2f below threshold; holding %d until cooldown ends %s"
                                  % (plan.peak, desired, format_ts(release_at)))
        if desired == 0 and plan.outcome == "forecast":
            plan.notes.append("predicted peak %.2f below threshold %.2f; release"
                              % (plan.peak, cfg.demand_threshold))
        plan.desired = desired
        return plan

    def _replicas_for(self, peak: float) -> int:
        cfg = self.config
        if peak < cfg.demand_threshold or peak <= 0:
            return 0
        if cfg.mode == "binary":
            return 1
        # Little's law: mean in-flight requests = arrival rate x service time.
        rate_per_s = peak / cfg.interval_seconds
        in_flight = rate_per_s * cfg.service_ms / 1000.0
        return max(1, math.ceil(in_flight / (cfg.target_concurrency * cfg.target_utilisation)))

"""Offline timing check: do the settings let a Pod become Ready before each burst?

No cluster, no network. A client trace is pushed through the real pipeline code on a
simulated clock:

  collector   5 s buckets; each one is written at bucket end + grace + publish delay
  predictor   the real predictor models (--model-config), live shared forecast format,
              stamped with that simulated publication time
  scheduler   the real ForecastBook and Policy, ticking every --tick seconds

and a deliberately simple model of Knative for ONE function (--function):

  min-scale 0 -> 1 with no Pod     Pod Ready --ready-s later (CORE-04 spike p95)
  request with no Pod              reactive cold start, Ready --ready-s later
  scale to zero                    Ready ends at max(min-scale released,
                                   last request + stable window) + scale-to-zero grace;
                                   the Pod is gone --termination-s after that

It ignores scale-out above one Pod and uses Knative's default autoscaler windows.
It answers "do the timing settings leave room?", not "what will the cluster do?".
Live evidence comes from tools.readiness_timeline.

Run from scheduler/:
    python -m tools.timing_check --workload long-gap --seed 401 --cooldown 20
    python -m tools.timing_check --workload long-gap --seed 401 --cooldown 20 --sweep
    python -m tools.timing_check --workload periodic-30s --seed 319 --duration 600 \
        --model-config ../predictor/configs/shadow-5s.json
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from prewarm.contracts import format_ts
from prewarm.policy import Policy, PolicyConfig
from prewarm.source import ForecastBook

ROOT = Path(__file__).resolve().parents[2]
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)   # on every bucket boundary
FUNCTIONS = ("periodic", "steady", "sparse")
PIPELINE_HEAD_START_S = 30.0    # collector/predictor/scheduler start before the replay


@dataclass(frozen=True)
class Settings:
    interval: int = 5               # bucket width, s
    horizons: int = 6               # forecast horizons per publication
    grace: float = 10.0             # collector finalisation grace, s
    publish_delay: float = 1.5      # collector tick (<=1 s) + predictor poll (0.5 s)
    tick: float = 5.0
    lead_time_ms: int = 5000
    cooldown: float = 60.0
    threshold: float = 0.5
    ready_s: float = 2.032          # patch -> Ready p95, docs/decisions/0001
    stable_window: float = 60.0     # Knative default
    scale_to_zero_grace: float = 30.0   # Knative default
    termination_s: float = 29.0     # observed, docs/14-shutdown-verification.md
    tick_phase: float = 0.0         # scheduler tick offset from the bucket grid
    run_offset: float = 0.0         # replay start offset from the bucket grid
    burst_gap: float = 30.0         # a quiet gap this long starts a new burst
    function: str = "periodic"


def budget(s: Settings) -> dict:
    """Worst-case arithmetic. All values in seconds.

    The newest forecast a tick can see may come from a bucket that ended
    grace + publish_delay + interval ago, and it covers horizons x interval from there.
    The scheduler must see lead + tick ahead. A burst bucket is first seen by a tick
    between (lead + tick) and lead before it starts, so its Pod is Ready at least
    lead - ready before the bucket starts.
    """
    lead = s.lead_time_ms / 1000.0
    coverage_slack = (s.horizons - 1) * s.interval - s.grace - s.publish_delay - lead - s.tick
    return {
        "forecast_reach_s": s.horizons * s.interval,
        "spent_s": {"bucket_must_finish": s.interval, "grace": s.grace,
                    "publish_delay": s.publish_delay, "tick": s.tick, "lead": lead},
        "coverage_slack_s": round(coverage_slack, 3),
        "worst_ready_margin_s": round(lead - s.ready_s, 3),
        "best_ready_margin_s": round(lead + s.tick - s.ready_s, 3),
        "max_extra_hold_vs_knative_s": round(max(0.0, s.interval + s.tick + s.cooldown - s.stable_window), 3),
        "ok": coverage_slack >= 0 and lead > s.ready_s,
    }


# --- workloads ---------------------------------------------------------------
def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_trace(workload, seed=None, duration=None, path=None):
    """Returns (trace rows, replay duration in seconds or None if unknown)."""
    if path:
        with open(path, encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()], duration
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    if workload == "long-gap":
        module = _load_module(ROOT / "scripts" / "prepare_long_gap.py", "prepare_long_gap")
        return module.build(seed), module.DURATION
    from evaluation.workload import generate_trace
    return generate_trace(seed, duration or 600), duration or 600


def _predictors(model_config: dict):
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from predictor.models import make_predictor
    return {fn: make_predictor(**model_config.get(fn, {"model": "sma"})) for fn in FUNCTIONS}


# --- simulation ----------------------------------------------------------------
def _forecast_lines(arrivals, start, end, s: Settings, model_config):
    """Bucket the arrivals and run the predictor; returns shared-format JSON lines."""
    predictors = _predictors(model_config)
    from predictor.main import live_forecast
    width = timedelta(seconds=s.interval)
    counts = {}
    for fn, times in arrivals.items():
        for t in times:
            index = int((t - EPOCH).total_seconds() // s.interval)
            counts[fn, index] = counts.get(fn, 0) + 1
    first = int((start - EPOCH).total_seconds() // s.interval)
    last = int((end - EPOCH).total_seconds() // s.interval)
    lines = []
    for index in range(first, last):
        bucket_start = EPOCH + index * width
        published = bucket_start + width + timedelta(seconds=s.grace + s.publish_delay)
        for fn in FUNCTIONS:
            bucket = {"function_id": fn, "bucket_start": format_ts(bucket_start),
                      "interval_seconds": s.interval,
                      "invocation_count": counts.get((fn, index), 0), "complete": True}
            if not predictors[fn].ingest(bucket):
                continue
            pred = predictors[fn].predict_horizons(fn, s.horizons)
            record = pred and live_forecast(pred, s.interval, published)
            if record:
                lines.append(json.dumps(record))
    return lines


class PodModel:
    def __init__(self, s: Settings):
        self.s = s
        self.min_scale = 0
        self.released_at = None
        self.pod = None            # {start, ready, cause, last_request}
        self.ready_spans = []      # (ready_from, ready_to, cause)
        self.gone_at = []
        self.patches = []          # {at, forecast_generated_at, pod_already_ready, released_at}

    def _retire_if_due(self, t):
        pod = self.pod
        if pod is None or self.min_scale:
            return
        idle_from = [pod["ready"]]
        if self.released_at is not None and self.released_at >= pod["start"]:
            idle_from.append(self.released_at)
        if pod["last_request"] is not None:
            idle_from.append(pod["last_request"] + timedelta(seconds=self.s.stable_window))
        zero_at = max(idle_from) + timedelta(seconds=self.s.scale_to_zero_grace)
        if t >= zero_at:
            self.ready_spans.append((pod["ready"], zero_at, pod["cause"]))
            self.gone_at.append(zero_at + timedelta(seconds=self.s.termination_s))
            self.pod = None

    def _start(self, t, cause):
        self.pod = {"start": t, "ready": t + timedelta(seconds=self.s.ready_s),
                    "cause": cause, "last_request": None}

    def tick(self, t, target, forecast_generated_at):
        self._retire_if_due(t)
        if target > 0 and not self.min_scale:
            self.min_scale = 1
            self.patches.append({"at": t, "forecast_generated_at": forecast_generated_at,
                                 "pod_already_ready": self.pod is not None and self.pod["ready"] <= t,
                                 "released_at": None})
            if self.pod is None:
                self._start(t, "prewarm")
        elif target <= 0 and self.min_scale:
            self.min_scale = 0
            self.released_at = t
            self.patches[-1]["released_at"] = t

    def request(self, t):
        self._retire_if_due(t)
        if self.pod is None:
            self._start(t, "reactive")
            label = "cold"
        else:
            label = "starting" if self.pod["ready"] > t else "warm"
        self.pod["last_request"] = t
        return label, dict(self.pod)

    def finish(self, t):
        self._retire_if_due(t)
        if self.pod is not None:
            self.ready_spans.append((self.pod["ready"], max(t, self.pod["ready"]), self.pod["cause"]))


def _bursts(times, gap_s):
    bursts = []
    for t in sorted(times):
        if bursts and (t - bursts[-1][-1]).total_seconds() < gap_s:
            bursts[-1].append(t)
        else:
            bursts.append([t])
    return bursts


def _ready_seconds(spans, a, b):
    total = 0.0
    for start, end, _ in spans:
        lo, hi = max(start, a), min(end, b)
        if hi > lo:
            total += (hi - lo).total_seconds()
    return total


def simulate(trace, model_config, s: Settings, use_scheduler=True, duration=None):
    """Ready pod-seconds are counted inside the replay window only, as in evaluation.
    The pipeline keeps running for 240 s afterwards so releases can be observed."""
    run_start = EPOCH + timedelta(hours=1, seconds=s.run_offset)
    arrivals = {fn: [] for fn in FUNCTIONS}
    for row in trace:
        if row.get("request_kind", "user") == "user":
            arrivals[row["function_id"]].append(run_start + timedelta(seconds=row["scheduled_at_s"]))
    last_arrival = max(t for times in arrivals.values() for t in times)
    replay_end = run_start + timedelta(seconds=duration) if duration else last_arrival
    pipeline_start = run_start - timedelta(seconds=PIPELINE_HEAD_START_S)
    end = last_arrival + timedelta(seconds=240)

    events = [(t, 1, "request", None) for t in arrivals[s.function]]
    decisions = []
    if use_scheduler:
        book = ForecastBook(FUNCTIONS, s.interval)
        for number, line in enumerate(_forecast_lines(arrivals, pipeline_start, end, s, model_config), 1):
            book.add_line(number, line)
        config = PolicyConfig(interval_seconds=s.interval, tick_seconds=s.tick,
                              lead_time_ms=s.lead_time_ms, cooldown_seconds=s.cooldown,
                              demand_threshold=s.threshold, dry_run=True)
        policy = Policy(config, FUNCTIONS)
        grid = EPOCH + timedelta(seconds=((pipeline_start - EPOCH).total_seconds() // s.tick) * s.tick)
        t = grid + timedelta(seconds=s.tick_phase)
        while t < end:
            out, audits = policy.decide(t, book.snapshot(t))
            for decision, audit in zip(out, audits):
                if decision["function_id"] == s.function:
                    decisions.append((t, decision, audit))
                    events.append((t, 0, "tick", decision))
            t += timedelta(seconds=s.tick)
    events.sort(key=lambda e: (e[0], e[1]))   # a tick at the same instant acts first

    pods = PodModel(s)
    labels = {}
    for t, _, kind, payload in events:
        if kind == "tick":
            pods.tick(t, payload["target_warm_replicas"], payload["forecast_generated_at"])
        else:
            labels[t] = pods.request(t)
    pods.finish(end)

    bursts = _bursts(arrivals[s.function], s.burst_gap)
    rows = []
    for i, burst in enumerate(bursts):
        first = burst[0]
        label, pod = labels[first]
        patch = next((p for p in reversed(pods.patches)
                      if p["at"] <= first and (p["released_at"] is None or p["released_at"] > first)), None)
        if label == "warm" and patch and not patch["pod_already_ready"] and pod["start"] >= patch["at"]:
            outcome = "prewarmed"
        elif label == "warm" and patch:
            outcome = "retained"
        elif label == "warm":
            outcome = "warm_without_forecast"
        elif label == "starting" and pod["cause"] == "prewarm":
            outcome = "late"
        else:
            outcome = "missed"
        exposed = sum(1 for t in burst if labels[t][0] != "warm")
        next_first = bursts[i + 1][0] if i + 1 < len(bursts) else end
        release = next((p["released_at"] for p in pods.patches
                        if p["released_at"] and burst[-1] <= p["released_at"] < next_first), None)
        gone = next((g for g in pods.gone_at if burst[-1] <= g < next_first), None)
        gap = (next_first - burst[-1]).total_seconds()
        rows.append({
            "burst": i + 1,
            "first_request_s": round((first - run_start).total_seconds(), 3),
            "requests": len(burst),
            "forecast_published_s": (round((datetime.fromisoformat(patch["forecast_generated_at"]
                                     .replace("Z", "+00:00")) - run_start).total_seconds(), 3)
                                     if patch and patch["forecast_generated_at"] else None),
            "min_scale_patch_s": round((patch["at"] - run_start).total_seconds(), 3) if patch else None,
            "pod_ready_s": round((pod["ready"] - run_start).total_seconds(), 3),
            "ready_margin_s": round((first - pod["ready"]).total_seconds(), 3),
            "outcome": outcome,
            "exposed_requests": exposed,
            "release_after_last_request_s": (round((release - burst[-1]).total_seconds(), 3)
                                             if release else None),
            "pod_gone_after_last_request_s": (round((gone - burst[-1]).total_seconds(), 3)
                                              if gone else None),
            "zero_ready_s_before_next": round(gap - _ready_seconds(pods.ready_spans, burst[-1], next_first), 3),
        })
    fallbacks = sum(1 for _, _, audit in decisions if audit["outcome"] == "fallback")
    return {
        "settings": asdict(s),
        "scheduler": use_scheduler,
        "bursts": rows,
        "outcomes": {k: sum(1 for r in rows if r["outcome"] == k) for k in
                     ("prewarmed", "late", "retained", "warm_without_forecast", "missed")},
        "exposed_requests": sum(r["exposed_requests"] for r in rows),
        "ready_pod_seconds_in_replay": round(_ready_seconds(pods.ready_spans, run_start, replay_end), 1),
        "prewarms_after_replay_end": sum(1 for p in pods.patches if p["at"] >= replay_end),
        "fallback_ticks": fallbacks,
        "ticks": len(decisions),
    }


def sweep(trace, model_config, s: Settings, duration=None):
    """Every run/bucket alignment and tick phase on a 1 s grid."""
    runs = []
    for offset in range(int(s.interval)):
        for phase in range(int(s.tick)):
            result = simulate(trace, model_config, replace(s, run_offset=offset, tick_phase=phase),
                              duration=duration)
            margins = [r["ready_margin_s"] for r in result["bursts"] if r["outcome"] == "prewarmed"]
            runs.append({"run_offset": offset, "tick_phase": phase, **result["outcomes"],
                         "exposed_requests": result["exposed_requests"],
                         "min_prewarm_margin_s": min(margins) if margins else None,
                         "ready_pod_seconds_in_replay": result["ready_pod_seconds_in_replay"]})
    return runs


def _print_table(result):
    cols = ("burst", "first_request_s", "forecast_published_s", "min_scale_patch_s", "pod_ready_s",
            "ready_margin_s", "outcome", "exposed_requests", "release_after_last_request_s",
            "pod_gone_after_last_request_s", "zero_ready_s_before_next")
    print("\t".join(cols))
    for row in result["bursts"]:
        print("\t".join("" if row[c] is None else str(row[c]) for c in cols))
    print("outcomes:", result["outcomes"], "exposed:", result["exposed_requests"],
          "ready pod-s in replay:", result["ready_pod_seconds_in_replay"],
          "prewarms after replay end:", result["prewarms_after_replay_end"])


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--workload", choices=("long-gap", "periodic-30s"), default="long-gap")
    p.add_argument("--seed", type=int, default=401)
    p.add_argument("--duration", type=float, help="periodic-30s only (default 600)")
    p.add_argument("--trace", help="use this trace.jsonl instead of generating one")
    p.add_argument("--model-config", default=str(ROOT / "predictor" / "configs" / "long-gap-5s.json"))
    p.add_argument("--output", help="write the JSON report here")
    p.add_argument("--sweep", action="store_true", help="all alignments and tick phases (1 s grid)")
    p.add_argument("--no-scheduler", action="store_true", help="reactive only, for comparison")
    d = Settings()
    for name, value in asdict(d).items():
        p.add_argument("--" + name.replace("_", "-"), type=type(value), default=value)
    args = p.parse_args(argv)
    s = Settings(**{name: getattr(args, name) for name in asdict(d)})
    trace, duration = load_trace(args.workload, args.seed, args.duration, args.trace)
    model_config = json.loads(Path(args.model_config).read_text())

    report = {"workload": args.workload, "seed": args.seed, "model_config": model_config,
              "budget": budget(s), "note": "Simulated pipeline and Knative model; not live evidence."}
    print("budget:", json.dumps(report["budget"]))
    if args.sweep:
        report["sweep"] = sweep(trace, model_config, s, duration)
        for row in report["sweep"]:
            print(row)
    else:
        report["run"] = simulate(trace, model_config, s, use_scheduler=not args.no_scheduler,
                                 duration=duration)
        _print_table(report["run"])
    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())

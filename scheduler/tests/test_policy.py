import json
import pathlib
import unittest
from datetime import datetime, timedelta, timezone

from prewarm.contracts import DECISION_FIELDS, Forecast, InvalidForecast, format_ts
from prewarm.policy import Policy, PolicyConfig

ROOT = pathlib.Path(__file__).resolve().parents[2]
T0 = datetime(2026, 10, 1, 0, 0, 30, tzinfo=timezone.utc)


def forecast(fn="periodic", generated=T0, counts=(6, 6), interval=30, valid_s=60, **overrides):
    raw = {
        "function_id": fn, "generated_at": format_ts(generated), "history_end": format_ts(generated),
        "interval_seconds": interval, "model": "moving-average",
        "predictions": [{"bucket_start": format_ts(generated + timedelta(seconds=interval * i)),
                         "invocation_count": c} for i, c in enumerate(counts)],
        "valid_until": format_ts(generated + timedelta(seconds=valid_s)),
    }
    raw.update(overrides)
    return Forecast(raw)


class ContractTests(unittest.TestCase):
    def test_shared_samples_are_accepted(self):
        sample = json.loads((ROOT / "contracts/forecast.example.json").read_text())
        self.assertEqual(Forecast(sample).function_id, "periodic")

    def test_decision_matches_contract_sample_fields(self):
        sample = json.loads((ROOT / "contracts/decision.example.json").read_text())
        decisions, _ = Policy(PolicyConfig(), ["periodic"]).decide(T0, {"periodic": forecast()})
        self.assertEqual(tuple(decisions[0]), DECISION_FIELDS)
        self.assertEqual(set(decisions[0]), set(sample))

    def test_bad_counts_rejected(self):
        for bad in (-1, float("nan"), float("inf"), True, "3"):
            with self.assertRaises(InvalidForecast):
                forecast(counts=(bad,))

    def test_prediction_before_history_end_rejected(self):
        with self.assertRaises(InvalidForecast):
            forecast(history_end=format_ts(T0 + timedelta(seconds=60)),
                     generated_at=format_ts(T0 + timedelta(seconds=60)))

    def test_missing_field_rejected(self):
        raw = forecast().raw.copy()
        del raw["valid_until"]
        with self.assertRaises(InvalidForecast):
            Forecast(raw)


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.cfg = PolicyConfig(lead_time_ms=5000, tick_seconds=5, cooldown_seconds=60)

    def decide(self, policy, now, forecasts):
        decisions, audits = policy.decide(now, forecasts)
        return {d["function_id"]: d for d in decisions}, {a["function_id"]: a for a in audits}

    def test_positive_demand_warms_one(self):
        d, a = self.decide(Policy(self.cfg, ["periodic"]), T0, {"periodic": forecast()})
        self.assertEqual(d["periodic"]["target_warm_replicas"], 1)
        self.assertTrue(d["periodic"]["dry_run"])
        self.assertEqual(d["periodic"]["forecast_generated_at"], "2026-10-01T00:00:30Z")
        self.assertEqual(a["periodic"]["outcome"], "forecast")

    def test_missing_forecast_is_reactive_fallback(self):
        d, a = self.decide(Policy(self.cfg, ["periodic"]), T0, {})
        self.assertEqual(d["periodic"]["target_warm_replicas"], 0)
        self.assertIsNone(d["periodic"]["forecast_generated_at"])
        self.assertEqual(a["periodic"]["outcome"], "fallback")
        self.assertIn("no forecast", d["periodic"]["reason"])

    def test_invalid_input_reason_is_fallback(self):
        d, _ = self.decide(Policy(self.cfg, ["periodic"]), T0, {"periodic": "bad timestamp"})
        self.assertIn("invalid forecast (bad timestamp)", d["periodic"]["reason"])

    def test_expired_forecast_is_fallback(self):
        d, _ = self.decide(Policy(self.cfg, ["periodic"]), T0 + timedelta(seconds=61),
                           {"periodic": forecast(counts=(6, 6, 6))})
        self.assertEqual(d["periodic"]["target_warm_replicas"], 0)
        self.assertIn("stale", d["periodic"]["reason"])

    def test_old_but_unexpired_forecast_is_fallback(self):
        cfg = PolicyConfig(max_forecast_age_seconds=20)
        d, _ = self.decide(Policy(cfg, ["periodic"]), T0 + timedelta(seconds=25),
                           {"periodic": forecast()})
        self.assertIn("older than", d["periodic"]["reason"])

    def test_future_forecast_is_rejected(self):
        d, _ = self.decide(Policy(self.cfg, ["periodic"]), T0 - timedelta(seconds=10),
                           {"periodic": forecast()})
        self.assertEqual(d["periodic"]["target_warm_replicas"], 0)
        self.assertIn("future", d["periodic"]["reason"])

    def test_interval_mismatch_is_fallback(self):
        d, _ = self.decide(Policy(self.cfg, ["periodic"]), T0,
                           {"periodic": forecast(interval=60, valid_s=120)})
        self.assertIn("interval", d["periodic"]["reason"])

    def test_window_only_sees_buckets_within_lead_time(self):
        # Demand only in the second bucket (starts 30s away), lead+tick = 10s: not yet.
        policy = Policy(self.cfg, ["periodic"])
        d, a = self.decide(policy, T0, {"periodic": forecast(counts=(0, 5))})
        self.assertEqual(d["periodic"]["target_warm_replicas"], 0)
        self.assertEqual(a["periodic"]["predicted_peak"], 0)
        # 22s later the second bucket is inside the window.
        d, _ = self.decide(policy, T0 + timedelta(seconds=22), {"periodic": forecast(counts=(0, 5))})
        self.assertEqual(d["periodic"]["target_warm_replicas"], 1)

    def test_below_threshold_is_zero(self):
        d, _ = self.decide(Policy(self.cfg, ["sparse"]), T0,
                           {"sparse": forecast("sparse", counts=(0.2, 0.2))})
        self.assertEqual(d["sparse"]["target_warm_replicas"], 0)
        self.assertIn("below threshold", d["sparse"]["reason"])

    def test_cooldown_holds_then_releases(self):
        policy = Policy(self.cfg, ["periodic"])
        self.decide(policy, T0, {"periodic": forecast(counts=(4, 4))})
        later = T0 + timedelta(seconds=30)
        quiet = forecast(generated=later, counts=(0, 0))
        d, a = self.decide(policy, later, {"periodic": quiet})
        self.assertEqual(d["periodic"]["target_warm_replicas"], 1)
        self.assertEqual(a["periodic"]["outcome"], "hold")
        d, _ = self.decide(policy, T0 + timedelta(seconds=61), {"periodic": quiet})
        self.assertEqual(d["periodic"]["target_warm_replicas"], 0)

    def test_fallback_releases_immediately(self):
        policy = Policy(self.cfg, ["periodic"])
        self.decide(policy, T0, {"periodic": forecast()})
        d, _ = self.decide(policy, T0 + timedelta(seconds=5), {})
        self.assertEqual(d["periodic"]["target_warm_replicas"], 0)

    def test_capacity_mode_and_per_function_cap(self):
        cfg = PolicyConfig(mode="capacity", service_ms=1000, target_utilisation=1.0, per_function_cap=2)
        # 60 requests / 30s * 1s service = 2 in flight -> 2 replicas.
        d, _ = self.decide(Policy(cfg, ["steady"]), T0, {"steady": forecast("steady", counts=(60, 60))})
        self.assertEqual(d["steady"]["target_warm_replicas"], 2)
        # 300 requests -> 10 wanted, capped at 2.
        d, a = self.decide(Policy(cfg, ["steady"]), T0, {"steady": forecast("steady", counts=(300, 300))})
        self.assertEqual(d["steady"]["target_warm_replicas"], 2)
        self.assertEqual(a["steady"]["requested"], 10)
        self.assertIn("per-function cap", d["steady"]["reason"])

    def test_capacity_mode_small_demand_is_one(self):
        cfg = PolicyConfig(mode="capacity", service_ms=10)
        d, _ = self.decide(Policy(cfg, ["steady"]), T0, {"steady": forecast("steady", counts=(1, 1))})
        self.assertEqual(d["steady"]["target_warm_replicas"], 1)

    def test_total_budget_stable_priority(self):
        cfg = PolicyConfig(total_budget=2)
        fns = ["a", "b", "c"]
        fc = {"a": forecast("a", counts=(3, 3)), "b": forecast("b", counts=(9, 9)),
              "c": forecast("c", counts=(3, 3))}
        d, a = self.decide(Policy(cfg, fns), T0, fc)
        # b has the highest peak; a beats c on the name tie-break.
        self.assertEqual([d[f]["target_warm_replicas"] for f in fns], [1, 1, 0])
        self.assertTrue(a["c"]["budget_limited"])
        self.assertIn("granted 0", d["c"]["reason"])

    def test_decisions_are_sorted_and_deterministic(self):
        fns = ["sparse", "periodic", "steady"]
        fc = {f: forecast(f) for f in fns}
        first, _ = Policy(self.cfg, fns).decide(T0, fc)
        second, _ = Policy(self.cfg, list(reversed(fns))).decide(T0, fc)
        self.assertEqual(first, second)
        self.assertEqual([d["function_id"] for d in first], sorted(fns))

    def test_bad_config_rejected(self):
        with self.assertRaises(ValueError):
            Policy(PolicyConfig(mode="magic"), ["a"])


if __name__ == "__main__":
    unittest.main()

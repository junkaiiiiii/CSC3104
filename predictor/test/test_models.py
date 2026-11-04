import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from predictor.backtest import compare
from predictor.main import live_forecast
from predictor.models import EWMAPredictor, SMAPredictor, SeasonalPredictor

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[2]


def bucket(index, count, complete=True, interval=30):
    return {"function_id": "periodic", "bucket_start": (T0 + timedelta(seconds=index*interval)).isoformat(),
            "interval_seconds": interval, "invocation_count": count, "complete": complete}


class ModelTests(unittest.TestCase):
    def test_seasonal_predicts_different_future_phases_using_past_cycles(self):
        model = SeasonalPredictor(season_buckets=6, season_cycles=3)
        pattern = [0, 0, 8, 8, 0, 0]
        for i in range(18):
            model.ingest(bucket(i, pattern[i % 6]))
        prediction = model.predict_horizons("periodic", 4)
        self.assertEqual(prediction["horizon_counts"], [0, 0, 8, 8])
        self.assertEqual(prediction["horizon_methods"], ["seasonal-mean"] * 4)
        self.assertTrue(all(model._parse_iso(row["bucket_start"]) < T0 + timedelta(seconds=18*30)
                            for row in prediction["input_buckets_used"]))

    def test_seasonal_falls_back_without_two_cycles_and_resets_after_gap(self):
        model = SeasonalPredictor(season_buckets=3)
        model.ingest(bucket(0, 6))
        self.assertEqual(model.predict_horizons("periodic")["horizon_methods"], ["fallback-sma"] * 2)
        for i in range(1, 9):
            model.ingest(bucket(i, 6))
        self.assertIn("seasonal-mean", model.predict_horizons("periodic")["horizon_methods"])
        self.assertFalse(model.ingest(bucket(9, 999, complete=False)))
        model.ingest(bucket(10, 0))
        self.assertEqual(model.predict_horizons("periodic")["horizon_counts"], [0, 0])
        self.assertEqual(model.predict_horizons("periodic")["horizon_methods"], ["fallback-sma"] * 2)

    def test_ewma_reacts_to_step_without_using_future_and_resets(self):
        model = EWMAPredictor(alpha=0.8)
        model.ingest(bucket(0, 0))
        model.ingest(bucket(1, 10))
        self.assertEqual(model.predict_horizons("periodic")["horizon_counts"], [8, 8])
        self.assertFalse(model.ingest(bucket(1, 999)))
        self.assertEqual(model.predict_horizons("periodic")["horizon_counts"], [8, 8])
        model.ingest(bucket(3, 2))
        self.assertEqual(model.predict_horizons("periodic")["horizon_counts"], [2, 2])
        self.assertEqual(model.state["periodic"]["training_buckets_seen"], 1)

    def test_seasonal_shared_contract_keeps_horizon_values(self):
        model = SeasonalPredictor(season_buckets=3)
        for i in range(9):
            model.ingest(bucket(i, [0, 6, 0][i % 3]))
        prediction = model.predict_horizons("periodic", 3)
        record = live_forecast(prediction, 30, T0 + timedelta(seconds=9*30+10))
        self.assertEqual([p["invocation_count"] for p in record["predictions"]], [0, 6, 0])
        self.assertEqual(record["valid_until"], "2026-10-01T00:06:00Z")

    def test_backtest_counts_unseen_burst_as_miss(self):
        report = compare([bucket(0, 0), bucket(1, 0), bucket(2, 100)],
                         warmup_buckets=0, season_buckets=3)
        for model in report["metrics"].values():
            self.assertEqual(model["periodic"]["missed_active_buckets"], 1)
            self.assertEqual(model["periodic"]["demand_recall"], 0)

    def test_late_forecasts_do_not_score_as_advance_predictions(self):
        report = compare([bucket(i, 4) for i in range(12)], warmup_buckets=0,
                         horizon_buckets=2, availability_delay=20, lead_time=20)
        for model in report["metrics"].values():
            self.assertEqual(model["periodic"]["advance_forecasts"], 0)
            self.assertEqual(model["periodic"]["fallback_buckets"], 12)
            self.assertEqual(model["periodic"]["mae"], 4)

    def test_learned_repeating_pattern_beats_flat_sma_on_deterministic_fixture(self):
        pattern = [0, 0, 8, 8, 0, 0]
        rows = [bucket(i, pattern[i % 6]) for i in range(120)]
        report = compare(rows, warmup_buckets=18, season_buckets=6)
        self.assertEqual(report["metrics"]["seasonal"]["periodic"]["mae"], 0)
        self.assertGreater(report["metrics"]["sma"]["periodic"]["mae"], 0)
        self.assertEqual(report["metrics"]["seasonal"]["periodic"]["scored_buckets"], 102)

    def test_batch_model_config_and_interval_guard(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, output, config = root / "buckets.jsonl", root / "forecasts.jsonl", root / "models.json"
            source.write_text("".join(json.dumps(bucket(i, i % 3)) + "\n" for i in range(12)))
            config.write_text(json.dumps({"periodic": {"model": "seasonal", "season_buckets": 3}}))
            args = [sys.executable, "-m", "predictor.main", "--run-id", "test-models",
                    "--input", str(source), "--output", str(output), "--model-config", str(config),
                    "--expected-interval", "30", "--horizon-buckets", "3"]
            result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            records = [json.loads(line) for line in output.read_text().splitlines()]
            self.assertEqual(records[-1]["model"], "seasonal-p3-c3")
            self.assertEqual(len(records[-1]["predictions"]), 3)
            scheduler_root = root / "scheduler"
            result = subprocess.run([
                sys.executable, "-m", "prewarm.main", "--run-id", "compatibility",
                "--forecasts", str(output), "--out-dir", str(scheduler_root),
                "--functions", "periodic", "--replay", "2026-10-01T00:00:00Z", "2026-10-01T00:10:00Z",
            ], cwd=ROOT / "scheduler", capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            manifest = json.loads((scheduler_root / "compatibility" / "scheduler_manifest.json").read_text())
            self.assertEqual(manifest["forecasts_accepted"], len(records))
            self.assertEqual((scheduler_root / "compatibility" / "forecast_rejects.jsonl").read_text(), "")
            # A misconfigured fine-resolution live experiment must fail explicitly.
            args[args.index("--expected-interval") + 1] = "5"
            args[args.index("--output") + 1] = str(root / "bad.jsonl")
            result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("input interval", result.stderr)

    def test_invalid_parameters_rejected(self):
        for alpha in (0, -1, float("nan"), 1.1):
            with self.assertRaises(ValueError):
                EWMAPredictor(alpha=alpha)
        with self.assertRaises(ValueError):
            SeasonalPredictor(season_buckets=1)
        with self.assertRaises(ValueError):
            SeasonalPredictor(season_cycles=1)


if __name__ == "__main__":
    unittest.main()

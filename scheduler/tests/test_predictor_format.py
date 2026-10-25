"""Sook Yee's predictor output format (predictor/README.md) as scheduler input."""
import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from prewarm import main as cli
from prewarm.contracts import Forecast, InvalidForecast, format_ts, from_predictor_record
from prewarm.policy import Policy, PolicyConfig
from prewarm.source import ForecastBook

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
T0 = datetime(2026, 10, 1, 0, 1, 30, tzinfo=timezone.utc)

# The example line from predictor/README.md.
README_LINE = {"function_id": "periodic", "forecast_bucket_start": "2026-10-01T00:01:30Z",
               "forecast_invocation_count": 0.67, "window_size": 3, "input_buckets_used": [
                   {"bucket_start": "2026-10-01T00:00:00Z", "invocation_count": 1},
                   {"bucket_start": "2026-10-01T00:00:30Z", "invocation_count": 0},
                   {"bucket_start": "2026-10-01T00:01:00Z", "invocation_count": 1}]}


def line(fn, start, count):
    return {"function_id": fn, "forecast_bucket_start": format_ts(start),
            "forecast_invocation_count": count, "window_size": 3,
            "input_buckets_used": [{"bucket_start": format_ts(start - timedelta(seconds=30)),
                                    "invocation_count": count}]}


class ConversionTests(unittest.TestCase):
    def test_readme_example_converts(self):
        f = Forecast(from_predictor_record(README_LINE))
        self.assertEqual(format_ts(f.history_end), "2026-10-01T00:01:30Z")
        self.assertEqual(format_ts(f.generated_at), "2026-10-01T00:01:40Z")
        self.assertEqual(format_ts(f.valid_until), "2026-10-01T00:02:30Z")
        self.assertEqual(f.model, "sma-w3")
        self.assertEqual([c for _s, c in f.predictions], [0.67, 0.67])

    def test_delay_setting(self):
        f = Forecast(from_predictor_record(README_LINE, availability_delay_s=25))
        self.assertEqual(format_ts(f.generated_at), "2026-10-01T00:01:55Z")

    def test_future_input_bucket_rejected(self):
        bad = dict(README_LINE, input_buckets_used=[
            {"bucket_start": "2026-10-01T00:01:30Z", "invocation_count": 1}])
        with self.assertRaises(InvalidForecast):
            from_predictor_record(bad)

    def test_bad_count_rejected(self):
        with self.assertRaises(InvalidForecast):
            Forecast(from_predictor_record(dict(README_LINE, forecast_invocation_count=-1)))

    def test_missing_field_rejected(self):
        bad = dict(README_LINE)
        del bad["forecast_invocation_count"]
        with self.assertRaises(InvalidForecast):
            from_predictor_record(bad)


class BookAndPolicyTests(unittest.TestCase):
    def test_hidden_until_available_then_used(self):
        book = ForecastBook(["periodic"])
        book.add_line(1, json.dumps(line("periodic", T0, 5)))
        self.assertIsNone(book.latest("periodic", T0 + timedelta(seconds=9)))
        self.assertIsNotNone(book.latest("periodic", T0 + timedelta(seconds=10)))
        self.assertEqual(book.accepted, 1)

    def test_steady_stream_never_gaps(self):
        # One forecast per bucket: no tick between them should fall back.
        book = ForecastBook(["periodic"])
        for i in range(6):
            book.add_line(i, json.dumps(line("periodic", T0 + timedelta(seconds=30 * i), 5)))
        policy = Policy(PolicyConfig(), ["periodic"])
        now = T0 + timedelta(seconds=10)
        while now < T0 + timedelta(seconds=30 * 5 + 10):
            decisions, audits = policy.decide(now, book.snapshot(now))
            self.assertEqual(audits[0]["outcome"], "forecast", format_ts(now))
            self.assertEqual(decisions[0]["target_warm_replicas"], 1)
            now += timedelta(seconds=5)

    def test_stream_stopping_falls_back(self):
        book = ForecastBook(["periodic"])
        book.add_line(1, json.dumps(line("periodic", T0, 5)))
        policy = Policy(PolicyConfig(), ["periodic"])
        decisions, _ = policy.decide(T0 + timedelta(seconds=61), book.snapshot(T0 + timedelta(seconds=61)))
        self.assertEqual(decisions[0]["target_warm_replicas"], 0)
        self.assertIn("stale", decisions[0]["reason"])

    def test_invalid_newest_line_falls_back(self):
        book = ForecastBook(["periodic"])
        book.add_line(1, json.dumps(line("periodic", T0, 5)))
        book.add_line(2, json.dumps(line("periodic", T0 + timedelta(seconds=30), float("nan"))
                                    ).replace("NaN", "-1"))
        now = T0 + timedelta(seconds=45)
        self.assertIsInstance(book.latest("periodic", now), str)


class RealPredictorTests(unittest.TestCase):
    """Runs predictor/main.py's SMAPredictor and feeds its lines to the scheduler."""

    def setUp(self):
        if REPO not in sys.path:
            sys.path.insert(0, REPO)
        try:
            from predictor.main import SMAPredictor
        except ImportError as exc:      # predictor package absent or changed
            self.skipTest("predictor not importable: %s" % exc)
        self.Predictor = SMAPredictor
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_predictor_output_is_accepted_and_warms_bursts(self):
        start = datetime(2026, 10, 1, tzinfo=timezone.utc)
        sma = self.Predictor(window_size=3)
        path = os.path.join(self.tmp, "forecasts.jsonl")
        with open(path, "w") as fh:
            for i in range(20):
                count = 12 if 8 <= i < 12 else 0          # one burst
                bucket = {"function_id": "periodic", "bucket_start": format_ts(start + timedelta(seconds=30 * i)),
                          "interval_seconds": 30, "invocation_count": count, "complete": True}
                sma.ingest(bucket)
                fh.write(json.dumps(sma.predict_next("periodic")) + "\n")
        argv = ["--run-id", "sma", "--forecasts", path, "--out-dir", self.tmp, "--functions", "periodic",
                "--replay", format_ts(start), format_ts(start + timedelta(minutes=10))]
        self.assertEqual(cli.main(argv), 0)
        with open(os.path.join(self.tmp, "sma", "scheduler_manifest.json")) as fh:
            manifest = json.load(fh)
        self.assertEqual(manifest["forecasts_accepted"], 20)
        self.assertNotIn("forecast_rejected", manifest["totals"])
        with open(os.path.join(self.tmp, "sma", "decisions.jsonl")) as fh:
            targets = [json.loads(l)["target_warm_replicas"] for l in fh]
        self.assertIn(1, targets)
        self.assertEqual(targets[-1], 0)


if __name__ == "__main__":
    unittest.main()

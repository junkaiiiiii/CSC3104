import json
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from predictor.main import SMAPredictor, live_forecast

ROOT = Path(__file__).resolve().parents[2]
T0 = datetime(2026, 11, 3, tzinfo=timezone.utc)


def bucket(at=T0, count=3, complete=True):
    return {"function_id": "periodic", "bucket_start": at.isoformat(),
            "interval_seconds": 30, "invocation_count": count, "complete": complete}


def wait_until(check, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for subprocess output")


class LivePredictorTests(unittest.TestCase):
    def test_gaps_reset_history_and_duplicates_do_not_train(self):
        model = SMAPredictor(3)
        self.assertTrue(model.ingest(bucket(count=99)))
        self.assertFalse(model.ingest(bucket(count=1)))
        self.assertFalse(model.ingest(bucket(T0 + timedelta(seconds=30), complete=False)))
        self.assertTrue(model.ingest(bucket(T0 + timedelta(seconds=60), count=6)))
        prediction = model.predict_next("periodic")
        self.assertEqual(prediction["forecast_invocation_count"], 6)
        self.assertEqual(len(prediction["input_buckets_used"]), 1)

    def test_actual_publication_time_and_expiry(self):
        model = SMAPredictor()
        model.ingest(bucket())
        prediction = model.predict_next("periodic")
        generated = T0 + timedelta(seconds=47)
        record = live_forecast(prediction, 30, generated)
        self.assertEqual(record["generated_at"], "2026-11-03T00:00:47Z")
        self.assertEqual(record["history_end"], "2026-11-03T00:00:30Z")
        self.assertEqual(len(record["predictions"]), 2)
        self.assertIsNone(live_forecast(prediction, 30, T0 + timedelta(seconds=90)))
        self.assertIsNone(live_forecast(prediction, 30, T0 + timedelta(seconds=10)))

    def test_follow_waits_for_file_and_partial_line_and_flushes_before_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            source, target = Path(temporary) / "buckets.jsonl", Path(temporary) / "forecasts.jsonl"
            process = subprocess.Popen([
                sys.executable, "-m", "predictor.main", "--run-id", "test-live",
                "--input", str(source), "--output", str(target),
                "--follow", "--poll-interval", "0.01",
            ], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                wait_until(target.exists)
                now = datetime.now(timezone.utc)
                at = now.replace(second=(now.second // 30) * 30, microsecond=0) - timedelta(seconds=30)
                line = json.dumps(bucket(at))
                source.write_text(line[:20])
                time.sleep(0.08)
                self.assertEqual(target.read_text(), "")
                with source.open("a") as stream:
                    stream.write(line[20:] + "\n")
                wait_until(lambda: bool(target.read_text().strip()))
                record = json.loads(target.read_text())
                self.assertIn("generated_at", record)
                self.assertIn("predictions", record)
                self.assertIsNone(process.poll())  # output visible while the process lives
                scheduler_root = Path(temporary) / "scheduler"
                completed = subprocess.run([
                    sys.executable, "-m", "prewarm.main", "--run-id", "test-live",
                    "--forecasts", str(target), "--out-dir", str(scheduler_root),
                    "--functions", "periodic", "--duration", "0.1", "--tick", "0.02",
                    "--actuator", "dry-run", "--backend", "none",
                ], cwd=ROOT / "scheduler", capture_output=True, text=True, timeout=5)
                self.assertEqual(completed.returncode, 0, completed.stderr)
                manifest = json.loads((scheduler_root / "test-live" / "scheduler_manifest.json").read_text())
                self.assertEqual(manifest["forecasts_accepted"], 1)
                decisions = [json.loads(line) for line in
                             (scheduler_root / "test-live" / "decisions.jsonl").read_text().splitlines()]
                self.assertTrue(any(d["target_warm_replicas"] == 1 for d in decisions))
                self.assertTrue(all(d["dry_run"] for d in decisions))
                with source.open("a") as stream:
                    stream.write(json.dumps(bucket(at, complete=False)) + "\n")
                    stream.write(line + "\n")
                time.sleep(0.08)
                self.assertEqual(len(target.read_text().splitlines()), 1)
            finally:
                if process.poll() is None:
                    process.terminate()
                process.communicate(timeout=5)

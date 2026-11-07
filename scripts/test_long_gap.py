"""Check genuine idle windows, reproducibility and causal seasonal availability."""
import hashlib
import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from scripts.prepare_long_gap import (
    build, offline_buckets, prepare, PERIOD, INTERVAL, DURATION, PREFIX, MODEL_CONFIG,
)
from predictor.models import make_predictor, SMAPredictor
from predictor.main import live_forecast


class LongGapTests(unittest.TestCase):
    def test_seeded_trace_has_strict_idle_gaps_and_valid_client_ids(self):
        for seed in (401, 402, 403):
            rows = build(seed)
            self.assertEqual(rows, build(seed))
            self.assertNotEqual(rows, build(seed + 1))
            self.assertEqual(len(rows), len({r["request_id"] for r in rows}))
            self.assertEqual(rows, sorted(rows, key=lambda r: (r["scheduled_at_s"], r["function_id"])))
            self.assertTrue(all(0 <= r["scheduled_at_s"] < DURATION for r in rows))
            periodic = [r["scheduled_at_s"] for r in rows if r["function_id"] == "periodic"]
            self.assertTrue(all(0 < (at - 20) % PERIOD < 10 for at in periodic))
            for start in range(20, DURATION, PERIOD):
                self.assertGreaterEqual(sum(start <= at < start + 10 for at in periodic), 16)

    def test_bucket_fixture_conserves_requests_and_explicit_zeros(self):
        rows = build(401)
        buckets = list(offline_buckets(rows))
        self.assertEqual(sum(b["invocation_count"] for b in buckets), len(rows))
        self.assertEqual(len(buckets), 3 * DURATION // INTERVAL)
        self.assertTrue(any(b["function_id"] == "periodic" and b["invocation_count"] == 0 for b in buckets))

    def test_manifest_hash_matches_bytes_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp) / "new"
            manifest = prepare(out, 401)
            self.assertEqual(manifest["trace_sha256"], hashlib.sha256((out / "trace.jsonl").read_bytes()).hexdigest())
            self.assertEqual(json.loads((out / "model_config.json").read_text()), MODEL_CONFIG)
            before = (out / "trace.jsonl").read_bytes()
            with self.assertRaises(FileExistsError):
                prepare(out, 402)
            self.assertEqual(before, (out / "trace.jsonl").read_bytes())

    def test_seasonal_can_forecast_each_post_training_burst_in_advance(self):
        # Simulated finalisation delay + poll/tick margin. This proves model
        # compatibility, not that a live Pod will be Ready before traffic.
        parse = SMAPredictor._parse_iso
        for seed, phase in ((seed, phase) for seed in (401, 402, 403) for phase in range(5)):
            model = make_predictor(**MODEL_CONFIG["periodic"])
            eligible = set()
            origin = None
            shifted = [{**r, "scheduled_at_s": r["scheduled_at_s"] + phase}
                       for r in build(seed) if r["scheduled_at_s"] + phase < DURATION]
            for bucket in offline_buckets(shifted):
                if bucket["function_id"] != "periodic":
                    continue
                at = parse(bucket["bucket_start"])
                origin = origin or at
                model.ingest(bucket)
                prediction = model.predict_horizons("periodic", 6)
                generated = at + timedelta(seconds=INTERVAL + 10.5)
                forecast = live_forecast(prediction, INTERVAL, generated)
                for target in forecast["predictions"]:
                    start = parse(target["bucket_start"])
                    if generated + timedelta(seconds=10) <= start and target["invocation_count"] >= 0.5:
                        eligible.add(int((start - origin).total_seconds()))
                self.assertTrue(all(parse(row["bucket_start"]) <= at for row in forecast["input_buckets_used"]))
            for burst in range(20, DURATION, PERIOD):
                if burst >= PREFIX:
                    self.assertIn((burst + phase) // INTERVAL * INTERVAL, eligible)


if __name__ == "__main__":
    unittest.main()

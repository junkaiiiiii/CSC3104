import json
import unittest
from collections import defaultdict
from datetime import datetime, timezone

from tests.test_telemetry import ROOT
from tools.synthetic_buckets import FUNCTIONS, generate

START = datetime(2026, 10, 1, tzinfo=timezone.utc)


class SyntheticTests(unittest.TestCase):
    def setUp(self):
        self.records, self.manifest = generate(101, 6, START)

    def test_deterministic_and_seed_sensitive(self):
        self.assertEqual(self.records, generate(101, 6, START)[0])
        self.assertNotEqual(self.records, generate(202, 6, START)[0])

    def test_matches_bucket_contract(self):
        sample = json.loads((ROOT / "contracts/bucket.example.json").read_text())
        self.assertEqual(len(self.records), 720 * len(FUNCTIONS))
        for r in self.records:
            self.assertEqual(set(r), set(sample))
            self.assertTrue(isinstance(r["invocation_count"], int) and r["invocation_count"] >= 0)
            self.assertEqual(r["interval_seconds"], 30)
        self.assertEqual(self.records, sorted(self.records, key=lambda r: (r["bucket_start"], r["function_id"])))

    def test_outages_hit_every_function_the_same_way(self):
        incomplete = defaultdict(set)
        for r in self.records:
            if not r["complete"]:
                incomplete[r["function_id"]].add(r["bucket_start"])
        self.assertTrue(incomplete)
        self.assertEqual(len({frozenset(v) for v in incomplete.values()}), 1)

    def test_patterns_look_like_their_description(self):
        complete = defaultdict(list)
        for r in self.records:
            if r["complete"]:
                complete[r["function_id"]].append(r["invocation_count"])
        self.assertGreater(max(complete["periodic"]), 8)
        self.assertGreater(sum(c == 0 for c in complete["sparse"]) / len(complete["sparse"]), 0.7)
        self.assertGreater(sum(complete["steady"]) / len(complete["steady"]), 3)
        self.assertEqual(self.manifest["unseen_burst"]["function_id"], "steady")


if __name__ == "__main__":
    unittest.main()

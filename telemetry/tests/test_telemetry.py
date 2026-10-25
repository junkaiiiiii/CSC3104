import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from collector import dedup
from collector.buckets import COUNTED, LATE, DemandAggregator
from collector.collector import Collector
from collector.schema import InvalidEvent, parse_ts, validate_event

ROOT = Path(__file__).resolve().parents[2]
T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)


def ts(seconds):
    return datetime.fromtimestamp(T0.timestamp() + seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def event(request_id="r-1", at=0, **over):
    base = {"schema_version": 1, "function_id": "periodic", "revision": "periodic-00001",
            "instance_id": "i-1", "request_id": request_id, "request_kind": "user",
            "started_at": ts(at), "service_ms": 12.3, "first_user_request": False, "init_ms": 0}
    base.update(over)
    return base


def line(**kw):
    return json.dumps(event(**kw))


class SchemaTests(unittest.TestCase):
    def test_contract_sample_is_valid(self):
        sample = json.loads((ROOT / "contracts/invocation.example.json").read_text())
        validate_event(sample)

    def test_z_and_offset_timestamps_agree(self):
        self.assertEqual(parse_ts("2026-10-01T00:00:00Z"), parse_ts("2026-10-01T00:00:00.000000+00:00"))
        self.assertEqual(parse_ts("2026-10-01T08:00:00+08:00"), parse_ts("2026-10-01T00:00:00Z"))

    def test_rejections(self):
        bad = [event(schema_version=2), event(request_kind="probe"), event(service_ms=True),
               event(service_ms=-1), event(service_ms=float("nan")), event(started_at="2026-10-01T00:00:00"),
               event(request_id=""), event(first_user_request="yes")]
        missing = event()
        del missing["init_ms"]
        for candidate in bad + [missing, [], "x"]:
            with self.assertRaises(InvalidEvent, msg=candidate):
                validate_event(candidate)


class DedupTests(unittest.TestCase):
    def test_new_duplicate_conflict_and_expiry(self):
        d = dedup.Deduper(ttl_s=100, prune_every_s=1)
        self.assertEqual(d.check(event(), 0), dedup.NEW)
        self.assertEqual(d.check(event(), 1), dedup.DUPLICATE)
        self.assertEqual(d.check(event(instance_id="i-2"), 2), dedup.CONFLICT)
        self.assertEqual(d.check(event("r-2"), 3), dedup.NEW)
        d.check(event("r-3"), 500)
        self.assertEqual(len(d), 1)  # old IDs expired


class AggregatorTests(unittest.TestCase):
    def make(self):
        return DemandAggregator(["a", "b"], T0, 30, 10)

    def test_boundary_belongs_to_next_bucket(self):
        agg = self.make()
        agg.add("a", parse_ts(ts(29.999)))
        agg.add("a", parse_ts(ts(30)))
        out = [r for r in agg.advance(parse_ts(ts(100))) if r["function_id"] == "a"]
        self.assertEqual([r["invocation_count"] for r in out], [1, 1, 0])

    def test_empty_is_zero_and_complete_gap_is_incomplete(self):
        agg = self.make()
        agg.mark_gap(parse_ts(ts(35)), parse_ts(ts(50)), "a")
        out = {(r["function_id"], r["bucket_start"]): r for r in agg.advance(parse_ts(ts(70)))}
        self.assertEqual(out[("a", ts(0))]["complete"], True)
        self.assertEqual(out[("a", ts(0))]["invocation_count"], 0)
        self.assertEqual(out[("a", ts(30))]["complete"], False)
        self.assertEqual(out[("b", ts(30))]["complete"], True)  # gap was function-specific

    def test_watermark_and_late(self):
        agg = self.make()
        self.assertEqual(agg.advance(parse_ts(ts(39))), [])  # grace not elapsed
        self.assertEqual(agg.add("a", parse_ts(ts(5))), COUNTED)
        self.assertEqual(len(agg.advance(parse_ts(ts(40)))), 2)
        self.assertEqual(agg.add("a", parse_ts(ts(6))), LATE)

    def test_output_matches_contract_fields(self):
        sample = json.loads((ROOT / "contracts/bucket.example.json").read_text())
        record = self.make().advance(parse_ts(ts(40)))[0]
        self.assertEqual(set(record), set(sample))
        self.assertEqual(record["interval_seconds"], 30)


class CollectorTests(unittest.TestCase):
    def run_lines(self, lines):
        directory = tempfile.mkdtemp()
        c = Collector(directory, ["periodic", "steady"], "t1", clock="event")
        outcomes = [c.process_line(x) for x in lines]
        ok = c.close()
        read = lambda n: [json.loads(x) for x in (Path(directory) / n).read_text().splitlines()]
        return c, outcomes, ok, read

    def test_end_to_end_accounting(self):
        lines = [
            line(request_id="r-1", at=5),
            line(request_id="r-1", at=5),                      # replay -> duplicate
            line(request_id="r-1", at=5, instance_id="i-9"),   # conflicting duplicate
            line(request_id="w-1", at=6, request_kind="warmup"),
            "not json at all",
            line(request_id="r-x", function_id="nope", at=7),  # unknown function
            line(request_id="r-2", at=60),                     # advances watermark past bucket 0
            line(request_id="r-3", at=10),                     # bucket 0 already final -> late
            line(request_id="r-4", at=45),                     # bucket 1 still open
            line(request_id="r-5", at=8, schema_version=2),    # quarantined
            "",
        ]
        c, outcomes, ok, read = self.run_lines(lines)
        self.assertEqual(outcomes, ["counted", "duplicate", "conflicting_duplicate", "warmup",
                                    "quarantined", "quarantined", "counted", "late", "counted",
                                    "quarantined", "blank"])
        counts = {(b["function_id"], b["bucket_start"]): b for b in read("demand_buckets.jsonl")}
        self.assertEqual(counts[("periodic", ts(0))]["invocation_count"], 1)
        self.assertEqual(counts[("periodic", ts(30))]["invocation_count"], 1)
        self.assertEqual(counts[("periodic", ts(60))]["invocation_count"], 1)
        self.assertEqual(counts[("steady", ts(0))]["invocation_count"], 0)  # confirmed empty
        self.assertTrue(all(b["complete"] for b in counts.values()))
        self.assertTrue(ok)
        self.assertEqual(c.totals["quarantined"], 3)
        self.assertEqual(len(read("quarantine.jsonl")), 3)
        # warmups are kept in the raw log but never counted as demand
        raw = {r["event"]["request_id"]: r["outcome"] for r in read("raw_events.jsonl")}
        self.assertEqual(raw["w-1"], "warmup")

    def test_conservation_holds_for_random_mix(self):
        import random
        rng = random.Random(7)
        lines = []
        for i in range(300):
            kind = rng.choice(["user", "user", "user", "warmup"])
            fn = rng.choice(["periodic", "steady"])
            lines.append(line(request_id="r-%d" % rng.randrange(200), at=rng.uniform(0, 600),
                              request_kind=kind, function_id=fn))
            if rng.random() < 0.1:
                lines.append("garbage")
        _, _, ok, _ = self.run_lines(lines)
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()

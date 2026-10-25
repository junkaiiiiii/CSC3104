"""End-to-end: forecast file -> CLI replay -> contract-shaped outputs (no cluster)."""
import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from prewarm import main as cli
from prewarm.contracts import DECISION_FIELDS, format_ts
from prewarm.source import FileTail, ForecastBook

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)


def fc(fn, at, counts):
    return {"function_id": fn, "generated_at": format_ts(at), "history_end": format_ts(at),
            "interval_seconds": 30, "model": "moving-average",
            "predictions": [{"bucket_start": format_ts(at + timedelta(seconds=30 * i)),
                             "invocation_count": c} for i, c in enumerate(counts)],
            "valid_until": format_ts(at + timedelta(seconds=60))}


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.path = os.path.join(self.tmp, "forecasts.jsonl")
        lines = [fc("periodic", T0 + timedelta(seconds=30 * i), (5, 5) if i % 4 == 0 else (0, 0))
                 for i in range(8)]
        lines.append(fc("steady", T0, (3, 3)))
        with open(self.path, "w") as fh:
            for line in lines:
                fh.write(json.dumps(line) + "\n")
            fh.write("not json\n")
            fh.write(json.dumps(fc("ghost", T0, (1,))) + "\n")
            bad = fc("steady", T0 + timedelta(seconds=90), (-1, 2))
            fh.write(json.dumps(bad) + "\n")

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def run_cli(self, run_id="r1"):
        argv = ["--run-id", run_id, "--forecasts", self.path, "--out-dir", self.tmp,
                "--functions", "periodic,steady,sparse",
                "--replay", format_ts(T0), format_ts(T0 + timedelta(minutes=4))]
        self.assertEqual(cli.main(argv), 0)
        out = os.path.join(self.tmp, run_id)

        def read(name):
            with open(os.path.join(out, name), encoding="utf-8") as fh:
                return [json.loads(line) for line in fh]
        return out, read

    def test_replay_outputs(self):
        out, read = self.run_cli()
        decisions = read("decisions.jsonl")
        self.assertEqual(len(decisions), 48 * 3)          # 4 min / 5s tick x 3 functions
        for d in decisions:
            self.assertEqual(tuple(d), DECISION_FIELDS)
            self.assertTrue(d["dry_run"])
            self.assertLessEqual(d["target_warm_replicas"], 2)
            if d["forecast_generated_at"]:                 # never a forecast from the future
                self.assertLessEqual(d["forecast_generated_at"], d["decided_at"])
        sparse = [d for d in decisions if d["function_id"] == "sparse"]
        self.assertTrue(all(d["target_warm_replicas"] == 0 for d in sparse))
        periodic = [d["target_warm_replicas"] for d in decisions if d["function_id"] == "periodic"]
        self.assertIn(1, periodic)
        self.assertIn(0, periodic)
        # steady's newest record (at 90s) is invalid -> fallback from then on, not the old forecast.
        steady_late = [d for d in decisions if d["function_id"] == "steady"
                       and d["decided_at"] >= format_ts(T0 + timedelta(seconds=90))]
        self.assertTrue(all("invalid forecast" in d["reason"] for d in steady_late))
        rejects = read("forecast_rejects.jsonl")
        self.assertEqual(len(rejects), 3)
        with open(os.path.join(out, "scheduler_manifest.json"), encoding="utf-8") as fh:
            manifest = json.load(fh)
        self.assertEqual(manifest["clock"], "replay")
        self.assertEqual(manifest["totals"]["ticks"], 48)
        self.assertEqual(manifest["totals"]["forecast_rejected"], 3)
        self.assertEqual(len(read("actuation.jsonl")), 48 * 3)

    def test_refuses_to_overwrite_run(self):
        self.run_cli("same")
        with self.assertRaises(SystemExit):
            self.run_cli("same")


class TailTests(unittest.TestCase):
    def test_partial_lines_wait_for_newline(self):
        tmp = tempfile.mkdtemp()
        try:
            path = os.path.join(tmp, "f.jsonl")
            tail = FileTail(path)
            self.assertEqual(tail.read_new(), [])
            with open(path, "w") as fh:
                fh.write('{"a": 1}\n{"b"')
            self.assertEqual(tail.read_new(), [(1, '{"a": 1}')])
            with open(path, "a") as fh:
                fh.write(': 2}\n')
            self.assertEqual(tail.read_new(), [(2, '{"b": 2}')])
        finally:
            shutil.rmtree(tmp)

    def test_book_hides_future_forecasts(self):
        book = ForecastBook(["periodic"])
        book.add_line(1, json.dumps(fc("periodic", T0, (1, 1))))
        book.add_line(2, json.dumps(fc("periodic", T0 + timedelta(seconds=30), (9, 9))))
        self.assertIsNone(book.latest("periodic", T0 - timedelta(seconds=1)))
        self.assertEqual(book.latest("periodic", T0 + timedelta(seconds=29)).predictions[0][1], 1)
        self.assertEqual(book.latest("periodic", T0 + timedelta(seconds=30)).predictions[0][1], 9)


if __name__ == "__main__":
    unittest.main()

"""Regression coverage for missing evidence and overlapping Ready Pods."""
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from tools import readiness_timeline as rt
from tests.test_timing_tools import at, lifecycle, requests


class RequiredEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.run = self.root / "run"
        self.sched = self.root / "scheduler"
        self.run.mkdir()
        self.sched.mkdir()
        records = {
            self.run / "client_measurements.jsonl": requests("periodic", 0, 1),
            self.run / "pod_lifecycle.jsonl": lifecycle("p", "periodic", 0, ready=2, gone=60),
            self.sched / "decisions.jsonl": [{"function_id": "periodic", "decided_at": at(0),
                                             "target_warm_replicas": 0}],
            self.sched / "policy_audit.jsonl": [{"function_id": "periodic", "decided_at": at(0),
                                                "outcome": "fallback"}],
            self.sched / "actuation.jsonl": [{"function_id": "periodic", "at": at(0),
                                             "status": "noop"}],
        }
        for path, rows in records.items():
            path.write_text("".join(json.dumps(r) + "\n" for r in rows))
        self.manifest = self.sched / "scheduler_manifest.json"
        self.manifest.write_text(json.dumps({
            "run_id": "run", "actuator": "min-scale/revision", "final": True,
            "policy": {"interval_seconds": 5, "dry_run": False}, "ended_at": at(65),
        }))
        self.required = list(records) + [self.manifest]

    def call(self, *extra, scheduler=True):
        args = ["--run-dir", str(self.run)]
        if scheduler:
            args += ["--scheduler-dir", str(self.sched)]
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return rt.main(args + list(extra))

    def assert_rejected(self, *extra):
        with self.assertRaises(SystemExit) as caught:
            self.call(*extra)
        self.assertEqual(caught.exception.code, 2)

    def test_wrong_scheduler_path_does_not_silently_become_reactive(self):
        out = self.run / "report.json"
        self.assert_rejected("--scheduler-dir", str(self.root / "typo"), "--output", str(out))
        self.assertFalse(out.exists())

    def test_each_missing_or_empty_required_file_is_rejected(self):
        for path in self.required:
            content = path.read_bytes()
            for mode in ("missing", "empty"):
                with self.subTest(file=path.name, mode=mode):
                    if mode == "missing":
                        path.unlink()
                    else:
                        path.write_text("\n")
                    self.assert_rejected()
                    path.write_bytes(content)

    def test_bad_manifest_and_non_object_records_are_rejected(self):
        original = self.manifest.read_text()
        for value in ({}, [], {"policy": {}}, {"policy": "predictive"}):
            self.manifest.write_text(json.dumps(value))
            self.assert_rejected()
        self.manifest.write_text(original)
        path = self.run / "client_measurements.jsonl"
        for text in ("not-json\n", "[]\n", "null\n"):
            path.write_text(text)
            self.assert_rejected()

    def test_explicit_forecast_path_must_exist_and_contain_records(self):
        path = self.root / "forecasts.jsonl"
        self.assert_rejected("--forecasts", str(path))
        path.write_text("")
        self.assert_rejected("--forecasts", str(path))

    def test_reactive_and_zero_actuation_runs_still_succeed(self):
        self.assertEqual(self.call(), 0)
        self.assertEqual(self.call(scheduler=False), 0)

    def test_incomplete_semantic_evidence_cannot_report_safe(self):
        self.assertFalse(rt.build([], [], [], [], [], {})["safe"])
        report = rt.build(requests("periodic", 0), lifecycle("p", "steady", 0, ready=1),
                          [], [], [], {})
        self.assertFalse(report["safe"])
        self.assertEqual(report["bursts"][0]["outcome"], "unknown")
        path = self.run / "client_measurements.jsonl"
        path.write_text(json.dumps({"function_id": "periodic", "sent_at": "bad"}) + "\n")
        self.assertEqual(self.call(), 2)


class ZeroReadyTimeTests(unittest.TestCase):
    def span(self, start, end, fn="periodic"):
        return {"function_id": fn, "pod": f"p-{start}-{end}",
                "created": rt.parse(at(start)), "ready_from": rt.parse(at(start)),
                "ready_to": rt.parse(at(end)) if end is not None else None,
                "gone": rt.parse(at(end)) if end is not None else None}

    def test_three_overlapping_pods_leave_fifty_seconds_at_zero(self):
        spans = [self.span(0, 50) for _ in range(3)]
        bursts = [[rt.parse(at(1))], [rt.parse(at(100))]]
        gap = rt.gap_rows("periodic", bursts, spans, [], rt.parse(at(110)))[0]
        self.assertEqual(gap["zero_ready_s"], 50)
        self.assertEqual(gap["no_ready_pod_after_s"], 49)
        self.assertTrue(gap["released"])

    def test_union_clips_overlaps_and_handles_touching_and_open_intervals(self):
        cases = [
            ([], 0),
            ([self.span(0, 50), self.span(10, 20), self.span(40, 80)], 80),
            ([self.span(-10, 20), self.span(20, 40), self.span(70, 120)], 70),
            ([self.span(50, None), self.span(60, 80)], 50),
            ([self.span(0, 100, "steady"), self.span(10, 20)], 10),
        ]
        for spans, expected in cases:
            with self.subTest(expected=expected):
                ready = rt.ready_seconds(spans, "periodic", rt.parse(at(0)), rt.parse(at(100)))
                self.assertEqual(ready, expected)
                self.assertGreaterEqual(100 - ready, 0)
                self.assertLessEqual(100 - ready, 100)


if __name__ == "__main__":
    unittest.main()

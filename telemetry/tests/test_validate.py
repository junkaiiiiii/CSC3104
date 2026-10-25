import json
import tempfile
import unittest
from pathlib import Path

from collector.validate import main, validate_run

T0 = "2026-10-25T10:00:00Z"
T1 = "2026-10-25T10:02:00Z"


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")


def build(root, n=3, **over):
    """A clean run: n successful requests to 'steady', one pod, all reconciled."""
    clients, raw = [], []
    for i in range(n):
        sent = "2026-10-25T10:00:%02dZ" % (10 + i)
        clients.append({"function_id": "steady", "request_id": "r%d" % i, "sent_at": sent,
                        "completed_at": sent.replace("Z", ".500Z"), "status_code": 200, "timeout": False,
                        "instance_id": "inst1", "revision": "steady-00001", "first_user_request": i == 0})
        raw.append({"received_at": sent, "outcome": "counted", "source": "k8s:x", "pod": "p1",
                    "event": {"schema_version": 1, "function_id": "steady", "revision": "steady-00001",
                              "instance_id": "inst1", "request_id": "r%d" % i, "request_kind": "user",
                              "started_at": sent.replace("Z", ".100000+00:00"), "service_ms": 5,
                              "first_user_request": i == 0}})
    manifest = {"run_id": "t", "started_at": T0, "ended_at": T1}
    tel = {"clock": "wall", "final": True, "conservation_ok": True, "functions": ["steady"],
           "grace_seconds": 10, "started_at": "2026-10-25T09:59:00Z", "ended_at": "2026-10-25T10:03:00Z",
           "totals": {"counted": n, "emitted_invocations": n}}
    buckets = [{"function_id": "steady", "bucket_start": "2026-10-25T10:00:00Z", "interval_seconds": 30,
                "invocation_count": n, "complete": True}]
    life = [{"observed_at": "2026-10-25T10:00:05Z", "event": "pod_seen", "pod": "p1", "created_at": "2026-10-25T10:00:00Z"},
            {"observed_at": "2026-10-25T10:01:00Z", "event": "pod_terminating", "pod": "p1"},
            {"observed_at": "2026-10-25T10:01:10Z", "event": "pod_gone", "pod": "p1"}]
    d = dict(clients=clients, raw=raw, manifest=manifest, tel=tel, buckets=buckets, life=life)
    d.update(over)
    root = Path(root)
    (root / "run_manifest.json").write_text(json.dumps(d["manifest"]))
    write_jsonl(root / "client_measurements.jsonl", d["clients"])
    (root / "telemetry_manifest.json").write_text(json.dumps(d["tel"]))
    write_jsonl(root / "raw_events.jsonl", d["raw"])
    write_jsonl(root / "demand_buckets.jsonl", d["buckets"])
    if d["life"] is not None:
        write_jsonl(root / "pod_lifecycle.jsonl", d["life"])
    elif (root / "pod_lifecycle.jsonl").exists():
        (root / "pod_lifecycle.jsonl").unlink()
    return d


class ValidateTests(unittest.TestCase):
    def run_case(self, mutate=None, **kw):
        with tempfile.TemporaryDirectory() as tmp:
            d = build(tmp)
            if mutate:
                mutate(d)
                build(tmp, **d)
            return validate_run(tmp, **kw)

    def codes(self, report, kind="errors"):
        return {e["code"] for e in report[kind]}

    def test_clean_run_passes(self):
        r = self.run_case()
        self.assertEqual(r["status"], "pass", r["errors"])
        self.assertEqual(r["summary"]["matched_requests"], 3)

    def test_lost_server_event(self):
        def m(d):
            d["raw"].pop()
            d["tel"]["totals"].update(counted=2, emitted_invocations=2)
            d["buckets"][0]["invocation_count"] = 2
        self.assertIn("E_CLIENT_NO_SERVER", self.codes(self.run_case(m)))

    def test_failed_client_without_server_event_ok(self):
        def m(d):
            d["raw"].pop()
            d["clients"][2].update(status_code=504, timeout=True)
            d["buckets"][0]["invocation_count"] = 2
            d["tel"]["totals"].update(counted=2, emitted_invocations=2)
        self.assertEqual(self.run_case(m)["status"], "pass")

    def test_stray_user_request(self):
        def m(d):
            extra = json.loads(json.dumps(d["raw"][0]))
            extra["event"]["request_id"] = "intruder"
            d["raw"].append(extra)
            d["buckets"][0]["invocation_count"] = 4
            d["tel"]["totals"].update(counted=4, emitted_invocations=4)
        self.assertIn("E_SERVER_NO_CLIENT", self.codes(self.run_case(m)))

    def test_scheduler_warmup_is_not_contamination(self):
        def m(d):
            w = json.loads(json.dumps(d["raw"][0]))
            w["outcome"] = "warmup"
            w["event"].update(request_id="w1", request_kind="warmup")
            d["raw"].append(w)
        self.assertEqual(self.run_case(m)["status"], "pass")

    def test_not_final_and_conservation(self):
        def m(d):
            d["tel"].update(final=False, conservation_ok=False)
        self.assertTrue({"E_NOT_FINAL", "E_CONSERVATION"} <= self.codes(self.run_case(m)))

    def test_quarantine_and_unemitted(self):
        def m(d):
            d["tel"]["totals"].update(quarantined=2, emitted_invocations=1)
        self.assertTrue({"E_QUARANTINED", "E_PENDING_UNEMITTED"} <= self.codes(self.run_case(m)))

    def test_late_in_window_error_but_before_window_warning(self):
        def m(d):
            d["raw"][0]["outcome"] = "late"
            old = json.loads(json.dumps(d["raw"][1]))
            old["outcome"] = "late"
            old["event"].update(request_id="old", started_at="2026-10-25T09:50:00+00:00")
            d["raw"].append(old)
        r = self.run_case(m)
        self.assertIn("E_LATE_IN_WINDOW", self.codes(r))
        self.assertIn("W_LATE_BEFORE_WINDOW", self.codes(r, "warnings"))

    def test_incomplete_bucket_in_window(self):
        def m(d):
            d["buckets"].append({"function_id": "steady", "bucket_start": "2026-10-25T10:00:30Z",
                                 "interval_seconds": 30, "invocation_count": 0, "complete": False})
        self.assertIn("E_INCOMPLETE_BUCKETS", self.codes(self.run_case(m)))

    def test_incomplete_bucket_outside_window_ignored(self):
        def m(d):
            d["buckets"].append({"function_id": "steady", "bucket_start": "2026-10-25T09:00:00Z",
                                 "interval_seconds": 30, "invocation_count": 0, "complete": False})
        self.assertEqual(self.run_case(m)["status"], "pass")

    def test_bucket_sum_mismatch(self):
        def m(d):
            d["buckets"][0]["invocation_count"] = 2
        self.assertIn("E_BUCKET_SUM_MISMATCH", self.codes(self.run_case(m)))

    def test_conflicting_duplicate(self):
        def m(d):
            d["raw"][0]["outcome"] = "conflicting_duplicate"
        self.assertIn("E_CONFLICTING_DUPLICATE", self.codes(self.run_case(m)))

    def test_instance_and_time_mismatch(self):
        def m(d):
            d["clients"][0]["instance_id"] = "other"
            d["clients"][1]["sent_at"] = "2026-10-25T10:01:30Z"
            d["clients"][1]["completed_at"] = "2026-10-25T10:01:31Z"
        c = self.codes(self.run_case(m))
        self.assertTrue({"E_FIELD_MISMATCH", "E_TIME_MISMATCH"} <= c)

    def test_collector_window_coverage(self):
        def m(d):
            d["tel"].update(started_at="2026-10-25T10:00:20Z", ended_at="2026-10-25T10:00:15Z")
        self.assertTrue({"E_COLLECTOR_STARTED_LATE", "E_COLLECTOR_ENDED_EARLY"} <= self.codes(self.run_case(m)))

    def test_lifecycle(self):
        def m(d):
            d["life"] = [{"observed_at": "2026-10-25T10:00:05Z", "event": "pod_seen", "pod": "other",
                          "created_at": "2026-10-25T09:00:00Z"},
                         {"observed_at": "2026-10-25T10:00:10Z", "event": "pod_terminating", "pod": "other"},
                         {"observed_at": "2026-10-25T10:05:10Z", "event": "pod_gone", "pod": "other"}]
        r = self.run_case(m)
        self.assertIn("E_UNKNOWN_POD", self.codes(r))
        w = self.codes(r, "warnings")
        self.assertTrue({"W_PODS_BEFORE_RUN", "W_SLOW_TERMINATION"} <= w)
        self.assertEqual(r["summary"]["termination_lag_max_s"], 300.0)

    def test_missing_lifecycle_file(self):
        self.assertIn("E_NO_LIFECYCLE", self.codes(self.run_case(lambda d: d.update(life=None))))

    def test_missing_and_malformed_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIn("E_MISSING_FILE", self.codes(validate_run(tmp)))
            build(tmp)
            (Path(tmp) / "raw_events.jsonl").write_text("{not json\n")
            self.assertIn("E_UNREADABLE", self.codes(validate_run(tmp)))

    def test_cli_exit_codes_and_report_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            build(tmp)
            self.assertEqual(main(["--run-dir", tmp]), 0)
            saved = json.loads((Path(tmp) / "telemetry_validation.json").read_text())
            self.assertEqual(saved["status"], "pass")
            self.assertIn("raw_events.jsonl", saved["files_sha256"])
            d = build(tmp)
            d["tel"]["final"] = False
            build(tmp, **d)
            self.assertEqual(main(["--run-dir", tmp]), 1)
        self.assertEqual(main(["--run-dir", "/nonexistent-dir"]), 2)


if __name__ == "__main__":
    unittest.main()

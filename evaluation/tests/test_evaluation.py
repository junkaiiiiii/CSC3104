import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from evaluation.analyze import analyze, summarize_run
from evaluation.common import iso_utc, parse_utc, sha256_file, write_json, write_jsonl
from evaluation.replay import replay
from evaluation.workload import generate, generate_trace


class EvaluationTests(unittest.TestCase):
    def test_workload_generation_is_repeatable_and_has_three_functions(self):
        first = generate_trace(17, 60)
        second = generate_trace(17, 60)
        self.assertEqual(first, second)
        self.assertEqual({row["function_id"] for row in first}, {"periodic", "steady", "sparse"})
        self.assertEqual(first, sorted(first, key=lambda row: (
            row["scheduled_at_s"], row["function_id"], row["request_id"]
        )))
        self.assertTrue(any(row["scheduled_at_s"] >= 54 for row in first if row["function_id"] == "sparse"))

    def test_replay_retains_success_and_http_failure_then_analyzes(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/fail":
                    status, body = 503, {"message": "unavailable"}
                else:
                    status, body = 200, {
                        "instance_id": "instance-1", "revision": "revision-1",
                        "first_user_request": self.headers.get("X-Request-ID", "").endswith("ok"),
                    }
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                trace_dir = root / "trace"
                trace_dir.mkdir()
                trace_path = trace_dir / "trace.jsonl"
                trace = [
                    {"scheduled_at_s": 0, "function_id": "periodic", "request_id": "ok",
                     "request_kind": "user", "workload_pattern": "periodic"},
                    {"scheduled_at_s": 0.01, "function_id": "periodic", "request_id": "failed",
                     "request_kind": "user", "workload_pattern": "periodic"},
                ]
                write_jsonl(trace_path, trace)
                write_json(trace_dir / "trace_manifest.json", {
                    "seed": 9, "patterns": {"periodic": "test"}, "request_count": 2,
                })
                endpoints_path = root / "endpoints.json"
                write_json(endpoints_path, {
                    "periodic": {"url": f"http://127.0.0.1:{server.server_port}/"},
                    "steady": {"url": f"http://127.0.0.1:{server.server_port}/fail"},
                })
                trace[1]["function_id"] = "steady"
                write_jsonl(trace_path, trace)
                run_dir = replay(trace_path, endpoints_path, "reactive", "reactive-9",
                                 root / "results", max_workers=2)
                records = [json.loads(line) for line in
                           (run_dir / "client_measurements.jsonl").read_text().splitlines()]
                self.assertEqual(len(records), 2)
                self.assertEqual({row["status_code"] for row in records}, {200, 503})
                self.assertTrue(all(row["latency_ms"] >= 0 for row in records))
                manifest = json.loads((run_dir / "run_manifest.json").read_text())
                self.assertEqual(manifest["measurements_sha256"],
                                 sha256_file(run_dir / "client_measurements.jsonl"))

                run_start = parse_utc(manifest["started_at"])
                run_end = parse_utc(manifest["ended_at"])
                ready_at = run_start + (run_end - run_start) / 2
                lifecycle = [
                    {"event": "pod_seen", "pod": "p1", "observed_at": iso_utc(run_start),
                     "cpu_request_m": 100, "mem_request_mi": 128},
                    {"event": "pod_ready", "pod": "p1", "observed_at": iso_utc(ready_at),
                     "cpu_request_m": 100, "mem_request_mi": 128},
                    {"event": "pod_gone", "pod": "p1", "observed_at": iso_utc(run_end),
                     "cpu_request_m": 100, "mem_request_mi": 128},
                ]
                write_jsonl(run_dir / "pod_lifecycle.jsonl", lifecycle)
                actual_start = "2026-01-01T00:00:00Z"
                write_jsonl(run_dir / "demand_buckets.jsonl", [{
                    "function_id": "periodic", "bucket_start": actual_start,
                    "invocation_count": 2, "complete": True,
                }])
                write_jsonl(run_dir / "forecasts.jsonl", [{
                    "function_id": "periodic",
                    "predictions": [{"bucket_start": actual_start, "invocation_count": 3}],
                }])
                summary = summarize_run(run_dir)
                self.assertEqual(summary["request_count"], 2)
                self.assertEqual(summary["http_errors"], 1)
                self.assertEqual(summary["forecast"]["mae"], 1.0)
                self.assertGreater(summary["resource_integrals"]["ready_replica_seconds"], 0)
                self.assertGreater(summary["resource_integrals"]["provisioned_replica_seconds"], 0)
                self.assertIsNone(summary["idle_replica_seconds"])
                changed_records = [dict(row) for row in records]
                changed_records[0]["status_code"] = 201
                write_jsonl(run_dir / "client_measurements.jsonl", changed_records)
                with self.assertRaisesRegex(ValueError, "hash does not match"):
                    summarize_run(run_dir)
                write_jsonl(run_dir / "client_measurements.jsonl", records)
                analyze([run_dir], root / "analysis")
                self.assertTrue((root / "analysis/policy_comparison.csv").is_file())
                self.assertTrue((root / "analysis/report.md").is_file())
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_trace_generation_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            generate(temp, seed=3, duration_s=5)
            with self.assertRaises(FileExistsError):
                generate(temp, seed=3, duration_s=5)


if __name__ == "__main__":
    unittest.main()

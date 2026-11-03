import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from evaluation.analyze import analyze, summarize_run, _forecast_metrics
from evaluation.finalize import finalize
from evaluation.common import iso_utc, parse_utc, sha256_file, write_json, write_jsonl
from evaluation.correlate import (classify_exposure, exposure_summary, idle_replica_seconds,
                                  scheduling_lag_summary)
from evaluation.replay import replay, validate_metadata
from evaluation.workload import generate, generate_trace


class EvaluationTests(unittest.TestCase):
    def test_scores_predictor_field_including_zero_forecast(self):
        actual = [{"function_id": "periodic", "bucket_start": "t", "invocation_count": 4, "complete": True}]
        forecast = [{"function_id": "periodic", "forecast_bucket_start": "t", "forecast_invocation_count": 3}]
        self.assertEqual(_forecast_metrics(actual, forecast)["mae"], 1)
        forecast[0]["forecast_invocation_count"] = 0
        self.assertEqual(_forecast_metrics(actual, forecast)["mae"], 4)

    def test_finalization_refuses_live_scheduler_then_copies_and_hashes_final_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            run, controller = Path(temporary) / "run", Path(temporary) / "controller"
            run.mkdir()
            controller.mkdir()
            write_json(run / "run_manifest.json", {"run_id": "p1", "policy": "predictive"})
            manifest = {"run_id": "p1", "final": False, "ended_at": None}
            write_json(controller / "scheduler_manifest.json", manifest)
            with self.assertRaisesRegex(ValueError, "stop the scheduler"):
                finalize(run, controller)
            manifest.update(final=True, ended_at="2026-11-03T12:00:00Z")
            write_json(controller / "scheduler_manifest.json", manifest)
            for name in ("decisions.jsonl", "actuation.jsonl"):
                (controller / name).write_text("{}\n")
            hashes = finalize(run, controller)
            final = json.loads((run / "run_manifest.json").read_text())
            self.assertTrue(final["controller_artifacts_final"])
            self.assertEqual(hashes, final["controller_artifacts"])
            for name, digest in hashes.items():
                self.assertEqual(sha256_file(run / name), digest)

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


class CorrelationTests(unittest.TestCase):
    T0 = "2026-01-01T00:00:{:02d}Z"

    def lifecycle(self):
        return [
            {"event": "pod_ready", "pod": "p1", "function_id": "f", "observed_at": self.T0.format(10)},
            {"event": "pod_gone", "pod": "p1", "function_id": "f", "observed_at": self.T0.format(20)},
        ]

    def request(self, sent, done, **extra):
        return {"function_id": "f", "request_kind": "user", "sent_at": self.T0.format(sent),
                "completed_at": self.T0.format(done), "scheduled_at": self.T0.format(sent), **extra}

    def test_exposure_labels_and_na_latency(self):
        rows = [self.request(5, 6, latency_ms=900.0), self.request(12, 13, latency_ms=10.0),
                self.request(15, 16, request_kind="warmup")]
        labels = classify_exposure(rows, self.lifecycle())
        self.assertEqual(labels, ["exposed", "not_exposed", "warmup"])
        summary = exposure_summary(rows, labels)
        self.assertEqual(summary["exposed_latency_ms"]["p95"], 900.0)
        none_exposed = exposure_summary(rows[1:2], ["not_exposed"])
        self.assertEqual(none_exposed["exposed_latency_ms"]["status"], "N/A")
        self.assertEqual(classify_exposure(rows[:1], []), ["unknown"])

    def test_idle_time_excludes_busy_replica_time(self):
        rows = [self.request(12, 14)]
        idle = idle_replica_seconds(rows, self.lifecycle(), parse_utc(self.T0.format(0)),
                                    parse_utc(self.T0.format(30)))
        self.assertEqual(idle["total"], 8.0)
        self.assertIsNone(idle_replica_seconds(rows, [], parse_utc(self.T0.format(0)),
                                               parse_utc(self.T0.format(30))))

    def test_scheduling_lag(self):
        row = self.request(5, 6)
        row["scheduled_at"] = "2026-01-01T00:00:04.500Z"
        lag = scheduling_lag_summary([row, {"function_id": "f"}])
        self.assertEqual((lag["n"], lag["unmeasured"], lag["max"]), (1, 1, 500.0))


class RunDesignTests(unittest.TestCase):
    GOOD = {"image_digest": "sha256:abc", "kubernetes_version": "1.30", "knative_version": "1.15",
            "hardware": "4 vCPU", "image_cache_state": "cached", "initial_replicas": {"periodic": 0}}

    def test_metadata_validation(self):
        self.assertEqual(validate_metadata(self.GOOD, "reactive"), [])
        self.assertTrue(validate_metadata({}, "reactive"))
        warm = dict(self.GOOD, initial_replicas={"periodic": 1})
        self.assertTrue(validate_metadata(warm, "predictive"))
        self.assertEqual(validate_metadata(warm, "fixed_minimum"), [])

    def test_strict_replay_rejects_incomplete_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_jsonl(root / "trace.jsonl", [{"scheduled_at_s": 0, "function_id": "periodic", "request_id": "a"}])
            write_json(root / "endpoints.json", {"periodic": {"url": "http://127.0.0.1:1/"}})
            write_json(root / "meta.json", self.GOOD)
            with self.assertRaisesRegex(ValueError, "incomplete run metadata"):
                replay(root / "trace.jsonl", root / "endpoints.json", "reactive", "r1", root / "out",
                       require_metadata=True)
            with self.assertRaisesRegex(ValueError, "controller-dir"):
                replay(root / "trace.jsonl", root / "endpoints.json", "predictive", "r2", root / "out",
                       metadata_path=root / "meta.json", require_metadata=True)

    def make_run(self, root, policy, seed, run_id, started, with_trace_gap=False):
        run = root / run_id
        run.mkdir()
        write_jsonl(run / "trace.jsonl", [
            {"scheduled_at_s": 0, "function_id": "f", "request_id": "a"},
            {"scheduled_at_s": 1, "function_id": "f", "request_id": "b"}])
        rows = [{"request_id": f"{run_id}-a", "trace_request_id": "a", "function_id": "f",
                 "request_kind": "user", "scheduled_at": started, "sent_at": started,
                 "completed_at": started, "latency_ms": 5.0, "status_code": 200, "timeout": False,
                 "first_user_request": False}]
        if not with_trace_gap:
            rows.append(dict(rows[0], request_id=f"{run_id}-b", trace_request_id="b"))
        write_jsonl(run / "client_measurements.jsonl", rows)
        write_json(run / "run_manifest.json", {
            "run_id": run_id, "policy": policy, "seed": seed, "request_count": len(rows),
            "trace_sha256": sha256_file(run / "trace.jsonl"),
            "measurements_sha256": sha256_file(run / "client_measurements.jsonl"),
            "started_at": started, "ended_at": "2026-01-01T01:00:00Z", "metadata": {}})
        return run

    def test_missing_client_records_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            run = self.make_run(Path(temp), "reactive", 1, "gap", "2026-01-01T00:00:00Z", True)
            with self.assertRaisesRegex(ValueError, "no client record"):
                summarize_run(run)

    def test_require_telemetry_and_warnings_and_charts(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            runs = [self.make_run(root, policy, 1, f"{policy}-1", f"2026-01-01T00:0{i}:00Z")
                    for i, policy in enumerate(("reactive", "predictive"))]
            with self.assertRaisesRegex(ValueError, "pod_lifecycle"):
                summarize_run(runs[0], require_telemetry=True)
            analyze(runs, root / "analysis")
            summary = json.loads((root / "analysis/summary.json").read_text())
            text = " ".join(summary["warnings"])
            self.assertIn("repetitions", text)
            self.assertIn("order cannot be checked", text)
            self.assertIn("predictive run has no controller artifacts", text)
            self.assertTrue((root / "analysis/p95_latency.svg").read_text().startswith("<svg"))
            self.assertIn("## Warnings", (root / "analysis/report.md").read_text())


if __name__ == "__main__":
    unittest.main()

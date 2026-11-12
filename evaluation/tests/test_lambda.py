import json
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from evaluation.common import write_json, write_jsonl
from evaluation.lambda_correlate import correlate, load_reports, parse_report
from evaluation.replay import _send_one, replay, validate_metadata

REPORT = ("REPORT RequestId: {id}\tDuration: {d} ms\tBilled Duration: {b} ms\t"
          "Memory Size: 256 MB\tMax Memory Used: 40 MB{init}\n")


def report(request_id, duration, billed, init=None):
    suffix = f"\tInit Duration: {init} ms" if init is not None else ""
    return REPORT.format(id=request_id, d=duration, b=billed, init=suffix)


class SignedReplayTests(unittest.TestCase):
    def test_signed_headers_are_sent_and_aws_request_id_is_recorded(self):
        seen = {}

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                seen["auth"] = self.headers.get("Authorization")
                payload = json.dumps({"aws_request_id": "aws-1", "init_ms": 0.0}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            calls = []

            def signer(url, region, headers):
                calls.append((url, region))
                return {**headers, "Authorization": "signed"}

            endpoint = {"url": f"http://127.0.0.1:{server.server_port}/", "auth": "sigv4", "region": "us-east-1"}
            event = {"scheduled_at_s": 0, "function_id": "periodic", "request_id": "r1"}
            record = _send_one(event, endpoint, "run", "2026-01-01T00:00:00Z", 5, time.monotonic(), signer)
        finally:
            server.shutdown()
            server.server_close()
        self.assertEqual(seen["auth"], "signed")
        self.assertEqual(calls[0][1], "us-east-1")
        self.assertEqual(record["aws_request_id"], "aws-1")
        self.assertEqual(record["status_code"], 200)

    def test_lambda_runs_require_sigv4_and_lambda_metadata(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            write_jsonl(root / "trace.jsonl", [
                {"scheduled_at_s": 0, "function_id": "periodic", "request_id": "r1"}])
            write_json(root / "endpoints.json", {"periodic": {"url": "https://x.lambda-url.us-east-1.on.aws/"}})
            with self.assertRaisesRegex(ValueError, "require auth: sigv4"):
                replay(root / "trace.jsonl", root / "endpoints.json", "lambda", "run", root / "out")
        self.assertEqual(len(validate_metadata({}, "lambda")), 5)
        self.assertEqual(validate_metadata({
            "region": "us-east-1", "runtime": "python3.12", "architecture": "arm64",
            "memory_mb": 256, "function_versions": {"periodic": "1"}}, "lambda"), [])


class CorrelationTests(unittest.TestCase):
    def _run(self, root, clients):
        run = root / "run"
        run.mkdir()
        write_jsonl(run / "client_measurements.jsonl", clients)
        write_json(run / "run_manifest.json", {
            "run_id": "run", "policy": "lambda", "measurements_file": "client_measurements.jsonl"})
        return run

    def test_parse_report_separates_duration_billed_and_init(self):
        parsed = parse_report(report("abc-1", "1.86", 2, "94.00"))
        self.assertEqual((parsed["duration_ms"], parsed["billed_duration_ms"], parsed["init_duration_ms"]),
                         (1.86, 2.0, 94.0))
        self.assertIsNone(parse_report(report("abc-2", "2.00", 3)).get("init_duration_ms"))

    def test_join_counts_orphans_and_usage(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            clients = [
                {"function_id": "periodic", "aws_request_id": "id-1", "status_code": 200, "latency_ms": 50.0},
                {"function_id": "periodic", "aws_request_id": "id-2", "status_code": 200, "latency_ms": 20.0},
                {"function_id": "steady", "aws_request_id": "id-3", "status_code": 200, "latency_ms": 30.0},
                {"function_id": "steady", "aws_request_id": None, "status_code": None, "latency_ms": 30000.0},
            ]
            run = self._run(root, clients)
            logs = root / "logs.jsonl"
            logs.write_text("\n".join(json.dumps({"message": m}) for m in [
                report("id-1", "2.00", 3, "94.00"), report("id-2", "1.00", 2),
                report("orphan", "1.00", 2),
            ]) + "\n", encoding="utf-8")
            summary = correlate(run, [logs], {
                "per_gb_second": 0.0000133334, "per_million_requests": 0.2,
                "source": "test price", "date": "2026-11-12"})
            self.assertEqual(summary["join_counts"], {
                "matched": 2, "unmatched": 1, "ambiguous": 0, "no_aws_request_id": 1})
            self.assertEqual(summary["reports_without_client_request"], 1)
            self.assertEqual(summary["initialisation"]["invocations_with_init_duration"], 1)
            self.assertAlmostEqual(summary["billed"]["billed_gb_seconds"], 5 / 1000 * 0.25, places=6)
            self.assertEqual(summary["estimated_cost_usd"]["price_date"], "2026-11-12")
            self.assertTrue((run / "lambda_joined.jsonl").is_file())

    def test_duplicate_reports_are_ambiguous_and_cloudwatch_cli_json_is_read(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            run = self._run(root, [
                {"function_id": "periodic", "aws_request_id": "dup", "status_code": 200, "latency_ms": 5.0}])
            logs = root / "cli.json"
            logs.write_text(json.dumps({"events": [
                {"message": report("dup", "1.0", 2)}, {"message": report("dup", "1.0", 2)}]}), encoding="utf-8")
            self.assertEqual(len(load_reports([logs])), 2)
            summary = correlate(run, [logs])
            self.assertEqual(summary["join_counts"]["ambiguous"], 1)
            self.assertNotIn("estimated_cost_usd", summary)


if __name__ == "__main__":
    unittest.main()

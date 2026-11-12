import contextlib
import importlib.util
import io
import json
import os
import threading
import urllib.request
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "src/handler.py"


def load(**environment):
    with patch.dict(os.environ, {"FUNCTION_ID": "periodic", "INIT_DELAY_MS": "0",
                                 "WORK_DELAY_MS": "0", **environment}):
        spec = importlib.util.spec_from_file_location("benchmark_lambda", SOURCE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


def http(path="/", headers=None, method="GET"):
    return {"version": "2.0", "rawPath": path, "rawQueryString": "", "headers": headers or {},
            "requestContext": {"http": {"method": method}}}


class HandlerTests(unittest.TestCase):
    def setUp(self):
        self.module = load()
        self.logs = io.StringIO()
        self.context = SimpleNamespace(function_version="7", aws_request_id="aws-request-1")

    def invoke(self, event):
        with contextlib.redirect_stdout(self.logs):
            result = self.module.lambda_handler(event, self.context)
        return result["statusCode"], json.loads(result["body"])

    def test_ids_logs_and_first_user_accounting(self):
        _, first = self.invoke(http(headers={"X-Request-ID": "client-1"}))
        _, second = self.invoke(http(headers={"x-request-id": "client-2"}))
        self.assertTrue(first["first_invocation"])
        self.assertTrue(first["first_user_request"])
        self.assertFalse(second["first_invocation"])
        self.assertFalse(second["first_user_request"])
        self.assertEqual(first["instance_id"], second["instance_id"])
        self.assertEqual(first["revision"], "lambda-7")
        self.assertEqual(first["aws_request_id"], "aws-request-1")
        self.assertEqual(first["request_id"], "client-1")
        lines = [json.loads(line) for line in self.logs.getvalue().splitlines()]
        self.assertEqual(lines, [first, second])

    def test_health_and_probes_do_not_create_demand_but_consume_invocations(self):
        for event in (http("/healthz"), http(headers={"K-Network-Probe": "probe"}),
                      http(headers={"K-Kubelet-Probe": "probe"})):
            self.assertEqual(self.invoke(event), (200, {"status": "ready"}))
        self.assertEqual(self.logs.getvalue(), "")
        _, user = self.invoke(http())
        self.assertTrue(user["first_user_request"])
        self.assertFalse(user["first_invocation"])

    def test_warmup_is_separate_from_user_and_platform_cold_marker(self):
        _, warmup = self.invoke(http(headers={"X-Request-Kind": "warmup"}))
        self.assertTrue(warmup["first_invocation"])
        self.assertFalse(warmup["first_user_request"])
        self.assertEqual(warmup["request_kind"], "warmup")
        _, user = self.invoke(http())
        self.assertTrue(user["first_user_request"])
        self.assertFalse(user["first_invocation"])

    def test_delay_parameters_execute_equivalent_application_work(self):
        with patch("time.sleep") as sleep:
            module = load(INIT_DELAY_MS="25", WORK_DELAY_MS="50")
            sleep.assert_called_once_with(0.025)
            with contextlib.redirect_stdout(io.StringIO()):
                module.lambda_handler(http(), self.context)
            self.assertEqual(sleep.call_args.args, (0.05,))

    def test_routing_errors_do_not_count_or_log_demand(self):
        for event, expected in (({}, 400), (http(method="POST"), 405),
                                (http("/missing"), 404), ({**http(), "rawQueryString": "x=1"}, 404)):
            self.assertEqual(self.invoke(event)[0], expected)
        self.assertEqual(self.module.USER_COUNT, 0)
        self.assertEqual(self.logs.getvalue(), "")

    def test_required_invocation_contract_is_preserved(self):
        _, result = self.invoke(http())
        required = {"schema_version", "function_id", "revision", "instance_id", "request_id",
                    "request_kind", "started_at", "service_ms", "first_user_request", "init_ms"}
        self.assertTrue(required <= result.keys())
        self.assertGreaterEqual(result["service_ms"], 0)
        self.assertGreaterEqual(result["init_ms"], 0)
        self.assertEqual(result["platform"], "aws_lambda")

    def test_new_process_has_new_identity(self):
        self.assertNotEqual(self.module.INSTANCE_ID, load().INSTANCE_ID)

    def test_invalid_delay_configuration_fails_before_serving(self):
        for value in ("-1", "nan", "inf", "not-a-number"):
            with self.assertRaises(ValueError):
                load(INIT_DELAY_MS=value)

    def test_http_and_lambda_match_supported_request_semantics(self):
        source = Path(__file__).resolve().parents[3] / "functions/benchmark/app.py"
        spec = importlib.util.spec_from_file_location("http_benchmark", source)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(os.environ, {"FUNCTION_ID": "periodic", "INIT_DELAY_MS": "0", "WORK_DELAY_MS": "0"}):
            spec.loader.exec_module(module)
        with module.BenchmarkServer(("127.0.0.1", 0), module.Handler) as server:
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
            thread.start()
            try:
                cases = [http("/healthz"), http(headers={"K-Network-Probe": "probe"}),
                         http(headers={"X-Request-Kind": "warmup", "X-Request-ID": "warm-1"}),
                         http(headers={"X-Request-ID": "user-1"}),
                         http(headers={"X-Request-ID": "user-2"})]
                with contextlib.redirect_stdout(io.StringIO()):
                    for case in cases:
                        req = urllib.request.Request(f"http://127.0.0.1:{server.server_port}" + case["rawPath"],
                                                     headers=case["headers"])
                        with urllib.request.urlopen(req, timeout=2) as reply:
                            native = json.load(reply)
                        status, adapted = self.invoke(case)
                        self.assertEqual(status, 200)
                        if "status" in native:
                            self.assertEqual(native, adapted)
                        else:
                            for field in ("schema_version", "function_id", "request_id", "request_kind", "first_user_request"):
                                self.assertEqual(native[field], adapted[field])
            finally:
                server.shutdown()
                thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()

import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from botocore.credentials import Credentials

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ToolTests(unittest.TestCase):
    def test_signing_uses_sdk_and_temporary_session_credentials(self):
        tool = load("smoke_url")
        credentials = Credentials("FAKEACCESS", "FAKESECRET", "FAKETOKEN")
        request = tool.signed_request("https://example.lambda-url.ap-southeast-1.on.aws/", "/",
                                      "ap-southeast-1", credentials, "client-1")
        headers = {name.lower(): value for name, value in request.header_items()}
        self.assertIn("/ap-southeast-1/lambda/aws4_request", headers["authorization"])
        self.assertEqual(headers["x-amz-security-token"], "FAKETOKEN")
        self.assertEqual(headers["x-request-id"], "client-1")
        self.assertNotIn("FAKESECRET", str(headers))

    def test_signer_rejects_redirects_arbitrary_hosts_and_wrong_regions(self):
        tool = load("smoke_url")
        invalid = ["http://example.lambda-url.ap-southeast-1.on.aws/",
                   "https://example.com/", "https://example.lambda-url.us-east-1.on.aws/",
                   "https://example.lambda-url.ap-southeast-1.on.aws/?x=1",
                   "https://user@example.lambda-url.ap-southeast-1.on.aws/"]
        for url in invalid:
            with self.assertRaises(ValueError):
                tool.endpoint(url, "ap-southeast-1")
        self.assertIsNone(tool.NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://example.com"))

    def test_environment_export_filters_secrets_and_unrelated_configuration(self):
        tool = load("export_environment")
        config = {"MemorySize": 256, "CodeSha256": "hash", "SecretField": "secret",
                  "Environment": {"Variables": {"FUNCTION_ID": "periodic", "TOKEN": "never-export", "WORK_DELAY_MS": "0"}}}
        result = tool.safe_configuration(config)
        self.assertEqual(result["MemorySize"], 256)
        self.assertEqual(result["benchmark_environment"], {"FUNCTION_ID": "periodic", "WORK_DELAY_MS": "0"})
        self.assertNotIn("never-export", json.dumps(result))
        self.assertNotIn("secret", json.dumps(result))

    def test_smoke_report_does_not_record_authorization_or_credentials(self):
        tool = load("smoke_url")
        credentials = Credentials("FAKEACCESS", "FAKESECRET", "FAKETOKEN")
        class Response:
            status = 200
            def __init__(self, request):
                self.request = request
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def read(self):
                if self.request.full_url.endswith("/healthz"):
                    value = {"status": "ready"}
                else:
                    headers = {k.lower(): v for k, v in self.request.header_items()}
                    value = {"function_id": "periodic", "request_id": headers["x-request-id"],
                             "request_kind": "user", "platform": "aws_lambda", "aws_request_id": "aws-1"}
                return json.dumps(value).encode()
        class Opener:
            def open(self, request, timeout):
                return Response(request)
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "smoke.json"
            argv = ["smoke_url", "--url", "https://example.lambda-url.ap-southeast-1.on.aws/",
                    "--region", "ap-southeast-1", "--function-id", "periodic", "--output", str(output)]
            stdout = io.StringIO()
            with patch.object(sys, "argv", argv), patch("boto3.Session") as session, \
                    patch.object(tool.urllib.request, "build_opener", return_value=Opener()), \
                    contextlib.redirect_stdout(stdout):
                session.return_value.get_credentials.return_value = credentials
                self.assertEqual(tool.main(), 0)
            content = output.read_text() + stdout.getvalue()
            for value in ("FAKEACCESS", "FAKESECRET", "FAKETOKEN", "Authorization"):
                self.assertNotIn(value, content)
            self.assertTrue(json.loads(output.read_text())["passed"])


if __name__ == "__main__":
    unittest.main()

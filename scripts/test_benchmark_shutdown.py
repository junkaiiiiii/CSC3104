"""Exercise actual HTTP handlers and process signals without requiring Docker."""
import concurrent.futures
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def wait_until(predicate, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("timed out waiting for benchmark")


class ShutdownTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        self.process = None

    def tearDown(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.kill()
            self.process.communicate(timeout=5)
        self.temporary.cleanup()

    def start(self, mode="normal", init_delay="0"):
        # A wrapper pauses a real response only in this test process. A file
        # handshake proves the request reached the handler before SIGTERM.
        wrapper = r'''
import os, time
from pathlib import Path
import app
directory = Path(os.environ["TEST_DIRECTORY"])
if os.environ["TEST_MODE"] == "response":
    original = app.Handler.reply
    def reply(self, status, payload):
        if self.path == "/":
            (directory / "active").touch()
            deadline = time.monotonic() + 5
            while not (directory / "release").exists():
                if time.monotonic() > deadline:
                    raise RuntimeError("test did not release response")
                time.sleep(0.01)
        return original(self, status, payload)
    app.Handler.reply = reply
if os.environ["TEST_MODE"] == "init":
    original_sleep = time.sleep
    def sleep(seconds):
        (directory / "initializing").touch()
        original_sleep(seconds)
    app.time.sleep = sleep
app.main()
'''
        env = dict(os.environ, PORT=str(self.port), INIT_DELAY_MS=init_delay,
                   WORK_DELAY_MS="0", TEST_DIRECTORY=str(self.directory), TEST_MODE=mode,
                   PYTHONPATH=str(ROOT / "functions/benchmark"))
        self.process = subprocess.Popen([sys.executable, "-c", wrapper], cwd=ROOT,
                                        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if mode != "init":
            def ready():
                try:
                    with urllib.request.urlopen(self.url("/healthz"), timeout=0.2) as response:
                        return response.status == 200
                except OSError:
                    return False
            wait_until(ready)

    def url(self, path):
        return f"http://127.0.0.1:{self.port}{path}"

    def test_sigterm_exits_cleanly_when_idle(self):
        self.start()
        self.process.send_signal(signal.SIGTERM)
        self.assertEqual(self.process.wait(timeout=3), 0)

    def test_sigint_exits_cleanly_when_idle(self):
        self.start()
        self.process.send_signal(signal.SIGINT)
        self.assertEqual(self.process.wait(timeout=3), 0)

    def test_sigterm_during_initialization_does_not_wait_for_full_delay(self):
        self.start(mode="init", init_delay="10000")
        wait_until(lambda: (self.directory / "initializing").exists())
        self.process.send_signal(signal.SIGTERM)
        self.assertEqual(self.process.wait(timeout=3), 0)

    def test_active_response_and_event_complete_before_exit(self):
        self.start(mode="response")
        def request():
            req = urllib.request.Request(self.url("/"), headers={"X-Request-ID": "drain-1"})
            with urllib.request.urlopen(req, timeout=5) as response:
                return response.status, json.load(response)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(request)
            try:
                wait_until(lambda: (self.directory / "active").exists())
                self.process.send_signal(signal.SIGTERM)
                time.sleep(0.15)
                self.assertIsNone(self.process.poll(), "active handler was abandoned")
                # Repeated stop requests must not interrupt the drain.
                self.process.send_signal(signal.SIGTERM)
            finally:
                (self.directory / "release").touch()
            status, body = result.result(timeout=5)
        self.assertEqual(status, 200)
        self.assertEqual(body["request_id"], "drain-1")
        self.assertTrue(body["first_user_request"])
        self.assertEqual(self.process.wait(timeout=3), 0)
        stdout, _ = self.process.communicate(timeout=3)
        events = [json.loads(line) for line in stdout.splitlines()]
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["request_id"], "drain-1")


if __name__ == "__main__":
    unittest.main()

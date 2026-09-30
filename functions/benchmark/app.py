"""Single-process benchmark; first use is a proxy, not proof of cold-start exposure."""
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

INSTANCE_ID = str(uuid.uuid4())
FUNCTION_ID = os.getenv("FUNCTION_ID", "benchmark")
REVISION = os.getenv("K_REVISION", "local")
INIT_START = time.perf_counter()
time.sleep(max(0, float(os.getenv("INIT_DELAY_MS", "0"))) / 1000)
INIT_MS = (time.perf_counter() - INIT_START) * 1000
WORK_DELAY = max(0, float(os.getenv("WORK_DELAY_MS", "0"))) / 1000
LOCK = threading.Lock()
USER_COUNT = 0


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global USER_COUNT
        if self.path == "/healthz":
            self.reply(200, {"status": "ready"})
            return
        if self.path != "/":
            self.reply(404, {"error": "not found"})
            return
        # Knative internal probes must not enter demand history.
        if self.headers.get("K-Network-Probe") or self.headers.get("K-Kubelet-Probe"):
            self.reply(200, {"status": "ready"})
            return
        start = time.perf_counter()
        started_at = datetime.now(timezone.utc).isoformat()
        kind = "warmup" if self.headers.get("X-Request-Kind") == "warmup" else "user"
        with LOCK:
            first_user = kind == "user" and USER_COUNT == 0
            if kind == "user":
                USER_COUNT += 1
        time.sleep(WORK_DELAY)
        event = {
            "schema_version": 1,
            "function_id": FUNCTION_ID,
            "revision": REVISION,
            "instance_id": INSTANCE_ID,
            "request_id": self.headers.get("X-Request-ID") or str(uuid.uuid4()),
            "request_kind": kind,
            "started_at": started_at,
            "service_ms": (time.perf_counter() - start) * 1000,
            "first_user_request": first_user,
            "init_ms": INIT_MS,
        }
        print(json.dumps(event), flush=True)
        self.reply(200, event)

    def reply(self, status, payload):
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *_args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", int(os.getenv("PORT", "8080"))), Handler).serve_forever()

"""Single-process benchmark; first use is a proxy, not proof of cold-start exposure."""
import json
import os
import signal
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

INSTANCE_ID = str(uuid.uuid4())
FUNCTION_ID = os.getenv("FUNCTION_ID", "benchmark")
REVISION = os.getenv("K_REVISION", "local")
INIT_DELAY = max(0, float(os.getenv("INIT_DELAY_MS", "0"))) / 1000
INIT_MS = 0.0
WORK_DELAY = max(0, float(os.getenv("WORK_DELAY_MS", "0"))) / 1000
LOCK = threading.Lock()
USER_COUNT = 0
STOPPING = False


class BenchmarkServer(ThreadingHTTPServer):
    # ThreadingHTTPServer defaults to daemon request threads. Explicitly join them
    # on close so a stop signal cannot abandon accepted requests or their logs.
    daemon_threads = False
    block_on_close = True


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global USER_COUNT
        if STOPPING:
            self.reply(503, {"status": "stopping"})
            return
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


def main():
    global STOPPING, INIT_MS

    def request_stop(_signum, _frame):
        # Keep signal handling minimal. In particular, shutdown() must not run in
        # the same thread as serve_forever(), or it can deadlock.
        global STOPPING
        STOPPING = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    init_start = time.perf_counter()
    while not STOPPING:
        remaining = INIT_DELAY - (time.perf_counter() - init_start)
        if remaining <= 0:
            break
        time.sleep(min(remaining, 0.05))
    INIT_MS = (time.perf_counter() - init_start) * 1000
    if STOPPING:
        return

    with BenchmarkServer(("0.0.0.0", int(os.getenv("PORT", "8080"))), Handler) as server:
        serving = threading.Thread(target=server.serve_forever,
                                   kwargs={"poll_interval": 0.05})
        serving.start()
        try:
            while not STOPPING:
                time.sleep(0.05)
        finally:
            server.shutdown()
            serving.join()
        # Exiting the context closes the socket and waits for active handlers.


if __name__ == "__main__":
    main()

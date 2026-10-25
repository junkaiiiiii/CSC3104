"""One-command local check: real benchmark function -> collector -> buckets.

Run from anywhere:  python3 telemetry/tests/e2e_local.py   (path relative to your cwd)
Starts the benchmark on a free port, sends probe/health/warmup/user traffic,
runs the collector on the function's real stdout, and asserts the outcome.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "telemetry"))
from collector.collector import Collector  # noqa: E402


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main():
    port = free_port()
    work = Path(tempfile.mkdtemp(prefix="telemetry-e2e-"))
    events_path = work / "events.jsonl"
    env = dict(os.environ, PORT=str(port), FUNCTION_ID="periodic", PYTHONUNBUFFERED="1")
    with open(events_path, "w") as out:
        server = subprocess.Popen([sys.executable, str(ROOT / "functions/benchmark/app.py")],
                                  env=env, stdout=out, stderr=subprocess.PIPE)
        try:
            def get(path="/", headers=None):
                req = urllib.request.Request("http://127.0.0.1:%d%s" % (port, path), headers=headers or {})
                return urllib.request.urlopen(req, timeout=3).read()

            deadline = time.monotonic() + 10
            while True:
                try:
                    get("/healthz")
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise SystemExit("FAIL: benchmark function did not start")
                    time.sleep(0.1)
            get(headers={"K-Network-Probe": "probe"})                       # must not appear
            get(headers={"X-Request-Kind": "warmup", "X-Request-ID": "e2e-w1"})
            for i in (1, 2, 3):
                get(headers={"X-Request-ID": "e2e-u%d" % i})
        finally:
            server.terminate()
            server.wait(timeout=5)

    lines = events_path.read_text().splitlines()
    collector = Collector(str(work / "run"), ["periodic", "steady"], "e2e", clock="event")
    for line in lines:
        collector.process_line(line)
    ok = collector.close()
    buckets = [json.loads(x) for x in (work / "run/demand_buckets.jsonl").read_text().splitlines()]
    by_fn = {b["function_id"]: b for b in buckets}

    checks = [
        ("function emitted 4 events (probe and /healthz excluded)", len(lines) == 4),
        ("3 user requests counted", collector.totals["counted"] == 3),
        ("1 warmup kept out of demand", collector.totals["warmup"] == 1),
        ("periodic bucket count is 3 and complete", by_fn["periodic"]["invocation_count"] == 3 and by_fn["periodic"]["complete"]),
        ("steady bucket is a confirmed 0", by_fn["steady"]["invocation_count"] == 0 and by_fn["steady"]["complete"]),
        ("conservation check passed", ok),
    ]
    for label, passed in checks:
        print(("PASS  " if passed else "FAIL  ") + label)
    print("output kept in", work)
    return 0 if all(p for _, p in checks) else 1


if __name__ == "__main__":
    sys.exit(main())

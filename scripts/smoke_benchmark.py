"""Check observable measurement semantics without requiring a cluster."""
import json
import os
import subprocess
import sys
import time
import urllib.request

env = dict(os.environ, PORT="18080", INIT_DELAY_MS="0", WORK_DELAY_MS="0")
process = subprocess.Popen([sys.executable, "functions/benchmark/app.py"], env=env,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
def request(path="/", headers=None):
    req = urllib.request.Request("http://127.0.0.1:18080" + path, headers=headers or {})
    with urllib.request.urlopen(req, timeout=2) as response:
        return json.load(response)
try:
    deadline = time.monotonic() + 10
    while True:
        try:
            request("/healthz")
            break
        except (OSError, TimeoutError):
            if time.monotonic() >= deadline:
                raise RuntimeError("benchmark did not become ready")
            time.sleep(0.05)
    request(headers={"K-Network-Probe": "probe"})
    warm = request(headers={"X-Request-Kind": "warmup", "X-Request-ID": "warm-1"})
    first = request(headers={"X-Request-ID": "user-1"})
    second = request(headers={"X-Request-ID": "user-2"})
    assert warm["request_kind"] == "warmup" and not warm["first_user_request"]
    assert first["first_user_request"] and not second["first_user_request"]
    assert first["request_id"] == "user-1"
    assert first["instance_id"] == second["instance_id"] == warm["instance_id"]
    print("PASS: readiness/probes/warmups excluded from first-user accounting; IDs correlate")
finally:
    process.terminate()
    process.communicate(timeout=5)

"""Verify idle SIGTERM handling with the application running as container PID 1."""
import argparse
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    path = Path(args.output)
    if path.exists():
        raise SystemExit("Output already exists; use a fresh evidence filename.")
    name = "shutdown-check-" + uuid.uuid4().hex[:12]
    created = False
    try:
        subprocess.run(["docker", "run", "-d", "--name", name, "--stop-signal", "SIGTERM",
                        "-e", "INIT_DELAY_MS=0", "-e", "WORK_DELAY_MS=0", args.image],
                       check=True, capture_output=True, text=True)
        created = True
        readiness = '''
import time, urllib.request
deadline = time.monotonic() + 10
while True:
    try:
        with urllib.request.urlopen("http://127.0.0.1:8080/healthz", timeout=1) as response:
            assert response.status == 200
        break
    except OSError:
        if time.monotonic() >= deadline:
            raise
        time.sleep(0.05)
'''
        subprocess.run(["docker", "exec", name, "python", "-c", readiness],
                       check=True, timeout=15, capture_output=True, text=True)
        started = time.perf_counter()
        subprocess.run(["docker", "stop", "--timeout", "10", name],
                       check=True, capture_output=True, text=True, timeout=30)
        elapsed = time.perf_counter() - started
        container = json.loads(subprocess.check_output(["docker", "inspect", name], text=True))[0]
        state = container["State"]
        report = {"tested_at": datetime.now(timezone.utc).isoformat(),
                  "image_reference": args.image, "image_id": container["Image"],
                  "stop_timeout_seconds": 10, "stop_elapsed_seconds": round(elapsed, 3),
                  "state": state,
                  "passed": state["ExitCode"] == 0 and not state["OOMKilled"] and elapsed < 10}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))
        print("Saved:", path)
        return 0 if report["passed"] else 1
    finally:
        if created:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True)


if __name__ == "__main__":
    raise SystemExit(main())

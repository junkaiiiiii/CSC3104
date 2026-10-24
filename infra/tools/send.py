# send.py  usage: python send.py periodic.csc3104.example.com run1 6
import json, sys, time, urllib.request
from datetime import datetime, timezone
host, run, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
for i in range(n):
    rid = f"{run}-{i}"
    req = urllib.request.Request("http://127.0.0.1:8080/", headers={"Host": host, "X-Request-ID": rid})
    sent = datetime.now(timezone.utc).isoformat()
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body, status = json.load(r), r.status
    except Exception as e:
        body, status = {}, str(e)
    ms = (time.perf_counter() - t0) * 1000
    print(json.dumps({"request_id": rid, "sent_at": sent, "latency_ms": round(ms, 1), "status": status,
        "instance_id": body.get("instance_id"), "revision": body.get("revision"),
        "first_user_request": body.get("first_user_request")}), flush=True)
    time.sleep(0.5)
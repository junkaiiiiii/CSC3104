# watch_pods.py  usage: python watch_pods.py csc3104 pods.jsonl
import json, subprocess, sys, time
from datetime import datetime, timezone
ns, out = sys.argv[1], sys.argv[2]
with open(out, "a") as f:
    while True:
        raw = subprocess.run(["kubectl", "get", "pods", "-n", ns, "-o", "json"],
                             capture_output=True, text=True).stdout
        if raw:
            pods = []
            for p in json.loads(raw).get("items", []):
                st = p["status"]
                pods.append({"name": p["metadata"]["name"], "phase": st.get("phase"),
                    "ready": any(c["type"] == "Ready" and c["status"] == "True" for c in st.get("conditions", [])),
                    "terminating": "deletionTimestamp" in p["metadata"],
                    "service": p["metadata"].get("labels", {}).get("serving.knative.dev/service")})
            f.write(json.dumps({"t": datetime.now(timezone.utc).isoformat(), "pods": pods}) + "\n")
            f.flush()
        time.sleep(1)
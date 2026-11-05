"""IAM-signed Function URL checks; not a policy latency benchmark."""
import argparse
import hashlib
import importlib.metadata
import json
import re
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit


def endpoint(url, region):
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    expected = rf"[a-z0-9]+\.lambda-url\.{re.escape(region)}\.on\.aws"
    if (parsed.scheme != "https" or not re.fullmatch(expected, host)
            or parsed.username or parsed.password or parsed.port not in (None, 443)
            or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
        raise ValueError("expected a root HTTPS Lambda Function URL in the specified region")
    return url.rstrip("/")


def signed_request(url, path, region, credentials, request_id):
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    if path not in ("/", "/healthz"):
        raise ValueError("smoke paths are restricted to / and /healthz")
    request = AWSRequest(method="GET", url=endpoint(url, region) + path,
                         headers={"X-Request-ID": request_id, "X-Request-Kind": "user",
                                  "X-Amz-Content-SHA256": hashlib.sha256(b"").hexdigest()})
    SigV4Auth(credentials, "lambda", region).add_auth(request)
    return urllib.request.Request(request.url, method="GET", headers=dict(request.headers))


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    import boto3
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--function-id", choices=("periodic", "steady", "sparse"), required=True)
    parser.add_argument("--profile")
    parser.add_argument("--output", required=True, help="new JSON evidence file; never stores auth headers")
    args = parser.parse_args()
    endpoint(args.url, args.region)
    target = Path(args.output)
    if target.exists():
        raise FileExistsError("use a new output filename")
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    credentials = session.get_credentials()
    if credentials is None:
        raise ValueError("no AWS credentials found; configure the intended profile first")
    opener = urllib.request.build_opener(NoRedirect())
    rows = []
    passed = True
    for path in ("/healthz", "/", "/"):
        request_id = "lambda-smoke-" + uuid.uuid4().hex
        req = signed_request(args.url, path, args.region, credentials.get_frozen_credentials(), request_id)
        start = time.perf_counter()
        try:
            with opener.open(req, timeout=35) as response:
                status, payload = response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            status, payload = error.code, {"error_body": error.read().decode(errors="replace")[:1000]}
            error.close()
        elapsed = (time.perf_counter() - start) * 1000
        ok = status == 200 and isinstance(payload, dict)
        if path == "/healthz":
            ok = ok and payload == {"status": "ready"}
        else:
            ok = ok and payload.get("function_id") == args.function_id \
                and payload.get("request_id") == request_id and payload.get("request_kind") == "user" \
                and payload.get("platform") == "aws_lambda" and bool(payload.get("aws_request_id"))
        passed = passed and ok
        rows.append({"path": path, "request_id": request_id, "status_code": status,
                     "diagnostic_http_ms": round(elapsed, 3), "passed": ok, "response": payload})
    report = {"tested_at": datetime.now(timezone.utc).isoformat(), "region": args.region,
              "url": args.url, "function_id": args.function_id, "passed": passed,
              "sdk_versions": {name: importlib.metadata.version(name) for name in ("boto3", "botocore")},
              "note": "Smoke traffic warms environments. These three diagnostic requests are not a performance sample.",
              "requests": rows}
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x") as stream:
        stream.write(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())

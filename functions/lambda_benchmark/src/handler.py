"""Lambda Function URL adapter for the CSC3104 instrumented benchmark.

No HTTP server or third-party runtime dependencies. Application init_ms is not
Lambda platform Init Duration; first_invocation is only a process-local marker.
"""
import json
import math
import os
import threading
import time
import uuid
from datetime import datetime, timezone


def delay_seconds(name):
    milliseconds = float(os.getenv(name, "0"))
    if not math.isfinite(milliseconds) or milliseconds < 0:
        raise ValueError(f"{name} must be finite and non-negative")
    return milliseconds / 1000


FUNCTION_ID = os.getenv("FUNCTION_ID", "benchmark")
INSTANCE_ID = str(uuid.uuid4())
WORK_DELAY = delay_seconds("WORK_DELAY_MS")
INIT_DELAY = delay_seconds("INIT_DELAY_MS")
INIT_START = time.perf_counter()
time.sleep(INIT_DELAY)
INIT_MS = (time.perf_counter() - INIT_START) * 1000
LOCK = threading.Lock()
USER_COUNT = 0
INVOCATION_COUNT = 0


def response(status, payload):
    return {"statusCode": status, "headers": {"content-type": "application/json"},
            "isBase64Encoded": False, "body": json.dumps(payload)}


def lambda_handler(event, context):
    global USER_COUNT, INVOCATION_COUNT
    with LOCK:
        first_invocation = INVOCATION_COUNT == 0
        INVOCATION_COUNT += 1

    # Function URLs deliver payload-format-v2 HTTP events, including when IAM
    # authorisation occurs upstream. Direct smoke invocations must supply one too.
    if not isinstance(event, dict) or event.get("version") != "2.0":
        return response(400, {"error": "expected a Function URL payload-format-v2 event"})
    request_context = event.get("requestContext") or {}
    if not isinstance(request_context, dict):
        return response(400, {"error": "invalid request context"})
    http = request_context.get("http") or {}
    if not isinstance(http, dict):
        return response(400, {"error": "invalid HTTP request context"})
    if http.get("method") != "GET":
        return response(405, {"error": "method not allowed"})
    path = event.get("rawPath")
    if not isinstance(path, str):
        return response(400, {"error": "missing rawPath"})
    # Match the existing HTTP benchmark's exact routing, including query strings.
    query = event.get("rawQueryString", "")
    if not isinstance(query, str):
        return response(400, {"error": "invalid query string"})
    if query:
        path += "?" + query
    if path == "/healthz":
        return response(200, {"status": "ready"})
    if path != "/":
        return response(404, {"error": "not found"})
    raw_headers = event.get("headers") or {}
    if (not isinstance(raw_headers, dict)
            or any(not isinstance(k, str) or not isinstance(v, str) for k, v in raw_headers.items())):
        return response(400, {"error": "invalid headers"})
    headers = {key.lower(): value for key, value in raw_headers.items()}
    if headers.get("k-network-probe") or headers.get("k-kubelet-probe"):
        return response(200, {"status": "ready"})

    start = time.perf_counter()
    started_at = datetime.now(timezone.utc).isoformat()
    kind = "warmup" if headers.get("x-request-kind") == "warmup" else "user"
    with LOCK:
        first_user = kind == "user" and USER_COUNT == 0
        if kind == "user":
            USER_COUNT += 1
    time.sleep(WORK_DELAY)
    event = {
        "schema_version": 1,
        "function_id": FUNCTION_ID,
        "revision": "lambda-" + str(getattr(context, "function_version", "$LATEST")),
        "instance_id": INSTANCE_ID,
        "request_id": headers.get("x-request-id") or str(uuid.uuid4()),
        "request_kind": kind,
        "started_at": started_at,
        "service_ms": (time.perf_counter() - start) * 1000,
        "first_user_request": first_user,
        "init_ms": INIT_MS,
        "platform": "aws_lambda",
        "aws_request_id": getattr(context, "aws_request_id", None),
        "first_invocation": first_invocation,
        "initialization_type": os.getenv("AWS_LAMBDA_INITIALIZATION_TYPE", "unknown"),
    }
    print(json.dumps(event), flush=True)
    return response(200, event)

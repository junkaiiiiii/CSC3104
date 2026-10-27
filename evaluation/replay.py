"""Replay a trace against function endpoints and retain every client outcome.

Usage: python -m evaluation.replay --trace trace.jsonl --endpoints endpoints.json \
  --policy reactive --run-id reactive-17 --output results
"""
from __future__ import annotations

import argparse
import concurrent.futures
import http.client
import json
import math
import re
import shutil
import socket
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .common import git_revision, iso_utc, parse_utc, read_jsonl, sha256_file, write_json, write_jsonl

POLICIES = ("reactive", "fixed_minimum", "predictive")


def _load_object(path: str | Path) -> dict:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"{path}: invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return value


def _is_timeout(error: BaseException) -> bool:
    if isinstance(error, (TimeoutError, socket.timeout)):
        return True
    return isinstance(error, urllib.error.URLError) and isinstance(
        error.reason, (TimeoutError, socket.timeout)
    )


def _send_one(event: dict, endpoint: dict, run_id: str, scheduled_at: str,
              timeout_s: float, origin: float) -> dict:
    delay = event["scheduled_at_s"] - (time.monotonic() - origin)
    if delay > 0:
        time.sleep(delay)
    sent_at = iso_utc()
    started = time.monotonic()
    request_id = f"{run_id}-{event['request_id']}"
    headers = {
        **endpoint.get("headers", {}),
    }
    headers["X-Request-ID"] = request_id
    headers["X-Request-Kind"] = event.get("request_kind", "user")
    if endpoint.get("host"):
        headers["Host"] = endpoint["host"]
    request = urllib.request.Request(endpoint["url"], headers=headers)
    status_code, body, error, timed_out = None, {}, None, False
    try:
        with urllib.request.urlopen(request, timeout=timeout_s) as response:
            status_code = response.status
            payload = response.read()
    except urllib.error.HTTPError as exception:
        status_code = exception.code
        payload = exception.read()
        exception.close()
    except (urllib.error.URLError, http.client.HTTPException, OSError, TimeoutError) as exception:
        payload = b""
        error = str(exception)[:500]
        timed_out = _is_timeout(exception)
    latency_ms = (time.monotonic() - started) * 1000
    completed_at = iso_utc()
    if payload:
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, dict):
                body = parsed
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
    return {
        "run_id": run_id,
        "policy": None,
        "seed": None,
        "request_id": request_id,
        "trace_request_id": event["request_id"],
        "function_id": event["function_id"],
        "scheduled_at": scheduled_at,
        "sent_at": sent_at,
        "completed_at": completed_at,
        "latency_ms": round(latency_ms, 3),
        "status_code": status_code,
        "timeout": timed_out,
        "error": error,
        "instance_id": body.get("instance_id"),
        "revision": body.get("revision"),
        "first_user_request": body.get("first_user_request"),
        "request_kind": event.get("request_kind", "user"),
        "scheduling_lag_ms": None,
        "init_ms": body.get("init_ms"),
        "service_ms": body.get("service_ms"),
    }


REQUIRED_METADATA = ("image_digest", "kubernetes_version", "knative_version", "hardware", "image_cache_state")
CACHE_STATES = ("cached", "uncached")
CONTROLLER_FILES = ("scheduler_manifest.json", "decisions.jsonl", "actuation.jsonl")


def validate_metadata(metadata: dict, policy: str) -> list[str]:
    """Return the problems that make a run's environment record insufficient for comparison."""
    problems = [f"missing metadata: {key}" for key in REQUIRED_METADATA
                if metadata.get(key) in (None, "", {}, [])]
    state = metadata.get("image_cache_state")
    if state not in (None, "") and state not in CACHE_STATES:
        problems.append(f"image_cache_state must be one of {', '.join(CACHE_STATES)}")
    initial = metadata.get("initial_replicas")
    if not isinstance(initial, dict) or not initial:
        problems.append("missing metadata: initial_replicas (function_id -> pod count before the run)")
    elif any(not isinstance(v, int) or isinstance(v, bool) or v < 0 for v in initial.values()):
        problems.append("initial_replicas values must be non-negative integers")
    elif policy != "fixed_minimum" and any(v != 0 for v in initial.values()):
        problems.append("reactive/predictive runs must start from zero replicas (reset state first)")
    elif policy == "fixed_minimum" and any(v < 1 for v in initial.values()):
        problems.append("fixed_minimum runs must start with at least one replica per function")
    return problems


def _copy_controller_artifacts(controller_dir: str | Path, output_dir: Path) -> dict:
    source = Path(controller_dir)
    copied = {}
    for name in CONTROLLER_FILES:
        if (source / name).is_file():
            shutil.copyfile(source / name, output_dir / name)
            copied[name] = sha256_file(output_dir / name)
    return copied


def replay(trace_path: str | Path, endpoints_path: str | Path, policy: str, run_id: str,
           output_root: str | Path, max_workers: int = 16, timeout_s: float = 30,
           policy_config_path: str | Path | None = None, metadata_path: str | Path | None = None,
           require_metadata: bool = False, controller_dir: str | Path | None = None) -> Path:
    if policy not in POLICIES:
        raise ValueError(f"policy must be one of {', '.join(POLICIES)}")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", run_id):
        raise ValueError("run_id must be 1-128 safe filename characters")
    if max_workers < 1 or not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError("max_workers and timeout_s must be positive")
    events = read_jsonl(trace_path)
    endpoints = _load_object(endpoints_path)
    if not events:
        raise ValueError("trace is empty")
    trace_manifest_path = Path(trace_path).with_name("trace_manifest.json")
    trace_manifest = _load_object(trace_manifest_path) if trace_manifest_path.exists() else {}
    policy_config = _load_object(policy_config_path) if policy_config_path else {}
    metadata = _load_object(metadata_path) if metadata_path else {}
    if require_metadata:
        problems = validate_metadata(metadata, policy)
        if policy == "predictive":
            if not controller_dir or not (Path(controller_dir) / "scheduler_manifest.json").is_file():
                problems.append("predictive runs need --controller-dir containing scheduler_manifest.json")
        if problems:
            raise ValueError("incomplete run metadata: " + "; ".join(problems))
    if controller_dir and not (Path(controller_dir) / "scheduler_manifest.json").is_file():
        raise ValueError(f"{controller_dir}: no scheduler_manifest.json")
    trace_request_ids = set()
    for event in events:
        if (not isinstance(event.get("scheduled_at_s"), (int, float))
                or not math.isfinite(event["scheduled_at_s"]) or event["scheduled_at_s"] < 0):
            raise ValueError("each trace event needs a non-negative scheduled_at_s")
        if not isinstance(event.get("request_id"), str) or not event["request_id"]:
            raise ValueError("each trace event needs a non-empty request_id")
        if event["request_id"] in trace_request_ids:
            raise ValueError(f"duplicate trace request_id: {event['request_id']}")
        trace_request_ids.add(event["request_id"])
        if event.get("request_kind", "user") not in ("user", "warmup"):
            raise ValueError("trace request_kind must be user or warmup")
        if not isinstance(event.get("function_id"), str) or event["function_id"] not in endpoints:
            raise ValueError(f"no endpoint configured for function {event.get('function_id')!r}")
        endpoint = endpoints[event["function_id"]]
        if not isinstance(endpoint, dict) or not isinstance(endpoint.get("url"), str):
            raise ValueError(f"endpoint for {event['function_id']} must contain a URL")
        parsed_url = urlsplit(endpoint["url"])
        if parsed_url.scheme not in ("http", "https") or not parsed_url.netloc:
            raise ValueError(f"endpoint URL for {event['function_id']} must be an absolute HTTP(S) URL")
        if not isinstance(endpoint.get("headers", {}), dict):
            raise ValueError(f"endpoint headers for {event['function_id']} must be a JSON object")
        if endpoint.get("host") is not None and not isinstance(endpoint["host"], str):
            raise ValueError(f"endpoint Host header for {event['function_id']} must be a string")
        if any(not isinstance(key, str) or not isinstance(value, str)
               for key, value in endpoint.get("headers", {}).items()):
            raise ValueError(f"endpoint headers for {event['function_id']} must contain strings")

    output_dir = Path(output_root) / run_id
    output_dir.mkdir(parents=True, exist_ok=False)
    saved_trace = output_dir / "trace.jsonl"
    shutil.copyfile(trace_path, saved_trace)
    trace_hash = sha256_file(saved_trace)
    if trace_manifest.get("trace_sha256") and trace_manifest["trace_sha256"] != trace_hash:
        raise ValueError("trace hash does not match trace_manifest.json")
    origin = time.monotonic()
    run_started = datetime.now(timezone.utc)
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(
                _send_one, event, endpoints[event["function_id"]], run_id,
                iso_utc(run_started + timedelta(seconds=event["scheduled_at_s"])),
                timeout_s, origin,
            )
            for event in events
        ]
        for future in concurrent.futures.as_completed(futures):
            record = future.result()
            record["policy"] = policy
            record["seed"] = trace_manifest.get("seed")
            results.append(record)
    results.sort(key=lambda record: (record["scheduled_at"], record["request_id"]))
    for record in results:
        record["scheduling_lag_ms"] = round(max(0.0, (
            parse_utc(record["sent_at"]) - parse_utc(record["scheduled_at"])).total_seconds() * 1000), 3)
    controller_files = _copy_controller_artifacts(controller_dir, output_dir) if controller_dir else {}
    measurements_path = output_dir / "client_measurements.jsonl"
    write_jsonl(measurements_path, results)

    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "policy": policy,
        "policy_configuration": policy_config,
        "policy_control_note": (
            "Configuration is descriptive. This harness sends requests only; deploy and verify "
            "the selected Knative policy separately."
        ),
        "seed": trace_manifest.get("seed"),
        "workload": trace_manifest,
        "trace_path": saved_trace.name,
        "trace_sha256": trace_hash,
        "endpoints": endpoints,
        "request_count": len(results),
        "started_at": iso_utc(run_started),
        "ended_at": iso_utc(),
        "code_revision": git_revision(),
        "metadata": metadata,
        "metadata_validated": bool(require_metadata),
        "controller_artifacts": controller_files,
        "replay": {"max_workers": max_workers, "timeout_seconds": timeout_s},
        "measurements_file": measurements_path.name,
        "measurements_sha256": sha256_file(measurements_path),
        "measurement_completeness": "All trace requests have a record; failures and timeouts are retained.",
    }
    write_json(output_dir / "run_manifest.json", manifest)
    return output_dir


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trace", required=True)
    parser.add_argument("--endpoints", required=True, help="JSON object: function_id -> {url, host?, headers?}")
    parser.add_argument("--policy", choices=POLICIES, required=True)
    parser.add_argument("--policy-config", help="JSON description of externally deployed policy settings")
    parser.add_argument("--metadata", help="JSON metadata: image digest, versions, hardware, cache state, initial replicas")
    parser.add_argument("--allow-incomplete-metadata", action="store_true",
                        help="skip the required-metadata and controller-artifact checks (not comparable runs)")
    parser.add_argument("--controller-dir", help="scheduler output dir (scheduler_manifest.json etc.); required for predictive")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", default="results")
    parser.add_argument("--max-workers", type=int, default=16)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args(argv)
    output = replay(args.trace, args.endpoints, args.policy, args.run_id, args.output,
                    args.max_workers, args.timeout, args.policy_config, args.metadata,
                    require_metadata=not args.allow_incomplete_metadata, controller_dir=args.controller_dir)
    print(f"Recorded measurements and manifest in {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

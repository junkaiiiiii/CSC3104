"""Summarize one or more evaluation runs and write comparison artifacts.

Usage: python -m evaluation.analyze --runs results/reactive-17 results/fixed-17
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

from .common import iso_utc, parse_utc, read_jsonl, sha256_file, write_json


def _quantile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(probability * len(ordered)) - 1)
    return round(ordered[index], 3)


def _resource_integrals(records: list[dict], start, end) -> dict:
    if not records or end <= start:
        return {
            "ready_replica_seconds": None,
            "provisioned_replica_seconds": None,
            "requested_cpu_core_seconds": None,
            "requested_memory_mib_seconds": None,
            "resource_observation": "not available",
        }
    events = []
    for record in records:
        if record.get("event") not in {
            "pod_seen", "pod_ready", "pod_unready", "pod_terminating", "pod_gone"
        } or not record.get("pod") or not record.get("observed_at"):
            continue
        events.append((parse_utc(record["observed_at"]), record))
    if not events:
        return {
            "ready_replica_seconds": None,
            "provisioned_replica_seconds": None,
            "requested_cpu_core_seconds": None,
            "requested_memory_mib_seconds": None,
            "resource_observation": "lifecycle file has no usable pod events",
        }
    events.sort(key=lambda item: item[0])
    pods = {}
    previous = start
    ready_seconds = provisioned_seconds = cpu_seconds = memory_seconds = 0.0

    def integrate(until):
        nonlocal ready_seconds, provisioned_seconds, cpu_seconds, memory_seconds
        left, right = max(previous, start), min(until, end)
        duration = max(0.0, (right - left).total_seconds())
        if duration == 0:
            return
        for pod in pods.values():
            if pod["active"]:
                provisioned_seconds += duration
                if pod["ready"]:
                    ready_seconds += duration
                if pod["cpu"] is not None:
                    cpu_seconds += duration * pod["cpu"] / 1000
                if pod["memory"] is not None:
                    memory_seconds += duration * pod["memory"]

    for at, record in events:
        integrate(at)
        pod = pods.setdefault(record["pod"], {
            "active": False, "ready": False, "cpu": None, "memory": None,
        })
        event = record["event"]
        if event == "pod_seen":
            pod["active"], pod["ready"] = True, False
        elif event == "pod_ready":
            pod["active"], pod["ready"] = True, True
        elif event in ("pod_unready", "pod_terminating"):
            pod["ready"] = False
        elif event == "pod_gone":
            pod["active"], pod["ready"] = False, False
        if record.get("cpu_request_m") is not None:
            pod["cpu"] = record["cpu_request_m"]
        if record.get("mem_request_mi") is not None:
            pod["memory"] = record["mem_request_mi"]
        previous = at
    integrate(end)
    return {
        "ready_replica_seconds": round(ready_seconds, 3),
        "provisioned_replica_seconds": round(provisioned_seconds, 3),
        "requested_cpu_core_seconds": round(cpu_seconds, 3),
        "requested_memory_mib_seconds": round(memory_seconds, 3),
        "resource_observation": (
            "Integrated from polled pod lifecycle intervals; CPU/memory are requested resources, not usage."
        ),
    }


def _forecast_metrics(actual_records: list[dict], forecast_records: list[dict]) -> dict:
    actual = {}
    for row in actual_records:
        count = row.get("invocation_count", row.get("request_count"))
        if row.get("complete", True) and isinstance(count, (int, float)):
            actual[(row.get("function_id"), row.get("bucket_start"))] = float(count)
    predictions = {}
    for row in forecast_records:
        if isinstance(row.get("predictions"), list):
            for prediction in row["predictions"]:
                count = prediction.get("invocation_count", prediction.get("forecast_request_count"))
                if isinstance(count, (int, float)):
                    predictions[(row.get("function_id"), prediction.get("bucket_start"))] = float(count)
        else:
            start = row.get("forecast_bucket_start", row.get("bucket_start"))
            count = row.get("forecast_request_count", row.get("invocation_count"))
            if start is not None and isinstance(count, (int, float)):
                predictions[(row.get("function_id"), start)] = float(count)
    errors = [predictions[key] - actual[key] for key in actual.keys() & predictions.keys()]
    return {
        "matched_buckets": len(errors),
        "mae": round(statistics.mean(abs(error) for error in errors), 4) if errors else None,
        "bias": round(statistics.mean(errors), 4) if errors else None,
        "note": "Only complete actual buckets with a matching forecast are scored; no MAPE is used.",
    }


def summarize_run(run_dir: str | Path) -> dict:
    run_dir = Path(run_dir)
    manifest_path = run_dir / "run_manifest.json"
    measurements_path = run_dir / "client_measurements.jsonl"
    if not manifest_path.is_file() or not measurements_path.is_file():
        raise FileNotFoundError(f"{run_dir}: expected run_manifest.json and client_measurements.jsonl")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    requests = read_jsonl(measurements_path)
    if manifest.get("request_count") != len(requests):
        raise ValueError(f"{run_dir}: manifest request_count does not match measurements")
    if not manifest.get("trace_sha256") or not manifest.get("measurements_sha256"):
        raise ValueError(f"{run_dir}: manifest is missing trace or measurement hashes")
    if sha256_file(measurements_path) != manifest["measurements_sha256"]:
        raise ValueError(f"{run_dir}: client measurement hash does not match manifest")
    saved_trace = run_dir / manifest.get("trace_path", "trace.jsonl")
    if saved_trace.is_file() and sha256_file(saved_trace) != manifest["trace_sha256"]:
        raise ValueError(f"{run_dir}: trace hash does not match manifest")
    request_ids = [row.get("request_id") for row in requests]
    if any(not isinstance(request_id, str) or not request_id for request_id in request_ids):
        raise ValueError(f"{run_dir}: client measurement has missing request_id")
    if len(set(request_ids)) != len(request_ids):
        raise ValueError(f"{run_dir}: client measurement contains duplicate request_id")

    statuses = [row.get("status_code") for row in requests]
    successes = sum(isinstance(status, int) and 200 <= status < 300 for status in statuses)
    http_errors = sum(isinstance(status, int) and not 200 <= status < 300 for status in statuses)
    timeouts = sum(row.get("timeout") is True for row in requests)
    latencies = [
        float(row["latency_ms"]) for row in requests
        if isinstance(row.get("latency_ms"), (int, float)) and math.isfinite(row["latency_ms"])
    ]
    first_user = [row for row in requests if isinstance(row.get("first_user_request"), bool)]
    exposed = [row for row in first_user if row["first_user_request"]]
    first_user_latencies = [
        float(row["latency_ms"]) for row in exposed
        if isinstance(row.get("latency_ms"), (int, float)) and math.isfinite(row["latency_ms"])
    ]
    if not requests:
        raise ValueError(f"{run_dir}: no client measurements")
    start_times = [parse_utc(row["scheduled_at"]) for row in requests if row.get("scheduled_at")]
    end_times = [parse_utc(row["completed_at"]) for row in requests if row.get("completed_at")]
    if not start_times or not end_times:
        raise ValueError(f"{run_dir}: client records lack scheduled_at or completed_at timestamps")
    start = parse_utc(manifest["started_at"]) if manifest.get("started_at") else min(start_times)
    end = parse_utc(manifest["ended_at"]) if manifest.get("ended_at") else max(end_times)
    if end <= start:
        raise ValueError(f"{run_dir}: manifest observation window is invalid")
    lifecycle_path = run_dir / "pod_lifecycle.jsonl"
    lifecycle = read_jsonl(lifecycle_path) if lifecycle_path.exists() else []
    actual_path = run_dir / "demand_buckets.jsonl"
    forecast_path = run_dir / "forecasts.jsonl"
    actual = read_jsonl(actual_path) if actual_path.exists() else []
    forecasts = read_jsonl(forecast_path) if forecast_path.exists() else []

    result = {
        "run_id": manifest.get("run_id", run_dir.name),
        "policy": manifest.get("policy", "unknown"),
        "seed": manifest.get("seed"),
        "trace_sha256": manifest["trace_sha256"],
        "workload": manifest.get("workload", {}).get("patterns"),
        "request_count": len(requests),
        "successful_responses": successes,
        "http_errors": http_errors,
        "timeouts": timeouts,
        "other_transport_errors": sum(status is None and not row.get("timeout") for status, row in zip(statuses, requests)),
        "success_rate": round(successes / len(requests), 4) if requests else None,
        "latency_sample_count": len(latencies),
        "latency_quantile_method": "nearest rank: sorted[ceil(p*n)-1]",
        "latency_ms": {
            "median": _quantile(latencies, 0.50),
            "p95": _quantile(latencies, 0.95),
            "p99": _quantile(latencies, 0.99),
        },
        "first_user_proxy": {
            "observed": len(first_user),
            "unknown": len(requests) - len(first_user),
            "count": len(exposed),
            "fraction_of_observed": round(len(exposed) / len(first_user), 4) if first_user else None,
            "latency_sample_count": len(first_user_latencies),
            "latency_median_ms": _quantile(first_user_latencies, 0.50),
            "interpretation": (
                "first_user_request is a per-process proxy, not verified cold-start exposure; "
                "it can be true after a warm-up."
            ),
        },
        "observation_window": {"start": iso_utc(start), "end": iso_utc(end)},
        "resource_integrals": _resource_integrals(lifecycle, start, end),
        "idle_replica_seconds": None,
        "idle_time_note": "Not measurable without per-replica in-flight request observations.",
        "forecast": _forecast_metrics(actual, forecasts) if actual and forecasts else {
            "matched_buckets": 0, "mae": None, "bias": None,
            "note": "Supply demand_buckets.jsonl and forecasts.jsonl to score forecasts.",
        },
    }
    return result


def _write_comparison(summaries: list[dict], output_path: Path) -> None:
    groups = defaultdict(list)
    for summary in summaries:
        groups[(summary["policy"], str(summary["workload"]))].append(summary)
    columns = [
        "policy", "workload", "runs", "requests_mean",
        "median_latency_ms_mean", "p95_latency_ms_mean", "p99_latency_ms_mean",
        "p95_latency_ms_sample_stddev", "success_rate_mean", "success_rate_sample_stddev",
        "timeouts_mean", "ready_replica_seconds_mean", "ready_replica_seconds_sample_stddev",
        "provisioned_replica_seconds_mean", "forecast_mae_mean",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for (policy, workload), rows in sorted(groups.items()):
            def values(path):
                result = []
                for row in rows:
                    value = row
                    for part in path.split("."):
                        value = value.get(part) if isinstance(value, dict) else None
                    if isinstance(value, (int, float)):
                        result.append(value)
                return result
            metrics = {
                "requests_mean": values("request_count"),
                "median_latency_ms_mean": values("latency_ms.median"),
                "p95_latency_ms_mean": values("latency_ms.p95"),
                "p99_latency_ms_mean": values("latency_ms.p99"),
                "p95_latency_ms_sample_stddev": values("latency_ms.p95"),
                "success_rate_mean": values("success_rate"),
                "success_rate_sample_stddev": values("success_rate"),
                "timeouts_mean": values("timeouts"),
                "ready_replica_seconds_mean": values("resource_integrals.ready_replica_seconds"),
                "ready_replica_seconds_sample_stddev": values("resource_integrals.ready_replica_seconds"),
                "provisioned_replica_seconds_mean": values("resource_integrals.provisioned_replica_seconds"),
                "forecast_mae_mean": values("forecast.mae"),
            }
            output = {"policy": policy, "workload": workload, "runs": len(rows)}
            for column, observed in metrics.items():
                if column.endswith("_sample_stddev"):
                    output[column] = round(statistics.stdev(observed), 4) if len(observed) > 1 else ""
                else:
                    output[column] = round(statistics.mean(observed), 4) if observed else ""
            writer.writerow(output)


def _write_report(summaries: list[dict], path: Path) -> None:
    lines = [
        "# Evaluation summary", "",
        "Latency quantiles use nearest rank. Runs are summarized individually before policy-level means; "
        "the CSV does not imply statistical significance.", "",
        "| Run | Policy | Seed | Requests | Success rate | Median ms | p95 ms | p99 ms | Ready replica-s | Forecast MAE |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        latency = row["latency_ms"]
        ready = row["resource_integrals"]["ready_replica_seconds"]
        lines.append(
            f"| {row['run_id']} | {row['policy']} | {row['seed']} | {row['request_count']} "
            f"| {row['success_rate']} | {latency['median']} | {latency['p95']} | {latency['p99']} "
            f"| {ready if ready is not None else 'N/A'} | {row['forecast']['mae'] if row['forecast']['mae'] is not None else 'N/A'} |"
        )
    lines.extend([
        "", "## Interpretation limits", "",
        "- First-user-per-process is a proxy, not a verified cold-start label.",
        "- Missing lifecycle observations remain unavailable; they are not treated as zero replicas.",
        "- CPU and memory integrals are based on pod resource requests, not measured utilization.",
        "- Idle replica time is unavailable without per-replica in-flight request measurements.",
        "- Timeouts and HTTP errors are retained and reported separately from latency quantiles.",
    ])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def analyze(run_dirs: list[str | Path], output_dir: str | Path) -> list[dict]:
    if not run_dirs:
        raise ValueError("at least one run directory is required")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries = [summarize_run(path) for path in run_dirs]
    traces_by_policy = defaultdict(dict)
    for row in summaries:
        traces_by_policy[row["policy"]].setdefault(row["seed"], set()).add(row["trace_sha256"])
    if len(traces_by_policy) > 1:
        policy_runs = list(traces_by_policy.values())
        seed_sets = [set(runs) for runs in policy_runs]
        if any(seeds != seed_sets[0] for seeds in seed_sets[1:]):
            raise ValueError("policies must have the same evaluation seeds for a fair comparison")
        if any(len(hashes) != 1 for runs in policy_runs for hashes in runs.values()):
            raise ValueError("a policy/seed group contains inconsistent trace hashes")
        for seed in seed_sets[0]:
            if len({next(iter(runs[seed])) for runs in policy_runs}) != 1:
                raise ValueError(f"policies did not replay the same trace for seed {seed!r}")
    write_json(output_dir / "summary.json", {
        "schema_version": 1,
        "generated_at": iso_utc(),
        "runs": summaries,
        "policy_aggregate_note": "See policy_comparison.csv; aggregate means do not replace per-run variability.",
    })
    _write_comparison(summaries, output_dir / "policy_comparison.csv")
    _write_report(summaries, output_dir / "report.md")
    return summaries


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", nargs="+", required=True, help="run directories to compare")
    parser.add_argument("--output", default="results/analysis")
    args = parser.parse_args(argv)
    summaries = analyze(args.runs, args.output)
    print(f"Analyzed {len(summaries)} runs; report: {Path(args.output) / 'report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

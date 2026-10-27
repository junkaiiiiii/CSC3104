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
from html import escape
from pathlib import Path

from .common import iso_utc, parse_utc, quantile as _quantile, read_jsonl, sha256_file, write_json
from .correlate import (IDLE_DEFINITION, application_init_summary, classify_exposure, exposure_summary,
                        idle_replica_seconds, scheduling_lag_summary)

MIN_REPETITIONS = 5



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


def _controller_overhead(path: Path) -> dict | None:
    """Controller cost from the scheduler manifest; None when no controller artifacts were saved."""
    if not path.is_file():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    return {
        "kube_api_calls": data.get("kube_api_calls"),
        "platform_reverts": data.get("platform_reverts"),
        "actuator": data.get("actuator"),
        "totals": data.get("totals"),
        "note": "Scheduler-side counts; controller CPU/memory use is not measured.",
    }


def summarize_run(run_dir: str | Path, require_telemetry: bool = False) -> dict:
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
    if require_telemetry and not lifecycle:
        raise ValueError(f"{run_dir}: pod_lifecycle.jsonl is missing or empty")
    if saved_trace.is_file():
        expected = {event["request_id"] for event in read_jsonl(saved_trace)}
        observed = {row.get("trace_request_id") for row in requests}
        if expected - observed:
            raise ValueError(f"{run_dir}: {len(expected - observed)} trace requests have no client record")
    controller_path = run_dir / "scheduler_manifest.json"
    if require_telemetry and manifest.get("policy") == "predictive" and not controller_path.is_file():
        raise ValueError(f"{run_dir}: predictive run has no scheduler_manifest.json")
    labels = classify_exposure(requests, lifecycle)
    concurrency = (manifest.get("metadata") or {}).get("container_concurrency", 1)
    if not isinstance(concurrency, (int, dict)) or isinstance(concurrency, bool) or concurrency == 0:
        concurrency = 1
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
        "scheduling_lag_ms": scheduling_lag_summary(requests),
        "cold_start_exposure": exposure_summary(requests, labels),
        "application_init": application_init_summary(requests),
        "observation_window": {"start": iso_utc(start), "end": iso_utc(end)},
        "resource_integrals": _resource_integrals(lifecycle, start, end),
        "idle_replica_seconds": idle_replica_seconds(requests, lifecycle, start, end, concurrency),
        "controller_overhead": _controller_overhead(controller_path),
        "environment": {
            key: (manifest.get("metadata") or {}).get(key)
            for key in ("image_digest", "kubernetes_version", "knative_version", "hardware",
                        "image_cache_state", "initial_replicas")
        },
        "metadata_validated": bool(manifest.get("metadata_validated")),
        "started_at": manifest.get("started_at"),
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
        "provisioned_replica_seconds_mean", "provisioned_replica_seconds_sample_stddev",
        "idle_replica_seconds_mean", "idle_replica_seconds_sample_stddev",
        "exposed_requests_mean", "scheduling_lag_p95_ms_mean", "kube_api_calls_mean", "forecast_mae_mean",
        "p95_latency_ms_min", "p95_latency_ms_max",
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
                "provisioned_replica_seconds_sample_stddev": values("resource_integrals.provisioned_replica_seconds"),
                "idle_replica_seconds_mean": values("idle_replica_seconds.total"),
                "idle_replica_seconds_sample_stddev": values("idle_replica_seconds.total"),
                "exposed_requests_mean": values("cold_start_exposure.exposed"),
                "scheduling_lag_p95_ms_mean": values("scheduling_lag_ms.p95"),
                "kube_api_calls_mean": values("controller_overhead.kube_api_calls"),
                "forecast_mae_mean": values("forecast.mae"),
                "p95_latency_ms_min": values("latency_ms.p95"),
                "p95_latency_ms_max": values("latency_ms.p95"),
            }
            output = {"policy": policy, "workload": workload, "runs": len(rows)}
            for column, observed in metrics.items():
                if column.endswith("_sample_stddev"):
                    output[column] = round(statistics.stdev(observed), 4) if len(observed) > 1 else ""
                elif column.endswith("_min"):
                    output[column] = min(observed) if observed else ""
                elif column.endswith("_max"):
                    output[column] = max(observed) if observed else ""
                else:
                    output[column] = round(statistics.mean(observed), 4) if observed else ""
            writer.writerow(output)


def _design_warnings(summaries: list[dict]) -> list[str]:
    warnings = []
    by_policy = defaultdict(list)
    for row in summaries:
        by_policy[row["policy"]].append(row)
    for policy, rows in sorted(by_policy.items()):
        if len(rows) < MIN_REPETITIONS:
            warnings.append(f"{policy}: {len(rows)} runs; at least {MIN_REPETITIONS} repetitions are needed to show variability")
    by_seed = defaultdict(list)
    for row in summaries:
        by_seed[row["seed"]].append(row)
    orders = {}
    for seed, rows in by_seed.items():
        timed = [r for r in rows if r.get("started_at")]
        if len(timed) == len(rows) and len({r["policy"] for r in rows}) > 1:
            orders[seed] = tuple(r["policy"] for r in sorted(timed, key=lambda r: r["started_at"]))
    if len(orders) > 1 and len(set(orders.values())) == 1:
        warnings.append("policy run order is identical for every seed; vary the order to separate policy from drift")
    elif len(orders) <= 1 and len(by_policy) > 1:
        warnings.append("run order cannot be checked for variation (need several seeds with all policies)")
    for key in ("image_digest", "kubernetes_version", "knative_version", "hardware", "image_cache_state"):
        values = {json.dumps(r["environment"].get(key), sort_keys=True) for r in summaries}
        if len(values) > 1:
            warnings.append(f"{key} differs between runs; baseline differences are not isolated")
    if any(not row["metadata_validated"] for row in summaries):
        warnings.append("some runs were recorded without required-metadata validation")
    for row in summaries:
        if row["resource_integrals"]["ready_replica_seconds"] is None:
            warnings.append(f"{row['run_id']}: no pod lifecycle data; exposure, idle and resource metrics are unavailable")
        if row["policy"] == "predictive" and row["controller_overhead"] is None:
            warnings.append(f"{row['run_id']}: predictive run has no controller artifacts")
    return warnings


def _svg_chart(title: str, unit: str, groups: dict[str, list[float]]) -> str:
    """Bar of the mean per policy with every individual run drawn as a dot."""
    width, height, left, bottom, top = 520, 320, 70, 50, 40
    peak = max((v for values in groups.values() for v in values), default=0) or 1
    plot_h = height - top - bottom
    slot = (width - left - 20) / max(1, len(groups))
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" font-family="sans-serif" font-size="12">',
             f'<text x="{width / 2}" y="22" text-anchor="middle" font-size="14">{escape(title)}</text>',
             f'<text x="12" y="{top + plot_h / 2}" transform="rotate(-90 12 {top + plot_h / 2})" text-anchor="middle">{escape(unit)}</text>',
             f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="black"/>',
             f'<line x1="{left}" y1="{top + plot_h}" x2="{width - 20}" y2="{top + plot_h}" stroke="black"/>',
             f'<text x="{left - 6}" y="{top + 4}" text-anchor="end">{peak:g}</text>',
             f'<text x="{left - 6}" y="{top + plot_h}" text-anchor="end">0</text>']
    for index, (name, values) in enumerate(sorted(groups.items())):
        x = left + slot * index + slot * 0.2
        bar_w = slot * 0.6
        label = f'<text x="{x + bar_w / 2}" y="{top + plot_h + 18}" text-anchor="middle">{escape(name)} (N={len(values)})</text>'
        if values:
            mean = statistics.mean(values)
            h = plot_h * mean / peak
            parts.append(f'<rect x="{x:.1f}" y="{top + plot_h - h:.1f}" width="{bar_w:.1f}" height="{h:.1f}" fill="#9ec5e8"/>')
            for dot in values:
                parts.append(f'<circle cx="{x + bar_w / 2:.1f}" cy="{top + plot_h - plot_h * dot / peak:.1f}" r="3" fill="#1f3b57"/>')
        else:
            parts.append(f'<text x="{x + bar_w / 2}" y="{top + plot_h / 2}" text-anchor="middle">N/A</text>')
        parts.append(label)
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


CHARTS = (
    ("p95_latency.svg", "p95 latency by policy", "ms", "latency_ms.p95"),
    ("exposed_requests.svg", "Cold-start exposed requests by policy", "requests", "cold_start_exposure.exposed"),
    ("provisioned_replica_seconds.svg", "Provisioned replica-seconds by policy", "replica-s",
     "resource_integrals.provisioned_replica_seconds"),
    ("idle_replica_seconds.svg", "Idle replica-seconds by policy", "replica-s", "idle_replica_seconds.total"),
)


def _write_charts(summaries: list[dict], directory: Path) -> list[str]:
    written = []
    for filename, title, unit, path in CHARTS:
        groups = defaultdict(list)
        for row in summaries:
            value = row
            for part in path.split("."):
                value = value.get(part) if isinstance(value, dict) else None
            groups[row["policy"]]
            if isinstance(value, (int, float)):
                groups[row["policy"]].append(value)
        (directory / filename).write_text(_svg_chart(title, unit, groups), encoding="utf-8")
        written.append(filename)
    return written


def _fmt(value) -> str:
    return "N/A" if value is None else str(value)


def _write_report(summaries: list[dict], path: Path, warnings: list[str] | None = None,
                  charts: list[str] | None = None) -> None:
    lines = [
        "# Evaluation summary", "",
        "Latency quantiles use nearest rank. Runs are summarized individually before policy-level means; "
        "the CSV does not imply statistical significance.", "",
        "| Run | Policy | Seed | Requests | Success rate | Median ms | p95 ms | p99 ms | Exposed (N) | Exposed p95 ms | Unknown | Ready replica-s | Idle replica-s | Lag p95 ms | Forecast MAE |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        latency = row["latency_ms"]
        exposure = row["cold_start_exposure"]
        idle = row["idle_replica_seconds"]
        exposed_p95 = exposure["exposed_latency_ms"]["p95"]
        lines.append(
            f"| {row['run_id']} | {row['policy']} | {row['seed']} | {row['request_count']} "
            f"| {row['success_rate']} | {latency['median']} | {latency['p95']} | {latency['p99']} "
            f"| {exposure['exposed']} | {_fmt(exposed_p95)} | {exposure['unknown']} "
            f"| {_fmt(row['resource_integrals']['ready_replica_seconds'])} "
            f"| {_fmt(idle['total'] if idle else None)} | {_fmt(row['scheduling_lag_ms']['p95'])} "
            f"| {_fmt(row['forecast']['mae'])} |"
        )
    if warnings:
        lines.extend(["", "## Warnings", ""] + [f"- {text}" for text in warnings])
    if charts:
        lines.extend(["", "## Charts (regenerated from raw run data on every analysis)", ""] + [f"![{name}]({name})" for name in charts])
    lines.extend([
        "", "## Interpretation limits", "",
        "- `first_user_proxy` is per-process and can be true after a warm-up; it is not cold-start exposure.",
        "- `cold_start_exposure` compares each request's send time with Ready pods observed for its function; "
        "unknown labels are kept, and exposed latency is N/A (not 0) when no request was exposed.",
        "- Application init is the function's own reported `init_ms`, reported separately from platform delay.",
        f"- Idle time: {IDLE_DEFINITION}",
        "- Missing lifecycle observations remain unavailable; they are not treated as zero replicas.",
        "- CPU and memory integrals are based on pod resource requests, not measured utilization.",
        "- Controller overhead is scheduler API call and revert counts only; controller CPU is not measured.",
        "- Timeouts and HTTP errors are retained and reported separately from latency quantiles.",
    ])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def analyze(run_dirs: list[str | Path], output_dir: str | Path, require_telemetry: bool = False) -> list[dict]:
    if not run_dirs:
        raise ValueError("at least one run directory is required")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries = [summarize_run(path, require_telemetry) for path in run_dirs]
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
    warnings = _design_warnings(summaries)
    write_json(output_dir / "summary.json", {
        "schema_version": 2,
        "generated_at": iso_utc(),
        "runs": summaries,
        "warnings": warnings,
        "policy_aggregate_note": "See policy_comparison.csv; aggregate means do not replace per-run variability.",
    })
    _write_comparison(summaries, output_dir / "policy_comparison.csv")
    charts = _write_charts(summaries, output_dir)
    _write_report(summaries, output_dir / "report.md", warnings, charts)
    return summaries


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", nargs="+", required=True, help="run directories to compare")
    parser.add_argument("--output", default="results/analysis")
    parser.add_argument("--require-telemetry", action="store_true",
                        help="reject runs without pod_lifecycle.jsonl (and scheduler artifacts for predictive)")
    args = parser.parse_args(argv)
    summaries = analyze(args.runs, args.output, args.require_telemetry)
    print(f"Analyzed {len(summaries)} runs; report: {Path(args.output) / 'report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

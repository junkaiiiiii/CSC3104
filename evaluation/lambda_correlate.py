"""Join Lambda client measurements to CloudWatch REPORT records by aws_request_id.

Subcommands:
  fetch      download REPORT lines for a time window into a JSONL file (needs AWS credentials)
  correlate  join a replayed lambda run to saved REPORT exports and summarise usage

HTTP round-trip latency, execution duration, initialisation duration and billed usage are
reported separately. Money is only estimated when a dated price source is supplied.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

from .common import iso_utc, quantile, read_jsonl, sha256_file, write_json, write_jsonl

REPORT_ID = re.compile(r"REPORT RequestId:\s*([0-9A-Za-z-]+)")
FIELDS = {
    "duration_ms": re.compile(r"(?<!Billed )Duration:\s*([\d.]+) ms"),
    "billed_duration_ms": re.compile(r"Billed Duration:\s*(\d+) ms"),
    "memory_size_mb": re.compile(r"Memory Size:\s*(\d+) MB"),
    "max_memory_used_mb": re.compile(r"Max Memory Used:\s*(\d+) MB"),
    "init_duration_ms": re.compile(r"Init Duration:\s*([\d.]+) ms"),
}


def parse_report(message: str) -> dict | None:
    found = REPORT_ID.search(message)
    if not found:
        return None
    record = {"aws_request_id": found.group(1)}
    for name, pattern in FIELDS.items():
        match = pattern.search(message)
        record[name] = float(match.group(1)) if match else None
    return record


def _messages(path: Path):
    text = path.read_text(encoding="utf-8")
    try:
        whole = json.loads(text)
    except json.JSONDecodeError:
        whole = None
    if isinstance(whole, dict) and isinstance(whole.get("events"), list):
        for event in whole["events"]:
            yield event.get("message", "")
        return
    if isinstance(whole, list):
        for event in whole:
            yield event.get("message", "") if isinstance(event, dict) else str(event)
        return
    for line in text.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            yield line
            continue
        yield (value.get("message") or value.get("@message") or "") if isinstance(value, dict) else line


def load_reports(paths) -> list[dict]:
    reports = []
    for path in paths:
        for message in _messages(Path(path)):
            report = parse_report(message)
            if report:
                reports.append(report)
    return reports


def _stats(values: list[float]) -> dict:
    values = [v for v in values if v is not None]
    return {"n": len(values), "mean": round(sum(values) / len(values), 3) if values else None,
            "p50": quantile(values, 0.5), "p95": quantile(values, 0.95), "p99": quantile(values, 0.99),
            "max": round(max(values), 3) if values else None}


def correlate(run_dir: str | Path, report_paths, price: dict | None = None) -> dict:
    run_dir = Path(run_dir)
    manifest = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
    if manifest.get("policy") != "lambda":
        raise ValueError("run_manifest.json is not a lambda run")
    clients = read_jsonl(run_dir / manifest["measurements_file"])
    reports = load_reports(report_paths)
    by_id: dict[str, list[dict]] = {}
    for report in reports:
        by_id.setdefault(report["aws_request_id"], []).append(report)
    duplicates = {key for key, rows in by_id.items() if len(rows) > 1}

    joined, counts = [], Counter()
    client_ids = Counter(c["aws_request_id"] for c in clients if c.get("aws_request_id"))
    for client in clients:
        aws_id = client.get("aws_request_id")
        record = dict(client)
        if not aws_id:
            counts["no_aws_request_id"] += 1
            record["join_status"] = "no_aws_request_id"
        elif client_ids[aws_id] > 1 or aws_id in duplicates:
            counts["ambiguous"] += 1
            record["join_status"] = "ambiguous"
        elif aws_id in by_id:
            counts["matched"] += 1
            record["join_status"] = "matched"
            record.update({k: v for k, v in by_id[aws_id][0].items() if k != "aws_request_id"})
        else:
            counts["unmatched"] += 1
            record["join_status"] = "unmatched"
        joined.append(record)
    orphans = sorted(set(by_id) - set(client_ids))
    matched = [r for r in joined if r["join_status"] == "matched"]

    billed_gb_s = sum(r["billed_duration_ms"] / 1000 * r["memory_size_mb"] / 1024
                      for r in matched if r.get("billed_duration_ms") is not None and r.get("memory_size_mb"))
    inits = [r for r in matched if r.get("init_duration_ms") is not None]
    summary = {
        "schema_version": 1,
        "run_id": manifest["run_id"],
        "client_requests": len(clients),
        "report_records": len(reports),
        "join_counts": {key: counts.get(key, 0) for key in
                        ("matched", "unmatched", "ambiguous", "no_aws_request_id")},
        "reports_without_client_request": len(orphans),
        "http_round_trip_ms": _stats([c["latency_ms"] for c in clients if c.get("status_code") == 200]),
        "execution_duration_ms": _stats([r.get("duration_ms") for r in matched]),
        "initialisation": {"invocations_with_init_duration": len(inits),
                           "init_duration_ms": _stats([r["init_duration_ms"] for r in inits])},
        "billed": {
            "billed_duration_ms_total": sum(r["billed_duration_ms"] for r in matched
                                            if r.get("billed_duration_ms") is not None),
            "billed_gb_seconds": round(billed_gb_s, 6),
            "invocations": len(matched),
            "max_memory_used_mb": _stats([r.get("max_memory_used_mb") for r in matched]),
        },
        "by_function": {},
        "report_files": {str(Path(p)): sha256_file(p) for p in report_paths},
        "generated_at": iso_utc(),
        "limits": [
            "Only matched requests contribute to usage; unmatched, ambiguous and unidentified requests are listed in counts.",
            "HTTP round-trip includes the network path from this client to AWS; it is not comparable to localhost ingress.",
            "Billed GB-seconds use the REPORT memory size; they are not Knative replica-seconds.",
        ],
    }
    for function_id in sorted({r["function_id"] for r in joined}):
        rows = [r for r in matched if r["function_id"] == function_id]
        summary["by_function"][function_id] = {
            "matched": len(rows),
            "execution_duration_ms": _stats([r.get("duration_ms") for r in rows]),
            "init_duration_ms": _stats([r["init_duration_ms"] for r in rows
                                        if r.get("init_duration_ms") is not None]),
        }
    if price:
        requests = summary["billed"]["invocations"]
        summary["estimated_cost_usd"] = {
            "compute": round(billed_gb_s * price["per_gb_second"], 8),
            "requests": round(requests / 1_000_000 * price["per_million_requests"], 8),
            "price_source": price["source"], "price_date": price["date"],
            "note": "Estimate for matched requests only; free tier and data transfer excluded.",
        }
    write_jsonl(run_dir / "lambda_joined.jsonl", joined)
    write_json(run_dir / "lambda_correlation.json", summary)
    return summary


def fetch(log_groups: dict, start_ms: int, end_ms: int, output: str, region: str, profile: str | None) -> int:
    import boto3
    client = boto3.Session(profile_name=profile, region_name=region).client("logs")
    target = Path(output)
    if target.exists():
        raise FileExistsError("use a new output filename")
    records = []
    for function_id, group in log_groups.items():
        pages = client.get_paginator("filter_log_events").paginate(
            logGroupName=group, startTime=start_ms, endTime=end_ms, filterPattern='"REPORT RequestId"')
        for page in pages:
            for event in page["events"]:
                records.append({"function_id": function_id, "log_group": group,
                                "timestamp": event["timestamp"], "message": event["message"]})
    write_jsonl(target, records)
    return len(records)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--log-group", action="append", required=True, metavar="FUNCTION=/aws/lambda/NAME")
    f.add_argument("--start", required=True, help="UTC start, e.g. 2026-11-13T10:00:00Z (use run started_at)")
    f.add_argument("--end", required=True, help="UTC end; allow a few minutes after the run for log delivery")
    f.add_argument("--region", required=True)
    f.add_argument("--profile")
    f.add_argument("--output", required=True)
    c = sub.add_parser("correlate")
    c.add_argument("--run-dir", required=True)
    c.add_argument("--reports", nargs="+", required=True)
    c.add_argument("--price-per-gb-second", type=float)
    c.add_argument("--price-per-million-requests", type=float)
    c.add_argument("--price-source")
    c.add_argument("--price-date")
    args = parser.parse_args(argv)
    if args.command == "fetch":
        from .common import parse_utc
        groups = dict(item.split("=", 1) for item in args.log_group)
        count = fetch(groups, int(parse_utc(args.start).timestamp() * 1000),
                      int(parse_utc(args.end).timestamp() * 1000), args.output, args.region, args.profile)
        print(f"Saved {count} REPORT records to {args.output}")
        return 0
    price_args = (args.price_per_gb_second, args.price_per_million_requests, args.price_source, args.price_date)
    if any(v is not None for v in price_args) and any(v is None for v in price_args):
        parser.error("cost estimation needs all of --price-per-gb-second, --price-per-million-requests, "
                     "--price-source and --price-date")
    price = None
    if args.price_source:
        price = {"per_gb_second": args.price_per_gb_second,
                 "per_million_requests": args.price_per_million_requests,
                 "source": args.price_source, "date": args.price_date}
    summary = correlate(args.run_dir, args.reports, price)
    print(json.dumps({"join_counts": summary["join_counts"],
                      "reports_without_client_request": summary["reports_without_client_request"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Prepare a seeded long-idle trace; never contacts the cluster or reads forecasts.

The period is a declared experimental parameter, not inferred from held-out traffic.
Only the client reads trace.jsonl. Offline buckets are synthetic fixtures, never
substitutes for the live collector's demand_buckets.jsonl.
"""
import argparse
import hashlib
import json
import random
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

PERIOD = 180
INTERVAL = 5
DURATION = 1080
PREFIX = 540
FUNCTIONS = ("periodic", "steady", "sparse")
MODEL_CONFIG = {
    "periodic": {"model": "seasonal", "season_buckets": 36, "season_cycles": 3},
    "steady": {"model": "ewma", "alpha": 0.3},
    "sparse": {"model": "sma", "window_size": 3},
}


def build(seed):
    # Separate streams keep changes in a control from changing periodic arrivals.
    rng = {fn: random.Random(f"long-gap-v1:{seed}:{fn}") for fn in FUNCTIONS}
    rows = []

    def add(fn, at):
        rows.append({"scheduled_at_s": round(at, 6), "function_id": fn,
                     "request_kind": "user", "workload_pattern": fn})

    # Six 10-second burst windows, with strictly no background periodic demand.
    for start in range(20, DURATION, PERIOD):
        for _ in range(rng["periodic"].randint(16, 24)):
            add("periodic", start + rng["periodic"].uniform(0.1, 9.9))

    # A steady control has one jittered arrival every two seconds.
    for at in range(0, DURATION, 2):
        add("steady", at + rng["steady"].uniform(0.1, 1.9))

    # Unannounced irregular bursts: no periodic template is supplied to SMA.
    at = rng["sparse"].uniform(35, 85)
    while at < DURATION - 10:
        for _ in range(rng["sparse"].randint(3, 8)):
            add("sparse", at + rng["sparse"].uniform(0.1, 4.9))
        at += rng["sparse"].uniform(95, 235)

    rows.sort(key=lambda r: (r["scheduled_at_s"], r["function_id"]))
    for index, row in enumerate(rows, 1):
        row["request_id"] = f"longgap-{seed}-{index:06d}"
    return rows


def offline_buckets(rows):
    origin = datetime(2026, 1, 1, tzinfo=timezone.utc)
    counts = Counter((r["function_id"], int(r["scheduled_at_s"] // INTERVAL))
                     for r in rows)
    for index in range(DURATION // INTERVAL):
        for fn in FUNCTIONS:
            yield {"function_id": fn,
                   "bucket_start": (origin + timedelta(seconds=index * INTERVAL))
                       .isoformat().replace("+00:00", "Z"),
                   "interval_seconds": INTERVAL,
                   "invocation_count": counts[fn, index], "complete": True}


def prepare(output, seed):
    rows = build(seed)
    output = Path(output)
    # Entire directory must be new: never partially overwrite an experiment.
    output.mkdir(parents=True, exist_ok=False)
    payload = "".join(json.dumps(r, sort_keys=True, separators=(",", ":")) + "\n"
                      for r in rows).encode()
    (output / "trace.jsonl").write_bytes(payload)
    fixture = "".join(json.dumps(r) + "\n" for r in offline_buckets(rows))
    (output / "offline_demand_buckets.jsonl").write_text(fixture)
    counts = dict(Counter(r["function_id"] for r in rows))
    manifest = {
        "schema_version": 1, "seed": seed, "duration_seconds": DURATION,
        "functions": list(FUNCTIONS), "request_count": len(rows),
        "requests_per_function": counts,
        "trace_file": "trace.jsonl",
        "trace_sha256": hashlib.sha256(payload).hexdigest(),
        "generator": "scripts/prepare_long_gap.py:v1",
        "generator_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "period_seconds": PERIOD, "burst_window_seconds": 10,
        "periodic_burst_starts_s": list(range(20, DURATION, PERIOD)),
        "training_prefix_seconds": PREFIX,
        "patterns": {
            "periodic": "16-24 requests per 10-second window; 180-second cycle; no background",
            "steady": "one jittered request every two seconds",
            "sparse": "irregular 3-8 request bursts, 95-235 seconds apart",
        },
        "limitations": [
            "Known synthetic period: model is configured, not discovering the period.",
            "Run boundaries are not aligned to collector UTC buckets; verify live phase effects.",
            "Idle gaps permit but do not guarantee scale-to-zero; verify lifecycle evidence.",
            "Training prefix remains in full-run latency/resource reports.",
            "No claim of generalisation to arbitrary or real production workloads.",
        ],
        "predictor_note": "Only live historical collector output may feed the online predictor.",
        "offline_fixture_note": "offline_demand_buckets.jsonl uses simulated timestamps, complete coverage and zero transport delay; never use as live input.",
    }
    for name, value in (("trace_manifest.json", manifest),
                        ("model_config.json", MODEL_CONFIG)):
        (output / name).write_text(json.dumps(value, indent=2) + "\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = prepare(args.output, args.seed)
    print(f"Generated {result['request_count']} requests: {result['requests_per_function']}")
    print(f"Duration: {DURATION}s; learning prefix: {PREFIX}s; period: {PERIOD}s")
    print("Trace SHA256:", result["trace_sha256"])
    print("Prepared files only. No requests sent and no cluster changes made.")


if __name__ == "__main__":
    main()

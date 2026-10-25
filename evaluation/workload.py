"""Seeded synthetic workload trace generator.

Usage: python -m evaluation.workload --seed 17 --duration 180 --output results/traces/seed-17
"""
from __future__ import annotations

import argparse
import math
import random
from pathlib import Path

from .common import sha256_file, write_json, write_jsonl

FUNCTIONS = ("periodic", "steady", "sparse")
TRACE_FILE = "trace.jsonl"


def _rate(function_id: str, elapsed: float, duration: float, rng: random.Random) -> float:
    progress = elapsed / duration
    if function_id == "periodic":
        phase = elapsed % 30
        return 1.2 if phase < 7 else 0.04
    if function_id == "steady":
        return 0.25 + 0.55 * progress
    if progress >= 0.9:
        # Held-out abrupt burst: keep it out of any predictor inputs during replay.
        return 2.0
    if rng.random() < 0.025:
        return 0.8
    return 0.015


def generate_trace(seed: int, duration_s: float = 180, max_requests: int = 100_000) -> list[dict]:
    """Generate a deterministic, time-ordered trace with three distinct patterns."""
    if duration_s <= 0 or not math.isfinite(duration_s):
        raise ValueError("duration_s must be a finite positive number")
    if max_requests < 1:
        raise ValueError("max_requests must be positive")

    rng = random.Random(seed)
    trace = []
    for function_id in FUNCTIONS:
        elapsed = 0.0
        sequence = 0
        while elapsed < duration_s:
            rate = _rate(function_id, elapsed, duration_s, rng)
            next_boundary = duration_s
            if function_id == "periodic":
                phase = elapsed % 30
                next_boundary = min(next_boundary, elapsed + (7 - phase if phase < 7 else 30 - phase))
            elif function_id == "sparse" and elapsed < duration_s * 0.9:
                next_boundary = min(next_boundary, duration_s * 0.9)
            arrival = elapsed + rng.expovariate(rate)
            if arrival >= next_boundary and next_boundary < duration_s:
                elapsed = next_boundary
                continue
            elapsed = arrival
            if elapsed >= duration_s:
                break
            sequence += 1
            trace.append({
                "scheduled_at_s": round(elapsed, 6),
                "function_id": function_id,
                "request_id": f"seed-{seed}-{function_id}-{sequence:06d}",
                "request_kind": "user",
                "workload_pattern": function_id,
            })
            if len(trace) > max_requests:
                raise ValueError(f"generated trace exceeds max_requests={max_requests}")
        if sequence == 0:
            sequence = 1
            trace.append({
                "scheduled_at_s": round(duration_s / 2, 6),
                "function_id": function_id,
                "request_id": f"seed-{seed}-{function_id}-{sequence:06d}",
                "request_kind": "user",
                "workload_pattern": function_id,
            })
        if function_id == "sparse" and not any(
            row["function_id"] == function_id and row["scheduled_at_s"] >= duration_s * 0.9
            for row in trace
        ):
            sequence += 1
            trace.append({
                "scheduled_at_s": round(duration_s * 0.9, 6),
                "function_id": function_id,
                "request_id": f"seed-{seed}-{function_id}-{sequence:06d}",
                "request_kind": "user",
                "workload_pattern": function_id,
            })
    if len(trace) > max_requests:
        raise ValueError(f"generated trace exceeds max_requests={max_requests}")
    trace.sort(key=lambda event: (event["scheduled_at_s"], event["function_id"], event["request_id"]))
    return trace


def generate(output: str | Path, seed: int, duration_s: float) -> dict:
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    trace_path = output / TRACE_FILE
    if trace_path.exists():
        raise FileExistsError(f"refusing to overwrite existing trace: {trace_path}")
    events = generate_trace(seed, duration_s)
    write_jsonl(trace_path, events)
    counts = {function_id: sum(e["function_id"] == function_id for e in events) for function_id in FUNCTIONS}
    manifest = {
        "schema_version": 1,
        "seed": seed,
        "duration_seconds": duration_s,
        "functions": list(FUNCTIONS),
        "request_count": len(events),
        "requests_per_function": counts,
        "patterns": {
            "periodic": "30-second cycle; 7-second high-rate phase followed by low background rate",
            "steady": "linearly increasing arrival rate",
            "sparse": "irregular low-rate traffic with random bursts and a held-out abrupt burst in the final 10%",
        },
        "trace_sha256": sha256_file(trace_path),
        "trace_file": TRACE_FILE,
        "predictor_note": "The full trace is a client input. Do not expose future trace rows to an online predictor.",
    }
    write_json(output / "trace_manifest.json", manifest)
    return manifest


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--duration", type=float, default=180, help="trace length in seconds")
    parser.add_argument("--output", required=True, help="new trace output directory")
    args = parser.parse_args(argv)
    manifest = generate(args.output, args.seed, args.duration)
    print(f"Generated {manifest['request_count']} requests in {args.output}/trace.jsonl")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

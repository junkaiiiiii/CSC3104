"""Seeded SYNTHETIC demand buckets for building and tuning the forecaster.

Not measured data: generated from the rules below, in exactly the demand-bucket
contract (docs/04-contracts.md), so the predictor can be developed before long
cluster runs exist. Use different seeds for tuning and final evaluation.

  python3 -m tools.synthetic_buckets --hours 6 --seed 101 --out data/synthetic/tuning_seed101

Patterns (30s buckets):
  periodic  bursts of 4 buckets (mean 14 requests) every 20 buckets, else mean 0.3
  steady    mean 4 with a slow +/-2 swing over one hour; one unseen abrupt burst late in the trace
  sparse    mean 0.1, with rare irregular 1-2 bucket spikes (mean 5)
Outages: a few windows where every function is incomplete=false; their counts are a
random fraction of the true count (a partial observation), never a clean zero.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
from datetime import datetime, timedelta, timezone

from collector.schema import format_ts, parse_ts

FUNCTIONS = ("periodic", "steady", "sparse")


def poisson(rng: random.Random, mean: float) -> int:
    if mean <= 0:
        return 0
    limit, k, p = math.exp(-mean), 0, 1.0
    while True:
        k += 1
        p *= rng.random()
        if p <= limit:
            return k - 1


def rates(function_id: str, n: int, rng: random.Random):
    """Mean requests per bucket for each bucket index, plus any unseen-burst info."""
    burst = None
    if function_id == "periodic":
        out = [14.0 if (i - 3) % 20 < 4 and i >= 3 else 0.3 for i in range(n)]
    elif function_id == "steady":
        out = [max(0.5, 4 + 2 * math.sin(2 * math.pi * i / 120)) for i in range(n)]
        start = int(n * 0.8)
        for i in range(start, min(n, start + 6)):
            out[i] *= 6
        burst = {"function_id": "steady", "first_bucket_index": start, "buckets": 6, "multiplier": 6}
    else:
        out, i = [0.1] * n, 0
        while i < n:
            if rng.random() < 0.02:
                for j in range(i, min(n, i + rng.choice((1, 2)))):
                    out[j] = 5.0
                i += 2
            i += 1
    return out, burst


def generate(seed: int, hours: float, start: datetime, interval_s: int = 30, functions=FUNCTIONS):
    n = int(hours * 3600 / interval_s)
    step = timedelta(seconds=interval_s)
    gap_rng = random.Random("%s:gaps" % seed)
    outages = set()
    for _ in range(max(1, n // 300)):
        first, length = gap_rng.randrange(n), gap_rng.choice((2, 3, 4))
        outages.update(range(first, min(n, first + length)))
    records, unseen = [], None
    for fid in functions:
        rng = random.Random("%s:%s" % (seed, fid))
        means, burst = rates(fid, n, rng)
        unseen = burst or unseen
        for i in range(n):
            count = poisson(rng, means[i])
            complete = i not in outages
            if not complete:
                count = int(count * gap_rng.random())  # partial observation
            records.append({"function_id": fid, "bucket_start": format_ts(start + i * step),
                            "interval_seconds": interval_s, "invocation_count": count, "complete": complete})
    records.sort(key=lambda r: (r["bucket_start"], r["function_id"]))
    manifest = {"synthetic": True, "seed": seed, "start": format_ts(start), "hours": hours,
                "interval_seconds": interval_s, "buckets_per_function": n, "functions": list(functions),
                "unseen_burst": unseen, "outage_bucket_starts": [format_ts(start + i * step) for i in sorted(outages)],
                "note": "Generated, not measured. Counts in incomplete buckets are partial observations."}
    return records, manifest


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--hours", type=float, default=6)
    parser.add_argument("--start", default="2026-10-01T00:00:00Z", help="must be a 30s boundary")
    parser.add_argument("--out", required=True, help="output directory")
    args = parser.parse_args(argv)
    start = parse_ts(args.start)
    if int(start.timestamp()) % 30:
        parser.error("--start must be on a 30-second boundary")
    records, manifest = generate(args.seed, args.hours, start)
    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, "demand_buckets.jsonl"), "w") as handle:
        for record in records:
            handle.write(json.dumps(record, separators=(",", ":")) + "\n")
    with open(os.path.join(args.out, "synthetic_manifest.json"), "w") as handle:
        json.dump(manifest, handle, indent=2)
        handle.write("\n")
    print("wrote %d buckets to %s" % (len(records), args.out), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

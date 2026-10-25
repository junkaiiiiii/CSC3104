"""CLI: python -m collector.main --run-id r1 --functions periodic,steady < events.jsonl"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

from .collector import RAW, Collector
from .sources import iter_lines


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ"))
    parser.add_argument("--functions", required=True, help="comma-separated function_id list")
    parser.add_argument("--input", default="-", help="JSONL file of events, or - for stdin")
    parser.add_argument("--out", default="data", help="output root; a run directory is created inside")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--grace", type=float, default=10)
    parser.add_argument("--clock", choices=("event", "wall"), help="default: event for files, wall for stdin")
    args = parser.parse_args(argv)

    clock = args.clock or ("wall" if args.input == "-" else "event")
    run_dir = os.path.join(args.out, args.run_id)
    if os.path.exists(os.path.join(run_dir, RAW)):
        parser.error("run directory %s already has data; choose a new --run-id" % run_dir)

    collector = Collector(run_dir, args.functions.split(","), args.run_id,
                          args.interval, args.grace, clock=clock,
                          source="stdin" if args.input == "-" else args.input)
    stream = sys.stdin if args.input == "-" else open(args.input, encoding="utf-8")
    try:
        for line in iter_lines(stream, idle_tick_s=1.0 if clock == "wall" else None):
            if line is None:
                collector.tick()
            else:
                collector.process_line(line)
    except KeyboardInterrupt:
        pass
    ok = collector.close()
    totals = collector.totals
    print("run %s: %d lines, %d counted, %d warmup, %d duplicate, %d late, %d quarantined, "
          "%d buckets (%d incomplete); conservation %s" % (
              args.run_id, totals["lines"], totals["counted"], totals["warmup"],
              totals["duplicate"] + totals["conflicting_duplicate"], totals["late"],
              totals["quarantined"], totals["buckets"], totals["incomplete_buckets"],
              "OK" if ok else "FAILED"), file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

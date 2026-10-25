"""Write hand-made forecasts for integration testing (NOT a predictor; see predictor/, CORE-07).

Appends one line per function every --every seconds in the predictor's output format
(predictor/README.md), saying "expect COUNT requests". Stop it with Ctrl-C: the last
forecast expires within about a minute, the scheduler falls back and releases.

    python tools/fake_forecast.py --out data/fake.jsonl --functions periodic --count 5
"""
import argparse
import json
import time
from datetime import datetime, timedelta, timezone


def ts(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", required=True)
    p.add_argument("--functions", default="periodic")
    p.add_argument("--count", type=float, default=5)
    p.add_argument("--every", type=float, default=15)
    p.add_argument("--interval", type=int, default=30)
    p.add_argument("--delay", type=float, default=10,
                   help="must match the scheduler's --forecast-delay")
    p.add_argument("--once", action="store_true")
    args = p.parse_args()
    try:
        while True:
            now = datetime.now(timezone.utc).replace(microsecond=0)
            # Newest bucket whose forecast the scheduler treats as already available.
            ready = now - timedelta(seconds=args.delay)
            bucket = ready - timedelta(seconds=ready.timestamp() % args.interval)
            previous = bucket - timedelta(seconds=args.interval)
            with open(args.out, "a", encoding="utf-8") as fh:
                for fn in args.functions.split(","):
                    fh.write(json.dumps({
                        "function_id": fn, "forecast_bucket_start": ts(bucket),
                        "forecast_invocation_count": args.count, "window_size": 1,
                        "input_buckets_used": [{"bucket_start": ts(previous),
                                                "invocation_count": args.count}]}) + "\n")
            print("wrote forecasts for bucket %s" % ts(bucket), flush=True)
            if args.once:
                return
            time.sleep(args.every)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

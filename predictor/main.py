import argparse
import json
import math
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone


from .models import BasePredictor, SMAPredictor, make_predictor


def live_forecast(prediction, interval, generated_at):
    """Use the shared contract and actual publication time, never inferred past time.

    Legacy SMA calls hold the count constant over two buckets. Models can supply
    distinct per-horizon counts; none changes the actual publication timestamp.
    """
    start = SMAPredictor._parse_iso(prediction["forecast_bucket_start"])
    width = timedelta(seconds=interval)
    counts = prediction.get("horizon_counts", [prediction["forecast_invocation_count"]] * 2)
    end = start + len(counts) * width
    if generated_at < start or generated_at >= end:
        return None  # historical backlogs must not become live forecasts
    stamp = lambda value: value.isoformat().replace("+00:00", "Z")
    return {
        **prediction,
        "generated_at": stamp(generated_at), "history_end": stamp(start),
        "interval_seconds": interval, "model": prediction.get("model", f"sma-w{prediction['window_size']}"),
        "predictions": [
            {"bucket_start": stamp(start + i * width),
             "invocation_count": count}
            for i, count in enumerate(counts)
        ],
        "valid_until": stamp(end),
    }


def input_lines(path, follow=False, poll_interval=0.5):
    while not path.exists():
        if not follow:
            raise FileNotFoundError(path)
        time.sleep(poll_interval)
    with path.open(encoding="utf-8") as stream:
        pending = ""
        while True:
            line = stream.readline()
            if line:
                pending += line
                if pending.endswith("\n"):
                    yield pending
                    pending = ""
            elif not follow:
                if pending:
                    yield pending
                return
            else:
                if path.stat().st_size < stream.tell():
                    raise ValueError("input was truncated; use a fresh run")
                time.sleep(poll_interval)


def run(args):
    defaults = {"model": args.model, "window_size": args.window_size, "alpha": args.alpha,
                "season_buckets": args.season_buckets, "season_cycles": args.season_cycles}
    # Construct once to validate CLI settings even if no complete input arrives.
    make_predictor(**defaults)
    overrides = json.loads(Path(args.model_config).read_text()) if args.model_config else {}
    if not isinstance(overrides, dict) or any(not isinstance(v, dict) for v in overrides.values()):
        raise ValueError("model-config must map function IDs to model settings")
    configured = {fn: make_predictor(**{**defaults, **values}) for fn, values in overrides.items()}
    input_path = Path(args.input)
    run_id = args.run_id
    out_dir = Path("predictor/data") / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = Path(args.output) if getattr(args, "output", None) else out_dir / "forecasts.jsonl"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    follow = getattr(args, "follow", False)
    poll = getattr(args, "poll_interval", 0.5)
    if not math.isfinite(poll) or poll <= 0:
        raise ValueError("poll_interval must be positive and finite")
    if args.horizon_buckets < 2:
        raise ValueError("horizon-buckets must be >= 2")
    if not math.isfinite(args.availability_delay) or args.availability_delay < 0:
        raise ValueError("availability-delay must be finite and non-negative")
    if args.expected_interval is not None and args.expected_interval <= 0:
        raise ValueError("expected-interval must be positive")

    # Refuse to overwrite forecasts, including when resuming an old run by mistake.
    with open(out_file, "x", encoding="utf-8") as f_out:
        print(f"{'Following' if follow else 'Reading'} {input_path}; output: {out_file}", flush=True)
        for line_num, line in enumerate(input_lines(input_path, follow, poll), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                bucket = json.loads(line)
            except json.JSONDecodeError:
                print(f"Skip malformed line {line_num}")
                continue
            if args.expected_interval is not None and bucket.get("interval_seconds", 30) != args.expected_interval:
                raise ValueError("input interval does not match --expected-interval")
            fn = bucket["function_id"]
            if fn not in configured:
                configured[fn] = make_predictor(**defaults)
            predictor = configured[fn]
            if not predictor.ingest(bucket):
                continue
            shared = (follow or args.shared_format or args.model != "sma"
                      or bool(overrides) or args.horizon_buckets != 2)
            pred = (predictor.predict_horizons(fn, args.horizon_buckets) if shared
                    else predictor.predict_next(fn))
            if pred is not None and shared:
                generated = (datetime.now(timezone.utc) if follow else
                             SMAPredictor._parse_iso(pred["forecast_bucket_start"]) +
                             timedelta(seconds=args.availability_delay))
                pred = live_forecast(pred, bucket.get("interval_seconds", 30),
                                     generated)
                if pred is None:
                    print(f"Skip forecast on line {line_num}: publication time outside valid horizon", flush=True)
            if pred is not None:
                f_out.write(json.dumps(pred) + "\n")
                f_out.flush()
    print(f"Done. Output: {out_file.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Online per-function SMA, EWMA and seasonal demand forecasts")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--window-size", type=int, default=3)
    parser.add_argument("--model", choices=("sma", "ewma", "seasonal"), default="sma")
    parser.add_argument("--alpha", type=float, default=0.3)
    parser.add_argument("--season-buckets", type=int, default=20)
    parser.add_argument("--season-cycles", type=int, default=3)
    parser.add_argument("--horizon-buckets", type=int, default=2)
    parser.add_argument("--model-config", help="JSON: function_id -> model parameter overrides")
    parser.add_argument("--shared-format", action="store_true", help="also emit shared forecast contract in batch mode")
    parser.add_argument("--availability-delay", type=float, default=10,
                        help="batch shared records only: simulated publication delay after history ends")
    parser.add_argument("--expected-interval", type=int, help="fail if input bucket interval differs")
    parser.add_argument("--follow", action="store_true", help="wait for input and follow appended complete lines")
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--output", help="forecast output path; must not already exist")
    args = parser.parse_args()
    try:
        run(args)
    except KeyboardInterrupt:
        print("Stopped predictor; forecast output flushed.")

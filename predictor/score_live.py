"""Score saved live forecasts using actual publication times, without actuation.

Reads the client-run window but does not change policy evaluation files. Each actual
bucket must fit fully inside that window after an explicitly declared training
prefix. Only forecasts available before bucket start minus lead time qualify.
"""
import argparse
import hashlib
import json
import math
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from .backtest import metrics
from .models import SMAPredictor

parse = SMAPredictor._parse_iso


def score(actual_records, forecast_records, start, end, lead_time=5):
    if start.tzinfo is None or end.tzinfo is None or end <= start:
        raise ValueError("score window must use aware timestamps with end after start")
    if not math.isfinite(lead_time) or lead_time < 0:
        raise ValueError("lead time must be finite and non-negative")
    actual, seen = {}, set()
    for row in actual_records:
        fn, at = row["function_id"], parse(row["bucket_start"])
        interval, count = row["interval_seconds"], row["invocation_count"]
        if (at.tzinfo is None or not isinstance(fn, str) or not fn
                or isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0
                or isinstance(count, bool) or not isinstance(count, (int, float))
                or not math.isfinite(count) or count < 0):
            raise ValueError("invalid actual bucket")
        key = fn, at
        if key in seen:
            raise ValueError("duplicate actual bucket")
        seen.add(key)
        if row.get("complete") is True and at >= start and at + timedelta(seconds=interval) <= end:
            actual[key] = interval, count
    predictions, late = {}, 0
    for record in forecast_records:
        fn = record["function_id"]
        generated, history_end = parse(record["generated_at"]), parse(record["history_end"])
        valid_until = parse(record["valid_until"])
        interval = record["interval_seconds"]
        if (any(at.tzinfo is None for at in (generated, history_end, valid_until))
                or generated < history_end or valid_until <= generated
                or isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0):
            raise ValueError("invalid forecast timing/interval")
        for used in record.get("input_buckets_used", []):
            if parse(used["bucket_start"]) + timedelta(seconds=interval) > history_end:
                raise ValueError("forecast audit contains input beyond history_end")
        for prediction in record["predictions"]:
            at, count = parse(prediction["bucket_start"]), prediction["invocation_count"]
            if (at.tzinfo is None or at < history_end
                    or at + timedelta(seconds=interval) > valid_until
                    or isinstance(count, bool) or not isinstance(count, (int, float))
                    or not math.isfinite(count) or count < 0):
                raise ValueError("invalid forecast prediction")
            key = fn, at
            if key not in actual:
                continue
            if actual[key][0] != interval:
                raise ValueError("forecast and actual bucket intervals differ")
            if generated + timedelta(seconds=lead_time) > at:
                late += 1
                continue
            previous = predictions.get(key)
            if previous is None or generated > previous[0]:
                predictions[key] = generated, count
    grouped = defaultdict(list)
    for (fn, at), (_, count) in sorted(actual.items()):
        prediction = predictions.get((fn, at))
        grouped[fn].append((count, prediction[1] if prediction else 0.0, prediction is not None))
    if not grouped:
        raise ValueError("no complete actual buckets inside the scoring window")
    return {"start": start.isoformat(), "end": end.isoformat(),
            "required_lead_seconds": lead_time,
            "late_predictions_for_scored_targets": late,
            "metrics": {fn: metrics(rows) for fn, rows in sorted(grouped.items())},
            "note": "Actual publication timestamps; newest eligible forecast per target. "
                    "Missing advance forecasts score as zero/reactive fallback. "
                    "Only complete buckets fully inside the declared window are scored. "
                    "This measures forecasts, not latency/resource savings."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actual", required=True)
    parser.add_argument("--forecasts", required=True)
    parser.add_argument("--run-dir", required=True, help="read-only client replay window")
    parser.add_argument("--warmup-seconds", type=float, default=90)
    parser.add_argument("--lead-time", type=float, default=5)
    parser.add_argument("--output", required=True, help="new report filename")
    args = parser.parse_args()
    if not math.isfinite(args.warmup_seconds) or args.warmup_seconds < 0:
        raise ValueError("warmup must be finite and non-negative")
    target = Path(args.output)
    if target.exists():
        raise FileExistsError("use a fresh score output filename")
    manifest_path = Path(args.run_dir) / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    payloads = {"actual": Path(args.actual).read_bytes(), "forecasts": Path(args.forecasts).read_bytes(),
                "run_manifest": manifest_path.read_bytes()}
    actual = [json.loads(line) for line in payloads["actual"].splitlines() if line.strip()]
    forecasts = [json.loads(line) for line in payloads["forecasts"].splitlines() if line.strip()]
    report = score(actual, forecasts,
                   parse(manifest["started_at"]) + timedelta(seconds=args.warmup_seconds),
                   parse(manifest["ended_at"]), args.lead_time)
    report.update(run_id=manifest["run_id"], warmup_seconds=args.warmup_seconds,
                  input_sha256={name: hashlib.sha256(value).hexdigest() for name, value in payloads.items()})
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x") as stream:
        stream.write(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["metrics"], indent=2))
    print("Saved:", target)


if __name__ == "__main__":
    main()

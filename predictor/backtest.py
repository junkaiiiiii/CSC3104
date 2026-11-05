"""Forecast-development backtest, separate from policy/latency evaluation.

Score each target against the newest forecast available at least lead-time seconds
before its bucket START. Partly elapsed buckets never count as advance prediction.
An unavailable forecast is an explicit zero/reactive fallback, included in errors.
"""
import argparse
import hashlib
import json
import math
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

from .models import SMAPredictor, make_predictor


def compare(records, *, season_buckets=20, season_cycles=3, window_size=3, alpha=0.3,
            warmup_buckets=60, horizon_buckets=3, availability_delay=10, lead_time=5):
    if warmup_buckets < 0 or horizon_buckets < 2:
        raise ValueError("warmup must be non-negative and horizon >= 2")
    if any(not math.isfinite(v) or v < 0 for v in (availability_delay, lead_time)):
        raise ValueError("delay/lead must be finite and non-negative")
    rows = sorted(records, key=lambda r: (SMAPredictor._parse_iso(r["bucket_start"]), r["function_id"]))
    if not rows:
        raise ValueError("no demand buckets")
    configurations = {
        "sma": {"model": "sma", "window_size": window_size},
        "ewma": {"model": "ewma", "window_size": window_size, "alpha": alpha},
        "seasonal": {"model": "seasonal", "window_size": window_size,
                     "season_buckets": season_buckets, "season_cycles": season_cycles},
    }
    models = {name: make_predictor(**config) for name, config in configurations.items()}
    forecasts = {name: {} for name in models}
    first, intervals, actual = {}, {}, {}
    for row in rows:
        fn = row["function_id"]
        start = SMAPredictor._parse_iso(row["bucket_start"])
        width = row.get("interval_seconds", 30)
        if fn in intervals and intervals[fn] != width:
            raise ValueError("backtest input must have a fixed interval per function")
        intervals[fn] = width
        first.setdefault(fn, start)
        key = fn, start
        if key in actual:
            raise ValueError("duplicate function/bucket pair")
        actual[key] = row
        for name, model in models.items():
            if not model.ingest(row):
                continue
            prediction = model.predict_horizons(fn, horizon_buckets)
            history_end = start + timedelta(seconds=width)
            generated = history_end + timedelta(seconds=availability_delay)
            for offset, count in enumerate(prediction["horizon_counts"]):
                target = history_end + timedelta(seconds=offset * width)
                if generated + timedelta(seconds=lead_time) <= target:
                    previous = forecasts[name].get((fn, target))
                    if previous is None or generated > previous[0]:
                        forecasts[name][fn, target] = generated, count

    summaries = {}
    for name in models:
        grouped = defaultdict(list)
        for (fn, target), row in sorted(actual.items()):
            if row.get("complete", True) is not True:
                continue
            if target < first[fn] + timedelta(seconds=warmup_buckets * intervals[fn]):
                continue
            forecast = forecasts[name].get((fn, target))
            count = forecast[1] if forecast else 0.0
            grouped[fn].append((row["invocation_count"], count, forecast is not None))
        summaries[name] = {fn: metrics(values) for fn, values in sorted(grouped.items())}
    return {"configuration": {"models": configurations, "warmup_buckets": warmup_buckets,
                               "horizon_buckets": horizon_buckets,
                               "availability_delay_seconds": availability_delay,
                               "required_lead_seconds": lead_time},
            "interval_seconds": intervals, "metrics": summaries,
            "note": "Chronological online updates; no target is used before prediction. "
                    "Only forecasts published before target start minus lead time qualify. "
                    "Missing forecasts are scored as zero/reactive fallback. "
                    "Publication delay is simulated, not measured. This is not latency or cost evaluation."}


def metrics(values):
    count = len(values)
    errors = [predicted - actual for actual, predicted, _ in values]
    true_positive = sum(actual > 0 and predicted >= 0.5 for actual, predicted, _ in values)
    false_positive = sum(actual == 0 and predicted >= 0.5 for actual, predicted, _ in values)
    false_negative = sum(actual > 0 and predicted < 0.5 for actual, predicted, _ in values)
    active_errors = [abs(predicted-actual) for actual, predicted, _ in values if actual > 0]
    return {"scored_buckets": count, "advance_forecasts": sum(available for _, _, available in values),
            "fallback_buckets": sum(not available for _, _, available in values),
            "mae": round(sum(abs(error) for error in errors) / count, 4),
            "bias": round(sum(errors) / count, 4),
            "active_bucket_mae": round(sum(active_errors) / len(active_errors), 4) if active_errors else None,
            "demand_precision": round(true_positive / (true_positive+false_positive), 4)
                                if true_positive+false_positive else None,
            "demand_recall": round(true_positive / (true_positive+false_negative), 4)
                             if true_positive+false_negative else None,
            "false_positive_buckets": false_positive, "missed_active_buckets": false_negative}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True, help="new JSON report filename")
    parser.add_argument("--season-buckets", type=int, default=20)
    parser.add_argument("--season-cycles", type=int, default=3)
    parser.add_argument("--window-size", type=int, default=3)
    parser.add_argument("--alpha", type=float, default=0.3)
    parser.add_argument("--warmup-buckets", type=int, default=60)
    parser.add_argument("--horizon-buckets", type=int, default=3)
    parser.add_argument("--availability-delay", type=float, default=10)
    parser.add_argument("--lead-time", type=float, default=5)
    args = parser.parse_args()
    target = Path(args.output)
    if target.exists():
        raise FileExistsError("use a fresh backtest output filename")
    payload = Path(args.input).read_bytes()
    records = [json.loads(line) for line in payload.splitlines() if line.strip()]
    options = vars(args).copy()
    del options["input"], options["output"]
    report = compare(records, **options)
    if not any(report["metrics"].values()):
        raise ValueError("no scored buckets after warmup; use a longer dataset or a shorter declared warmup")
    report.update(input_path=args.input, input_sha256=hashlib.sha256(payload).hexdigest())
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x") as stream:
        stream.write(json.dumps(report, indent=2) + "\n")
    for model, functions in report["metrics"].items():
        for fn, values in functions.items():
            print(f"{model:8} {fn:8} MAE={values['mae']:.4f} "
                  f"recall={values['demand_recall']} false-positive={values['false_positive_buckets']}")
    print("Saved:", target)


if __name__ == "__main__":
    main()

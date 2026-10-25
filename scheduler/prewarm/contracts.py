"""Forecast input and decision output.

Input: the predictor's output (predictor/README.md, "Output Contract"), one line per
function per finalised bucket:
    function_id, forecast_bucket_start, forecast_invocation_count, window_size,
    input_buckets_used [{bucket_start, invocation_count}]
It has no timing fields, so from_predictor_record() derives them with fixed rules
(see its docstring). The older shared multi-prediction format in
contracts/forecast.example.json is still accepted unchanged.
Output: the decision contract in docs/04-contracts.md.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

FORECAST_FIELDS = ("function_id", "generated_at", "history_end", "interval_seconds",
                   "model", "predictions", "valid_until")
PREDICTOR_FIELDS = ("function_id", "forecast_bucket_start", "forecast_invocation_count")
DECISION_FIELDS = ("function_id", "decided_at", "forecast_generated_at",
                   "target_warm_replicas", "lead_time_ms", "reason", "dry_run")


class InvalidForecast(ValueError):
    """Raised with a short reason that ends up in the policy audit log."""


def parse_ts(value) -> datetime:
    """ISO 8601 with 'Z' or an offset -> aware UTC datetime."""
    if not isinstance(value, str):
        raise InvalidForecast("timestamp is not a string: %r" % (value,))
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise InvalidForecast("bad timestamp: %r" % value)
    if parsed.tzinfo is None:
        raise InvalidForecast("timestamp has no timezone: %r" % value)
    return parsed.astimezone(timezone.utc)


def format_ts(moment: datetime) -> str:
    moment = moment.astimezone(timezone.utc)
    if moment.microsecond:
        return moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


class Forecast:
    """A validated forecast. Predictions are (bucket_start, count) sorted by time."""

    __slots__ = ("function_id", "generated_at", "history_end", "interval_seconds",
                 "model", "predictions", "valid_until", "raw")

    def __init__(self, raw: dict):
        if not isinstance(raw, dict):
            raise InvalidForecast("forecast is not a JSON object")
        missing = [key for key in FORECAST_FIELDS if key not in raw]
        if missing:
            raise InvalidForecast("missing fields: " + ",".join(missing))
        if not isinstance(raw["function_id"], str) or not raw["function_id"]:
            raise InvalidForecast("function_id must be a non-empty string")
        interval = raw["interval_seconds"]
        if isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0:
            raise InvalidForecast("interval_seconds must be a positive integer")
        self.function_id = raw["function_id"]
        self.generated_at = parse_ts(raw["generated_at"])
        self.history_end = parse_ts(raw["history_end"])
        self.valid_until = parse_ts(raw["valid_until"])
        self.interval_seconds = interval
        self.model = str(raw["model"])
        if self.history_end > self.generated_at:
            raise InvalidForecast("history_end is after generated_at")
        if self.valid_until <= self.generated_at:
            raise InvalidForecast("valid_until is not after generated_at")
        if not isinstance(raw["predictions"], list) or not raw["predictions"]:
            raise InvalidForecast("predictions must be a non-empty list")
        predictions = []
        for item in raw["predictions"]:
            if not isinstance(item, dict):
                raise InvalidForecast("prediction is not an object")
            start = parse_ts(item.get("bucket_start"))
            count = item.get("invocation_count")
            if (isinstance(count, bool) or not isinstance(count, (int, float))
                    or not math.isfinite(count) or count < 0):
                raise InvalidForecast("invocation_count must be finite and non-negative")
            # Contract: prediction buckets follow history_end (no future data in training).
            if start < self.history_end:
                raise InvalidForecast("prediction bucket %s is before history_end" % format_ts(start))
            predictions.append((start, float(count)))
        predictions.sort(key=lambda p: p[0])
        starts = [p[0] for p in predictions]
        if len(set(starts)) != len(starts):
            raise InvalidForecast("duplicate prediction bucket_start")
        self.predictions = predictions
        self.raw = raw


def is_predictor_record(raw) -> bool:
    return isinstance(raw, dict) and "forecast_bucket_start" in raw and "predictions" not in raw


def from_predictor_record(raw: dict, interval_seconds: int = 30,
                          availability_delay_s: float = 10.0) -> dict:
    """Predictor output line -> the shared forecast shape, so one validator serves both.

    Rules (record them in the run manifest; they are scheduler assumptions):
      history_end   = forecast_bucket_start. The predictor forecasts the bucket right
                      after the newest bucket it has seen, so history ends there.
      generated_at  = history_end + availability_delay_s. Telemetry finalises a bucket
                      `grace` seconds after it ends (default 10s), so the forecast cannot
                      exist earlier. Replays use this time; the line is invisible before it.
      predictions   = forecast count for forecast_bucket_start AND the bucket after it.
                      A simple moving average forecasts the same value for every horizon,
                      and the second bucket bridges the gap until the next forecast lands.
      valid_until   = end of that second bucket.
      model         = "sma-w<window_size>".
    Leakage check: every input bucket must start before forecast_bucket_start.
    """
    missing = [key for key in PREDICTOR_FIELDS if key not in raw]
    if missing:
        raise InvalidForecast("missing fields: " + ",".join(missing))
    start = parse_ts(raw["forecast_bucket_start"])
    for used in raw.get("input_buckets_used") or []:
        if not isinstance(used, dict) or parse_ts(used.get("bucket_start")) >= start:
            raise InvalidForecast("input bucket is not before forecast_bucket_start (future data)")
    width = timedelta(seconds=interval_seconds)
    count = raw["forecast_invocation_count"]
    window = raw.get("window_size")
    return {
        "function_id": raw["function_id"],
        "generated_at": format_ts(start + timedelta(seconds=availability_delay_s)),
        "history_end": format_ts(start),
        "interval_seconds": interval_seconds,
        "model": "sma-w%s" % window if window is not None else "predictor",
        "predictions": [{"bucket_start": format_ts(start), "invocation_count": count},
                        {"bucket_start": format_ts(start + width), "invocation_count": count}],
        "valid_until": format_ts(start + 2 * width),
    }


def decision_record(function_id, decided_at, forecast_generated_at, target, lead_time_ms,
                    reason, dry_run) -> dict:
    """Exactly the v1 decision contract, in contract field order."""
    return {
        "function_id": function_id,
        "decided_at": format_ts(decided_at),
        "forecast_generated_at": format_ts(forecast_generated_at) if forecast_generated_at else None,
        "target_warm_replicas": int(target),
        "lead_time_ms": int(lead_time_ms),
        "reason": reason,
        "dry_run": bool(dry_run),
    }

import argparse
import json
import math
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional


class BasePredictor:
    def ingest(self, bucket: Dict[str, Any]) -> None:
        raise NotImplementedError
    def predict_next(self, function_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError


class SMAPredictor(BasePredictor):
    def __init__(self, window_size: int = 3):
        if window_size < 1:
            raise ValueError("window_size must be positive")
        self.window_size = window_size
        self.history: Dict[str, List[Dict[str, Any]]] = {}

    @staticmethod
    def _parse_iso(ts: str) -> datetime:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    def ingest(self, bucket: Dict[str, Any]) -> bool:
        if not bucket.get("complete", True):
            return False
        func_id = bucket["function_id"]
        start = self._parse_iso(bucket["bucket_start"])
        interval = bucket.get("interval_seconds", 30)
        count = bucket["invocation_count"]
        if (not isinstance(func_id, str) or not func_id or start.tzinfo is None
                or isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0
                or isinstance(count, bool) or not isinstance(count, (int, float))
                or not math.isfinite(count) or count < 0):
            raise ValueError("invalid complete demand bucket")
        previous = self.history.get(func_id, [])
        if previous:
            last = previous[-1]
            last_start = self._parse_iso(last["bucket_start"])
            if start <= last_start:
                return False  # repeated or out-of-order data must not train twice
            if (interval != last.get("interval_seconds", 30)
                    or start != last_start + timedelta(seconds=interval)):
                self.history[func_id] = []  # a missing interval is not observed zero demand
        else:
            self.history[func_id] = []
        self.history[func_id].append(bucket)
        if len(self.history[func_id]) > self.window_size:
            self.history[func_id] = self.history[func_id][-self.window_size:]
        return True

    def predict_next(self, function_id: str) -> Optional[Dict[str, Any]]:
        buckets = self.history.get(function_id, [])
        if not buckets:
            return None
        latest = buckets[-1]
        latest_start = self._parse_iso(latest["bucket_start"])
        interval_sec = latest.get("interval_seconds", 30)
        next_start = (latest_start + timedelta(seconds=interval_sec)).isoformat().replace("+00:00", "Z")
        counts = [b["invocation_count"] for b in buckets]
        avg = sum(counts) / len(counts)
        return {
            "function_id": function_id,
            "forecast_bucket_start": next_start,
            "forecast_invocation_count": round(avg, 2),
            "window_size": self.window_size,
            "input_buckets_used": [
                {"bucket_start": b["bucket_start"], "invocation_count": b["invocation_count"]}
                for b in buckets
            ]
        }


def live_forecast(prediction, interval, generated_at):
    """Use the shared contract and actual publication time, never inferred past time.

    SMA is held constant over two buckets. The first has already begun when the
    collector finalises history; the second supplies a genuinely future horizon.
    """
    start = SMAPredictor._parse_iso(prediction["forecast_bucket_start"])
    width = timedelta(seconds=interval)
    end = start + 2 * width
    if generated_at < start or generated_at >= end:
        return None  # historical backlogs must not become live forecasts
    stamp = lambda value: value.isoformat().replace("+00:00", "Z")
    return {
        **prediction,
        "generated_at": stamp(generated_at), "history_end": stamp(start),
        "interval_seconds": interval, "model": f"sma-w{prediction['window_size']}",
        "predictions": [
            {"bucket_start": stamp(start + i * width),
             "invocation_count": prediction["forecast_invocation_count"]}
            for i in range(2)
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
    predictor: BasePredictor = SMAPredictor(window_size=args.window_size)
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
            if not predictor.ingest(bucket):
                continue
            pred = predictor.predict_next(bucket["function_id"])
            if pred is not None and follow:
                pred = live_forecast(pred, bucket.get("interval_seconds", 30),
                                     datetime.now(timezone.utc))
            if pred is not None:
                f_out.write(json.dumps(pred) + "\n")
                f_out.flush()
    print(f"Done. Output: {out_file.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Per-function demand predictor, SMA baseline")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--window-size", type=int, default=3)
    parser.add_argument("--follow", action="store_true", help="wait for input and follow appended complete lines")
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--output", help="forecast output path; must not already exist")
    args = parser.parse_args()
    try:
        run(args)
    except KeyboardInterrupt:
        print("Stopped predictor; forecast output flushed.")

import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


class BasePredictor:
    def ingest(self, bucket: Dict[str, Any]) -> None:
        raise NotImplementedError
    def predict_next(self, function_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError


class SMAPredictor(BasePredictor):
    def __init__(self, window_size: int = 3):
        self.window_size = window_size
        self.history: Dict[str, List[Dict[str, Any]]] = {}

    @staticmethod
    def _parse_iso(ts: str) -> datetime:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    def ingest(self, bucket: Dict[str, Any]) -> None:
        if not bucket.get("complete", True):
            return
        func_id = bucket["function_id"]
        if func_id not in self.history:
            self.history[func_id] = []
        self.history[func_id].append(bucket)
        if len(self.history[func_id]) > self.window_size:
            self.history[func_id] = self.history[func_id][-self.window_size:]

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


def run(args):
    predictor: BasePredictor = SMAPredictor(window_size=args.window_size)
    input_path = Path(args.input)
    run_id = args.run_id
    out_dir = Path("predictor/data") / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "forecasts.jsonl"

    with open(input_path, "r", encoding="utf-8") as f_in, open(out_file, "w", encoding="utf-8") as f_out:
        for line_num, line in enumerate(f_in, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                bucket = json.loads(line)
            except json.JSONDecodeError:
                print(f"Skip malformed line {line_num}")
                continue
            predictor.ingest(bucket)
            pred = predictor.predict_next(bucket["function_id"])
            if pred is not None:
                f_out.write(json.dumps(pred) + "\n")
    print(f"Done. Output: {out_file.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Per-function demand predictor, SMA baseline")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--window-size", type=int, default=3)
    args = parser.parse_args()
    run(args)

import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any


class SMAForecaster:
    def __init__(self, window_size: int = 3):
        self.window_size = window_size
        self.history: Dict[str, List[Dict[str, Any]]] = {}

    @staticmethod
    def _parse_iso(ts: str) -> datetime:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    def ingest_bucket(self, bucket: Dict[str, Any]) -> None:
        if not bucket.get("complete", True):
            return
        func_id = bucket["function_id"]
        if func_id not in self.history:
            self.history[func_id] = []
        self.history[func_id].append(bucket)
        if len(self.history[func_id]) > self.window_size:
            self.history[func_id] = self.history[func_id][-self.window_size:]

    def predict_next(self, function_id: str) -> Dict[str, Any] | None:
        buckets = self.history.get(function_id, [])
        if not buckets:
            return None
        latest = buckets[-1]
        latest_end = self._parse_iso(latest["bucket_end"])
        next_start = latest_end.isoformat().replace("+00:00", "Z")

        req_counts = [b["request_count"] for b in buckets]
        avg = sum(req_counts) / len(req_counts)

        return {
            "function_id": function_id,
            "forecast_bucket_start": next_start,
            "forecast_request_count": round(avg, 2),
            "window_size": self.window_size,
            "input_buckets_used": buckets.copy()
        }


def run(args):
    forecaster = SMAForecaster(window_size=args.window_size)
    input_path = Path(args.input)
    run_id = args.run_id

    out_dir = Path("data") / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "forecasts.jsonl"

    with open(input_path, "r", encoding="utf-8") as f_in, open(out_file, "w", encoding="utf-8") as f_out:
        for line in f_in:
            line = line.strip()
            if not line:
                continue
            bucket = json.loads(line)
            forecaster.ingest_bucket(bucket)
            pred = forecaster.predict_next(bucket["function_id"])
            if pred is not None:
                f_out.write(json.dumps(pred) + "\n")

    print(f"✅ Predictor finished. Output -> {out_file.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SMA Predictor, consumes telemetry demand_buckets.jsonl")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--input", required=True, help="Path to telemetry demand_buckets.jsonl")
    parser.add_argument("--window-size", type=int, default=3)
    args = parser.parse_args()
    run(args)

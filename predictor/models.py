"""Online demand-count models using complete historical buckets only."""
import math
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional


class BasePredictor:
    def ingest(self, bucket: Dict[str, Any]) -> None:
        raise NotImplementedError
    def predict_next(self, function_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError


class SMAPredictor(BasePredictor):
    def __init__(self, window_size: int = 3, history_limit=None):
        if isinstance(window_size, bool) or not isinstance(window_size, int) or window_size < 1:
            raise ValueError("window_size must be a positive integer")
        self.window_size = window_size
        self.history_limit = max(window_size, history_limit or window_size)
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
        if len(self.history[func_id]) > self.history_limit:
            self.history[func_id] = self.history[func_id][-self.history_limit:]
        return True

    def predict_next(self, function_id: str) -> Optional[Dict[str, Any]]:
        buckets = self.history.get(function_id, [])[-self.window_size:]
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

    def predict_horizons(self, function_id, horizons=2):
        if horizons < 1:
            raise ValueError("horizons must be positive")
        result = self.predict_next(function_id)
        if result is None:
            return None
        return {**result, "model": f"sma-w{self.window_size}",
                "horizon_counts": [result["forecast_invocation_count"]] * horizons,
                "horizon_methods": ["sma"] * horizons}


class EWMAPredictor(SMAPredictor):
    """Simple exponential smoothing; reset level after gaps or interval changes."""
    def __init__(self, window_size=3, alpha=0.3):
        super().__init__(window_size)
        if not math.isfinite(alpha) or not 0 < alpha <= 1:
            raise ValueError("alpha must be in (0, 1]")
        self.alpha = alpha
        self.levels = {}
        self.state = {}

    def ingest(self, bucket):
        fn = bucket["function_id"]
        previous = self.history.get(fn, [])
        last = previous[-1] if previous else None
        if not super().ingest(bucket):
            return False
        interval = bucket.get("interval_seconds", 30)
        contiguous = (last is not None and interval == last.get("interval_seconds", 30)
                      and self._parse_iso(bucket["bucket_start"]) ==
                      self._parse_iso(last["bucket_start"]) + timedelta(seconds=interval))
        if contiguous:
            self.levels[fn] = self.alpha * bucket["invocation_count"] + (1-self.alpha) * self.levels[fn]
            self.state[fn]["training_buckets_seen"] += 1
        else:
            self.levels[fn] = float(bucket["invocation_count"])
            self.state[fn] = {"state_started_at": bucket["bucket_start"], "training_buckets_seen": 1}
        return True

    def predict_horizons(self, function_id, horizons=2):
        result = super().predict_horizons(function_id, horizons)
        if result is None:
            return None
        count = round(self.levels[function_id], 4)
        return {**result, "model": f"ewma-a{self.alpha:g}",
                "forecast_invocation_count": count, "horizon_counts": [count] * horizons,
                "horizon_methods": ["ewma"] * horizons,
                "smoothing_state": {**self.state[function_id], "level": self.levels[function_id]}}


class SeasonalPredictor(SMAPredictor):
    """Average observed counts at the same phase in the previous cycles.

    Period is an explicit configuration learned/chosen on training data, never
    inferred from a held-out request schedule. Fall back to SMA until each phase
    has at least two complete cycles; gaps reset history as for the baseline.
    """
    def __init__(self, window_size=3, season_buckets=20, season_cycles=3):
        if not isinstance(season_buckets, int) or isinstance(season_buckets, bool) or season_buckets < 2:
            raise ValueError("season_buckets must be an integer >= 2")
        if not isinstance(season_cycles, int) or isinstance(season_cycles, bool) or season_cycles < 2:
            raise ValueError("season_cycles must be an integer >= 2")
        super().__init__(window_size, history_limit=season_buckets * season_cycles)
        self.season_buckets, self.season_cycles = season_buckets, season_cycles

    def predict_horizons(self, function_id, horizons=2):
        result = super().predict_horizons(function_id, horizons)
        if result is None:
            return None
        rows = self.history[function_id]
        width = timedelta(seconds=rows[-1].get("interval_seconds", 30))
        period = width * self.season_buckets
        start = self._parse_iso(result["forecast_bucket_start"])
        counts, methods = [], []
        used = {row["bucket_start"]: row for row in rows[-self.window_size:]}
        for offset in range(horizons):
            target = start + offset * width
            phase = [row for row in rows
                     if (target - self._parse_iso(row["bucket_start"])) % period == timedelta(0)]
            if len(phase) >= 2:
                counts.append(round(sum(row["invocation_count"] for row in phase) / len(phase), 4))
                methods.append("seasonal-mean")
                used.update({row["bucket_start"]: row for row in phase})
            else:
                counts.append(result["forecast_invocation_count"])
                methods.append("fallback-sma")
        return {**result, "model": f"seasonal-p{self.season_buckets}-c{self.season_cycles}",
                "forecast_invocation_count": counts[0], "horizon_counts": counts,
                "horizon_methods": methods,
                "input_buckets_used": [{"bucket_start": row["bucket_start"],
                                        "invocation_count": row["invocation_count"]}
                                       for row in sorted(used.values(), key=lambda r: self._parse_iso(r["bucket_start"]))]}


def make_predictor(model="sma", window_size=3, alpha=0.3, season_buckets=20, season_cycles=3):
    if model == "sma":
        return SMAPredictor(window_size)
    if model == "ewma":
        return EWMAPredictor(window_size, alpha)
    if model == "seasonal":
        return SeasonalPredictor(window_size, season_buckets, season_cycles)
    raise ValueError(f"unknown model: {model}")



"""Per-function demand buckets in UTC with watermark-based finalisation."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .schema import format_ts

COUNTED = "counted"
LATE = "late"


class DemandAggregator:
    """Counts user requests per function in fixed UTC buckets.

    A bucket [s, s+interval) is finalised once now >= s + interval + grace.
    Finalised buckets emit exactly the demand-bucket contract fields. A bucket
    with a healthy collector and no events is a confirmed 0; a bucket that
    overlaps a recorded gap is emitted with complete=false (unknown, not 0).
    Events for an already-finalised bucket are reported as LATE, never added.
    """

    def __init__(self, functions, start: datetime, interval_s: int = 30, grace_s: float = 10):
        self.interval_s = int(interval_s)
        self._interval = timedelta(seconds=self.interval_s)
        self._grace = timedelta(seconds=grace_s)
        first = self.floor(start)
        self._cursor = {fid: first for fid in functions}
        self._counts = {fid: {} for fid in functions}
        self._gaps = []  # (function_id | None, start, end)

    def floor(self, moment: datetime) -> datetime:
        epoch = int(moment.timestamp())
        return datetime.fromtimestamp(epoch - epoch % self.interval_s, tz=timezone.utc)

    def add(self, function_id: str, started_at: datetime) -> str:
        bucket = self.floor(started_at)
        if bucket < self._cursor[function_id]:
            return LATE
        counts = self._counts[function_id]
        counts[bucket] = counts.get(bucket, 0) + 1
        return COUNTED

    def mark_gap(self, start: datetime, end: datetime, function_id=None) -> None:
        """Record that observation was unreliable over [start, end)."""
        self._gaps.append((function_id, start, end))

    def advance(self, now: datetime) -> list:
        """Finalise every bucket whose watermark has passed; return contract dicts."""
        emitted = []
        for function_id, cursor in self._cursor.items():
            while cursor + self._interval + self._grace <= now:
                count = self._counts[function_id].pop(cursor, 0)
                emitted.append(self._record(function_id, cursor, count))
                cursor += self._interval
            self._cursor[function_id] = cursor
        emitted.sort(key=lambda r: (r["bucket_start"], r["function_id"]))
        low = min(self._cursor.values())
        self._gaps = [g for g in self._gaps if g[2] > low]
        return emitted

    def pending(self) -> int:
        """User requests counted in buckets that are not finalised yet."""
        return sum(sum(c.values()) for c in self._counts.values())

    def _record(self, function_id, start, count):
        end = start + self._interval
        complete = not any(
            (fid is None or fid == function_id) and gap_start < end and gap_end > start
            for fid, gap_start, gap_end in self._gaps)
        return {
            "function_id": function_id,
            "bucket_start": format_ts(start),
            "interval_seconds": self.interval_s,
            "invocation_count": count,
            "complete": complete,
        }

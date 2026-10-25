"""Pipeline: line -> validate -> dedup -> classify -> bucket, with audit outputs."""
from __future__ import annotations

import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from . import dedup
from .buckets import COUNTED, DemandAggregator
from .schema import InvalidEvent, format_ts, validate_event
from .writer import RunWriter

RAW = "raw_events.jsonl"
BUCKETS = "demand_buckets.jsonl"
QUARANTINE = "quarantine.jsonl"
STATS = "collector_stats.jsonl"
MANIFEST = "telemetry_manifest.json"


def _utc(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


class Collector:
    """clock='event' derives time from the newest started_at (deterministic replay);
    clock='wall' uses the wall clock (live pipe)."""

    def __init__(self, out_dir, functions, run_id, interval_s=30, grace_s=10,
                 dedup_ttl_s=900.0, clock="event", source="stdin"):
        if clock not in ("event", "wall"):
            raise ValueError("clock must be 'event' or 'wall'")
        self.functions = tuple(sorted(functions))
        self.run_id, self.clock = run_id, clock
        self.interval_s, self.grace_s = interval_s, grace_s
        self.source = source
        self.writer = RunWriter(out_dir)
        self.deduper = dedup.Deduper(ttl_s=dedup_ttl_s)
        self.totals = Counter()
        self.per_function = {f: Counter() for f in self.functions}
        self._agg = None
        self._event_clock = None
        self._started = time.time()
        self.writer.write_json(MANIFEST, self._manifest(final=False))

    # -- ingestion ---------------------------------------------------------
    def process_line(self, line, wall_now=None) -> str:
        line = line.strip()
        if not line:
            return "blank"
        wall_now = time.time() if wall_now is None else wall_now
        self.totals["lines"] += 1
        try:
            event = json.loads(line)
        except ValueError:
            return self._quarantine(line, "malformed_json", wall_now)
        try:
            started_at = validate_event(event)
        except InvalidEvent as error:
            return self._quarantine(line, str(error), wall_now)
        function_id = event["function_id"]
        if function_id not in self.per_function:
            return self._quarantine(line, "unknown_function: " + function_id, wall_now)

        self.totals["valid"] += 1
        counters = self.per_function[function_id]
        event_epoch = started_at.timestamp()
        if self._event_clock is None or event_epoch > self._event_clock:
            self._event_clock = event_epoch
        now = self._now(wall_now)
        if self._agg is None:
            self._agg = DemandAggregator(self.functions, _utc(
                self._started if self.clock == "wall" else event_epoch),
                self.interval_s, self.grace_s)

        verdict = self.deduper.check(event, now)
        if verdict != dedup.NEW:
            outcome = verdict
        elif event["request_kind"] == "warmup":
            outcome = "warmup"
        else:
            outcome = self._agg.add(function_id, started_at)  # counted | late
        self.totals[outcome] += 1
        counters[outcome] += 1
        self.writer.append(RAW, {"received_at": format_ts(_utc(now)),
                                 "outcome": outcome, "source": self.source, "event": event})
        self._advance(now)
        return outcome

    def tick(self, wall_now=None) -> None:
        """Advance the watermark without input (wall clock only)."""
        if self.clock == "wall" and self._agg is not None:
            self._advance(time.time() if wall_now is None else wall_now)

    def mark_gap(self, start: datetime, end: datetime, function_id=None) -> None:
        if self._agg is not None:
            self._agg.mark_gap(start, end, function_id)

    # -- output ------------------------------------------------------------
    def close(self, wall_now=None) -> bool:
        """Finalise what the watermark allows and write the manifest.
        Returns the conservation check result."""
        wall_now = time.time() if wall_now is None else wall_now
        if self._agg is not None:
            if self.clock == "event":
                edge = self._agg.floor(_utc(self._event_clock)).timestamp()
                self._advance(edge + self.interval_s + self.grace_s)
            else:
                self._advance(wall_now)
        ok = self.conservation_ok()
        self._write_stats()
        self.writer.write_json(MANIFEST, self._manifest(final=True, conservation_ok=ok))
        self.writer.close()
        return ok

    def conservation_ok(self) -> bool:
        """Every line is accounted for exactly once."""
        t = self.totals
        pending = self._agg.pending() if self._agg else 0
        return (t["lines"] == t["quarantined"] + t["valid"]
                and t["valid"] == t["counted"] + t["late"] + t["warmup"]
                + t["duplicate"] + t["conflicting_duplicate"]
                and t["counted"] == t["emitted_invocations"] + pending)

    # -- internals ---------------------------------------------------------
    def _now(self, wall_now: float) -> float:
        return wall_now if self.clock == "wall" else self._event_clock

    def _advance(self, now_epoch: float) -> None:
        records = self._agg.advance(_utc(now_epoch))
        for record in records:
            self.writer.append(BUCKETS, record)
            self.totals["buckets"] += 1
            self.totals["emitted_invocations"] += record["invocation_count"]
            if not record["complete"]:
                self.totals["incomplete_buckets"] += 1
        if records:
            self._write_stats()

    def _quarantine(self, line, reason, wall_now) -> str:
        self.totals["quarantined"] += 1
        self.writer.append(QUARANTINE, {"received_at": format_ts(_utc(self._now(wall_now) or wall_now)),
                                        "reason": reason, "line": line[:500]})
        return "quarantined"

    def _write_stats(self) -> None:
        self.writer.append(STATS, {
            "at": format_ts(_utc(self._now(time.time()) or time.time())),
            "run_id": self.run_id, "totals": dict(self.totals),
            "per_function": {f: dict(c) for f, c in self.per_function.items()},
            "pending": self._agg.pending() if self._agg else 0})

    def _manifest(self, final, conservation_ok=None) -> dict:
        manifest = {
            "run_id": self.run_id, "collector": "telemetry/collector",
            "contracts": {"invocation_schema_version": 1, "demand_bucket": "v1"},
            "functions": list(self.functions), "interval_seconds": self.interval_s,
            "grace_seconds": self.grace_s, "clock": self.clock, "source": self.source,
            "python": sys.version.split()[0],
            "started_at": format_ts(_utc(self._started)), "final": final,
        }
        if final:
            manifest.update(ended_at=format_ts(_utc(time.time())),
                            conservation_ok=conservation_ok, totals=dict(self.totals))
        return manifest

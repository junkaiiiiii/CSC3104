"""Forecast input: tail a JSONL file of forecast-contract records.

Only whole lines are read, so a predictor appending concurrently is safe. For each
function the book answers "what was the newest forecast at time t?", which keeps
replays honest: a forecast generated after t is never visible at t.
If the newest record for a function is invalid, the policy sees the rejection reason
and falls back to reactive scaling instead of silently using an older forecast.
"""
from __future__ import annotations

import bisect
import json
import os
from datetime import timedelta

from .contracts import (Forecast, InvalidForecast, from_predictor_record, is_predictor_record,
                        parse_ts)


class ForecastBook:
    def __init__(self, functions, interval_seconds=30, availability_delay_s=10.0):
        self.functions = set(functions)
        self.interval_seconds = interval_seconds
        self.availability_delay_s = availability_delay_s
        self._times = {fn: [] for fn in self.functions}
        self._items = {fn: [] for fn in self.functions}
        self.rejected = []    # (line_no, reason, raw) for the audit log
        self.accepted = 0

    def add_line(self, line_no: int, text: str):
        text = text.strip()
        if not text:
            return
        try:
            raw = json.loads(text)
        except ValueError:
            self.rejected.append((line_no, "not JSON", text[:200]))
            return
        fn = raw.get("function_id") if isinstance(raw, dict) else None
        if fn not in self.functions:
            self.rejected.append((line_no, "unknown function_id %r" % (fn,), raw))
            return
        try:
            shaped = (from_predictor_record(raw, self.interval_seconds, self.availability_delay_s)
                      if is_predictor_record(raw) else raw)
            item = Forecast(shaped)
            moment = item.generated_at
            self.accepted += 1
        except InvalidForecast as exc:
            self.rejected.append((line_no, str(exc), raw))
            try:
                if is_predictor_record(raw):
                    moment = parse_ts(raw.get("forecast_bucket_start")) + timedelta(
                        seconds=self.availability_delay_s)
                else:
                    moment = parse_ts(raw.get("generated_at"))
            except InvalidForecast:
                return    # cannot place it in time; audit only
            item = str(exc)
        index = bisect.bisect_right(self._times[fn], moment)
        self._times[fn].insert(index, moment)
        self._items[fn].insert(index, item)

    def latest(self, function_id, now):
        times = self._times.get(function_id) or []
        index = bisect.bisect_right(times, now)
        return self._items[function_id][index - 1] if index else None

    def snapshot(self, now):
        return {fn: self.latest(fn, now) for fn in self.functions}


class FileTail:
    """Reads complete new lines from a file that may not exist yet or may grow."""

    def __init__(self, path):
        self.path = path
        self._offset = 0
        self._line_no = 0
        self._partial = b""

    def read_new(self):
        if not os.path.exists(self.path):
            return []
        with open(self.path, "rb") as fh:
            size = os.fstat(fh.fileno()).st_size
            if size < self._offset:          # truncated / rotated: start again
                self._offset, self._partial = 0, b""
            fh.seek(self._offset)
            data = fh.read()
            self._offset = fh.tell()
        data = self._partial + data
        lines = data.split(b"\n")
        self._partial = lines.pop()
        out = []
        for raw in lines:
            self._line_no += 1
            out.append((self._line_no, raw.rstrip(b"\r").decode("utf-8", errors="replace")))
        return out

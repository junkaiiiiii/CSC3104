"""Request-ID deduplication keyed on (function_id, request_id)."""
from __future__ import annotations

NEW = "new"
DUPLICATE = "duplicate"      # identical payload seen before (e.g. log replay)
CONFLICT = "conflicting_duplicate"  # same request_id, different instance/time


class Deduper:
    """Remember request IDs for ttl_s seconds of the caller-supplied clock.

    The first event wins. A repeat with the same (instance_id, started_at) is a
    replay; a repeat that differs is reported separately for CORE-16 review.
    Request IDs must be unique per run (prefix them with the run_id).
    """

    def __init__(self, ttl_s: float = 900.0, prune_every_s: float = 60.0):
        self.ttl_s = ttl_s
        self._prune_every_s = prune_every_s
        self._seen = {}
        self._last_prune = None

    def check(self, event: dict, now: float) -> str:
        self._prune(now)
        key = (event["function_id"], event["request_id"])
        fingerprint = (event["instance_id"], event["started_at"])
        previous = self._seen.get(key)
        if previous is None:
            self._seen[key] = (fingerprint, now)
            return NEW
        self._seen[key] = (previous[0], now)
        return DUPLICATE if previous[0] == fingerprint else CONFLICT

    def _prune(self, now: float) -> None:
        if self._last_prune is None:
            self._last_prune = now
            return
        if now - self._last_prune < self._prune_every_s:
            return
        self._last_prune = now
        cutoff = now - self.ttl_s
        for key in [k for k, (_, seen) in self._seen.items() if seen < cutoff]:
            del self._seen[key]

    def __len__(self) -> int:
        return len(self._seen)

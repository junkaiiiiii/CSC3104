"""Validation of the v1 invocation event (see docs/04-contracts.md)."""
from __future__ import annotations

import math
from datetime import datetime, timezone

SCHEMA_VERSION = 1
REQUEST_KINDS = ("user", "warmup")
_STRINGS = ("function_id", "revision", "instance_id", "request_id")
REQUIRED = _STRINGS + (
    "schema_version", "request_kind", "started_at",
    "service_ms", "first_user_request", "init_ms",
)


class InvalidEvent(ValueError):
    """Raised with a short, stable reason string used in quarantine records."""


def parse_ts(value) -> datetime:
    """Parse an ISO 8601 timestamp ('Z' or offset) to an aware UTC datetime."""
    if not isinstance(value, str):
        raise InvalidEvent("timestamp is not a string")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise InvalidEvent("bad timestamp: %r" % value)
    if parsed.tzinfo is None:
        raise InvalidEvent("timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


def format_ts(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _non_negative_number(value) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0)


def validate_event(event) -> datetime:
    """Return the parsed started_at, or raise InvalidEvent."""
    if not isinstance(event, dict):
        raise InvalidEvent("event is not a JSON object")
    if event.get("schema_version") != SCHEMA_VERSION:
        raise InvalidEvent("unsupported schema_version: %r" % (event.get("schema_version"),))
    missing = [key for key in REQUIRED if key not in event]
    if missing:
        raise InvalidEvent("missing fields: " + ",".join(missing))
    for key in _STRINGS:
        if not isinstance(event[key], str) or not event[key]:
            raise InvalidEvent("%s must be a non-empty string" % key)
    if event["request_kind"] not in REQUEST_KINDS:
        raise InvalidEvent("bad request_kind: %r" % (event["request_kind"],))
    if not isinstance(event["first_user_request"], bool):
        raise InvalidEvent("first_user_request must be boolean")
    for key in ("service_ms", "init_ms"):
        if not _non_negative_number(event[key]):
            raise InvalidEvent("%s must be a finite non-negative number" % key)
    return parse_ts(event["started_at"])

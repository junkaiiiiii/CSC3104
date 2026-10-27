"""Correlate client requests with pod readiness: scheduling lag, cold-start exposure, idle time."""
from __future__ import annotations

import bisect
import math
from collections import Counter

from .common import parse_utc, quantile

LIFECYCLE_EVENTS = frozenset({"pod_seen", "pod_ready", "pod_unready", "pod_terminating", "pod_gone"})

EXPOSURE_CRITERION = (
    "A user request is exposed when its sent_at is not covered by any Ready pod of its function in "
    "pod_lifecycle observations (Kubernetes Ready time, or the observation time if that is later). "
    "It does not detect waiting caused by all Ready replicas being busy (scale-out), and Kubernetes "
    "Ready is not the same as being routable by Knative. The collector must have been running "
    "before the replay started; use collector.validate to check coverage."
)
IDLE_DEFINITION = (
    "Per function, integral over the observation window of max(0, ready replicas - busy replicas), "
    "where busy = ceil(client in-flight user requests / container concurrency). In-flight time is "
    "measured by the client (sent_at to completed_at), so it includes network and activator "
    "queueing and slightly overstates busy time."
)


def _parse(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        return parse_utc(value)
    except ValueError:
        return None


def _finite(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def ready_timelines(lifecycle: list[dict], api_ready_times: bool = False):
    """function_id -> (change times, Ready pod counts after each change).

    Returns None when there are no usable events or a record lacks function_id, because a
    per-function timeline cannot be built. With api_ready_times, a pod counts as Ready from
    its Kubernetes ready_at when that is earlier than the (poll-delayed) observation.
    """
    events = []
    for record in lifecycle:
        if record.get("event") not in LIFECYCLE_EVENTS or not record.get("pod") or not record.get("observed_at"):
            continue
        if not record.get("function_id"):
            return None
        at = _parse(record["observed_at"])
        if at is None:
            continue
        if api_ready_times and record["event"] == "pod_ready":
            api = _parse(record.get("ready_at"))
            if api is not None and api < at:
                at = api
        events.append((at, record))
    if not events:
        return None
    events.sort(key=lambda item: item[0])
    pod_ready, counts, series = {}, {}, {}
    for at, record in events:
        function_id, pod, event = record["function_id"], record["pod"], record["event"]
        times, values = series.setdefault(function_id, ([], []))
        was = pod_ready.get(pod, False)
        if event == "pod_ready":
            now = True
        elif event == "pod_seen":
            now = was
        else:
            now = False
        if now != was:
            counts[function_id] = counts.get(function_id, 0) + (1 if now else -1)
            times.append(at)
            values.append(counts[function_id])
        pod_ready[pod] = now
    return series


def _count_at(series: dict, function_id: str, moment) -> int:
    times, values = series.get(function_id, ([], []))
    index = bisect.bisect_right(times, moment)
    return values[index - 1] if index else 0


def scheduling_lag_summary(requests: list[dict]) -> dict:
    """Delay between the intended send time and the actual send time."""
    lags = []
    for row in requests:
        value = row.get("scheduling_lag_ms")
        if _finite(value):
            lags.append(max(0.0, float(value)))
            continue
        sent, scheduled = _parse(row.get("sent_at")), _parse(row.get("scheduled_at"))
        if sent is not None and scheduled is not None:
            lags.append(max(0.0, (sent - scheduled).total_seconds() * 1000))
    return {
        "n": len(lags),
        "unmeasured": len(requests) - len(lags),
        "median": quantile(lags, 0.50),
        "p95": quantile(lags, 0.95),
        "max": round(max(lags), 3) if lags else None,
        "note": "Actual minus intended send time. Large values mean the load generator, not the platform, delayed requests.",
    }


def classify_exposure(requests: list[dict], lifecycle: list[dict]) -> list[str]:
    """One label per request: exposed, not_exposed, unknown or warmup (excluded from demand)."""
    series = ready_timelines(lifecycle, api_ready_times=True) if lifecycle else None
    labels = []
    for row in requests:
        if row.get("request_kind", "user") != "user":
            labels.append("warmup")
            continue
        sent = _parse(row.get("sent_at"))
        if series is None or sent is None or not row.get("function_id"):
            labels.append("unknown")
        else:
            labels.append("exposed" if _count_at(series, row["function_id"], sent) == 0 else "not_exposed")
    return labels


def _latency_block(values: list[float]) -> dict:
    if not values:
        return {"n": 0, "median": None, "p95": None, "p99": None, "status": "N/A"}
    return {"n": len(values), "median": quantile(values, 0.50), "p95": quantile(values, 0.95),
            "p99": quantile(values, 0.99), "status": "ok"}


def exposure_summary(requests: list[dict], labels: list[str]) -> dict:
    counts = Counter(labels)

    def latencies(label):
        return [float(row["latency_ms"]) for row, got in zip(requests, labels)
                if got == label and _finite(row.get("latency_ms"))]

    agreement = Counter()
    for row, label in zip(requests, labels):
        if label in ("exposed", "not_exposed"):
            first = row.get("first_user_request")
            suffix = "first_user" if first is True else "not_first_user" if first is False else "first_user_unknown"
            agreement[f"{label}_{suffix}"] += 1
    return {
        "criterion": EXPOSURE_CRITERION,
        "user_requests": counts["exposed"] + counts["not_exposed"] + counts["unknown"],
        "exposed": counts["exposed"],
        "not_exposed": counts["not_exposed"],
        "unknown": counts["unknown"],
        "excluded_warmup": counts["warmup"],
        "exposed_latency_ms": _latency_block(latencies("exposed")),
        "not_exposed_latency_ms": _latency_block(latencies("not_exposed")),
        "first_user_proxy_crosstab": dict(sorted(agreement.items())),
    }


def application_init_summary(requests: list[dict]) -> dict:
    values = [float(row["init_ms"]) for row in requests
              if row.get("first_user_request") is True and _finite(row.get("init_ms"))]
    return {
        "n": len(values),
        "median": quantile(values, 0.50),
        "max": round(max(values), 3) if values else None,
        "status": "ok" if values else "N/A",
        "note": "Application startup work reported by the function, not platform cold-start delay.",
    }


def idle_replica_seconds(requests: list[dict], lifecycle: list[dict], start, end, concurrency=1):
    """Observable idle replica time (see IDLE_DEFINITION), or None if it cannot be built."""
    series = ready_timelines(lifecycle) if lifecycle else None
    if series is None or end <= start:
        return None
    by_function = {}
    for function_id in series:
        sent, done = [], []
        for row in requests:
            if row.get("function_id") != function_id or row.get("request_kind", "user") != "user":
                continue
            began, finished = _parse(row.get("sent_at")), _parse(row.get("completed_at"))
            if began is not None and finished is not None and finished > began:
                sent.append(began)
                done.append(finished)
        sent.sort()
        done.sort()
        per_replica = concurrency.get(function_id, 1) if isinstance(concurrency, dict) else concurrency
        points = {start, end}
        points.update(t for t in series[function_id][0] + sent + done if start < t < end)
        ordered = sorted(points)
        total = 0.0
        for left, right in zip(ordered, ordered[1:]):
            in_flight = bisect.bisect_right(sent, left) - bisect.bisect_right(done, left)
            busy = math.ceil(in_flight / per_replica) if in_flight > 0 else 0
            total += max(0, _count_at(series, function_id, left) - busy) * (right - left).total_seconds()
        by_function[function_id] = round(total, 3)
    return {
        "total": round(sum(by_function.values()), 3),
        "by_function": by_function,
        "container_concurrency": concurrency,
        "definition": IDLE_DEFINITION,
    }

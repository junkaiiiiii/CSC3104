"""Actuators: turn a decision's target_warm_replicas into capacity (CORE-10).

DryRunActuator    changes nothing; optionally observes Ready pods.
MinScaleActuator  sets autoscaling.knative.dev/min-scale on the function's active
                  Revision (default) or its PodAutoscaler. Never touches the Service
                  template, so it never creates revisions. Which target actually works on
                  Knative 1.23 is what the CORE-04 spike decides (docs/decisions/0001).
WarmupActuator    sends idempotent warm-up requests (X-Request-Kind: warmup). Proves
                  predictive activation only, not replica control; fallback option.

Safety rules (all actuators return one result dict per call; nothing raises):
  * Remember each object's original min-scale before the first change, and store it on
    the object too (MARKER) so a restarted scheduler, or `--restore`, can put it back.
  * Never go below the original min-scale (a fixed-minimum baseline stays intact).
  * Never exceed the object's max-scale.
  * Idempotent: no API write when the object already has the wanted value.
  * After max_failures consecutive errors for a function: try to restore it, then
    leave it to Knative's reactive autoscaler for backoff_s before trying again.
  * If the active revision changes, restore the old revision before touching the new.
"""
from __future__ import annotations

import threading
import time
import urllib.error
import urllib.request

from .contracts import format_ts
from .kube import KubeError

MIN_SCALE = "autoscaling.knative.dev/min-scale"
MAX_SCALE = "autoscaling.knative.dev/max-scale"
MARKER = "prewarm.csc3104.dev/original-min-scale"
ABSENT = "absent"
SERVICE_LABEL = "serving.knative.dev/service"
TARGET_KINDS = ("revision", "pa")


def observe_readiness(client, function_id):
    """Ready / total pods for a function, or None if it cannot be observed."""
    if client is None:
        return None
    try:
        pods = client.list("pod", "%s=%s" % (SERVICE_LABEL, function_id))
    except KubeError:
        return None
    ready = 0
    for pod in pods:
        if (pod.get("metadata") or {}).get("deletionTimestamp"):
            continue
        conditions = (pod.get("status") or {}).get("conditions") or []
        if any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions):
            ready += 1
    return {"ready_pods": ready, "pods": len(pods)}


def _result(actuator, function_id, now, target, status, **extra):
    record = {"function_id": function_id, "at": format_ts(now), "actuator": actuator,
              "target_warm_replicas": target, "status": status}
    record.update(extra)
    return record


def _as_int(value):
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


class DryRunActuator:
    name = "dry-run"

    def __init__(self, client=None):
        self.client = client

    def apply(self, function_id, target, now):
        return _result(self.name, function_id, now, target, "dry_run",
                       readiness=observe_readiness(self.client, function_id))

    def restore_all(self, now):
        return []


class MinScaleActuator:
    def __init__(self, client, target_kind="revision", max_failures=3, backoff_s=60.0,
                 clock=time.monotonic):
        if target_kind not in TARGET_KINDS:
            raise ValueError("target_kind must be one of %s" % (TARGET_KINDS,))
        self.client = client
        self.kind = target_kind
        self.name = "min-scale/" + target_kind
        self.max_failures = max_failures
        self.backoff_s = backoff_s
        self._clock = clock
        self._touched = {}      # function_id -> revision we changed
        self._originals = {}    # (function_id, revision) -> original min-scale string or None
        self._applied = {}      # (function_id, revision) -> value we last wrote
        self._failures = {}
        self._suspended_until = {}
        self.reverts = 0

    # --- helpers -------------------------------------------------------------
    def _active_revision(self, function_id):
        ksvc = self.client.get("ksvc", function_id)
        status = ksvc.get("status") or {}
        revision = status.get("latestReadyRevisionName")
        if not revision:
            raise KubeError("%s has no ready revision" % function_id)
        others = [t.get("revisionName") for t in status.get("traffic") or []
                  if t.get("percent") and t.get("revisionName") != revision]
        return revision, others

    def _annotations(self, revision):
        obj = self.client.get(self.kind, revision)
        return dict((obj.get("metadata") or {}).get("annotations") or {})

    def _restore(self, function_id, revision):
        key = (function_id, revision)
        original = self._originals.get(key)
        self.client.merge_patch(self.kind, revision,
                                {"metadata": {"annotations": {MIN_SCALE: original, MARKER: None}}})
        self._touched.pop(function_id, None)
        self._applied.pop(key, None)
        return original

    # --- public --------------------------------------------------------------
    def apply(self, function_id, target, now):
        calls_before = self.client.calls
        until = self._suspended_until.get(function_id)
        if until is not None and self._clock() < until:
            return _result(self.name, function_id, now, target, "suspended",
                           detail="backing off after %d failures; Knative reactive only"
                           % self.max_failures, api_calls=0)
        extra = {}
        try:
            revision, others = self._active_revision(function_id)
            extra["revision"] = revision
            if others:
                extra["warning"] = "traffic also routed to %s" % ",".join(others)
            old = self._touched.get(function_id)
            if old and old != revision:
                try:
                    self._restore(function_id, old)
                    extra["restored_previous_revision"] = old
                except KubeError as exc:
                    if not exc.not_found:
                        raise
                    self._touched.pop(function_id, None)
            key = (function_id, revision)
            ann = self._annotations(revision)
            if key not in self._originals:
                if MARKER in ann:      # left by an earlier scheduler process
                    self._originals[key] = None if ann[MARKER] == ABSENT else ann[MARKER]
                    self._touched[function_id] = revision
                else:
                    self._originals[key] = ann.get(MIN_SCALE)
            original = self._originals[key]
            base = _as_int(original) or 0
            current = ann.get(MIN_SCALE)
            extra["previous_min_scale"] = current

            if key in self._applied and current != self._applied[key]:
                self.reverts += 1
                extra["reverted_by_platform"] = True

            if target <= 0 or max(base, target) == base:
                if MARKER in ann or function_id in self._touched:
                    restored = self._restore(function_id, revision)
                    status, extra["applied_min_scale"] = "restored", restored
                else:
                    status = "noop"
            else:
                desired = max(base, target)
                max_scale = _as_int(ann.get(MAX_SCALE))
                if max_scale and desired > max_scale:
                    desired = max_scale
                    extra["capped_by_max_scale"] = max_scale
                if current == str(desired) and MARKER in ann:
                    status = "noop"
                else:
                    self.client.merge_patch(self.kind, revision, {"metadata": {"annotations": {
                        MIN_SCALE: str(desired), MARKER: original if original is not None else ABSENT}}})
                    status = "applied"
                    self._touched[function_id] = revision
                    self._applied[key] = str(desired)
                extra["applied_min_scale"] = str(desired)
            self._failures[function_id] = 0
            self._suspended_until.pop(function_id, None)
        except KubeError as exc:
            count = self._failures.get(function_id, 0) + 1
            self._failures[function_id] = count
            status, extra["detail"] = "error", str(exc)
            if count >= self.max_failures:
                status = "error_suspended"
                self._suspended_until[function_id] = self._clock() + self.backoff_s
                revision = self._touched.get(function_id)
                if revision:
                    try:
                        self._restore(function_id, revision)
                        extra["restored_after_failures"] = revision
                    except KubeError as exc2:
                        extra["restore_failed"] = str(exc2)
        extra["api_calls"] = self.client.calls - calls_before
        extra["readiness"] = observe_readiness(self.client, function_id)
        return _result(self.name, function_id, now, target, status, **extra)

    def restore_all(self, now):
        results = []
        for function_id, revision in sorted(self._touched.items()):
            try:
                original = self._restore(function_id, revision)
                results.append(_result(self.name, function_id, now, 0, "restored",
                                       revision=revision, applied_min_scale=original))
            except KubeError as exc:
                results.append(_result(self.name, function_id, now, 0, "restore_failed",
                                       revision=revision, detail=str(exc)))
        return results


def restore_namespace(client, kind="revision"):
    """Crash recovery: put back every min-scale this scheduler changed (marker present)."""
    restored = []
    for obj in client.list(kind):
        meta = obj.get("metadata") or {}
        ann = meta.get("annotations") or {}
        if MARKER not in ann:
            continue
        original = None if ann[MARKER] == ABSENT else ann[MARKER]
        client.merge_patch(kind, meta["name"],
                           {"metadata": {"annotations": {MIN_SCALE: original, MARKER: None}}})
        restored.append((meta["name"], original))
    return restored


class WarmupActuator:
    """Sends `target` concurrent warm-up requests per tick while target > 0.

    With containerConcurrency 1 this asks Knative for up to `target` pods. Warm-ups are
    excluded from demand by telemetry (request_kind=warmup) but cost resources: their
    results are kept here for overhead accounting.
    """
    name = "warmup"

    def __init__(self, url="http://127.0.0.1:8080/", host_template="{fn}.csc3104.example.com",
                 timeout_s=30.0, client=None, opener=None):
        self.url = url
        self.host_template = host_template
        self.timeout_s = timeout_s
        self.client = client
        self._open = opener or urllib.request.urlopen
        self._seq = 0

    def _send(self, function_id, request_id, out, index):
        req = urllib.request.Request(self.url, headers={
            "Host": self.host_template.format(fn=function_id),
            "X-Request-ID": request_id, "X-Request-Kind": "warmup"})
        start = time.perf_counter()
        try:
            with self._open(req, timeout=self.timeout_s) as resp:
                resp.read()
                status = resp.status
        except urllib.error.HTTPError as exc:
            status = exc.code
        except (urllib.error.URLError, OSError) as exc:
            status = "error: %s" % exc
        out[index] = {"request_id": request_id, "status": status,
                      "latency_ms": round((time.perf_counter() - start) * 1000, 1)}

    def apply(self, function_id, target, now):
        if target <= 0:
            return _result(self.name, function_id, now, target, "noop",
                           readiness=observe_readiness(self.client, function_id))
        self._seq += 1
        out = [None] * target
        threads = []
        for i in range(target):
            rid = "warmup-%s-%s-%d-%d" % (function_id, now.strftime("%Y%m%dT%H%M%S"), self._seq, i)
            thread = threading.Thread(target=self._send, args=(function_id, rid, out, i), daemon=True)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join(self.timeout_s + 5)
        ok = all(r and isinstance(r["status"], int) and r["status"] < 300 for r in out)
        return _result(self.name, function_id, now, target, "applied" if ok else "error",
                       requests=out, readiness=observe_readiness(self.client, function_id))

    def restore_all(self, now):
        return []

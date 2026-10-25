"""Kubernetes source: follow Knative pod logs and observe pod lifecycles via kubectl.

Standard library only. Shells out to kubectl like infra/tools/watch_pods.py, so it
works with any kubeconfig (no RBAC setup on a local kind cluster).

Items put on KubeSource.out (consumed by one thread, see main.py):
  ("line", pod, text)                    one stdout line from user-container
  ("lifecycle", record)                  pod_lifecycle record (see contracts/)
  ("gap", function_id|None, start, end)  observation unreliable over [start, end)
  ("error", message)                     human-readable warning
"""
from __future__ import annotations

import json
import queue
import subprocess
import threading
import time
from datetime import datetime, timezone

from .schema import parse_ts

SERVICE_LABEL = "serving.knative.dev/service"
REVISION_LABEL = "serving.knative.dev/revision"
USER_CONTAINER = "user-container"

RECORD_FIELDS = ("schema_version", "observed_at", "event", "function_id", "revision", "pod",
                 "created_at", "ready_at", "deletion_timestamp", "deletion_grace_period_s", "phase",
                 "cpu_request_m", "mem_request_mi", "cpu_limit_m", "mem_limit_mi")

_MEM_UNITS = {"Ki": 1 / 1024, "Mi": 1.0, "Gi": 1024.0, "Ti": 1024.0 ** 2,
              "k": 1000 / 1048576, "M": 1e6 / 1048576, "G": 1e9 / 1048576, "T": 1e12 / 1048576}


def _utc(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


def iso_ms(epoch: float) -> str:
    return _utc(epoch).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_cpu_m(quantity):
    """Kubernetes CPU quantity -> millicores, or None if absent/unparseable."""
    if quantity is None:
        return None
    text = str(quantity)
    try:
        if text.endswith("m"):
            return round(float(text[:-1]), 3)
        if text.endswith("u"):
            return round(float(text[:-1]) / 1000, 3)
        if text.endswith("n"):
            return round(float(text[:-1]) / 1e6, 3)
        return round(float(text) * 1000, 3)
    except ValueError:
        return None


def parse_mem_mi(quantity):
    """Kubernetes memory quantity -> MiB, or None if absent/unparseable."""
    if quantity is None:
        return None
    text = str(quantity)
    try:
        for suffix in ("Ki", "Mi", "Gi", "Ti", "k", "M", "G", "T"):
            if text.endswith(suffix):
                return round(float(text[:-len(suffix)]) * _MEM_UNITS[suffix], 3)
        return round(float(text) / 1048576, 3)
    except ValueError:
        return None


def _sum_resource(containers, section, key, parser):
    total = None
    for container in containers:
        value = parser(((container.get("resources") or {}).get(section) or {}).get(key))
        if value is not None:
            total = (total or 0) + value
    return total


def parse_pod(item):
    """One `kubectl get pods -o json` item -> info dict, or None if not a Knative pod."""
    meta = item.get("metadata") or {}
    labels = meta.get("labels") or {}
    function_id = labels.get(SERVICE_LABEL)
    if not function_id:
        return None
    status = item.get("status") or {}
    ready_condition = next((c for c in status.get("conditions") or [] if c.get("type") == "Ready"), None)
    ready = bool(ready_condition and ready_condition.get("status") == "True")
    started = container_terminated = False
    for container in status.get("containerStatuses") or []:
        if container.get("name") == USER_CONTAINER:
            state = container.get("state") or {}
            started = "running" in state or "terminated" in state
            container_terminated = "terminated" in state
    containers = (item.get("spec") or {}).get("containers") or []
    return {
        "name": meta.get("name"), "function_id": function_id,
        "revision": labels.get(REVISION_LABEL), "created_at": meta.get("creationTimestamp"),
        "ready": ready, "ready_at": ready_condition.get("lastTransitionTime") if ready else None,
        "terminating": bool(meta.get("deletionTimestamp")),
        "deletion_timestamp": meta.get("deletionTimestamp"),
        "deletion_grace_period_s": meta.get("deletionGracePeriodSeconds"), "phase": status.get("phase"),
        "started": started, "container_terminated": container_terminated,
        "cpu_request_m": _sum_resource(containers, "requests", "cpu", parse_cpu_m),
        "mem_request_mi": _sum_resource(containers, "requests", "memory", parse_mem_mi),
        "cpu_limit_m": _sum_resource(containers, "limits", "cpu", parse_cpu_m),
        "mem_limit_mi": _sum_resource(containers, "limits", "memory", parse_mem_mi),
    }


def lifecycle_record(event: str, info: dict, observed_at: str) -> dict:
    record = {"schema_version": 1, "observed_at": observed_at, "event": event, "pod": info["name"]}
    for key in RECORD_FIELDS:
        if key not in record:
            record[key] = info.get(key)
    return {key: record[key] for key in RECORD_FIELDS}


class PodTracker:
    """Turns successive pod listings into lifecycle events (pure, no I/O).

    Events: pod_seen, pod_ready, pod_unready, pod_terminating, pod_gone. Times from
    the Kubernetes API (created_at, ready_at, deletion_timestamp) are exact to a
    second; observed_at is when this collector noticed (poll granularity).
    """

    def __init__(self):
        self._pods = {}

    def update(self, infos, observed_at: str) -> list:
        records, current = [], {}
        for info in sorted(infos, key=lambda i: i["name"]):
            name, previous = info["name"], self._pods.get(info["name"])
            current[name] = info
            if previous is None:
                records.append(lifecycle_record("pod_seen", info, observed_at))
            if info["ready"] and not (previous or {}).get("ready"):
                records.append(lifecycle_record("pod_ready", info, observed_at))
            elif previous and previous["ready"] and not info["ready"] and not info["terminating"]:
                records.append(lifecycle_record("pod_unready", info, observed_at))
            if info["terminating"] and not (previous or {}).get("terminating"):
                records.append(lifecycle_record("pod_terminating", info, observed_at))
        for name in sorted(set(self._pods) - set(current)):
            records.append(lifecycle_record("pod_gone", self._pods[name], observed_at))
        self._pods = current
        return records


def _default_run(cmd):
    return subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=20).stdout


def _default_spawn(cmd):
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)


def split_stamp(raw: str):
    """Split a `kubectl logs --timestamps` line into (timestamp | None, payload)."""
    raw = raw.rstrip("\n")
    head, sep, rest = raw.partition(" ")
    if sep and len(head) >= 20 and head[4] == "-" and "T" in head and head.endswith("Z"):
        return head, rest
    return None, raw


class KubeSource:
    def __init__(self, namespace, functions, context=None, poll_interval_s=2.0, kubectl="kubectl",
                 run=None, spawn=None, now=time.time, retry_s=1.0):
        self.namespace, self.functions = namespace, tuple(functions)
        self.poll_interval_s, self.retry_s = poll_interval_s, retry_s
        self.out = queue.Queue()
        self._base = [kubectl] + (["--context", context] if context else [])
        self._selector = "%s in (%s)" % (SERVICE_LABEL, ",".join(self.functions))
        self._run, self._spawn, self._now = run or _default_run, spawn or _default_spawn, now
        self._tracker = PodTracker()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._live, self._followed, self._procs, self._threads = {}, set(), set(), []
        self._fails, self._last_ok = 0, None

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        thread = threading.Thread(target=self._poll_loop, daemon=True)
        thread.start()
        self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            procs = list(self._procs)
        for proc in procs:
            try:
                proc.terminate()
            except (OSError, AttributeError):
                pass
        for thread in self._threads:
            thread.join(timeout=3)

    # -- pod discovery + lifecycle ----------------------------------------
    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            self.poll_once()
            self._stop.wait(self.poll_interval_s)

    def poll_once(self, start_followers=True) -> None:
        cmd = self._base + ["get", "pods", "-n", self.namespace, "-l", self._selector, "-o", "json"]
        try:
            items = json.loads(self._run(cmd))["items"]
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
            self._fails += 1
            if self._fails == 1 or self._fails % 15 == 0:
                self.out.put(("error", "kubectl get pods failed: %s" % error))
            return
        now = self._now()
        if self._last_ok is not None and now - self._last_ok > 3 * self.poll_interval_s:
            self.out.put(("gap", None, _utc(self._last_ok), _utc(now)))
        self._fails, self._last_ok = 0, now
        infos = [i for i in map(parse_pod, items) if i and i["function_id"] in self.functions]
        for record in self._tracker.update(infos, iso_ms(now)):
            self.out.put(("lifecycle", record))
        with self._lock:
            self._live = {i["name"]: i for i in infos}
        if start_followers:
            for info in infos:
                if info["name"] not in self._followed:
                    self._followed.add(info["name"])
                    thread = threading.Thread(target=self._follow, daemon=True,
                                              args=(info["name"], info["function_id"], info["created_at"]))
                    thread.start()
                    self._threads.append(thread)

    # -- per-pod log following --------------------------------------------
    def _follow(self, name, function_id, created_at) -> None:
        """Read a pod's log from its start; reconnect with --since-time if the stream
        drops; finish when the container has exited or the pod is gone. If the pod
        disappears before a clean drain, report a gap over the pod's lifetime."""
        since, saw_lines, ever_started, clean = None, False, False, False
        while not self._stop.is_set():
            cmd = self._base + ["logs", "-f", "--timestamps", "-n", self.namespace, name, "-c", USER_CONTAINER]
            if since:
                cmd += ["--since-time", since]
            proc = self._spawn(cmd)
            with self._lock:
                self._procs.add(proc)
            try:
                for raw in proc.stdout:
                    stamp, payload = split_stamp(raw)
                    since = stamp or since
                    saw_lines = True
                    self.out.put(("line", name, payload))
                rc = proc.wait()
            finally:
                with self._lock:
                    self._procs.discard(proc)
                close = getattr(proc.stdout, "close", None)
                if close:
                    close()
            with self._lock:
                info = self._live.get(name)
            if info is None:                      # pod no longer listed
                clean = rc == 0
                break
            ever_started = ever_started or info["started"]
            if rc == 0 and (info["terminating"] or info["container_terminated"]):
                clean = True
                break
            self._stop.wait(self.retry_s)         # container still starting, or stream dropped
        else:
            return                                 # stopping: no verdict
        if not clean and (saw_lines or ever_started):
            self.out.put(("gap", function_id, parse_ts(created_at), _utc(self._now())))

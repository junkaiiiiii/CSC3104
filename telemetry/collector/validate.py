"""CORE-16: measurement-integrity gate for one experiment run.

Reconciles Hasan's client records (client_measurements.jsonl, run_manifest.json) with
the collector's artifacts for the same run, and rejects the run if observations are
missing, contaminated or unreliable. It never repairs or fills data.

  python3 -m collector.validate --run-dir ../results/reactive-seed-17

The run directory must hold run_manifest.json and client_measurements.jsonl plus the
telemetry artifacts (telemetry_manifest.json, raw_events.jsonl, demand_buckets.jsonl,
pod_lifecycle.jsonl), or pass --telemetry-dir. Writes telemetry_validation.json.
Exit code: 0 pass, 1 fail, 2 unusable input.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .schema import InvalidEvent, format_ts, parse_ts

CLIENT_FILES = ("run_manifest.json", "client_measurements.jsonl")
TELEMETRY_FILES = ("telemetry_manifest.json", "raw_events.jsonl", "demand_buckets.jsonl")
HASHED = CLIENT_FILES + TELEMETRY_FILES + ("pod_lifecycle.jsonl", "quarantine.jsonl", "forecasts.jsonl")
SLOW_TERMINATION_S = 60.0
EXAMPLES = 5


class Findings:
    def __init__(self):
        self.errors, self.warnings = {}, {}

    def add(self, kind: str, code: str, message: str, items) -> None:
        items = list(items)
        if not items:
            return
        store = self.errors if kind == "error" else self.warnings
        store[code] = {"code": code, "message": message, "count": len(items),
                       "examples": [str(i) for i in items[:EXAMPLES]]}

    def error(self, code, message, items):
        self.add("error", code, message, items)

    def warn(self, code, message, items):
        self.add("warning", code, message, items)


def _read_jsonl(path: Path) -> list:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, 1):
            if line.strip():
                try:
                    rows.append(json.loads(line))
                except ValueError as error:
                    raise ValueError("%s:%d: invalid JSON (%s)" % (path.name, number, error))
    return rows


def _ts(value, label: str) -> datetime:
    try:
        return parse_ts(value)
    except InvalidEvent as error:
        raise ValueError("%s: %s" % (label, error))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _success(row: dict) -> bool:
    status = row.get("status_code")
    return isinstance(status, int) and 200 <= status < 300 and row.get("timeout") is not True


def validate_run(run_dir, telemetry_dir=None, tolerance_s: float = 2.0) -> dict:
    run_dir = Path(run_dir)
    tdir = Path(telemetry_dir) if telemetry_dir else run_dir
    findings = Findings()
    report = {"schema_version": 1, "validated_at": format_ts(datetime.now(timezone.utc)),
              "run_dir": str(run_dir), "telemetry_dir": str(tdir)}

    needed = [run_dir / n for n in CLIENT_FILES] + [tdir / n for n in TELEMETRY_FILES]
    missing = [p.name for p in needed if not p.is_file()]
    if missing:
        findings.error("E_MISSING_FILE", "Required files are missing; the run cannot be validated.", missing)
        return _finish(report, findings, {}, run_dir, tdir)
    try:
        summary = _check(run_dir, tdir, tolerance_s, findings, report)
    except (ValueError, KeyError, TypeError) as error:
        findings.error("E_UNREADABLE", "A run file is malformed.", [repr(error)])
        summary = {}
    return _finish(report, findings, summary, run_dir, tdir)


def _finish(report, findings, summary, run_dir, tdir) -> dict:
    report["files_sha256"] = {}
    for name in HASHED:
        for base in (run_dir, tdir):
            if (base / name).is_file():
                report["files_sha256"][name] = _sha256(base / name)
                break
    report.update(status="fail" if findings.errors else "pass", summary=summary,
                  errors=list(findings.errors.values()), warnings=list(findings.warnings.values()))
    return report


def _check(run_dir: Path, tdir: Path, tolerance_s: float, f: Findings, report: dict) -> dict:
    tol = timedelta(seconds=tolerance_s)
    run = json.loads((run_dir / "run_manifest.json").read_text(encoding="utf-8"))
    clients = _read_jsonl(run_dir / "client_measurements.jsonl")
    tm = json.loads((tdir / "telemetry_manifest.json").read_text(encoding="utf-8"))
    raw = _read_jsonl(tdir / "raw_events.jsonl")
    buckets = _read_jsonl(tdir / "demand_buckets.jsonl")
    report["run_id"] = run.get("run_id")

    starts = [_ts(r["sent_at"], "client sent_at") for r in clients if r.get("sent_at")]
    ends = [_ts(r["completed_at"], "client completed_at") for r in clients if r.get("completed_at")]
    w0 = _ts(run["started_at"], "run started_at") if run.get("started_at") else min(starts)
    w1 = _ts(run["ended_at"], "run ended_at") if run.get("ended_at") else max(ends)
    report["window"] = {"start": format_ts(w0), "end": format_ts(w1)}

    # --- collector ended cleanly ------------------------------------------------
    totals = tm.get("totals") or {}
    if tm.get("final") is not True:
        f.error("E_NOT_FINAL", "Collector did not shut down cleanly (manifest not final).", [tm.get("final")])
    if tm.get("conservation_ok") is not True:
        f.error("E_CONSERVATION", "Collector line accounting failed or is missing.", [tm.get("conservation_ok")])
    f.error("E_QUARANTINED", "Input lines the collector could not use were quarantined.",
            ["%d lines" % totals["quarantined"]] if totals.get("quarantined") else [])
    if totals.get("counted", 0) != totals.get("emitted_invocations", 0):
        f.error("E_PENDING_UNEMITTED", "Counted requests were never written into a bucket.",
                ["counted=%s emitted=%s" % (totals.get("counted"), totals.get("emitted_invocations"))])
    if tm.get("clock") == "wall" and tm.get("started_at") and tm.get("ended_at"):
        if _ts(tm["started_at"], "telemetry started_at") > w0:
            f.error("E_COLLECTOR_STARTED_LATE", "Collector started after the replay began.",
                    [tm["started_at"] + " > " + format_ts(w0)])
        last = max(ends) if ends else w1
        needed = last + timedelta(seconds=float(tm.get("grace_seconds", 0)))
        if _ts(tm["ended_at"], "telemetry ended_at") < needed:
            f.error("E_COLLECTOR_ENDED_EARLY", "Collector stopped before the last request's grace period elapsed.",
                    [tm["ended_at"] + " < " + format_ts(needed)])
    missing_fn = sorted({r.get("function_id") for r in clients} - set(tm.get("functions") or []))
    f.error("E_FUNCTION_NOT_COLLECTED", "Client sent requests to functions the collector was not watching.", missing_fn)

    # --- server events ----------------------------------------------------------
    outcomes, server, conflicts, late_in, late_out, event_pods = Counter(), {}, [], [], [], set()
    for record in raw:
        event, outcome = record["event"], record["outcome"]
        key = (event["function_id"], event["request_id"])
        started = _ts(event["started_at"], "event started_at")
        outcomes[outcome] += 1
        if record.get("pod"):
            event_pods.add(record["pod"])
        if outcome == "conflicting_duplicate":
            conflicts.append("%s/%s" % key)
        if outcome == "late":
            (late_in if w0 - tol <= started <= w1 + tol else late_out).append("%s/%s" % key)
        if outcome in ("counted", "late", "warmup") and key not in server:
            server[key] = (event, started, outcome)
    f.error("E_CONFLICTING_DUPLICATE", "One request ID was seen on different instances or times.", conflicts)
    f.error("E_LATE_IN_WINDOW", "Events arrived after their bucket was final, inside the replay window; demand is undercounted.", late_in)
    f.warn("W_LATE_BEFORE_WINDOW", "Late events from before the replay window (old pods or earlier traffic); not counted.", late_out)

    # --- client <-> server reconciliation ---------------------------------------
    by_key = {(r.get("function_id"), r.get("request_id")): r for r in clients}
    no_server, mismatch, off_time, processed_after_failure = [], [], [], 0
    for key, row in by_key.items():
        label = "%s/%s" % key
        if key not in server:
            if _success(row):
                no_server.append(label)
            continue
        event, started, _ = server[key]
        if not _success(row):
            processed_after_failure += 1
        for name in ("instance_id", "revision", "first_user_request"):
            if row.get(name) is not None and row[name] != event.get(name):
                mismatch.append("%s %s client=%r server=%r" % (label, name, row[name], event.get(name)))
        if row.get("sent_at") and row.get("completed_at"):
            if not (_ts(row["sent_at"], "sent_at") - tol <= started <= _ts(row["completed_at"], "completed_at") + tol):
                off_time.append("%s server started_at %s outside client window" % (label, format_ts(started)))
    f.error("E_CLIENT_NO_SERVER", "Client got a successful response but the collector has no event for it (lost telemetry).", no_server)
    f.error("E_FIELD_MISMATCH", "Client and server disagree about which instance or revision served a request.", mismatch)
    f.error("E_TIME_MISMATCH", "Server event time does not fit inside the client's send/complete window (clock skew or wrong match).", off_time)
    stray_in, stray_out, scheduler_warmups = [], [], 0
    for key, (event, started, outcome) in server.items():
        if key in by_key:
            continue
        if outcome == "warmup":
            scheduler_warmups += 1
        elif w0 - tol <= started <= w1 + tol:
            stray_in.append("%s/%s" % key)
        else:
            stray_out.append("%s/%s" % key)
    f.error("E_SERVER_NO_CLIENT", "User requests reached the functions during the run that this client did not send (contaminated demand).", stray_in)
    f.warn("W_STRAY_OUTSIDE_WINDOW", "User requests outside the replay window were observed.", stray_out)

    # --- buckets ----------------------------------------------------------------
    counted = Counter(r["event"]["function_id"] for r in raw if r["outcome"] == "counted")
    summed, incomplete = Counter(), []
    for bucket in buckets:
        start = _ts(bucket["bucket_start"], "bucket_start")
        end = start + timedelta(seconds=bucket["interval_seconds"])
        summed[bucket["function_id"]] += bucket["invocation_count"]
        if bucket["complete"] is not True and start < w1 and end > w0:
            incomplete.append("%s@%s" % (bucket["function_id"], bucket["bucket_start"]))
    f.error("E_INCOMPLETE_BUCKETS", "Demand buckets overlapping the replay window are incomplete (observation gap).", incomplete)
    f.error("E_BUCKET_SUM_MISMATCH", "Bucket totals differ from counted events.",
            ["%s buckets=%d events=%d" % (fn, summed[fn], counted[fn])
             for fn in sorted(set(summed) | set(counted)) if summed[fn] != counted[fn]])

    # --- pod lifecycle -----------------------------------------------------------
    pods_seen = lag_max = lag_total = None
    lifecycle_path = tdir / "pod_lifecycle.jsonl"
    if not lifecycle_path.is_file():
        f.error("E_NO_LIFECYCLE", "pod_lifecycle.jsonl is missing; replica time cannot be measured.", ["pod_lifecycle.jsonl"])
    else:
        by_pod = defaultdict(dict)
        for record in _read_jsonl(lifecycle_path):
            by_pod[record["pod"]].setdefault(record["event"], record)
        pods_seen = len(by_pod)
        f.error("E_UNKNOWN_POD", "Events came from pods with no lifecycle record.", sorted(event_pods - set(by_pod)))
        f.warn("W_PODS_ALIVE_AT_END", "Pods were still alive when observation ended; replica time is integrated to the window end.",
               sorted(p for p, e in by_pod.items() if "pod_gone" not in e))
        before = [p for p, e in by_pod.items()
                  if e.get("pod_seen", {}).get("created_at") and _ts(e["pod_seen"]["created_at"], "created_at") < w0 - tol]
        f.warn("W_PODS_BEFORE_RUN", "Pods existed before the replay started; the run did not start from the recorded initial state.", sorted(before))
        lags = [(_ts(e["pod_gone"]["observed_at"], "observed_at") - _ts(e["pod_terminating"]["observed_at"], "observed_at")).total_seconds()
                for e in by_pod.values() if "pod_gone" in e and "pod_terminating" in e]
        if lags:
            lag_max, lag_total = round(max(lags), 1), round(sum(lags), 1)
            if lag_max > SLOW_TERMINATION_S:
                f.warn("W_SLOW_TERMINATION", "Pods stayed Terminating for over %.0fs (function ignores SIGTERM); resources were held but unused." % SLOW_TERMINATION_S,
                       ["max %.1fs, total %.1fs over %d pods" % (lag_max, lag_total, len(lags))])

    return {
        "client_requests": len(clients), "client_successes": sum(_success(r) for r in clients),
        "client_timeouts": sum(r.get("timeout") is True for r in clients),
        "matched_requests": sum(1 for k in by_key if k in server),
        "server_outcomes": dict(outcomes), "scheduler_warmups_without_client": scheduler_warmups,
        "failed_by_client_but_processed": processed_after_failure,
        "pods_seen": pods_seen, "termination_lag_max_s": lag_max, "termination_lag_total_s": lag_total,
    }


def format_report(report: dict) -> str:
    lines = ["%s  run=%s  window %s -> %s" % (report["status"].upper(), report.get("run_id"),
                                              report.get("window", {}).get("start"), report.get("window", {}).get("end"))]
    for kind, items in (("ERROR", report["errors"]), ("warning", report["warnings"])):
        for item in items:
            lines.append("  %s %s (%d): %s" % (kind, item["code"], item["count"], item["message"]))
            lines += ["      - " + e for e in item["examples"]]
    s = report.get("summary") or {}
    if s:
        lines.append("  matched %s of %s client requests; %s successes, %s timeouts; pods seen %s"
                     % (s["matched_requests"], s["client_requests"], s["client_successes"], s["client_timeouts"], s["pods_seen"]))
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--telemetry-dir", help="if telemetry artifacts are not in --run-dir")
    parser.add_argument("--tolerance", type=float, default=2.0, help="seconds of allowed clock/window slack")
    parser.add_argument("--out", help="report path (default: <run-dir>/telemetry_validation.json)")
    args = parser.parse_args(argv)
    if not Path(args.run_dir).is_dir():
        print("run directory not found: %s" % args.run_dir, file=sys.stderr)
        return 2
    report = validate_run(args.run_dir, args.telemetry_dir, args.tolerance)
    out = Path(args.out) if args.out else Path(args.run_dir) / "telemetry_validation.json"
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(format_report(report))
    print("report: %s" % out)
    codes = {e["code"] for e in report["errors"]}
    return 2 if codes & {"E_MISSING_FILE", "E_UNREADABLE"} else (1 if report["errors"] else 0)


if __name__ == "__main__":
    sys.exit(main())

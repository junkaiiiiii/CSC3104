"""Per-burst readiness timeline for a live run: did a Pod become Ready BEFORE traffic?

Joins, for each burst of user requests to a function:

    forecast published -> positive decision -> min-scale patch -> Pod Ready -> first request

and classifies the burst:

    prewarmed              patch found no Ready Pod; a Pod created after the patch was
                           Ready before the first request  (the evidence we want)
    late                   patch found no Ready Pod; still none Ready at the first request
    retained               a Pod was already Ready when the patch was applied: this only
                           shows capacity was kept, not that it was started in time
    warm_without_forecast  no active patch, but Knative still had a Ready Pod
    missed                 no active patch and no Ready Pod: reactive cold start
    dry_run                the scheduler did not actuate (readiness is still reported)

It also checks that capacity went back to zero between bursts and that forecast
fallbacks never kept or raised min-scale.

Inputs are the normal run files (paths can be given one by one instead of --run-dir):
    <run-dir>/client_measurements.jsonl   evaluation.replay
    <run-dir>/pod_lifecycle.jsonl         collector (copy it in, docs/13)
    <scheduler-dir>/decisions.jsonl, actuation.jsonl, policy_audit.jsonl,
    scheduler_manifest.json
    --forecasts predictor/data/<run>/forecasts.jsonl (optional)

Run from scheduler/:
    python -m tools.readiness_timeline --run-dir ../results/predictive-longgap-401 \
        --scheduler-dir ../results/scheduler/predictive-longgap-401 \
        --forecasts ../predictor/data/predictive-longgap-401/forecasts.jsonl \
        --output ../results/predictive-longgap-401/readiness_timeline.json

Timing resolution: Kubernetes ready_at has 1 s resolution and lifecycle polling adds up
to the collector's --poll-interval (2 s). Margins under ~2 s are within that error.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

RELEASE_STATUSES = ("restored",)
OUTCOMES = ("prewarmed", "late", "retained", "warm_without_forecast", "missed", "dry_run", "unknown")


def parse(value):
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    return moment.astimezone(timezone.utc) if moment.tzinfo else None


def stamp(moment):
    if moment is None:
        return None
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def seconds(a, b):
    return None if a is None or b is None else round((b - a).total_seconds(), 3)


def read_jsonl(path):
    path = Path(path)
    if not path.is_file():
        return []
    with path.open(encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


# --- Pods -------------------------------------------------------------------------
def pod_spans(lifecycle):
    """pod name -> {function_id, created, ready_from, ready_to, gone}.

    Ready from the Kubernetes ready_at when it is earlier than the poll observation
    (same rule as evaluation.correlate); Ready ends at the first unready, terminating
    or gone observation. A Pod that is Ready twice keeps one span per Ready period.
    """
    pods, spans = {}, []
    for record in sorted(lifecycle, key=lambda r: r.get("observed_at") or ""):
        name, event = record.get("pod"), record.get("event")
        at = parse(record.get("observed_at"))
        if not name or at is None:
            continue
        pod = pods.setdefault(name, {"pod": name, "function_id": record.get("function_id"),
                                     "created": parse(record.get("created_at")),
                                     "ready_from": None, "gone": None})
        if event == "pod_ready" and pod["ready_from"] is None:
            api = parse(record.get("ready_at"))
            pod["ready_from"] = api if api is not None and api < at else at
        elif event in ("pod_unready", "pod_terminating", "pod_gone") and pod["ready_from"] is not None:
            spans.append({**pod, "ready_to": at})
            pod["ready_from"] = None
        if event == "pod_gone":
            pod["gone"] = at
    for pod in pods.values():
        if pod["ready_from"] is not None:
            spans.append({**pod, "ready_to": None})
    gone = {name: pod["gone"] for name, pod in pods.items()}
    for span in spans:
        span["gone"] = gone.get(span["pod"])
    return spans


def ready_at(spans, function_id, moment):
    return [s for s in spans if s["function_id"] == function_id and s["ready_from"] <= moment
            and (s["ready_to"] is None or s["ready_to"] > moment)]


def ready_seconds(spans, function_id, a, b):
    total = 0.0
    for s in spans:
        if s["function_id"] != function_id:
            continue
        lo = max(s["ready_from"], a)
        hi = min(s["ready_to"] or b, b)
        if hi > lo:
            total += (hi - lo).total_seconds()
    return round(total, 3)


def first_zero_after(spans, function_id, moment, until):
    """First time in [moment, until) with no Ready Pod, counted only after every Pod that
    served the burst (created by `moment`, including one still starting) has left Ready."""
    served = [s for s in spans if s["function_id"] == function_id
              and (s["created"] or s["ready_from"]) <= moment
              and (s["ready_to"] is None or s["ready_to"] > moment)]
    if any(s["ready_to"] is None or s["ready_to"] >= until for s in served):
        return None
    start = max([moment] + [s["ready_to"] for s in served])
    candidates = [start] + [s["ready_to"] for s in spans if s["function_id"] == function_id
                            and s["ready_to"] is not None and start <= s["ready_to"] < until]
    for t in sorted(candidates):
        if not ready_at(spans, function_id, t):
            return t
    return None


# --- scheduler ------------------------------------------------------------------------
def active_patch(actuation, function_id, moment):
    """When the min-scale currently in force at `moment` was first applied, else None."""
    since = None
    for record in actuation:
        if record.get("function_id") != function_id:
            continue
        at = parse(record.get("at"))
        if at is None or at > moment:
            break
        status = record.get("status")
        if status == "applied" and since is None:
            since = at
        elif status in RELEASE_STATUSES or (status == "error_suspended" and record.get("restored_after_failures")):
            since = None
    return since


def positive_since(decisions, function_id, moment):
    """(decided_at, decision) of the decision that started the positive run in force."""
    start = None
    for d in decisions:
        if d.get("function_id") != function_id:
            continue
        at = parse(d.get("decided_at"))
        if at is None or at > moment:
            break
        if d.get("target_warm_replicas", 0) > 0:
            if start is None:
                start = (at, d)
        else:
            start = None
    return start


def first_forecast_for(forecasts, function_id, bucket_start, threshold, before):
    """Earliest forecast published before `before` predicting demand for that bucket."""
    for f in forecasts:
        if f.get("function_id") != function_id:
            continue
        generated = parse(f.get("generated_at"))
        if generated is None or generated > before:
            continue
        for p in f.get("predictions") or []:
            if parse(p.get("bucket_start")) == bucket_start and (p.get("invocation_count") or 0) >= threshold:
                return generated
    return None


# --- report -------------------------------------------------------------------------
def bursts_of(requests, function_id, gap_s):
    times = sorted(t for t in (parse(r.get("sent_at")) for r in requests
                               if r.get("function_id") == function_id
                               and r.get("request_kind", "user") == "user") if t is not None)
    bursts = []
    for t in times:
        if bursts and (t - bursts[-1][-1]).total_seconds() < gap_s:
            bursts[-1].append(t)
        else:
            bursts.append([t])
    return bursts


def burst_rows(function_id, bursts, spans, decisions, actuation, forecasts, policy, dry_run, have_pods):
    interval = policy.get("interval_seconds") or 5
    threshold = policy.get("demand_threshold", 0.5)
    rows = []
    for index, burst in enumerate(bursts):
        first = burst[0]
        patch = active_patch(actuation, function_id, first)
        positive = positive_since(decisions, function_id, first)
        epoch = first.timestamp()
        bucket = datetime.fromtimestamp(epoch - epoch % interval, tz=timezone.utc)
        published = first_forecast_for(forecasts, function_id, bucket, threshold, first) if forecasts else None
        ready_now = ready_at(spans, function_id, first)
        ready_at_patch = ready_at(spans, function_id, patch) if patch else []
        # A Pod "started by the patch" was created no earlier than 1 s (API resolution) before it.
        started = [s for s in ready_now if patch and s["created"] and s["created"] >= patch - timedelta(seconds=1)]
        if not have_pods:
            outcome = "unknown"
        elif dry_run:
            outcome = "dry_run"
        elif patch and ready_at_patch:
            outcome = "retained"
        elif patch and started:
            outcome = "prewarmed"
        elif patch:   # a Ready Pod not started by the patch counts as Knative's own capacity
            outcome = "late" if not ready_now else "warm_without_forecast"
        else:
            outcome = "warm_without_forecast" if ready_now else "missed"
        pod = min(started or ready_now, key=lambda s: s["ready_from"], default=None)
        if pod is None:   # late or missed: the first Pod that became Ready after the request
            later = [s for s in spans if s["function_id"] == function_id and s["ready_from"] > first]
            pod = min(later, key=lambda s: s["ready_from"], default=None)
        exposed = sum(1 for t in burst if not ready_at(spans, function_id, t)) if have_pods else None
        rows.append({
            "function_id": function_id, "burst": index + 1, "requests": len(burst),
            "first_request_at": stamp(first), "last_request_at": stamp(burst[-1]),
            "forecast_first_saw_burst_at": stamp(published),
            "forecast_used_at": positive[1].get("forecast_generated_at") if positive else None,
            "positive_decision_at": stamp(positive[0]) if positive else None,
            "min_scale_patch_at": stamp(patch),
            "ready_pods_at_patch": len(ready_at_patch) if patch else None,
            "pod": pod["pod"] if pod else None,
            "pod_created_at": stamp(pod["created"]) if pod else None,
            "pod_ready_at": stamp(pod["ready_from"]) if pod else None,
            "ready_margin_s": seconds(pod["ready_from"], first) if pod else None,
            "patch_to_ready_s": seconds(patch, pod["ready_from"]) if pod and patch else None,
            "outcome": outcome, "exposed_requests": exposed,
        })
    return rows


def gap_rows(function_id, bursts, spans, actuation, end):
    rows = []
    for index, burst in enumerate(bursts):
        last = burst[-1]
        until = bursts[index + 1][0] if index + 1 < len(bursts) else end
        if until is None or until <= last:
            continue
        restored = next((parse(a.get("at")) for a in actuation
                         if a.get("function_id") == function_id and a.get("status") in RELEASE_STATUSES
                         and parse(a.get("at")) and last <= parse(a.get("at")) < until), None)
        zero_from = first_zero_after(spans, function_id, last, until)
        gone = [s["gone"] for s in spans if s["function_id"] == function_id and s["gone"]
                and last <= s["gone"] < until]
        span = (until - last).total_seconds()
        rows.append({
            "function_id": function_id, "after_burst": index + 1,
            "gap_s": round(span, 3),
            "min_scale_restored_after_s": seconds(last, restored),
            "no_ready_pod_after_s": seconds(last, zero_from),
            "last_pod_gone_after_s": seconds(last, max(gone)) if gone else None,
            "zero_ready_s": round(span - ready_seconds(spans, function_id, last, until), 3),
            "released": zero_from is not None,
            "final_gap": index + 1 == len(bursts),
        })
    return rows


def fallback_check(decisions, audits, actuation):
    """Fallback must mean target 0 and no min-scale raised or kept by the scheduler."""
    by_key = {(a.get("function_id"), a.get("decided_at")): a for a in audits}
    act = {(a.get("function_id"), a.get("at")): a for a in actuation}
    reasons, violations, fallback = {}, [], 0
    for d in decisions:
        key = (d.get("function_id"), d.get("decided_at"))
        audit = by_key.get(key, {})
        if audit.get("outcome") != "fallback":
            continue
        fallback += 1
        reason = (d.get("reason") or "").split(";")[0].split("(")[0].strip() or "unspecified"
        reasons[reason] = reasons.get(reason, 0) + 1
        result = act.get(key)
        if d.get("target_warm_replicas") != 0:
            violations.append({"at": d.get("decided_at"), "function_id": key[0], "problem": "fallback target not 0"})
        elif result and result.get("status") == "applied":
            violations.append({"at": d.get("decided_at"), "function_id": key[0],
                               "problem": "min-scale applied during fallback"})
    statuses = {}
    for a in actuation:
        statuses[a.get("status")] = statuses.get(a.get("status"), 0) + 1
    return {"fallback_decisions": fallback, "reasons": reasons, "violations": violations,
            "actuation_statuses": statuses, "safe": not violations}


FAILURE_STATUSES = ("error", "error_suspended", "restore_failed")


def actuation_check(actuation):
    """Every failed Kubernetes call, by function and time. Any failure makes the run unsafe
    to quote without explanation; suspended ticks show how long Knative ran reactive-only."""
    problems = [{k: a.get(k) for k in ("function_id", "at", "status", "detail", "revision",
                                       "restored_after_failures", "restore_failed")
                 if a.get(k) is not None}
                for a in actuation if a.get("status") in FAILURE_STATUSES]
    suspended = {}
    for a in actuation:
        if a.get("status") == "suspended":
            suspended[a.get("function_id")] = suspended.get(a.get("function_id"), 0) + 1
    reverts = [{k: a.get(k) for k in ("function_id", "at", "previous_min_scale")}
               for a in actuation if a.get("reverted_by_platform")]
    return {"problems": problems, "suspended_ticks": suspended, "platform_reverts": reverts,
            "safe": not problems and not reverts}


def shutdown_check(actuation, manifest):
    """Each function the scheduler patched must end with a 'restored' record, and the
    scheduler manifest must be final (written after shutdown)."""
    last_applied, last_restored, failed = {}, {}, []
    for a in actuation:
        fn, at = a.get("function_id"), parse(a.get("at"))
        if at is None:
            continue
        if a.get("status") == "applied":
            last_applied[fn] = at
        elif a.get("status") == "restored":
            last_restored[fn] = at
        elif a.get("status") == "restore_failed":
            failed.append({"function_id": fn, "at": a.get("at"), "detail": a.get("detail")})
    not_restored = sorted(fn for fn, at in last_applied.items()
                          if fn not in last_restored or last_restored[fn] < at)
    final = manifest.get("final") is True
    return {"manifest_final": final, "stop_reason": manifest.get("stop_reason"),
            "patched_functions": sorted(last_applied),
            "last_restored_at": {fn: stamp(at) for fn, at in sorted(last_restored.items())},
            "not_restored": not_restored, "restore_failed": failed,
            "safe": final and not not_restored and not failed}


def build(requests, lifecycle, decisions, audits, actuation, manifest, forecasts=(),
          functions=("periodic",), burst_gap=30.0):
    policy = manifest.get("policy") or {}
    dry_run = bool(policy.get("dry_run", manifest.get("actuator") == "dry-run"))
    spans = pod_spans(lifecycle)
    have_pods = bool(lifecycle)
    key = lambda field: (lambda r: parse(r.get(field)) or datetime.min.replace(tzinfo=timezone.utc))
    decisions = sorted(decisions, key=key("decided_at"))
    actuation = sorted(actuation, key=key("at"))
    forecasts = sorted(forecasts, key=key("generated_at"))
    ends = [parse(manifest.get("ended_at"))] + [parse(r.get("completed_at")) for r in requests]
    end = max((e for e in ends if e), default=None)
    bursts, gaps = [], []
    for fn in functions:
        found = bursts_of(requests, fn, burst_gap)
        bursts += burst_rows(fn, found, spans, decisions, actuation, forecasts, policy, dry_run, have_pods)
        if have_pods:
            gaps += gap_rows(fn, found, spans, actuation, end)
    fallback = fallback_check(decisions, audits, actuation)
    has_scheduler = bool(manifest.get("policy"))
    actuation_report = actuation_check(actuation)
    shutdown = shutdown_check(actuation, manifest) if has_scheduler else None
    return {
        "run_id": manifest.get("run_id"),
        "actuator": manifest.get("actuator"),
        "scheduler_final": manifest.get("final"),
        "policy": policy,
        "functions": list(functions),
        "burst_gap_s": burst_gap,
        "outcomes": {o: sum(1 for b in bursts if b["outcome"] == o) for o in OUTCOMES},
        "bursts": bursts,
        "gaps": gaps,
        "fallback": fallback,
        "actuation": actuation_report,
        "shutdown": shutdown,
        "safe": fallback["safe"] and actuation_report["safe"] and (shutdown is None or shutdown["safe"]),
        "resolution_note": "Kubernetes ready_at is 1 s; lifecycle polling adds up to the collector poll interval.",
        "evidence_rule": "Only 'prewarmed' shows a Pod started by the scheduler and Ready before traffic. "
                         "'retained' shows capacity kept from earlier traffic.",
    }


def markdown(report):
    lines = ["# Readiness timeline: %s" % (report.get("run_id") or "run"), "",
             "Actuator `%s`; outcomes: %s" % (report.get("actuator"), ", ".join(
                 "%s %d" % (k, v) for k, v in report["outcomes"].items() if v)), "",
             "| Fn | Burst | First request | Forecast saw burst | Positive decision | Patch | Ready Pods at patch | Pod Ready | Margin s | Outcome | Exposed |",
             "|---|---:|---|---|---|---|---:|---|---:|---|---:|"]
    short = lambda v: v[11:23] if v else "-"
    for b in report["bursts"]:
        lines.append("| %s | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
            b["function_id"], b["burst"], short(b["first_request_at"]), short(b["forecast_first_saw_burst_at"]),
            short(b["positive_decision_at"]), short(b["min_scale_patch_at"]),
            "-" if b["ready_pods_at_patch"] is None else b["ready_pods_at_patch"], short(b["pod_ready_at"]),
            "-" if b["ready_margin_s"] is None else b["ready_margin_s"], b["outcome"],
            "-" if b["exposed_requests"] is None else b["exposed_requests"]))
    if report["gaps"]:
        lines += ["", "| Fn | After burst | Gap s | min-scale restored +s | No Ready Pod +s | Zero-Ready s | Released |",
                  "|---|---:|---:|---:|---:|---:|---|"]
        for g in report["gaps"]:
            lines.append("| %s | %d%s | %s | %s | %s | %s | %s |" % (
                g["function_id"], g["after_burst"], " (final)" if g["final_gap"] else "", g["gap_s"],
                "-" if g["min_scale_restored_after_s"] is None else g["min_scale_restored_after_s"],
                "-" if g["no_ready_pod_after_s"] is None else g["no_ready_pod_after_s"],
                g["zero_ready_s"], "yes" if g["released"] else "NO"))
    f, a, s = report["fallback"], report["actuation"], report["shutdown"]
    lines += ["", "## Safety checks (overall: %s)" % ("OK" if report["safe"] else "PROBLEMS"), "",
              "- Fallback decisions: %d %s; safe: %s" % (f["fallback_decisions"], f["reasons"] or "", f["safe"]),
              "- Actuation failures: %d; platform reverts: %d; suspended ticks: %s; safe: %s" % (
                  len(a["problems"]), len(a["platform_reverts"]), a["suspended_ticks"] or 0, a["safe"])]
    for problem in a["problems"]:
        lines.append("  - %s %s %s: %s" % (problem.get("at"), problem.get("function_id"),
                                           problem.get("status"), problem.get("detail", "")))
    if s is None:
        lines.append("- Shutdown: no scheduler in this run")
    else:
        lines.append("- Shutdown: manifest final %s; stop reason: %s; patched %s; not restored %s; "
                     "restore failures %d; safe: %s" % (
                         s["manifest_final"], s["stop_reason"], s["patched_functions"] or "none",
                         s["not_restored"] or "none", len(s["restore_failed"]), s["safe"]))
    lines += ["", report["resolution_note"], report["evidence_rule"]]
    return "\n".join(lines) + "\n"


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-dir", help="evaluation run directory")
    p.add_argument("--scheduler-dir", help="omit for a reactive run (no scheduler)")
    p.add_argument("--client", help="default <run-dir>/client_measurements.jsonl")
    p.add_argument("--lifecycle", help="default <run-dir>/pod_lifecycle.jsonl")
    p.add_argument("--forecasts")
    p.add_argument("--functions", default="periodic")
    p.add_argument("--burst-gap", type=float, default=30.0)
    p.add_argument("--output", help="JSON report; a .md table is written next to it")
    args = p.parse_args(argv)
    run = Path(args.run_dir) if args.run_dir else None
    client = args.client or (run and run / "client_measurements.jsonl")
    lifecycle = args.lifecycle or (run and run / "pod_lifecycle.jsonl")
    if not client:
        raise SystemExit("--run-dir or --client is required")
    sched = Path(args.scheduler_dir) if args.scheduler_dir else None
    manifest_path = sched / "scheduler_manifest.json" if sched else None
    manifest = json.loads(manifest_path.read_text()) if manifest_path and manifest_path.is_file() else {}
    if sched is None:
        manifest = {"actuator": "none (reactive)", "run_id": run.name if run else None}
        sched = Path("__no_scheduler__")
    lifecycle_rows = read_jsonl(lifecycle) if lifecycle else []
    if not lifecycle_rows:
        print("warning: no pod_lifecycle records; readiness cannot be judged", file=sys.stderr)
    report = build(read_jsonl(client), lifecycle_rows, read_jsonl(sched / "decisions.jsonl"),
                   read_jsonl(sched / "policy_audit.jsonl"), read_jsonl(sched / "actuation.jsonl"),
                   manifest, read_jsonl(args.forecasts) if args.forecasts else [],
                   [f.strip() for f in args.functions.split(",") if f.strip()], args.burst_gap)
    text = markdown(report)
    print(text)
    if args.output:
        out = Path(args.output)
        if out.exists():
            raise SystemExit("refusing to overwrite %s" % out)
        out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        out.with_suffix(".md").write_text(text, encoding="utf-8")
    if not report["safe"]:
        print("SAFETY CHECKS FAILED: see 'Safety checks' above", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

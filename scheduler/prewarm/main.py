"""CLI for the pre-warming scheduler. Run from scheduler/.

Dry run against a live forecast file (no cluster changes):
    python -m prewarm.main --run-id r1 --forecasts ../predictor/data/r1/forecasts.jsonl
Replay a forecast file offline on a simulated clock (always dry run):
    python -m prewarm.main --run-id r2 --forecasts f.jsonl \
        --replay 2026-10-01T00:00:00Z 2026-10-01T01:00:00Z
Change Knative min-scale (only after the CORE-04 ADR says "go"):
    python -m prewarm.main --run-id r3 --forecasts f.jsonl --actuator min-scale \
        --target revision --kube-context kind-csc3104
Put back every min-scale this scheduler changed (e.g. after a crash):
    python -m prewarm.main --restore --kube-context kind-csc3104

Outputs in scheduler/data/<run_id>/ (git-ignored):
    decisions.jsonl         decision contract records, one per function per tick
    policy_audit.jsonl      why: window, peak, requested/granted, fallback outcome
    actuation.jsonl         what the actuator did, readiness, API calls, errors
    forecast_rejects.jsonl  forecast lines refused on input, with reason
    scheduler_manifest.json config, run times, totals
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from .adapter import (DryRunActuator, MinScaleActuator, WarmupActuator, restore_namespace)
from .contracts import format_ts, parse_ts
from .kube import InClusterClient, KubeError, KubectlClient
from .policy import MODES, Policy, PolicyConfig
from .source import FileTail, ForecastBook

DEFAULT_FUNCTIONS = "periodic,steady,sparse"


def _say(*parts):
    try:
        print(*parts, file=sys.stderr, flush=True)
    except OSError:
        pass


def _interrupt(_signum, _frame):
    raise KeyboardInterrupt


class JsonlWriter:
    def __init__(self, path):
        self._fh = open(path, "a", encoding="utf-8")

    def write(self, record):
        self._fh.write(json.dumps(record, separators=(",", ":")) + "\n")
        self._fh.flush()

    def close(self):
        self._fh.close()


class Scheduler:
    """One tick = read new forecasts, decide, actuate, log."""

    def __init__(self, policy, actuator, tail, book, out_dir):
        self.policy = policy
        self.actuator = actuator
        self.tail = tail
        self.book = book
        self._rejects_seen = 0
        self.decisions = JsonlWriter(os.path.join(out_dir, "decisions.jsonl"))
        self.audit = JsonlWriter(os.path.join(out_dir, "policy_audit.jsonl"))
        self.actuation = JsonlWriter(os.path.join(out_dir, "actuation.jsonl"))
        self.rejects = JsonlWriter(os.path.join(out_dir, "forecast_rejects.jsonl"))
        self.stats = Counter()

    def ingest(self):
        for line_no, text in self.tail.read_new():
            self.book.add_line(line_no, text)
        for line_no, reason, raw in self.book.rejected[self._rejects_seen:]:
            self.rejects.write({"line": line_no, "reason": reason, "record": raw})
            self.stats["forecast_rejected"] += 1
        self._rejects_seen = len(self.book.rejected)

    def tick(self, now):
        self.ingest()
        decisions, audits = self.policy.decide(now, self.book.snapshot(now))
        self.stats["ticks"] += 1
        for decision, audit in zip(decisions, audits):
            self.decisions.write(decision)
            self.audit.write(audit)
            self.stats["outcome_" + audit["outcome"]] += 1
            if audit["budget_limited"]:
                self.stats["budget_limited"] += 1
            result = self.actuator.apply(decision["function_id"], decision["target_warm_replicas"], now)
            self.actuation.write(result)
            self.stats["actuation_" + result["status"]] += 1
        return decisions

    def shutdown(self, now):
        for result in self.actuator.restore_all(now):
            self.actuation.write(result)
            self.stats["actuation_" + result["status"]] += 1
        for writer in (self.decisions, self.audit, self.actuation, self.rejects):
            writer.close()


def build_parser():
    p = argparse.ArgumentParser(prog="prewarm.main", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id")
    p.add_argument("--forecasts", help="forecast-contract JSONL file (tailed)")
    p.add_argument("--functions", default=DEFAULT_FUNCTIONS)
    p.add_argument("--out-dir", default="data")
    p.add_argument("--actuator", choices=("dry-run", "min-scale", "warmup"), default="dry-run")
    p.add_argument("--target", choices=("revision", "pa"), default="revision",
                   help="min-scale actuator: object to annotate (decided by CORE-04 spike)")
    p.add_argument("--backend", choices=("kubectl", "in-cluster", "none"), default="kubectl")
    p.add_argument("--kube-context")
    p.add_argument("--namespace", default="csc3104")
    p.add_argument("--ingress-url", default="http://127.0.0.1:8080/")
    p.add_argument("--host-template", default="{fn}.csc3104.example.com")
    p.add_argument("--max-failures", type=int, default=3)
    p.add_argument("--backoff", type=float, default=60.0)
    p.add_argument("--duration", type=float, help="stop after this many seconds")
    p.add_argument("--replay", nargs=2, metavar=("START", "END"),
                   help="simulated clock over [START, END); forces dry run, no cluster")
    p.add_argument("--restore", action="store_true", help="restore marked min-scale values and exit")
    defaults = PolicyConfig()
    p.add_argument("--interval", type=int, default=defaults.interval_seconds)
    p.add_argument("--tick", type=float, default=defaults.tick_seconds)
    p.add_argument("--lead-time-ms", type=int, default=defaults.lead_time_ms)
    p.add_argument("--per-function-cap", type=int, default=defaults.per_function_cap)
    p.add_argument("--total-budget", type=int, default=defaults.total_budget)
    p.add_argument("--cooldown", type=float, default=defaults.cooldown_seconds)
    p.add_argument("--threshold", type=float, default=defaults.demand_threshold)
    p.add_argument("--mode", choices=MODES, default=defaults.mode)
    p.add_argument("--service-ms", type=float, default=defaults.service_ms)
    p.add_argument("--target-concurrency", type=int, default=defaults.target_concurrency)
    p.add_argument("--target-utilisation", type=float, default=defaults.target_utilisation)
    p.add_argument("--max-forecast-age", type=float, default=defaults.max_forecast_age_seconds)
    p.add_argument("--forecast-delay", type=float, default=10.0,
                   help="predictor format: seconds after forecast_bucket_start that a forecast "
                        "is assumed to exist (telemetry finalisation grace)")
    return p


def make_client(args):
    if args.backend == "none":
        return None
    if args.backend == "in-cluster":
        return InClusterClient(args.namespace)
    return KubectlClient(args.namespace, context=args.kube_context)


def make_actuator(args, client):
    if args.replay or args.actuator == "dry-run":
        return DryRunActuator(None if args.replay else client)
    if args.actuator == "warmup":
        return WarmupActuator(args.ingress_url, args.host_template, client=client)
    if client is None:
        raise SystemExit("--actuator min-scale needs a Kubernetes backend")
    return MinScaleActuator(client, args.target, args.max_failures, args.backoff)


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.restore:
        client = make_client(args)
        for name, original in restore_namespace(client, args.target):
            _say("restored %s %s min-scale -> %s" % (args.target, name, original or "(removed)"))
        return 0
    if not args.run_id or not args.forecasts:
        raise SystemExit("--run-id and --forecasts are required")

    functions = [f.strip() for f in args.functions.split(",") if f.strip()]
    config = PolicyConfig(
        interval_seconds=args.interval, lead_time_ms=args.lead_time_ms, tick_seconds=args.tick,
        per_function_cap=args.per_function_cap, total_budget=args.total_budget,
        cooldown_seconds=args.cooldown, demand_threshold=args.threshold, mode=args.mode,
        service_ms=args.service_ms, target_concurrency=args.target_concurrency,
        target_utilisation=args.target_utilisation, max_forecast_age_seconds=args.max_forecast_age,
        dry_run=bool(args.replay) or args.actuator == "dry-run")
    client = None if args.replay else make_client(args)
    actuator = make_actuator(args, client)

    out_dir = os.path.join(args.out_dir, args.run_id)
    if os.path.exists(os.path.join(out_dir, "scheduler_manifest.json")):
        raise SystemExit("run %s already exists in %s; choose a new --run-id" % (args.run_id, out_dir))
    os.makedirs(out_dir, exist_ok=True)
    scheduler = Scheduler(Policy(config, functions), actuator, FileTail(args.forecasts),
                          ForecastBook(functions, args.interval, args.forecast_delay), out_dir)
    started = datetime.now(timezone.utc)
    _say("scheduler run %s: actuator=%s functions=%s dry_run=%s"
         % (args.run_id, actuator.name, ",".join(functions), config.dry_run))

    signal.signal(signal.SIGINT, _interrupt)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _interrupt)
    def write_manifest(final):
        manifest = {
            "final": final,
            "run_id": args.run_id, "started_at": format_ts(started),
            "ended_at": format_ts(datetime.now(timezone.utc)) if final else None,
            "clock": "replay" if args.replay else "wall", "replay": args.replay,
            "functions": functions, "actuator": actuator.name, "forecasts": args.forecasts,
            "namespace": args.namespace, "kube_context": args.kube_context,
            "policy": config.to_dict(), "forecasts_accepted": scheduler.book.accepted,
            "predictor_format_rules": {
                "history_end": "forecast_bucket_start",
                "generated_at": "actual generated_at for shared records; legacy records use forecast_bucket_start + forecast_delay_s",
                "predictions": "forecast count for forecast_bucket_start and the next bucket",
                "valid_until": "forecast_bucket_start + 2 x interval",
                "forecast_delay_s": args.forecast_delay},
            "totals": dict(sorted(scheduler.stats.items())),
            "kube_api_calls": getattr(client, "calls", 0),
            "platform_reverts": getattr(actuator, "reverts", 0),
        }
        temporary = os.path.join(out_dir, "scheduler_manifest.json.tmp")
        with open(temporary, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2)
        os.replace(temporary, os.path.join(out_dir, "scheduler_manifest.json"))

    write_manifest(final=False)
    last = started
    try:
        if args.replay:
            now, end = parse_ts(args.replay[0]), parse_ts(args.replay[1])
            step = timedelta(seconds=args.tick)
            while now < end:
                scheduler.tick(now)
                last = now
                now += step
        else:
            stop_at = time.monotonic() + args.duration if args.duration else None
            while stop_at is None or time.monotonic() < stop_at:
                tick_start = time.monotonic()
                last = datetime.now(timezone.utc)
                scheduler.tick(last)
                time.sleep(max(0.0, args.tick - (time.monotonic() - tick_start)))
    except KeyboardInterrupt:
        _say("stopping: restoring original min-scale values")
    finally:
        scheduler.shutdown(last if args.replay else datetime.now(timezone.utc))
        write_manifest(final=True)
        _say("wrote %s" % out_dir)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KubeError as exc:
        _say("error: %s" % exc)
        sys.exit(1)

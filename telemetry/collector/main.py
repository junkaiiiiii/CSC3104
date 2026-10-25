"""CLI.

Replay a file:   python -m collector.main --run-id r1 --input events.jsonl
Pipe:            docker logs -f <c> | python -m collector.main --run-id r2 --functions periodic
Live cluster:    python -m collector.main --run-id r3 --source k8s --kube-context kind-csc3104
"""
from __future__ import annotations

import argparse
import os
import queue
import shutil
import signal
import sys
import time
from datetime import datetime, timezone

from .collector import RAW, Collector
from .sources import iter_lines


def _say(*parts) -> None:
    try:
        print(*parts, file=sys.stderr)
    except OSError:  # terminal already closed
        pass


def _interrupt(_signum, _frame):
    raise KeyboardInterrupt


def _handle(collector, item) -> None:
    kind = item[0]
    if kind == "line":
        collector.process_line(item[2], pod=item[1])
    elif kind == "lifecycle":
        collector.record_lifecycle(item[1])
    elif kind == "gap":
        collector.mark_gap(item[2], item[3], item[1])
        _say("warning: observation gap %s -> %s (%s)" % (
            item[2].strftime("%H:%M:%S"), item[3].strftime("%H:%M:%S"), item[1] or "all functions"))
    elif kind == "error":
        _say("warning: " + item[1])


def _pump(collector, source, seconds=None) -> None:
    """Handle queued items until interrupted (or until `seconds` have passed)."""
    deadline = None if seconds is None else time.time() + seconds
    while deadline is None or time.time() < deadline:
        try:
            _handle(collector, source.out.get(timeout=0.5))
        except queue.Empty:
            collector.tick()


def run_cluster(args, collector) -> None:
    from .k8s import KubeSource
    source = KubeSource(args.namespace, collector.functions, context=args.kube_context,
                        poll_interval_s=args.poll_interval)
    source.start()
    _say("collecting from namespace %s (Ctrl-C to stop)" % args.namespace)
    try:
        _pump(collector, source)
    except KeyboardInterrupt:
        pass
    wait = args.grace + args.poll_interval + 1
    _say("stopping: draining %.0fs for late log lines (Ctrl-C again to skip)" % wait)
    try:
        _pump(collector, source, wait)
    except KeyboardInterrupt:
        pass
    source.stop()
    while True:
        try:
            _handle(collector, source.out.get_nowait())
        except queue.Empty:
            break


def run_stream(args, collector, clock) -> None:
    stream = sys.stdin if args.input == "-" else open(args.input, encoding="utf-8")
    try:
        for line in iter_lines(stream, idle_tick_s=1.0 if clock == "wall" else None):
            if line is None:
                collector.tick()
            else:
                collector.process_line(line)
    except KeyboardInterrupt:
        pass


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ"))
    parser.add_argument("--functions", default="periodic,steady,sparse", help="comma-separated function_id list")
    parser.add_argument("--source", choices=("file", "k8s"), default="file")
    parser.add_argument("--input", default="-", help="[file source] JSONL file of events, or - for stdin")
    parser.add_argument("--out", default="data", help="output root; a run directory is created inside")
    parser.add_argument("--interval", type=int, default=30)
    parser.add_argument("--grace", type=float, default=10)
    parser.add_argument("--clock", choices=("event", "wall"), help="default: event for files, wall for stdin/k8s")
    parser.add_argument("--namespace", default="csc3104", help="[k8s source]")
    parser.add_argument("--kube-context", default=None, help="[k8s source] e.g. kind-csc3104")
    parser.add_argument("--poll-interval", type=float, default=2.0, help="[k8s source] seconds between pod listings")
    args = parser.parse_args(argv)
    for name in ("SIGTERM", "SIGHUP"):  # shut down cleanly if killed or the terminal closes
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), _interrupt)

    if args.source == "k8s":
        if args.clock == "event":
            parser.error("--source k8s needs the wall clock")
        if shutil.which("kubectl") is None:
            parser.error("kubectl not found on PATH")
        clock, source_name = "wall", "k8s:" + args.namespace
    else:
        clock = args.clock or ("wall" if args.input == "-" else "event")
        source_name = "stdin" if args.input == "-" else args.input

    run_dir = os.path.join(args.out, args.run_id)
    if os.path.exists(os.path.join(run_dir, RAW)):
        parser.error("run directory %s already has data; choose a new --run-id" % run_dir)

    collector = Collector(run_dir, args.functions.split(","), args.run_id,
                          args.interval, args.grace, clock=clock, source=source_name)
    if args.source == "k8s":
        run_cluster(args, collector)
    else:
        run_stream(args, collector, clock)
    ok = collector.close(flush=(clock == "wall"))
    t = collector.totals
    _say("run %s: %d lines, %d counted, %d warmup, %d duplicate, %d late, %d quarantined, "
          "%d buckets (%d incomplete), %d pod events; conservation %s" % (
              args.run_id, t["lines"], t["counted"], t["warmup"],
              t["duplicate"] + t["conflicting_duplicate"], t["late"], t["quarantined"],
              t["buckets"], t["incomplete_buckets"], t["pod_events"], "OK" if ok else "FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

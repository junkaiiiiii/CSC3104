"""CORE-04 actuation spike: which way of raising Knative min-scale actually works?

Run from scheduler/ with the infra cluster up (bash infra/scripts/up.sh) and no traffic:
    python -m spike.actuation_spike --kube-context kind-csc3104 --function periodic \
        --methods revision,pa --trials 3 --out ../results/spike1

Each trial, per method:
  1. wait until the function has zero pods (scale-to-zero baseline)
  2. record revisions, Revision + PodAutoscaler annotations and PA desired/actual scale
  3. set min-scale=1 by the method, then sample every 0.5s:
       time until a Ready pod exists        -> decision-to-Ready latency (lead time input)
  4. hold for --hold seconds (longer than Knative's 60s stable window) and check that the
     pod stays and the annotation is not reverted by a Knative controller
  5. put the original value back and time how long until zero pods
  6. compare revisions before/after (revision churn)

Methods:
  revision  merge-patch the active Revision's annotations (adapter default)
  pa        merge-patch the PodAutoscaler's annotations
  service   patch the Service template (expected to create a new revision; opt-in, for
            evidence only; restore also creates one)

Writes samples.jsonl, trials.jsonl and summary.json. Fill the results into
docs/decisions/0001-knative-actuation.md.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

from prewarm.adapter import MARKER, MIN_SCALE, observe_readiness
from prewarm.kube import KubeError, KubectlClient

SERVICE_LABEL = "serving.knative.dev/service"


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def percentile(values, q):
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    k = (len(values) - 1) * q        # linear interpolation (numpy default)
    lo = int(k)
    hi = min(lo + 1, len(values) - 1)
    return round(values[lo] + (values[hi] - values[lo]) * (k - lo), 1)


class Spike:
    def __init__(self, client, function_id, out_dir, poll_s=0.5):
        self.c = client
        self.fn = function_id
        self.poll_s = poll_s
        self.samples = open(os.path.join(out_dir, "samples.jsonl"), "a", encoding="utf-8")

    def revisions(self):
        return sorted(r["metadata"]["name"]
                      for r in self.c.list("revision", "%s=%s" % (SERVICE_LABEL, self.fn)))

    def active_revision(self):
        return self.c.get("ksvc", self.fn)["status"]["latestReadyRevisionName"]

    def state(self, revision):
        def ann(kind):
            try:
                return (self.c.get(kind, revision)["metadata"].get("annotations") or {}).get(MIN_SCALE)
            except KubeError:
                return "?"
        try:
            pa = self.c.get("pa", revision)
            pa_status = pa.get("status") or {}
            pa_ann = (pa["metadata"].get("annotations") or {}).get(MIN_SCALE)
        except KubeError:
            pa_status, pa_ann = {}, "?"
        ready = observe_readiness(self.c, self.fn) or {"ready_pods": None, "pods": None}
        return {"revision_min_scale": ann("revision"), "pa_min_scale": pa_ann,
                "pa_desired_scale": pa_status.get("desiredScale"),
                "pa_actual_scale": pa_status.get("actualScale"), **ready}

    def log(self, trial, phase, t0, revision):
        record = {"trial": trial, "phase": phase, "at": now_iso(),
                  "t_ms": round((time.monotonic() - t0) * 1000, 1), **self.state(revision)}
        self.samples.write(json.dumps(record) + "\n")
        self.samples.flush()
        return record

    def wait_for_zero(self, timeout_s):
        end = time.monotonic() + timeout_s
        while time.monotonic() < end:
            if (observe_readiness(self.c, self.fn) or {}).get("pods") == 0:
                return True
            time.sleep(2)
        return False

    def set_min_scale(self, method, revision, value, original):
        if method == "service":
            self.c.merge_patch("ksvc", self.fn, {"spec": {"template": {"metadata": {
                "annotations": {MIN_SCALE: value}}}}})
            return
        kind = "revision" if method == "revision" else "pa"
        marker = None if value == original else (original if original is not None else "absent")
        self.c.merge_patch(kind, revision, {"metadata": {"annotations": {
            MIN_SCALE: value, MARKER: marker}}})

    def trial(self, trial_id, method, hold_s, ready_timeout_s, zero_timeout_s):
        result = {"trial": trial_id, "method": method, "function_id": self.fn, "started_at": now_iso()}
        if not self.wait_for_zero(zero_timeout_s):
            result["error"] = "function did not reach zero pods before the trial"
            return result
        before = self.revisions()
        revision = self.active_revision()
        start_state = self.state(revision)
        original = start_state["revision_min_scale"] if method != "pa" else start_state["pa_min_scale"]
        if method == "service":
            original = (self.c.get("ksvc", self.fn)["spec"]["template"]["metadata"]
                        .get("annotations") or {}).get(MIN_SCALE)
        result.update(revision=revision, revisions_before=before, original_min_scale=original)

        t0 = time.monotonic()
        self.set_min_scale(method, revision, "1", original)
        result["patched_at"] = now_iso()
        ready_ms = None
        reverted = False
        watched = "pa_min_scale" if method == "pa" else "revision_min_scale"
        while time.monotonic() - t0 < ready_timeout_s:
            rec = self.log(trial_id, "warming", t0, revision)
            if method != "service" and rec[watched] != "1":
                reverted = True          # a controller put the old value back
            if (rec.get("ready_pods") or 0) >= 1:
                ready_ms = rec["t_ms"]
                break
            time.sleep(self.poll_s)
        result["patch_to_ready_ms"] = ready_ms

        held, hold_end = True, time.monotonic() + hold_s
        while ready_ms is not None and time.monotonic() < hold_end:
            rec = self.log(trial_id, "holding", t0, revision)
            if (rec.get("ready_pods") or 0) < 1:
                held = False
            if method != "service" and rec[watched] != "1":
                reverted = True
            time.sleep(2)
        hold_state = self.state(revision)
        result.update(held_through_stable_window=held if ready_ms is not None else None,
                      annotation_reverted=reverted, state_after_hold=hold_state)

        t1 = time.monotonic()
        self.set_min_scale(method, revision, original, original)
        # Two release times: Ready capacity gone (what the scheduler controls) and the
        # pod object gone (Terminating can take minutes; it still holds its requests).
        zero_ms = unready_ms = None
        while time.monotonic() - t1 < zero_timeout_s:
            rec = self.log(trial_id, "releasing", t0, revision)
            elapsed = round((time.monotonic() - t1) * 1000, 1)
            if unready_ms is None and (rec.get("ready_pods") or 0) == 0:
                unready_ms = elapsed
            if rec.get("pods") == 0:
                zero_ms = elapsed
                break
            time.sleep(1)
        after = self.revisions()
        result.update(release_to_unready_ms=unready_ms, release_to_zero_ms=zero_ms, revisions_after=after,
                      new_revisions=sorted(set(after) - set(before)),
                      active_revision_after=self.active_revision(), ended_at=now_iso())
        return result


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--kube-context")
    p.add_argument("--namespace", default="csc3104")
    p.add_argument("--function", default="periodic")
    p.add_argument("--methods", default="revision,pa")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--hold", type=float, default=90.0)
    p.add_argument("--ready-timeout", type=float, default=120.0)
    p.add_argument("--zero-timeout", type=float, default=300.0)
    p.add_argument("--out", required=True)
    args = p.parse_args(argv)
    os.makedirs(args.out, exist_ok=True)
    client = KubectlClient(args.namespace, context=args.kube_context)
    spike = Spike(client, args.function, args.out)
    methods = [m.strip() for m in args.methods.split(",") if m.strip()]
    trials = []
    with open(os.path.join(args.out, "trials.jsonl"), "a", encoding="utf-8") as fh:
        for n in range(args.trials):
            for method in (methods if n % 2 == 0 else list(reversed(methods))):  # vary order
                trial_id = "%s-%d" % (method, n)
                print("trial %s ..." % trial_id, file=sys.stderr, flush=True)
                try:
                    result = spike.trial(trial_id, method, args.hold, args.ready_timeout, args.zero_timeout)
                except KubeError as exc:
                    result = {"trial": trial_id, "method": method, "error": str(exc)}
                fh.write(json.dumps(result) + "\n")
                fh.flush()
                trials.append(result)
                print(json.dumps({k: result.get(k) for k in (
                    "trial", "patch_to_ready_ms", "held_through_stable_window",
                    "annotation_reverted", "release_to_unready_ms", "release_to_zero_ms", "new_revisions", "error")}),
                    file=sys.stderr, flush=True)
    summary = {}
    for method in methods:
        rows = [t for t in trials if t.get("method") == method]
        ok = [t for t in rows if "error" not in t]
        summary[method] = {
            "trials": len(rows), "errors": len(rows) - len(ok),
            "patch_to_ready_ms": [t.get("patch_to_ready_ms") for t in ok],
            "patch_to_ready_p50_ms": percentile([t.get("patch_to_ready_ms") for t in ok], 0.5),
            "patch_to_ready_p95_ms": percentile([t.get("patch_to_ready_ms") for t in ok], 0.95),
            "all_held": all(t.get("held_through_stable_window") for t in ok) if ok else None,
            "any_reverted": any(t.get("annotation_reverted") for t in ok),
            "new_revisions": sorted({r for t in ok for r in t.get("new_revisions", [])}),
            "release_to_unready_ms": [t.get("release_to_unready_ms") for t in ok],
            "release_to_zero_ms": [t.get("release_to_zero_ms") for t in ok],
        }
    summary_doc = {"function_id": args.function, "namespace": args.namespace,
                   "kube_context": args.kube_context, "generated_at": now_iso(),
                   "hold_s": args.hold, "methods": summary}
    with open(os.path.join(args.out, "summary.json"), "w", encoding="utf-8") as fh:
        json.dump(summary_doc, fh, indent=2)
    print(json.dumps(summary_doc, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

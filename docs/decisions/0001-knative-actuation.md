# ADR 0001: How the scheduler prepares Knative capacity (CORE-04)

| | |
|---|---|
| Status | **Accepted (26 Oct 2026).** Option A: set min-scale on the active Revision. Measured on the local kind cluster, image cached. |
| Lead | Isaac (with Vania, Hasan) |
| Platform | Knative Serving v1.23.0, Kourier v1.23.0, Kubernetes v1.35.8 on kind (see infra/VERSIONS.md) |
| Spike tool | `scheduler/spike/actuation_spike.py` |
| Adapter | `scheduler/prewarm/adapter.py` |

## Context
The scheduler must make a function have a Ready pod *before* predicted demand arrives, and let it return to zero afterwards. Knative's autoscaler (KPA) owns the Deployment, so writing replica counts directly would fight it (docs/03-architecture.md). The supported lever is the `autoscaling.knative.dev/min-scale` annotation, but where it is set matters:

- On the **Service template**, every change creates a new Revision. Changing it every 30 seconds would cause revision churn, route changes and retired pods. That is not acceptable for a controller loop.
- On the **Revision**, the annotation might be copied to the PodAutoscaler and take effect without a new revision. Whether Knative 1.23 does this needs to be measured.
- On the **PodAutoscaler (PA)**, the KPA reads its scale bounds from the PA's annotations. But the Revision reconciler might overwrite the change from the Revision.

## Options
| Option | What it changes | Expected risk | Proves |
|---|---|---|---|
| A. Revision annotation (adapter default) | `revisions.serving.knative.dev/<rev>` metadata | Change may not reach the PA | Replica control |
| B. PodAutoscaler annotation | `podautoscalers.autoscaling.internal.knative.dev/<rev>` metadata | Revision reconciler may revert it (internal API) | Replica control |
| C. Service template | `services.serving.knative.dev/<fn>` spec | New revision on every change | Evidence only, not for the loop |
| D. Warm-up requests | HTTP request with `X-Request-Kind: warmup` | Pod leaves after the stable window unless repeated; uses resources | Predictive activation only |

## Procedure
With the cluster from `bash infra/scripts/up.sh` and no traffic, from `scheduler/`:
```bash
python -m spike.actuation_spike --kube-context kind-csc3104 --function periodic \
    --methods revision,pa --trials 3 --out ../results/spike-ab
python -m spike.actuation_spike --kube-context kind-csc3104 --function steady \
    --methods service --trials 1 --out ../results/spike-c
```
Each trial starts from zero pods, sets min-scale=1, times **patch → first Ready pod**, holds 90s (longer than the 60s stable window) watching for pod loss and annotation reverts, restores the original value, times **release → zero pods**, and compares the revision list before and after. Method order alternates between trials. Raw files: `samples.jsonl`, `trials.jsonl`, `summary.json`. Store them outside git and record a link and hash here.

## Results
Two spike runs on 26 Oct 2026 (function `periodic`, revision `periodic-00001`, hold 90s, Isaac's laptop, Windows + Docker Desktop + kind). Option C was not run; Knative documents that template changes create revisions.

| Method | Trials | Patch→Ready (ms) | p50 / p95 (ms) | Held 90s? | Reverted? | New revisions | Ready→0 after restore | Pod object gone |
|---|---|---|---|---|---|---|---|---|
| **A. Revision** | 6 | 956, 1627, 2056, 1819, 1958, 1843 | **1831 / 2032** | **6/6 yes** | **never** | **none** | ≤ 1 s (first sample) | > 300 s (Terminating; timed out) |
| B. PA | 5 | Ready in 2 of 5 (1860, 1755); no pod in 3 | n/a | **0/2**: lost at ~62 s | **always**, within 0.4 s | none | ≤ 1 s | ~269 s |

What the samples show (`samples.jsonl`):
- **A:** the Revision annotation reaches the PA within one sample (about 0.1–0.4 s). PA desired/actual scale go to 1 and stay at 1 for the whole hold. After the restore, desired/actual/Ready all drop to 0 at the first sample. The pod object then stays `Terminating` for minutes, the same slow termination seen in CORE-03.
- **B:** the Revision reconciler copies the Revision's annotations back onto the PA, so the patched value returns to 0 within 0.4 s. KPA sometimes starts a pod from the brief desired=1. That pod is removed when the stable window ends (~62 s), and in other trials the pod disappears immediately. Not usable.
- The active revision stayed `periodic-00001` throughout; `kubectl get ksvc` afterwards still shows it as LATESTREADY.

Image cached on node: **yes** (later trials of a warm cluster). Uncached starts will be slower and must be measured separately.

The first spike version only checked for reverts during the hold, so B trials with no Ready pod were logged as `annotation_reverted: false`. The samples show the revert, and the script now also checks while warming. It also now records `release_to_unready_ms` separately from `release_to_zero_ms`.

Raw data: `results/spike1/` on Isaac's machine (git-ignored; two runs appended to the same files). Share it outside git and record the link here. SHA-256 at time of writing (the second run was still appending):
- samples.jsonl `a0396c48d6c9bd082cbfb493f8f2bd152f9a164e0bf8e1013de75f5f1b4c9f90`
- trials.jsonl `ba63ac867972f3432a247731890a9dfd5c9781b2cea2043689c0b4c8254a0200`
- summary.json (first run) `de0ff1d8afa370f044e43727f8a5e53a5cb75cfa4c6b02cef19864a3767f888a`

## Decision rule
Pick the first of A, B that has, across every trial: a Ready pod after the patch, the pod held for the whole hold, no revert, **no new revisions**, and a return to zero after restore. Then:

- set the experiment `--lead-time-ms` to the measured **p95 patch→Ready** plus a safety margin (record the margin), replacing the 5000 ms placeholder in `scheduler/prewarm/policy.py`;
- run the scheduler with `--actuator min-scale --target revision|pa` to match.

If neither A nor B passes, fall back to D (`--actuator warmup`), say in the report that it shows predictive *activation*, not replica control, and count warm-up requests and their pods as overhead. C is never used by the scheduler.

## Decision
**Option A: merge-patch `autoscaling.knative.dev/min-scale` on the active Revision** (`--actuator min-scale --target revision`, the adapter default). It passed every criterion in all 6 trials. Option B is rejected: Knative reverts it. Option C is never used. Option D stays as the documented fallback only.

**Lead time:** p95 patch→Ready = 2032 ms. The scheduler keeps `lead_time_ms = 5000`, which is p95 plus a ~3 s margin for scheduler tick jitter, API latency and slower starts. This is for cached images on this laptop; re-measure on any other host.

**Resource accounting consequence:** the scheduler releases Ready capacity within about a second, but the pod object (and its CPU/memory requests) stays for minutes while Terminating. The evaluation must count provisioned time up to `pod_gone` (telemetry contract), not just Ready time, for every policy.

## Safety behaviour already in the adapter (CORE-10)
- It only patches Revision or PA metadata. It never edits the Service, so it never creates revisions.
- It stores the original value on the object (`prewarm.csc3104.dev/original-min-scale`) before the first change. Stopping the scheduler (Ctrl-C or SIGTERM) restores every change. After a crash, `python -m prewarm.main --restore` restores anything still marked.
- It never goes below the original min-scale, so the fixed-minimum baseline is untouched, and never above max-scale.
- No write is made when the object already has the wanted value.
- After 3 failed API calls in a row for a function, it restores that function and leaves it to Knative's reactive autoscaler for 60s.
- If a new revision becomes active, the old one is restored before the new one is changed.
- Reverts by a Knative controller are detected and counted (`platform_reverts` in the run manifest).
- Missing, invalid, stale or future-dated forecasts fall back to 0 proactive replicas (reactive Knative).

## Permissions the scheduler needs (input to CORE-11, Vania)
All namespace-scoped to `csc3104`. No create or delete on anything.

| API group | Resource | Verbs | Why |
|---|---|---|---|
| serving.knative.dev | services | get, list | Find the active revision |
| serving.knative.dev | revisions | get, list, patch | Read and set min-scale (option A chosen) |
| autoscaling.internal.knative.dev | podautoscalers | get, list | Read only, for observing desired/actual scale. No patch, since B was rejected |
| "" (core) | pods | get, list | Observe Ready capacity |

## Limitations
- One single-node kind cluster with cached images. Patch→Ready times will differ on other hardware or with uncached images.
- Measured on one function, one revision and one machine, with 6 trials. The p95 from 6 samples is rough; add trials before the final report.
- Release→zero (pod fully gone) exceeded the 300s timeout for every A trial, so its exact value is unknown. Re-run with `--zero-timeout 600` to measure it.
- The 30s buckets and 5s tick limit how early capacity can be prepared. The lead time comes from measurement, not theory.

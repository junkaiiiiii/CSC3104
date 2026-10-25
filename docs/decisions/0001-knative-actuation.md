# ADR 0001: How the scheduler prepares Knative capacity (CORE-04)

| | |
|---|---|
| Status | **Proposed. Waiting for spike measurements.** Do not run `--actuator min-scale` in experiments until this says Accepted. |
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

## Results (fill in from summary.json)
| Method | Trials | Patch→Ready p50 / p95 (ms) | Held 90s? | Reverted? | New revisions | Release→zero (ms) |
|---|---|---|---|---|---|---|
| A. Revision | | | | | | |
| B. PA | | | | | | |
| C. Service | | | | | | |

Image cached on node: yes / no. Raw data link and SHA-256:

## Decision rule
Pick the first of A, B that has, across every trial: a Ready pod after the patch, the pod held for the whole hold, no revert, **no new revisions**, and a return to zero after restore. Then:

- set the experiment `--lead-time-ms` to the measured **p95 patch→Ready** plus a safety margin (record the margin), replacing the 5000 ms placeholder in `scheduler/prewarm/policy.py`;
- run the scheduler with `--actuator min-scale --target revision|pa` to match.

If neither A nor B passes, fall back to D (`--actuator warmup`), say in the report that it shows predictive *activation*, not replica control, and count warm-up requests and their pods as overhead. C is never used by the scheduler.

## Decision
_Pending spike results._

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
| serving.knative.dev | revisions | get, list, patch (patch only if A is chosen) | Read and set min-scale |
| autoscaling.internal.knative.dev | podautoscalers | get, list, patch (patch only if B is chosen) | Read and set min-scale |
| "" (core) | pods | get, list | Observe Ready capacity |

## Limitations
- One single-node kind cluster with cached images. Patch→Ready times will differ on other hardware or with uncached images.
- Changing PA annotations (option B) depends on an internal Knative API that may change between versions.
- The 30s buckets and 5s tick limit how early capacity can be prepared. The lead time comes from measurement, not theory.

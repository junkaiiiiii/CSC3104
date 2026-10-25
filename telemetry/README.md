# telemetry
Lead: Felix.

Collect user invocation events, deduplicate request IDs, preserve missing data, aggregate 30-second buckets and observe replica resources.

## Status
| Piece | State |
|---|---|
| Validation, dedup, 30s buckets, audit outputs, conservation check (CORE-05/06) | Implemented, unit-tested, run against real benchmark output |
| Kubernetes log source (`kubectl logs -f` per pod, reconnect, gap detection) | Implemented; tested only against a fake kubectl, **not yet run on a real cluster** |
| Pod lifecycle observer (`pod_lifecycle.jsonl`, contract PROPOSED in docs/04-contracts.md) | Implemented; same caveat; contract needs review |
| Actual CPU/memory usage | Not collected: no metrics-server on the cluster |
| Measurement integrity validator (CORE-16) | Not started |

No cluster measurements exist yet.

## Run
Standard library only (Python 3.9+). From `telemetry/`:
```bash
# live cluster (start BEFORE sending traffic, with `kubectl get pods -n csc3104` empty)
python3 -m collector.main --source k8s --kube-context kind-csc3104 --run-id r1
# replay a captured file (deterministic, event-time clock)
python3 -m collector.main --run-id r2 --input events.jsonl
# live pipe
docker logs -f <container> | python3 -m collector.main --run-id r3 --functions periodic
# tests
python3 -m unittest discover -s tests -t .
python3 tests/e2e_local.py
```
Ctrl-C stops a live run: it drains for grace+poll seconds, then writes the final bucket as `complete: false` (observation ended inside it). Wait at least 40s after your last request before stopping.

Output goes to `telemetry/data/<run_id>/` (git-ignored; keep large raw data outside git):

| File | Content |
|---|---|
| `demand_buckets.jsonl` | Demand-bucket contract records, one per function per finalised bucket |
| `raw_events.jsonl` | Every valid event with its outcome (counted, warmup, duplicate, conflicting_duplicate, late) and, from the cluster, the pod |
| `pod_lifecycle.jsonl` | pod_seen / pod_ready / pod_unready / pod_terminating / pod_gone with API timestamps and resource requests |
| `quarantine.jsonl` | Malformed lines, wrong schema version, unknown function_id, with reason |
| `collector_stats.jsonl` | Counter snapshots, including pending |
| `telemetry_manifest.json` | Run config and final totals, with `conservation_ok` |

## Semantics
- Buckets are 30s UTC, assigned by `started_at`; a bucket is final at `end + grace` (default 10s, tune from pilot latency).
- Empty bucket with a healthy collector is a confirmed 0; a bucket overlapping a recorded gap is `complete: false`.
- Late events are counted separately, never added to a finalised bucket.
- Warmups stay out of demand but are kept for overhead accounting. Probes are never emitted by the function.
- Dedup key is `(function_id, request_id)`; request IDs must be unique per run.
- Every input line is accounted for exactly once; a run with `conservation_ok: false` must be rejected.

## CORE-16: run validation

After a replay, check that the run's measurements are trustworthy before analysing it:

```bash
cd telemetry
python3 -m collector.validate --run-dir ../results/<run>        # telemetry files in the same dir
python3 -m collector.validate --run-dir ../results/<run> --telemetry-dir data/<collector-run>
```

Reads `run_manifest.json` + `client_measurements.jsonl` (evaluation harness) and the collector
artifacts, writes `telemetry_validation.json` (status, error/warning codes, file SHA-256s).
Exit code 0 = pass, 1 = fail, 2 = unusable input. Errors reject the run (lost or unattributed
telemetry, incomplete buckets in the window, late/conflicting events, collector not cleanly
finished or not covering the run, bucket sums off, unknown pods). Warnings are reported only
(pre-run pods, pods alive at end, slow termination). It never repairs data.

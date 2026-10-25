# telemetry
Lead: Felix.

Collect user invocation events, deduplicate request IDs, preserve missing data, aggregate 30-second buckets and observe replica resources.

## Status
| Piece | State |
|---|---|
| Validation, dedup, 30s buckets, audit outputs, conservation check (CORE-05/06) | Implemented, unit-tested, run against real benchmark output |
| Kubernetes log source (stream `user-container` logs per pod) | Not started; needs the cluster |
| Pod lifecycle watch + resource sampler (replica-seconds, requests/usage) | Not started; needs the cluster and a new resource contract |
| Measurement integrity validator (CORE-16) | Not started |

No cluster measurements exist yet; everything above was exercised with local runs only.

## Run
Standard library only (Python 3.11+). From `telemetry/`:
```bash
# replay a captured file (deterministic, event-time clock)
python -m collector.main --run-id r1 --functions periodic,steady --input events.jsonl
# live pipe (wall clock)
docker logs -f <container> | python -m collector.main --run-id r2 --functions periodic
# tests
python -m unittest discover -s tests -t .
```
Output goes to `telemetry/data/<run_id>/` (git-ignored; keep large raw data outside git):

| File | Content |
|---|---|
| `demand_buckets.jsonl` | Demand-bucket contract records, one per function per finalised bucket |
| `raw_events.jsonl` | Every valid event with its outcome: counted, warmup, duplicate, conflicting_duplicate, late |
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

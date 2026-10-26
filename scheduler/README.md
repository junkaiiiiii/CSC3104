# scheduler
Lead: Isaac. Tasks: CORE-04 (control path), CORE-09 (bounded dry-run policy), CORE-10 (adapter and safe failure).

Reads the predictor's forecasts (`predictor/data/<run_id>/forecasts.jsonl`), decides how many warm replicas each function should have, and either logs that decision (dry run) or applies it to Knative.

## Forecast input
The input is the predictor's output format (predictor/README.md): `function_id`, `forecast_bucket_start`, `forecast_invocation_count`, `window_size`, `input_buckets_used`. Those lines carry no timing information, so the scheduler derives it with fixed rules. The rules are recorded in every run manifest under `predictor_format_rules`:

| Derived field | Rule | Why |
|---|---|---|
| history end | `forecast_bucket_start` | The predictor forecasts the bucket right after the newest one it saw |
| generated at | `forecast_bucket_start + --forecast-delay` (default 10s) | Telemetry finalises a bucket 10s (its grace) after it ends, so the forecast can't exist earlier. Replays hide the line until then |
| predictions | The same count for that bucket and the next one | A moving average forecasts the same value at every horizon; the second bucket covers the gap until the next forecast |
| valid until | `forecast_bucket_start + 2 × 30s` | The forecast expires if the predictor stops |

A line is rejected if any `input_buckets_used` entry is not before `forecast_bucket_start`, which would mean it used future data. The older multi-prediction format in `contracts/forecast.example.json` is still accepted.

**Timing limitation:** a forecast for a bucket only arrives about 10s after that bucket has started. Pre-warming for a burst after silence therefore relies on the next-bucket forecast and the 60s cooldown. The first requests of a brand-new burst can still hit a cold start.

## Status
| Piece | State |
|---|---|
| Forecast validation, stale/future/invalid fallback | Implemented, unit-tested |
| Bounded policy: lead-time window, threshold, cooldown, per-function cap 2, total budget 6, stable priority | Implemented, unit-tested (CORE-09) |
| Binary mode (1 warm replica) and capacity mode (Little's law, capped) | Implemented, unit-tested |
| Min-scale adapter (Revision or PodAutoscaler), restore, crash recovery, failure backoff, revert detection | Implemented, tested against a fake cluster only (CORE-10) |
| Warm-up request actuator (fallback option D) | Implemented, tested with a fake HTTP opener |
| Actuation spike script | Run on the kind cluster, 26 Oct (CORE-04) |
| Actuation decision record | [docs/decisions/0001](../docs/decisions/0001-knative-actuation.md), **Accepted**: Revision min-scale, p95 patch→Ready 2.0 s |
| Live end-to-end run with predictor and evaluation harness | Not done; needs the predictor (CORE-07) and the spike result |

The 5000 ms lead time is the spike's p95 patch→Ready (2.0 s) plus a margin. The scheduler loop itself has not yet run against the live cluster.

## Run
Standard library only (Python 3.9+). From `scheduler/`:
```bash
# tests
python -m unittest discover -s tests -t .

# offline replay of a forecast file on a simulated clock (always dry run, no cluster)
python -m prewarm.main --run-id replay1 --forecasts ../predictor/data/<run_id>/forecasts.jsonl \
    --replay 2026-10-01T00:00:00Z 2026-10-01T01:00:00Z

# live dry run: tails the forecast file every 5s, reads pod readiness, changes nothing
python -m prewarm.main --run-id dry1 --forecasts path/to/forecasts.jsonl --kube-context kind-csc3104

# real actuation: only after ADR 0001 is Accepted; use the target and lead time it records
python -m prewarm.main --run-id live1 --forecasts path/to/forecasts.jsonl --kube-context kind-csc3104 \
    --actuator min-scale --target revision --lead-time-ms <measured p95 + margin>

# put back every min-scale this scheduler changed (after a crash)
python -m prewarm.main --restore --kube-context kind-csc3104 --target revision

# CORE-04 spike (cluster up, no traffic)
python -m spike.actuation_spike --kube-context kind-csc3104 --function periodic \
    --methods revision,pa --trials 3 --out ../results/spike-ab
```
Ctrl-C stops a live run and restores the original min-scale values before exiting.

Output goes to `scheduler/data/<run_id>/` (git-ignored):

| File | Content |
|---|---|
| `decisions.jsonl` | Decision contract records, one per function per tick |
| `policy_audit.jsonl` | Outcome (forecast / hold / fallback), window, predicted peak, requested vs granted, budget left |
| `actuation.jsonl` | What the actuator did (applied, noop, restored, error, suspended), revision, API calls, Ready pods |
| `forecast_rejects.jsonl` | Forecast lines refused on input, with reason |
| `scheduler_manifest.json` | Policy config, clock, totals, Kubernetes API calls, platform reverts |

## Policy, in short
Each tick (default 5s), per function:
1. Use the newest forecast generated at or before now. Missing, invalid, expired, older than 90s, future-dated or wrong-interval forecasts give **0 proactive replicas**, and Knative's reactive autoscaler serves traffic as normal.
2. Take the peak predicted count over buckets overlapping `[now, now + lead_time + tick)`, so capacity is Ready before the next decision could ask again.
3. Binary mode: peak ≥ 0.5 → 1 warm replica. Capacity mode: `ceil(rate × service_time / (concurrency × utilisation))`. Both are capped at 2 per function.
4. Zero demand releases capacity only after a 60s cooldown from the last positive prediction.
5. The total budget of 6 is shared out highest peak first, ties by function_id. Reductions are written in `reason` and the audit log.

All of these values are experiment settings exposed as CLI flags (`--per-function-cap`, `--total-budget`, `--cooldown`, `--threshold`, `--mode`, `--lead-time-ms`, ...) and recorded in the manifest.

## Semantics and limits
- `target_warm_replicas` is *proactive* capacity only. Knative can still scale above it reactively, up to max-scale.
- The adapter never sets min-scale below the revision's original value, so running it on the fixed-minimum baseline changes nothing.
- Dry-run decisions are produced exactly as live ones; only the actuator differs.
- Forecasts are trusted to follow the contract. The scheduler checks shape and timing, not accuracy.

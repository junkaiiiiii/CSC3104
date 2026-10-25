# Evaluation harness

This package provides deterministic synthetic workload generation, HTTP replay, per-run manifests and measurement analysis. It uses only the Python standard library (Python 3.9+).

## What the harness does—and does not do

- It creates an immutable request trace for three independently routed functions: periodic bursts, increasing steady traffic, and sparse irregular traffic. The sparse function has an abrupt burst in the final 10% to exercise fallback behavior.
- It replays identical trace files against a selected policy deployment and records one client measurement per trace request, including HTTP failures and timeouts.
- It records the selected policy and its supplied configuration in the manifest. It does **not** deploy or actuate policies. Deploy reactive, fixed-minimum, and predictive configurations separately, verify their starting state, and do not label a run predictive unless a validated controller actually ran.
- The full trace is known to the replay client. Do not expose future rows or trace manifests to an online predictor; have the controller consume only finalized telemetry/forecasts.
- It analyzes latency, success/error/timeout counts, the benchmark's first-user-per-process proxy, pod readiness/provisioned time, requested CPU/memory integrals, and forecast MAE/bias when their input files are present. It does not infer idle time without in-flight request observations or actual CPU/memory utilization from resource requests.

## Generate one trace

Run from the repository root:

```bash
python3 -m evaluation.workload --seed 17 --duration 180 --output results/traces/seed-17
```

The seed and workload parameters are recorded in `trace_manifest.json`; the exact JSONL trace is hashed. Generate separate tuning and evaluation traces with independent seeds. The same final trace file and hash must be replayed under every policy in one comparison; analysis rejects policy comparisons with mismatched seed sets or trace hashes.

## Configure endpoints and replay

Create an endpoint JSON file. Host headers route requests through the local Kourier NodePort:

```json
{
  "periodic": {"url": "http://127.0.0.1:8080/", "host": "periodic.csc3104.example.com"},
  "steady": {"url": "http://127.0.0.1:8080/", "host": "steady.csc3104.example.com"},
  "sparse": {"url": "http://127.0.0.1:8080/", "host": "sparse.csc3104.example.com"}
}
```

Run from the repository root, once per externally deployed policy:

```bash
python3 -m evaluation.replay \
  --trace results/traces/seed-17/trace.jsonl \
  --endpoints endpoints.json \
  --policy reactive \
  --policy-config reactive-config.json \
  --metadata environment.json \
  --run-id reactive-seed-17
```

`--policy` is one of `reactive`, `fixed_minimum`, or `predictive`. `--policy-config` and `--metadata` are optional JSON objects for the deployed manifest/settings, image digest, cluster versions, hardware, and other provenance. They document the experiment; they do not cause deployment changes. Each run ID must be new. Replay schedules requests against a monotonic clock and uses concurrent workers so overlapping arrivals can overlap at the endpoint.

For comparable runs, use the same trace hash, endpoint behavior, timeout, workload delay, image digest, and function settings. Record policy-specific settings (including fixed minimum capacity) rather than hiding their resource cost. Vary policy order across repetitions to reduce order/caching effects and complete at least five independent seeded repetitions for initial comparisons.

## Analyze runs

Copy or link each run's telemetry artifacts into its run directory when available: `pod_lifecycle.jsonl`, `demand_buckets.jsonl`, and `forecasts.jsonl`. Then analyze runs together:

```bash
python3 -m evaluation.analyze \
  --runs results/reactive-seed-17 results/fixed-seed-17 results/predictive-seed-17 \
  --output results/analysis
```

The resource observation window is the full replay interval (`started_at` to `ended_at`), not just the span between the first and last scheduled requests. This includes fixed-minimum capacity during leading/trailing idle portions of the replay. Record and verify initial readiness separately in the supplied metadata and lifecycle observations.

Outputs:

- `summary.json`: per-run measures and limitations.
- `policy_comparison.csv`: policy/workload aggregate means and sample standard deviations over the supplied runs where at least two observations exist.
- `report.md`: compact comparison table with explicit interpretation caveats.

Latency quantiles use the nearest-rank method. Timeout/error counts are reported separately; latency quantiles use all calls with a recorded duration (including HTTP error responses). First-user-per-process is reported only as a proxy, with unknowns retained. Pod replica-seconds use lifecycle event observation timestamps and may be uncertain by the polling interval. CPU/memory-seconds use Kubernetes **requests**, not utilization. Missing resource/forecast data remains `null`/N/A; it is never silently treated as zero. Forecast scoring supports complete demand buckets and both the shared multi-prediction contract and the current single-prediction format.

## Local verification

```bash
python3 -m unittest evaluation.tests.test_evaluation
```

Actual cluster trials still require the platform setup in [infra/README.md](../infra/README.md) and a validated pre-warming control path. See [the evaluation protocol](../docs/05-evaluation.md) for fairness, metrics and success criteria; thresholds should be selected after a pilot and before final runs.

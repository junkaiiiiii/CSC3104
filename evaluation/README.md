# Evaluation harness

This package provides deterministic synthetic workload generation, HTTP replay, per-run manifests and measurement analysis. It uses only the Python standard library (Python 3.9+).

## What the harness does—and does not do

- It creates an immutable request trace for three independently routed functions: periodic bursts, increasing steady traffic, and sparse irregular traffic. The sparse function has an abrupt burst in the final 10% to exercise fallback behavior.
- It replays identical trace files against a selected policy deployment and records one client measurement per trace request, including HTTP failures and timeouts.
- It records the selected policy and its supplied configuration in the manifest. It does **not** deploy or actuate policies. Deploy reactive, fixed-minimum, and predictive configurations separately, verify their starting state, and do not label a run predictive unless a validated controller actually ran.
- The full trace is known to the replay client. Do not expose future rows or trace manifests to an online predictor; have the controller consume only finalized telemetry/forecasts.
- It analyzes latency, success/error/timeout counts, scheduling lag, request-vs-readiness cold-start exposure (with the first-user-per-process proxy and `init_ms` reported separately), pod readiness/provisioned time, observable idle replica time, requested CPU/memory integrals, scheduler overhead counts and forecast MAE/bias when their input files are present, and writes SVG charts regenerated from the raw run data. Exposure and idle time need `pod_lifecycle.jsonl` records with `function_id`; their definitions are printed in `report.md`. It does not measure actual CPU/memory utilization or controller CPU.

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

`--policy` is one of `reactive`, `fixed_minimum`, or `predictive`. `--policy-config` and `--metadata` are JSON objects describing the deployed settings and provenance; they do not cause deployment changes. By default replay is strict: the metadata must contain `image_digest`, `kubernetes_version`, `knative_version`, `hardware`, `image_cache_state` (`cached`/`uncached`) and `initial_replicas` (function_id -> pod count; zero for reactive/predictive, at least one for fixed_minimum), and `predictive` runs need `--controller-dir` (the scheduler output directory, whose `scheduler_manifest.json`, `decisions.jsonl` and `actuation.jsonl` are copied and hashed into the run). `--allow-incomplete-metadata` skips these checks; analysis then warns. Optional `container_concurrency` (default 1) feeds the idle-time calculation. Each run ID must be new. Replay schedules requests against a monotonic clock and uses concurrent workers so overlapping arrivals can overlap at the endpoint. Vary the policy order between seeds and run at least 5 repetitions per policy; `analyze` warns otherwise.

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

## AWS Lambda workload runs

Lambda runs use the same traces and replay client with `--policy lambda`. Every endpoint needs `"auth": "sigv4"` and a `"region"`; only root HTTPS Lambda Function URLs in that region are accepted, and custom headers and redirects are rejected. Requests are signed with the standard AWS credential chain (or `--aws-profile`), are never retried, and credentials are never stored. `signing_ms` is measured separately; `sent_at` and `latency_ms` begin after signing, immediately before the HTTP request. A signing failure is recorded without a send timestamp or HTTP latency.

```json
{"periodic": {"url": "https://<id>.lambda-url.us-east-1.on.aws/", "auth": "sigv4", "region": "us-east-1"}}
```

The `--metadata` file must contain `region`, `runtime`, `architecture`, `memory_mb` and `function_versions`. Then join the run to CloudWatch (allow a few minutes for log delivery):

```bash
python -m evaluation.replay --trace results/traces/long-gap-402/trace.jsonl --endpoints lambda-endpoints.json --policy lambda --metadata lambda-metadata.json --run-id lambda-402-r1
python -m evaluation.lambda_correlate fetch --log-group periodic=/aws/lambda/<name> --log-group steady=/aws/lambda/<name> --log-group sparse=/aws/lambda/<name> --start <run started_at> --end <run ended_at + 5 min> --region us-east-1 --output results/lambda-402-r1/reports.jsonl
python -m evaluation.lambda_correlate correlate --run-dir results/lambda-402-r1 --reports results/lambda-402-r1/reports.jsonl
```

`fetch` saves unfiltered log events, including application, START, END and REPORT records. CloudWatch events may arrive late: check coverage and refetch to a new filename if needed. `correlate` writes `lambda_joined.jsonl` and `lambda_correlation.json`: matched/unmatched/ambiguous/unidentified counts, orphan REPORT records, and HTTP round-trip, execution, initialisation and billed usage (GB-seconds) reported separately. Cost is only estimated when all of `--price-per-gb-second`, `--price-per-million-requests`, `--price-source` and `--price-date` are supplied. Billed usage is not equivalent to Knative replica-seconds, and the client-to-AWS network path differs from localhost ingress.

## Local verification

```bash
python3 -m unittest evaluation.tests.test_evaluation evaluation.tests.test_lambda
```

Actual cluster trials still require the platform setup in [infra/README.md](../infra/README.md) and a validated pre-warming control path. See [the evaluation protocol](../docs/05-evaluation.md) for fairness, metrics and success criteria; thresholds should be selected after a pilot and before final runs.

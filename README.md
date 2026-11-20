# CSC3104 - Predictive Prewarming Scheduler

Group 14, Cloud and Distributed Systems

This project uses past invocation counts to prepare serverless functions before predicted requests arrive. It runs on Kubernetes with Knative Serving and Kourier.

The system connects the benchmark application, telemetry collector, predictor and scheduler. The evaluation compares reactive scaling, a fixed minimum of one replica per function, and predictive prewarming. A separate AWS Lambda test uses the same request traces.

Predictive prewarming is established prior work. Our contribution is the implementation, Knative integration and evaluation of this pipeline.

## Repository layout

| Folder | Contents | Lead |
| --- | --- | --- |
| infra/ | kind cluster, Knative manifests and Lambda deployment | Vania; Jun Kai for Lambda |
| functions/benchmark/ | HTTP benchmark, Docker image and shutdown handling | Jun Kai |
| functions/lambda_benchmark/ | Equivalent Lambda handler | Jun Kai |
| telemetry/ | Event collection, demand buckets and validation | Felix |
| predictor/ | SMA, EWMA and seasonal forecasts | Sook Yee |
| scheduler/ | Prewarming policy, Knative actuator and readiness reports | Isaac |
| evaluation/ | Request replay, policy comparison and Lambda log correlation | Hasan |
| contracts/ | Shared record formats and examples | Team |
| docs/ | Architecture, experiment settings and runbooks | Team |
| .github/ | Tests and image publishing workflows | Jun Kai |

## How it runs

Three services, periodic, steady and sparse, run the same benchmark with different function IDs. The collector groups user requests into five-second buckets with ten seconds of finalisation grace. Health checks and probes do not create invocation events; explicit warmups are excluded from demand.

The predictor reads complete buckets and publishes forecasts. For the long-gap workload, periodic uses a seasonal phase mean with a configured 180-second period, steady uses EWMA, and sparse uses SMA. The scheduler reads these forecasts and changes the active Knative Revision's minimum-replica annotation. Knative continues to handle reactive scaling.

The final scheduler configuration uses a five-second tick, five-second lead time and twenty-second cooldown. Missing, invalid or expired forecasts release proactive capacity. The scheduler restores its changes when replay finishes.

## Run the benchmark locally

The benchmark uses the Python standard library. Python 3.12 is the runtime used in the deployed benchmarks.

```bash
python3 functions/benchmark/app.py
```

In another terminal:

```bash
curl -H 'X-Request-ID: demo-1' http://localhost:8080/
curl http://localhost:8080/healthz
```

The first endpoint returns the function, instance and request identifiers with application timing fields. The health endpoint returns `{"status": "ready"}`. `INIT_DELAY_MS` and `WORK_DELAY_MS` default to zero.

## Run on Kubernetes

Start Docker and install kind and kubectl. From the repository root:

```bash
bash infra/scripts/up.sh
kubectl --context kind-csc3104 get ksvc -n csc3104
curl -H 'Host: periodic.csc3104.example.com' http://127.0.0.1:8080/
```

The setup script creates a new cluster named csc3104. The three services should report Ready. The manifests pin the platform versions and benchmark image digest; the published image supports amd64 and arm64.

See [platform setup](infra/README.md) and [version settings](infra/VERSIONS.md) for requirements and cleanup.

## Tests and evaluation

```bash
python3 scripts/smoke_benchmark.py
python3 -m unittest discover -s scripts -p 'test_benchmark_shutdown.py'
python3 -m unittest discover -s scripts -p 'test_long_gap.py'
python3 -m unittest discover -s predictor/test -t .
python3 -m unittest evaluation.tests.test_evaluation
(cd telemetry && python3 -m unittest discover -s tests -t .)
(cd scheduler && python3 -m unittest discover -s tests -t .)
python3 telemetry/tests/e2e_local.py
```

These checks cover application accounting, shutdown, forecasts, scheduling and telemetry. Live experiments also need the collector, predictor and scheduler running with the recorded settings.

- [Long-gap workload](docs/15-long-gap-pilot.md)
- [Scheduler settings and readiness reports](docs/16-scheduler-settings-and-readiness.md)
- [Live integration and finalisation](docs/13-live-integration.md)
- [Evaluation commands and metrics](evaluation/README.md)
- [AWS Lambda deployment and testing](infra/lambda/README.md)

The runbooks also describe development pilots. Use `predictor/configs/long-gap-5s.json` and `scheduler/configs/long-gap-5s.json` for the final long-gap configuration.

## Results

The final Knative comparison used five held-out seeds, 402-406, with all three policies. Each run lasted eighteen minutes and included the learning period.

| Mean across five runs | Reactive | Fixed minimum | Predictive |
| --- | ---: | ---: | ---: |
| p95 latency (ms) | 565.550 | 29.871 | 82.166 |
| p99 latency (ms) | 1857.566 | 40.082 | 1856.771 |
| Ready replica-seconds | 3098.632 | 3236.357 | 2742.723 |
| Request success rate | 100% | 100% | 100% |

On this workload, predictive control reduced mean p95 latency by 85.5% and Ready replica time by 11.5% compared with reactive scaling. Fixed minimum had the lowest latency. Predictive p99 was essentially unchanged, and an earlier short-cycle workload showed no benefit.

The Lambda evaluation matched all 3,475 requests to CloudWatch records. It is a separate platform reference: remote Lambda HTTPS and local Knative HTTP have different network paths and resources, so these results do not establish superiority over Lambda.

Raw runs are excluded from Git. The evaluation tools generate reports from saved client records, telemetry, forecasts and controller logs. Ready replica-seconds measure capacity time, not actual CPU use or monetary cost. Results depend on the workload and host.

## Team

Vania Teng, Wong Jun Kai, Felix Koh Xiang Shun, Lau Sook Yee, Isaac Lim Yee Shao and Muhammad Hasan Bin Suwandi.

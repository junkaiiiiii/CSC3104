# CSC3104 — Predictive Pre-Warming Scheduler
Group 14 · Kubernetes + Knative Serving

Build and evaluate a per-function forecasting controller that prepares capacity before predicted demand. Predictive pre-warming already exists in research; our contribution is the specific policy, Knative integration, and reproducible evaluation.

## Project sections
| Folder | Purpose | Lead |
|---|---|---|
| infra/ | Cluster, Knative, manifests, version lock, RBAC | Vania |
| functions/benchmark/ | Instrumented reference function and Docker image | Jun Kai |
| telemetry/ | Invocation collection and demand aggregation | Felix |
| predictor/ | Per-function forecasting | Sook Yee |
| scheduler/ | Forecast-to-capacity policy and Knative adapter | Isaac |
| evaluation/ | Trace replay, baseline runs, analysis | Hasan |
| observability/ | Metrics and optional dashboards | Felix |
| contracts/ | Shared samples and formats | Felix + Sook Yee + Isaac + Hasan |
| docs/ | Design, roadmap, evidence, demo and report | All |
| .github/ | CI and collaboration templates | Jun Kai |

## What is included
Planning documents, role-based issues, contract samples, a runnable Python benchmark function, Dockerfile, a Knative manifest template, and starter CI. Telemetry, forecasting, scheduler, and evaluation are work packages, not completed services. No cluster has been deployed and no performance improvement has been measured.

## Run the starter function
Requires Python 3.11+.
```bash
python functions/benchmark/app.py
```
In another terminal:
```bash
curl -H 'X-Request-ID: demo-1' http://localhost:8080/
curl http://localhost:8080/healthz
```
`INIT_DELAY_MS` and `WORK_DELAY_MS` default to zero; nonzero values are explicitly synthetic experiment settings. Read [benchmark notes](functions/benchmark/README.md).

## Core scope
Three functions with independently generated traffic; moving-average forecasting first; bounded scheduler; validated Knative actuation; reactive and fixed-minimum Knative baselines; reproducible latency/resource measurements. ARIMA and other extensions come after the first end-to-end run.

See [architecture](docs/03-architecture.md), [evaluation](docs/05-evaluation.md), [roadmap](docs/06-roadmap.md), [extensions](docs/08-extensions.md), and [proposal corrections](docs/09-proposal-corrections.md).

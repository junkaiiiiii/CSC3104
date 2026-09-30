# Optional extensions
Start only after reactive/fixed/predictive comparisons run correctly.

| Priority | Extension | Lead/support | Prerequisite | Evaluation |
|---|---|---|---|---|
| 1 | ARIMA versus moving average per function | Sook Yee / Hasan | History-only moving-average pipeline | Forecast error and latency/replica-time tradeoff, training overhead |
| 2 | Budget-aware allocation and SLO tradeoff | Isaac / Hasan | Validated multi-replica actuation | Sweep warm budgets; report p99/SLO violations and total replica-time |
| 3 | Live experiment dashboard | Felix / Jun Kai | Reliable metrics | Demand versus prediction, Ready replicas, decisions, errors, latency; include overhead |
| 4 | Controller restart / telemetry outage tests | Vania / Isaac / Hasan | Stable fallback and restore behavior | Recovery time, user failures, stale decisions, retained capacity |
| 5 | Public workload trace replay | Hasan / Felix | Seeded synthetic harness | Provenance/license, trace resolution, no leakage, per-function variety |

A simple seasonal/history model may be a better early comparison than a complex model. A public trace with minute-level counts cannot establish sub-second invocation timing without an explicit synthesis method.
An uncertainty-aware allocation policy is a possible project-specific contribution, but compare prior work before claiming novelty.
Avoid adding a database, web frontend, multiple clouds, or custom operator merely to increase feature count.

# Roadmap
Internal planning targets based on Figure A1; verify course deadlines independently.

| Target window | Gate | Leads | Required evidence |
|---|---|---|---|
| 30 Sep–4 Oct | Platform and control feasibility | Vania + Isaac + Hasan | One service, cold/warm cycle, actuation ADR |
| 5–11 Oct | Independently runnable components | Jun Kai + Felix + Sook Yee + Isaac | Image/CI, telemetry buckets, forecasts, dry-run decisions |
| 12–18 Oct | First end-to-end predictive run | Isaac + Hasan, all supporting | Forecast → Ready capacity → user request correlation |
| 19–25 Oct | Repeatable comparison | Hasan + Felix | Reactive/fixed/predictive runs with resource observations |
| 26 Oct–1 Nov | Progression demo and experiments | All | Demonstration, raw data, pilot tradeoffs |
| 2–8 Nov | Analysis, optional extension, report/video drafts | All | Charts, limitations, reproducibility guide |
| 9–15 Nov | Freeze and final handover | All | Clean rerun, report/video, submission checklist |

## Critical path
Platform → validated actuation → integrated predictive controller → controlled experiments → report.
Benchmark image, telemetry and forecasting can start in parallel using the contracts. Do not postpone the feasibility spike until the scheduler has been written.

## Working states
Backlog → Ready → In progress → Review → Done; Blocked records its prerequisite and owner. Keep one lead per issue and small PRs. The task index is a lightweight tracking page; a native GitHub Projects board can be added manually if desired.

## Scope cut
If the end-to-end run is late, defer ARIMA, custom dashboard, public trace replay, and fault injection. Keep the moving-average policy, two same-platform baselines, raw measurement collection, and a reproducible demo.

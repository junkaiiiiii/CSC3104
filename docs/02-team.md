# Team responsibilities
Derived from the names on the proposal cover and the role labels in Figure A1.

| Member | Primary role | Deliverables | Review partner |
|---|---|---|---|
| Vania Teng | Kubernetes/Knative cluster setup | Versioned installation, routing, manifests, scoped RBAC, recovery guide | Isaac |
| Wong Jun Kai | Docker images and CI pipeline | Benchmark function, reproducible image builds, checks, registry workflow, image digests | Vania |
| Felix Koh Xiang Shun | Invocation telemetry collector | Event capture, deduplication, bucketing, resource observations | Hasan |
| Lau Sook Yee | Per-function prediction model | Moving average, history-only evaluation, optional ARIMA | Felix |
| Isaac Lim Yee Shao | Pre-warming scheduler and integration | Decision policy, actuation adapter, dry-run/failure handling | Vania + Hasan |
| Muhammad Hasan Bin Suwandi | Integration testing and evaluation harness | Replay, baselines, metrics, repeat runs and analysis | Felix + Isaac |

All members contribute experiment analysis, progression presentation, final report, video, and submission.

## GitHub ownership
Issues record the responsible member by name. Jun Kai's verified account is `junkaiiiiii`; his issues can use GitHub assignees. Other account identities were not supplied, so do not infer usernames from names. Once each member provides their exact account and accepts repository access, assign their existing issues and add verified paths to CODEOWNERS if desired.

## Collaboration
Use branches such as `feat/12-moving-average`; open one reviewable PR per task. Include the issue number, evidence, and limitations. Ask the listed review partner for review. Integration tasks have one lead but multiple contributors. Track blockers explicitly, with the prerequisite issue number.

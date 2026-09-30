# First sprint: 30 September–4 October 2026
These are proposed internal targets, not verified module deadlines.

## First meeting
Agree on the cluster host and available RAM/CPU; confirm submission dates from the module brief; confirm GitHub usernames and collaborator access; approve the contracts and policy comparisons. Keep the roles shown in the proposal's Figure A1.

## First technical milestone
One instrumented function is reachable through Knative, scales to zero, returns under a real request, and produces a request log linked to client latency. Then demonstrate capacity preparation before a held-out request using the agreed control mechanism.

| Person | First action | Evidence to commit |
|---|---|---|
| Vania | Choose and pin Kubernetes/Knative/networking versions; deploy the first service | Installation commands, versions, Ready status, scale-to-zero observations |
| Jun Kai | Run benchmark locally, build image, enable checks; confirm registry pull access | Image digest, CI run, smoke-test output |
| Felix | Validate invocation sample and exclude probes/warmups | Deduplicated events and a 30-second bucket |
| Sook Yee | Implement moving-average forecast from historical buckets | Forecast matching shared format, no future-data use |
| Isaac | Pair with Vania on the Knative actuation spike; draft dry-run policy | Decision log and measured actuation delay |
| Hasan | Capture reactive cold and warm requests; draft replay configuration | Client timestamps, request IDs, raw latency and workload seed |

## Gate before integration
Isaac + Vania + Hasan must show how the selected Knative version can prepare capacity and return to zero without controller fights or unintended revision churn. Do not repeatedly patch the Service template and assume this updates the existing revision. Record the actual resources touched and any new revisions.
If dynamic capacity control is not viable, decide explicitly whether an idempotent warm-up request approach is acceptable; that only proves predictive activation, not arbitrary replica provisioning. Separate warm-up traffic from real traffic and account for its resources.

## End-of-sprint checklist
- [ ] Cluster and networking documented.
- [ ] Benchmark image is reachable by cluster nodes.
- [ ] Reactive cold/warm cycle captured.
- [ ] Contracts agreed.
- [ ] Actuation spike has a go/no-go outcome.
- [ ] Every member can identify their next issue.

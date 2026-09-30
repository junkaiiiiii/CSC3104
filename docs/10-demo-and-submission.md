# Demo and submission checklist
## Demo narrative
Show the same periodic trace under reactive and predictive policies; display forecast, decision, Ready capacity, and user-request timestamps. Show irregular traffic and explain fallback. Show the latency versus running-replica-time tradeoff, including a case where prediction wastes capacity.

## Reproducibility
- [ ] Clone from a fixed commit; install documented versions.
- [ ] Pull function images by digest.
- [ ] Run one documented command per experiment after cluster setup.
- [ ] Recover raw data with durable links/hashes.
- [ ] Regenerate tables/charts from raw data.
- [ ] Explain first-use proxy versus actual cold-start exposure.
- [ ] Include controller overhead and failed requests.
- [ ] Report synthetic delays and image cache assumptions.
- [ ] Verify citations, submission dates and report/video requirements against module brief.

## Report sections
Problem and related work; precise contribution; architecture and contracts; forecasting and policy; Knative actuation; controlled evaluation; results/tradeoffs; limitations; reproducibility; member contributions.

Do not present planned components as built or projected improvements as measured results.

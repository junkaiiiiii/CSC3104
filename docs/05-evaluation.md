# Evaluation protocol
## Primary comparisons
1. Knative reactive autoscaling with scale-to-zero and the recorded default/configured settings.
2. Knative fixed minimum replica count (initially 1 per function).
3. Predictive policy on the same Knative setup.
4. Optional simple history-based keep-warm policy, then optional ARIMA.

AWS Provisioned Concurrency is an optional separate-platform study, not the primary controlled baseline. Fix runtime/image behavior as far as possible, disclose platform differences and billing, and do not combine its costs with local replica-time as equivalent measures.

## Workload matrix
Run at least three functions with different patterns: periodic bursts, steady/ramping traffic, irregular sparse traffic. Add an unseen abrupt burst to test fallback. Use seeded generators and independent seeds for tuning and final evaluation. No future trace values may be fed to the online predictor.
Keep application initialization settings identical across policies. Report zero artificial delay separately from synthetic-delay experiments. Record image-cached versus uncached starts; do not mix them silently.

## Metrics
- End-to-end latency: median, p95, p99 across user requests, with sample size and quantile method. Report timeouts/error rate separately.
- Cold-start exposure: predefine an operational criterion using readiness/activation timing and client-server correlation. First-user-per-process is only a proxy, not an exact cold-start rate. Report unknown cases.
- Conditional latency for exposed requests only: report N; if none occur, result is N/A, not 0. Pre-warming mostly avoids exposure rather than accelerating initialization itself.
- Cumulative running replica-seconds: integrate observed replica count over time. Also capture requested/used CPU and memory, including collector/predictor/scheduler overhead.
- Idle time: integrate time at zero measured in-flight work per replica; first implement an observable definition. A running-replica total is not an idle-time measurement.
- Forecast error: MAE for held-out demand buckets; track bias and expiry/fallback counts. Avoid MAPE where true demand is zero.
- Controller overhead: runtime, API calls, failures, warmups and revisions.

## Fair experiment procedure
Pin code commit, image digest, cluster versions, hardware, manifest, model parameters and seed in a run manifest. Reuse the exact trace across policies; vary policy order between repetitions to reduce caching/order effects.
Start from a recorded, verified initial replica state; include warmup/training and startup resource costs or report them separately. Do not hide the fixed baseline's standing capacity.
Use at least five independent seeded repetitions initially, save all raw data, and show per-run variability. p99 from small samples is unstable; choose run length/request counts after a pilot and report uncertainty. Reject a run with missing observations rather than silently filling measurements.

## Success and tradeoff
Choose numerical thresholds after a pilot and before final experiments. A useful result can reduce latency at additional replica-time cost; show the tradeoff and the cases where prediction loses. Matching the near-zero idle cost of reactive scaling during sparse periods while pre-warming is not guaranteed.
No claims of arbitrary fleet scale, cost savings, uninterrupted availability, or all-workload improvement without supporting measurements.

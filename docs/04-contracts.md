# Shared data contracts (v1 proposal)
Agree these before components are connected. Samples are in contracts/. UTC timestamps use ISO 8601; durations are milliseconds.

## Invocation event
Required: schema_version, function_id, revision, instance_id, request_id, request_kind (user/warmup), started_at, service_ms, first_user_request, init_ms. Probes are excluded.
The starter is single-process per container; instance_id is generated at process start. A first user request identifies first use of that process, not proof that the caller waited for provisioning. init_ms is application startup work, not total platform cold-start delay.

## Demand bucket
function_id, bucket_start, interval_seconds, invocation_count, complete.
A confirmed empty bucket is 0. Missing telemetry is unknown, never silently converted to 0. Count only user requests, deduplicate request IDs, and record dropped/late events.

## Forecast
function_id, generated_at, history_end, interval_seconds, model, predictions [{bucket_start, invocation_count}], valid_until.
Count forecasts must be finite and nonnegative. Train using only data at or before history_end; prediction buckets follow history_end. Expired/incomplete inputs trigger fallback.

## Scheduler decision
function_id, decided_at, forecast_generated_at, target_warm_replicas, lead_time_ms, reason, dry_run.
Record current readiness and actuation result separately. Use stable ordering for budget competition and log rejected decisions.

## Client measurement
run_id, policy, seed, request_id, function_id, scheduled_at, sent_at, completed_at, latency_ms, status_code, timeout, instance_id, revision.
Use a monotonic clock for durations, UTC for correlation. A timeout remains in the dataset and is reported separately; never drop failed calls from success-rate analysis.

## Integration acceptance
Each component accepts and emits a sample; event and forecast identifiers remain stable; request-kind exclusions are tested; schema changes have a version and coordinated review. Reconcile missing client or server records rather than claiming all first-use requests are cold starts.

## Pod lifecycle observation (PROPOSED v1, needs review by Hasan and Isaac)
Sample: contracts/pod_lifecycle.example.json. Written by telemetry/ from `kubectl get pods`; not yet agreed.
Fields: schema_version, observed_at (collector clock, about the poll interval of lag), event (pod_seen, pod_ready, pod_unready, pod_terminating, pod_gone), function_id, revision, pod, created_at, ready_at, deletion_timestamp and deletion_grace_period_s (set once termination is requested), phase, cpu/memory requests and limits (summed over all containers, including Knative's queue-proxy; null if unset).
Replica time is derived offline, not stored. deletion_timestamp is when Kubernetes will force-kill the pod (termination request + grace period), NOT when termination began: the request time is deletion_timestamp minus deletion_grace_period_s (about the pod_terminating observation). Ready interval = ready_at to the termination request. Provisioned interval = created_at to the pod_gone observation. A pod that is slow to exit stays provisioned, and holds resources, for the whole grace period. The report must say which definition it uses. Actual CPU/memory usage is not recorded because the platform has no metrics-server; absence means unknown.

# Long-gap integration pilot

## Question and scope

Does the mixed predictor request capacity and obtain Ready Pods before later
periodic bursts, after reactive Pods have had an opportunity to scale to zero?
Keep the seed-319 pilot as a negative result: six exposed requests in both policies
and about 1.8% more Ready replica time for predictive. This new workload is a
separate, controlled experiment; it must not replace or be pooled with that trace.

No scheduler or evaluation implementation changes are required by this preparation.
The experiment does not establish superiority to AWS Lambda.

## Frozen workload v1

- Duration: 1080 seconds (18 minutes) per policy; seed 401 is the development pilot.
- Periodic: 16–24 randomly timed requests in each 10-second burst, starting at
  offsets 20, 200, 380, 560, 740 and 920 seconds. No periodic background traffic.
  Nominal idle gaps are 170 seconds. Confirm actual scale-to-zero from lifecycle
  observations; do not assume it from the schedule or force-delete Pods.
- Steady control: one jittered request every two seconds.
- Sparse control: irregular bursts of 3–8 requests, 95–235 seconds apart.
- The period is supplied explicitly as an experimental parameter. The seasonal
  model learns phase counts from past observed buckets; it does not discover the
  period. This is a favourable periodic case, not a production generalisation.
- Declare a 540-second learning prefix before running. Report full-run latency
  and resources, including learning costs, plus a separately labelled forecast
  score after the prefix. Do not silently exclude early cold requests from latency.
- Use later seeds (e.g. 402 onward) as held-out runs only after freezing choices.
  If retuned on them, they become development data and new held-out seeds are needed.

## Prepare and check (does not send traffic)

From the repo root:

```bash
python3 -m unittest discover -s scripts -p 'test_long_gap.py' -v
python3 scripts/prepare_long_gap.py --seed 401 --output results/traces/long-gap-401
python3 -m predictor.main --run-id long-gap-offline-401 \
  --input results/traces/long-gap-401/offline_demand_buckets.jsonl \
  --model-config predictor/configs/long-gap-5s.json --expected-interval 5 \
  --horizon-buckets 6 --availability-delay 10 \
  --output results/traces/long-gap-401/offline-forecasts.jsonl
```

The generator refuses an existing output directory. It creates the trace, its
hash-bearing manifest, a copy of model settings, and clearly labelled synthetic
offline buckets. Offline fixtures use simulated times and ideal collection;
they are only a compatibility check. Never feed them, the request schedule or
re-timestamped forecasts into an online predictor.

Tests check seeded reproducibility, genuine periodic idle gaps, unique client IDs,
bucket-count conservation, trace hashes, overwrite refusal, causal forecast inputs,
and positive advance forecasts at later burst starts for three seeds and five
collector-bucket phase offsets. Simulated publication is 10.5 seconds after bucket
end and eligibility allows 10 seconds for scheduling/startup. These tests are
not evidence that the live cluster reaches Ready in time.

## Live settings to agree with Isaac before replay

Preparation validation: 20 existing predictor tests and four new workload tests
passed locally. Seed 401 generated 699 requests (123 periodic, 540 steady, 36 sparse).
The offline predictor emitted 648 forecasts; the unchanged scheduler accepted all
648 with zero rejects in a 5-second-interval dry-run replay. These are simulated
compatibility results, not live prewarming or performance evidence.


- Collector: `--interval 5 --grace 10`.
- Predictor: `--model-config predictor/configs/long-gap-5s.json`,
  `--expected-interval 5 --horizon-buckets 6 --follow`.
- Scheduler pilot settings (agreed with Isaac, frozen in
  `scheduler/configs/long-gap-5s.json`; see [16-scheduler-settings-and-readiness.md](16-scheduler-settings-and-readiness.md)):
  interval 5 seconds, tick 5 seconds, lead time 5000 ms, cooldown **20** seconds
  (changed from 60 so the scheduler never holds a Pod longer than Knative's own
  60-second window would), binary mode, threshold 0.5, per-function cap 2, total budget 6.
  Use `--config configs/long-gap-5s.json`, `min-scale`, target `revision`, backend
  `kubectl`, correct context/namespace. Check release between bursts with
  `scheduler/tools/readiness_timeline.py`. Record settings, do not tune them mid-run.
- Zero initial Pods, current revisions at min-scale 0, same pinned benchmark image,
  cached images confirmed, unchanged application delays and Docker resources.
- Run reactive with collector only; predictive with collector, predictor and scheduler.
  Replay the exact same trace bytes. Record run order; vary it in repetitions.
- Snapshot stable hardware descriptors (CPU, memory, node capacity/allocatable,
  architecture, kernel and runtime), not entire changing node-status objects.
  Keep operational status separately. Preserve original seed-319 metadata and
  disclose its broad hardware-equality warning rather than rewriting history.
- Save all active autoscaler overrides, current revision specs, source commit,
  model-config hash, startup/final scheduler settings and actual initial state.
  Disable laptop sleep for the run and avoid other intensive work.

Use fresh IDs, e.g. `reactive-longgap-401` and `predictive-longgap-401`. Follow
`docs/13-live-integration.md` for collection, shutdown, validation and finalization.
The existing replay requires fresh metadata and a controller directory for predictive.
Do not reuse metadata from the seed-319 experiments. The host alias remains
`<function>.csc3104.example.com` through localhost port 8080.

## Evidence and decision

For each later periodic burst, retain a timeline joining forecast publication,
positive decision, actuator patch, observed zero-to-Ready transition, and actual
client request send time. Report timing resolution and misses. A min-scale patch
with already-Ready Pods only proves retention. Startup readiness snapshots alone
cannot establish future readiness; use collector lifecycle events.

Measure success/failures, exposed requests and their latency, overall tail latency,
Ready/idle replica-seconds, and whether capacity returns to zero between bursts.
Do not claim savings by checking latency alone. If predictive simply stays warm,
compare with fixed-minimum and describe it as retention, not successful timed startup.
Keep irregular misses and first bursts: an online predictor cannot foresee an
unprecedented burst. First-user flags are not cold-start labels.

After a validated pilot, freeze configuration and hand the trace/settings to Hasan
for repeated reactive/fixed/predictive comparisons and his Lambda evaluation.
Preserve raw ignored outputs with hashes and a durable backup; Git tracks the
generator/config/protocol, not all live data. One successful pilot is not a final
statistical result. Local Knative HTTP and remote Lambda HTTP are not a controlled
platform-only latency comparison.

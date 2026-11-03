# Integration runbook — CSC3104
Prepared against main commit 298f72a688fe3e325981a020a034336becc8f51b, 3 November 2026.

This is a sequence of integration gates. Passing an earlier gate does not prove the later one.
Commands target macOS/bash/zsh. Use Python 3.12+ and work from the repository root unless a step explicitly changes directory. No Python third-party packages are required by the current modules.
Use one integration machine first: cluster, collector, predictor, scheduler and replay client all run there. Local JSONL files are the current communication mechanism; separate laptops do not automatically exchange them.

## 1. Synchronise and preserve work
```bash
cd /Users/junkai/Cloud/CSC3104
git status
```
If modified or untracked work exists, preserve/review it before pulling. If clean:
```bash
git pull --ff-only
git rev-parse --short HEAD
git switch -c integration/end-to-end
```
If that branch already exists, switch to it and reconcile its base rather than overwriting it.

Preflight:
```bash
python3 --version
docker info
kind version
kubectl version --client
```
Missing kind/kubectl need installation before cluster gates, but do not block offline tests.

## 2. Run the component checks
From the repository root, one command at a time:
```bash
python3 scripts/smoke_benchmark.py
python3 -m unittest evaluation.tests.test_evaluation
(cd telemetry && python3 -m unittest discover -s tests -t .)
python3 -m unittest discover -s predictor/test -t .
(cd scheduler && python3 -m unittest discover -s tests -t .)
python3 telemetry/tests/e2e_local.py
```
At the reviewed commit, the four unit-test groups total 111 passing tests. Some tests intentionally print a validation FAIL while testing rejection; the unittest exit code and final OK determine suite success.
The last script starts the real benchmark locally and passes its logs to the collector. This is a local file-processing check, not a live Kubernetes test.

Gate: all commands succeed. Save the commit and terminal output.

## 3. Verify the offline bucket → forecast → decision interface
This does not require Docker or Kubernetes. Use these run names once; choose new names for another attempt so outputs are not mixed.

Generate 180 synthetic buckets:
```bash
(cd telemetry && python3 -m tools.synthetic_buckets --seed 101 --hours 0.5 --out ../results/integration-offline/buckets)
```

Run the batch predictor:
```bash
python3 -m predictor.main --run-id integration-offline --input results/integration-offline/buckets/demand_buckets.jsonl --window-size 3
```

Run the scheduler with a simulated clock:
```bash
(cd scheduler && python3 -m prewarm.main --run-id integration-offline --out-dir ../results/scheduler --forecasts ../predictor/data/integration-offline/forecasts.jsonl --replay 2026-10-01T00:00:00Z 2026-10-01T00:30:00Z)
```

Inspect:
```bash
python3 - <<'PY'
import json
from pathlib import Path
p = Path("results/scheduler/integration-offline")
m = json.loads((p / "scheduler_manifest.json").read_text())
print("Forecasts accepted:", m["forecasts_accepted"])
print("Rejected lines:", len((p / "forecast_rejects.jsonl").read_text().splitlines()))
print("Decisions:", len((p / "decisions.jsonl").read_text().splitlines()))
print("Actuator:", m["actuator"])
PY
```
Reviewed output: 180 accepted forecasts, 0 format rejections, 1080 decisions, dry-run actuator.
This proves interface compatibility for these inputs, not forecast accuracy, correct incomplete-history handling, or latency improvement.

## 4. Bring up the platform
Vania + Jun Kai lead this gate. Ensure standalone Docker demos are stopped because they also bind port 8080:
```bash
docker ps
kind get clusters
```
Only if the csc3104 cluster does not exist:
```bash
bash infra/scripts/up.sh
```
Follow infra/README.md and infra/VERSIONS.md for compatible tool versions. Do not delete an existing cluster merely to retry installation.

Verify the intended cluster:
```bash
kubectl --context kind-csc3104 get nodes
kubectl --context kind-csc3104 get ksvc -n csc3104
```
The three services periodic, steady and sparse should show Ready True. Services can be Ready while their workload Pods have scaled to zero.

Test routing:
```bash
curl -H "Host: periodic.csc3104.example.com" -H "X-Request-ID: setup-periodic-1" http://127.0.0.1:8080/
curl -H "Host: steady.csc3104.example.com" -H "X-Request-ID: setup-steady-1" http://127.0.0.1:8080/
curl -H "Host: sparse.csc3104.example.com" -H "X-Request-ID: setup-sparse-1" http://127.0.0.1:8080/
```
Each response must have the matching function_id and a Knative revision rather than local.
Resolve ImagePullBackOff through package visibility/access or a correctly configured pull secret; Mac Docker login does not authenticate cluster nodes.
Gate: pin/record actual deployed digest, architecture and platform versions. Confirm the original image digest remains available.

## 5. First live test: reactive only
Felix + Hasan lead; no predictor or scheduler runs yet.
Wait until the three function Pods are actually gone after setup traffic:
```bash
kubectl --context kind-csc3104 get pods -n csc3104
```
Terminating is not the same as gone.

Generate the replay trace:
```bash
python3 -m evaluation.workload --seed 17 --duration 180 --output results/traces/seed-17
```
Create results/endpoints.json containing:
```json
{
  "periodic": {"url": "http://127.0.0.1:8080/", "host": "periodic.csc3104.example.com"},
  "steady": {"url": "http://127.0.0.1:8080/", "host": "steady.csc3104.example.com"},
  "sparse": {"url": "http://127.0.0.1:8080/", "host": "sparse.csc3104.example.com"}
}
```

For a formal run, create results/reactive-environment.json with actual values for image_digest, kubernetes_version, knative_version, hardware, image_cache_state and initial_replicas. Record container_concurrency too. Verify counts at the start; do not invent metadata to pass validation. Create results/reactive-config.json describing the actual min-scale/max-scale and request/runtime settings.

Terminal A, from root:
```bash
(cd telemetry && python3 -m collector.main --source k8s --kube-context kind-csc3104 --run-id reactive-17)
```
Keep it running BEFORE replay.

Terminal B, from root:
```bash
python3 -m evaluation.replay --trace results/traces/seed-17/trace.jsonl --endpoints results/endpoints.json --policy reactive --policy-config results/reactive-config.json --metadata results/reactive-environment.json --run-id reactive-17
```
After replay completes, leave collection running at least 40 seconds after the last response, then stop Terminal A with Ctrl-C and let its drain finish. The measurement window and collection window differ. Record prolonged termination and any resource-accounting tail outside replay; do not imply it is included automatically.

Validate:
```bash
(cd telemetry && python3 -m collector.validate --run-dir ../results/reactive-17 --telemetry-dir data/reactive-17)
```
Investigate any errors; do not suppress them or edit raw observations.

Copy the lifecycle and demand files for analysis:
```bash
cp telemetry/data/reactive-17/pod_lifecycle.jsonl results/reactive-17/
cp telemetry/data/reactive-17/demand_buckets.jsonl results/reactive-17/
python3 -m evaluation.analyze --runs results/reactive-17 --output results/analysis-reactive-17 --require-telemetry
```
One run is only a smoke check. Comparison/repetition warnings are expected at this stage.
Gate: three routes work; successful client request IDs reconcile with logs; counts preserve user-only traffic; validator passes; analysis reads actual files. Keep every failure/timeout in the dataset.

## 6. Resolve live-integration blockers before predictive experiments
These are still present at the reviewed commit.

| Owner | Required fix | Verification |
|---|---|---|
| Sook Yee + Felix | Predictor follows newly finalised buckets, processes each once, appends and flushes forecasts; rejects invalid/out-of-order data; does not regenerate an old forecast on an incomplete input | Append multiple buckets while it is running; receive multiple fresh forecasts without restart, and no duplicates |
| Isaac + Sook Yee | Emit actual generated_at/history_end/expiry and agree forecast horizon and collector grace; scheduler uses real availability in live runs | A forecast only becomes usable after its data exists; can cover capacity lead time |
| Isaac + Hasan | Scheduler writes an initial manifest at startup and finalises it at shutdown; evaluation gathers final artifacts AFTER shutdown | Replay can start with a currently active controller; stored final artifacts include the full experiment and restoration |
| Hasan + Sook Yee | evaluation/analyze.py reads forecast_invocation_count | Forecast 3 against actual 4 yields one match, MAE 1, bias -1 |

The current batch predictor exits at EOF and opens its forecast output in write mode. Repeatedly restarting it against the growing input is not a reliable streaming connection.
The current replay rejects predictive startup without a scheduler_manifest.json, while the scheduler writes that file only on shutdown.
The scheduler already accepts the predictor's single-prediction shape. Do not duplicate that conversion unnecessarily.
Do not reuse a previous run's manifest or precompute future forecasts from the final test trace.

## 7. Live pipeline: dry run, then actuation
After gate 6:
1. Reset to the chosen initial state and select a fresh shared run ID.
2. Start collector and confirm healthy observation.
3. Start the continuous predictor pointing to that collector's demand_buckets.jsonl.
4. Start scheduler pointing to that predictor's forecasts.jsonl, initially --actuator dry-run.
5. Start replay only after startup files/status are valid.
6. Verify buckets/forecasts/decisions continue growing and no prediction sees future requests.
7. Repeat on a fresh run with --actuator min-scale --target revision after checking local actuation behavior.

ADR 0001 records Revision min-scale as accepted on Isaac's machine. Revalidate on the integration host; the 5000 ms lead time is not universal. Do not patch Deployment replicas or the Knative Service template in the control loop.
Knative fallback should continue for missing/stale forecasts and unexpected traffic.

Shutdown: replay finishes; stop predictor cleanly; stop scheduler cleanly and confirm restoration; allow telemetry to cover late events and the declared observation period; stop collector and let it drain; finalise/copy all artifacts; validate; analyze.
Coordinate final artifact hashes/manifests rather than silently replacing files already hashed by replay.
Record post-run retained capacity separately if the existing replay-window analysis does not cover it.

## 8. Controlled comparisons
Hasan + all component leads:
- Reactive: scheduler off, min-scale zero, verified reset.
- Fixed minimum: scheduler off, min-scale one, capacity Ready before replay. Provision baseline configs outside the running controller and record revision changes.
- Predictive: min-scale zero initially, fresh causal forecasts, live controller enabled.
Use identical trace files/hashes per seed, same image digest and runtime settings, independent held-out seeds, varied policy order and at least five repetitions initially. Keep warmup/training/resource costs visible.
A moving average may not anticipate the first burst after silence; measure rather than assume benefit.

AWS Lambda comparison remains required after lecturer feedback and is NOT implemented by these scripts. Keep its equivalent benchmark, deployment/configuration, logs, invocation interface and billing measurements as a separate integration track. Current replay policy choices are Knative policies and must not be mislabelled as Lambda.

## Definition of integrated
- All three functions reachable through Knative.
- Live logs become complete demand buckets.
- New buckets produce fresh causal forecasts continuously.
- Forecasts produce bounded decisions and observable Ready capacity.
- Stopping the controller restores the original settings.
- Client/log/pod records reconcile and validation passes.
- Analysis includes real final artifacts, failures, uncertainty and resource limitations.
- The whole sequence can be repeated by another member from a pinned commit.

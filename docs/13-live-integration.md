# Live predictive integration

The historical replay proves data compatibility, not latency improvements. First
verify the live connection in dry-run, then measure the real min-scale actuator.
The benchmark image and cluster manifests do not need to be rebuilt for these
Python changes. All paths below are relative to the repository root unless stated.

## Before traffic

1. Stop any previous collector, predictor and scheduler. Restore the three current
   Revision min-scale annotations to `0`. Wait until no benchmark Pods remain.
2. Choose a fresh ID, such as `predictive-17`. Reuse the exact baseline trace and
   endpoints. Capture current environment metadata with initial replicas all zero;
   do not invent or reuse warm initial counts. Keep image cache state comparable.
3. Start the collector, from `telemetry/`:

   ```bash
   python3 -m collector.main --source k8s --kube-context kind-csc3104 \
     --namespace csc3104 --run-id predictive-17
   ```

4. In a second terminal, from the root, start the predictor:

   ```bash
   python3 -m predictor.main --run-id predictive-17 \
     --input telemetry/data/predictive-17/demand_buckets.jsonl --window-size 3 --follow
   ```

5. In a third terminal, from `scheduler/`, start the scheduler. For the first live
   wiring check keep `--actuator dry-run`; this is not a predictive performance run.

   ```bash
   python3 -m prewarm.main --run-id predictive-17 \
     --forecasts ../predictor/data/predictive-17/forecasts.jsonl \
     --out-dir ../results/scheduler --actuator dry-run --backend none
   ```

   For a subsequent fresh-ID real experiment, use `--actuator min-scale --target
   revision --backend kubectl --kube-context kind-csc3104` instead. The accepted
   actuation ADR supports Revision min-scale. Do not mutate Deployment replicas.
   Recheck the host-specific lead time before drawing timing conclusions.

6. Check that `results/scheduler/predictive-17/scheduler_manifest.json` exists,
   contains the intended actuator and policy, and has `final: false`. Verify Pods
   still match recorded zero initial state immediately before workload replay.
   Use the existing evaluation CLI with `--controller-dir
   results/scheduler/predictive-17`; its configuration remains descriptive, so pass
   the actual scheduler policy and fresh environment metadata. Label a dry-run
   wiring check as a pilot and exclude it from predictive policy comparisons.

## After replay

1. Keep the collector and predictor running for at least 40 seconds after the last
   response to finalise buckets. Preserve sufficient matched actual buckets when
   evaluating future forecast horizons; forecasts beyond observation are unscored.
2. Stop the scheduler once with Ctrl+C. It restores managed min-scale annotations,
   closes logs and atomically replaces its startup manifest with `final: true`.
   A startup manifest is sufficient for replay setup, not final evaluation.
3. Stop the predictor with Ctrl+C. If collecting resource shutdown tails, keep the
   collector alive until Pods disappear, then stop it and allow its drain to finish.
   Analysis still integrates resource time only within the replay window. Report
   shutdown-tail observations separately; do not silently extend one policy's window.
4. Validate telemetry against client records as in the baseline. Copy lifecycle
   and demand buckets into the evaluation run directory.
5. Replace the replay's startup controller snapshot with final artifacts and update
   their hashes:

   ```bash
   python3 -m evaluation.finalize --run-dir results/predictive-17 \
     --controller-dir results/scheduler/predictive-17 \
     --forecasts predictor/data/predictive-17/forecasts.jsonl
   ```

   Evaluation and scheduler run IDs must match. Finalization refuses a still-running
   scheduler. Analysis rejects an explicitly non-final controller snapshot or hashes
   that no longer match. Do not manually replace logs without updating hashes.

6. Analyze all validated policies with `--require-telemetry`. Repeat with several
   seeds and varied policy order. One successful wiring test is not evidence of a
   latency/resource improvement or superiority to AWS Lambda.

## Model limitations

Live forecasts use actual publication timestamps; historical compact forecasts still
use the scheduler's explicit availability-delay assumption. The SMA holds its count
constant for two horizons, and cannot foresee an unprecedented first burst. Bucket
finalisation delay, scheduler tick delay, platform startup and scale-down tails all
affect results. Do not shift historical forecasts onto a live clock or use future
request schedules as predictor input.

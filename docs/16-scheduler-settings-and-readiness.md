# Scheduler settings and readiness timeline (long-gap pilot)

Owner: Isaac (scheduler). For: Hasan (evaluation), Jun Kai (predictor/workload).
Workload and predictor settings come from [15-long-gap-pilot.md](15-long-gap-pilot.md).

**Status.** The settings below are frozen for the long-gap pilot. They have been
checked with arithmetic and with an offline simulation that runs the real predictor
and scheduler code. **They have not been checked on the live cluster yet.** Section 6
covers the live check. Until that check passes, quote the numbers in sections 3–5
as "simulated", not as results.

## 1. Final settings

| Setting | Value | Where |
|---|---|---|
| Collector bucket / grace | 5 s / 10 s | `--interval 5 --grace 10` (unchanged grace) |
| Predictor | `predictor/configs/long-gap-5s.json` (periodic seasonal 36 × 5 s = 180 s, 3 cycles; steady EWMA 0.3; sparse SMA 3) | `--expected-interval 5 --horizon-buckets 6 --follow` |
| Forecast horizons | 6 × 5 s = 30 s ahead of the last complete bucket | predictor |
| Scheduler interval | 5 s (must equal the bucket width or every forecast is rejected) | `scheduler/configs/long-gap-5s.json` |
| Tick | 5 s | same file |
| Lead time | 5000 ms (ADR 0001 p95 patch→Ready 2.03 s + ~3 s margin) | same file |
| Cooldown | **20 s** (was 60 s in doc 15; see section 4) | same file |
| Mode / threshold | binary, 0.5 predicted requests per bucket | same file |
| Caps | 2 per function, 6 total (binary mode uses at most 1 each) | same file |
| Max forecast age | 90 s (in practice `valid_until`, 30 s after history end, expires first) | same file |
| Actuator | `min-scale`, target `revision`, backend `kubectl` | CLI |

Scheduler command, from `scheduler/`:

```bash
python3 -m prewarm.main --run-id predictive-longgap-401 \
  --config configs/long-gap-5s.json \
  --forecasts ../predictor/data/predictive-longgap-401/forecasts.jsonl \
  --out-dir ../results/scheduler --actuator min-scale --target revision \
  --backend kubectl --kube-context kind-csc3104 \
  --stop-when-exists ../results/predictive-longgap-401/run_manifest.json
```

The manifest stores the resolved policy, the config file path and its SHA-256
(`policy_config_file`). Explicit flags override the file. Unknown keys are refused.

`--stop-when-exists` points at the replay's `run_manifest.json`. `evaluation.replay` writes that
file only after the last response. The scheduler checks for it every 0.5 s, then stops and
restores min-scale. The manifest records `stop_reason`. The scheduler refuses to start if
the file already exists (an old run). `--stop-at <UTC time>` and `--duration` are
alternatives. Ctrl+C still works.

## 2. Is there enough time? (arithmetic)

A forecast is published when the bucket it builds on is finished: bucket end + 10 s grace +
≤1.5 s (collector tick ≤1 s, predictor poll 0.5 s). Its six horizons start at that
bucket end, so it reaches 30 s past it. At every tick the scheduler asks for the next
lead + tick = 10 s.

| Item | Seconds |
|---|---:|
| Forecast reach after the newest finished bucket: 6 × 5 | 30 |
| − the next bucket may be almost finished (not yet published) | −5 |
| − collector grace | −10 |
| − publication delay (collector tick + predictor poll) | −1.5 |
| − lead time | −5 |
| − tick | −5 |
| **Coverage slack** | **+3.5** |

A burst bucket is first seen by a tick between 10 s and 5 s before it starts. The patch
then has at least lead − patch→Ready = 5 − 2.03 = **2.97 s** of margin, and at most 7.97 s.
Up to 15 s from bucket end to publication still covers the window. Six horizons are
the minimum; with five the slack is −1.5 s.

## 3. Simulated timeline (seed 401, cooldown 20 s)

`python -m tools.timing_check --seed 401 --cooldown 20` (from `scheduler/`). Times are
seconds from replay start. The Knative model uses 2.03 s patch→Ready, default
60 s stable window, 30 s scale-to-zero grace and 29 s observed termination.

| Burst | First request | Forecast published | min-scale patch | Pod Ready | Margin | Outcome | Exposed requests |
|---:|---:|---:|---:|---:|---:|---|---:|
| 1 | 20.9 | – | – | 22.9 | −2.0 | missed (learning) | 8 |
| 2 | 200.6 | – | – | 202.7 | −2.0 | missed (learning) | 4 |
| 3 | 381.0 | 371.5 | 375.0 | 377.0 | +3.9 | prewarmed | 0 |
| 4 | 561.5 | 551.5 | 555.0 | 557.0 | +4.5 | prewarmed | 0 |
| 5 | 740.3 | 731.5 | 735.0 | 737.0 | +3.3 | prewarmed | 0 |
| 6 | 920.9 | 911.5 | 915.0 | 917.0 | +3.8 | prewarmed | 0 |

The seasonal model needs two complete cycles per phase, so bursts 1–2 are cold in
both policies. They fall inside the declared 540 s learning prefix but stay in
full-run latency.

Sweep over every bucket alignment and tick phase (1 s grid, 25 runs per seed, `--sweep`):

| Seed | Prewarmed / late (bursts 3–6) | Min margin | Exposed periodic requests: predictive / reactive | Ready Pod-s in replay: cooldown 20 / 60 / reactive |
|---:|---|---:|---|---|
| 401 | 4 / 0 every run | 3.27 s | 12 / 29 | 602–634 / 645–681 / 578 |
| 402 | 4 / 0 every run | 3.13 s | 13 / 39 | 604–636 / 645–681 / 581 |
| 403 | 4 / 0 every run | 3.15 s | 10 / 26 | 603–635 / 644–680 / 581 |
| 404 | 4 / 0 every run | 3.11 s | 7 / 25 | 604–636 / 645–681 / 583 |

Sensitivity (seed 401, worst alignment): prewarming stays on time with publication
up to 9 s late (margin falls to 0.3 s) and patch→Ready up to 4.5 s (margin 0.8 s). At
6 s patch→Ready, 3 of 4 bursts are late; at 8 s all are. **The 2.03 s figure was measured on
Isaac's laptop.** Re-measure it on the pilot host first (section 6, step 1).

## 4. Release between bursts and the cooldown change

Knative already keeps a Pod for the 60 s stable window + 30 s grace after the last
request. A scheduler cooldown longer than about 50 s (60 − one bucket − one tick) keeps the
Pod past the point Knative would release it. A false-positive bucket then costs a
cooldown of Ready time.

- With 20 s, min-scale is restored about 15 s after each burst. The Pod goes at the
  same time as under reactive scaling (~119 s after the last request). The simulated
  gaps show ~77 s with zero Ready Pods before each prewarm.
- With 60 s, prewarming is identical, but Ready time rises from about +4% to about +11.5% over
  reactive (seed 401). The extra time comes from the learning-phase bursts and false
  positives.
- Flapping is cheap here: the scheduler only rewrites one annotation, and Knative's
  60 s window stops a Pod from being dropped and restarted within a burst.

The scheduler must stop when the replay ends. The predictor cannot know that the trace
stopped. In simulation it prewarms for a "next burst" at ~1095 s, 15 s after the 1080 s replay
ends. `--stop-when-exists` (section 1) stops it within about 0.5 s of the last response.
Keep the collector and predictor running for the 40 s tail as in doc 13. If a run did patch
after the replay ended, report it rather than hiding it.

## 5. Missing or stale forecasts

The policy treats each of these as **0 proactive replicas**, which restores the original
min-scale. Knative's reactive autoscaler then serves traffic. This applies even during a cooldown hold:

| Case | Reason in `decisions.jsonl` | Test |
|---|---|---|
| No forecast yet / file missing | `no forecast` | `test_missing_forecast_file_never_actuates` |
| Predictor stopped (past `valid_until`) | `stale forecast (expired …)` | `test_predictor_stopping_ends_a_hold_immediately` |
| Older than 90 s, future-dated, wrong interval, invalid newest line | `stale…`, `…future…`, `forecast interval…`, `invalid forecast…` | `tests/test_policy.py`, `tests/test_predictor_format.py` |
| Forecast reaches only part of the window | decides on the part it covers; a burst may be seen one tick late | `test_publication_later_than_fifteen_seconds_leaves_window_uncovered` |
| Kubernetes API errors ×3 | restore, then 60 s reactive-only backoff | `tests/test_adapter.py` |
| Knative or someone else changes min-scale | detected as `reverted_by_platform` | `tests/test_adapter.py` |
| Scheduler stops (replay end, Ctrl+C, SIGTERM) | restores every min-scale it changed; manifest `final: true` + `stop_reason` | `StopTests`, `tests/test_replay.py` |
| Scheduler crashes | `python -m prewarm.main --restore` uses the marker annotation | `tests/test_adapter.py` |

`tools.readiness_timeline` checks every live run and exits with code 2 if any check fails:

- `fallback.safe`: every fallback decision had target 0 and no `applied` actuation;
- `actuation.safe`: no `error`, `error_suspended`, `restore_failed` or platform revert.
  Each failure is listed with function, time and detail, and suspended ticks are counted
  per function;
- `shutdown.safe`: the scheduler manifest is final, and every function that got a min-scale
  change ends with a `restored` record and no `restore_failed`.

Overall `safe` is all three. An unsafe run is not discarded: report it with the listed
problems.

## 6. Live check: forecast → min-scale → Ready → request

1. **Re-measure patch→Ready on the pilot host** with the cluster idle:
   `python -m spike.actuation_spike --kube-context kind-csc3104 --function periodic --methods revision --trials 5 --out ../results/spike-<host>`.
   If p95 > 4 s, set `lead_time_ms` to p95 + 3 s in a **new** config file. Recheck that
   the section 2 slack stays ≥ 0 (each extra second of lead costs a second of slack;
   add a horizon if needed). Decide this before replay.
2. Run reactive and predictive as in doc 15/13 with fresh IDs and the same trace bytes.
   The collector must start before the replay (lifecycle coverage).
3. Copy `pod_lifecycle.jsonl` into the run directory, finalize, then from `scheduler/`:

   ```bash
   python3 -m tools.readiness_timeline --run-dir ../results/predictive-longgap-401 \
     --scheduler-dir ../results/scheduler/predictive-longgap-401 \
     --forecasts ../predictor/data/predictive-longgap-401/forecasts.jsonl \
     --output ../results/predictive-longgap-401/readiness_timeline.json
   ```

   Run it on the reactive run too, without `--scheduler-dir`, to get Knative's own
   release timing for comparison.
4. Pass criteria for the pilot (fixed before running):
   - every periodic burst after the 540 s prefix is `prewarmed`. That means no Ready Pod
     when min-scale was patched, a Pod created after the patch, and Ready before the
     first request. A margin under ~2 s is within timing resolution (1 s `ready_at`,
     2 s lifecycle polling), so report it as such;
   - `retained` means a Pod was already warm. It is **not** evidence of timed startup.
     If most bursts are `retained`, the workload is still keeping Pods warm;
   - each gap reports `released: yes` with a non-zero `zero_ready_s`. Release is counted only
     after every Pod that served the burst has left Ready;
   - the report's overall `safe` is true (exit code 0): fallback, actuation and shutdown.
5. Hand Hasan the JSON + `.md` table with the run. Raw files stay git-ignored. Back them up
   with hashes as for other runs.

Outcome meanings: `prewarmed`, `late`, `retained`, `warm_without_forecast`, `missed`,
`dry_run`, `unknown` (no lifecycle data). The full definitions are in the docstring of
`scheduler/tools/readiness_timeline.py`.

## 7. Keep the 30-second-cycle results

Keep the seed-319 runs on the original 30 s periodic workload as a **negative result**. Do not
pool them with long-gap runs. Doc 15 reports six exposed requests in both policies and
about 1.8% more Ready replica time for predictive. Raw data is on Jun Kai's machine.

Why prediction could not help there: periodic traffic keeps a background rate between
7 s bursts every 30 s. Knative's 60 s stable window never expires, so reactive scaling
keeps the Pod warm all run. In simulation (`--workload periodic-30s --seed 319
--model-config ../predictor/configs/shadow-5s.json`), one periodic Pod is Ready for
598 of 600 s with or without the scheduler, and the periodic stream never has a 30 s quiet gap.
The only cold request is the very first, which no online forecast can anticipate. Any
min-scale patch on that workload could only be `retained`. This is the case where predictive
control adds cost without benefit, and the report should show it next to the long-gap
result.

## 8. Checklist status

| Item | Status |
|---|---|
| Review settings: buckets, ticks, lead, cooldown, caps, budget | Done (sections 1–4). Cooldown changed 60 → 20 s; caps and budget never bind in binary mode |
| Timeline: forecast → decision → min-scale → Ready → request | Tool done (section 6); simulated timeline in section 3; **live run pending** |
| Pods scale down between bursts; prewarmed vs kept warm | Tool reports release per gap and separates `prewarmed` from `retained`; simulated ~77 s at zero per gap; **live run pending** |
| Missing/stale fallback, actuation failures, shutdown restoration | Done: tests plus automatic checks per live run (section 5) |
| Fix scheduler issues | Done: settings from a hashed config file (no accidental 30 s interval); automatic stop at replay end (no phantom prewarm) |
| Document final settings and readiness evidence | Settings done; **live readiness table to be added** after the pilot |

## 9. Limits

- The simulation models one Pod per function and Knative defaults. It does not model scale-out,
  activator queueing or host noise. It shows the timing has room, not that the cluster
  will meet it.
- The 180 s period is configured, not discovered. This is a favourable periodic case.
- One pilot is not a statistical result. Hasan's repeated runs (≥5 seeds, varied order)
  and the fixed-minimum comparison are still needed. Nothing here bears on AWS Lambda.

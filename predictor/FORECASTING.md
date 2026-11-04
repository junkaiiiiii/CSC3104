# Forecast development and handoff

Scope: changes are confined to `predictor/`. Scheduler and policy evaluation code
remain owned by Isaac and Hasan and are unchanged. No cluster settings are changed
by the development commands below.

## Why the live SMA did not anticipate the first burst

The existing SMA averages three completed buckets and repeats one value at each
horizon. After a quiet interval its history contains zeros, so a previously unseen
first burst cannot be anticipated. The replay workload's periodic function has a
30-second cycle with seven busy seconds. Its current 30-second telemetry buckets
aggregate busy and quiet parts of each cycle; a model cannot recover those exact
within-bucket arrival times from the counts alone. These three-minute runs also
provide little training history. Changing models alone cannot establish a latency
advantage on that workload.

## Available models

- `sma`: the existing baseline; last `window-size` complete counts, equal weights.
- `ewma`: simple exponential smoothing. `alpha` gives recent counts more weight;
  all future horizons use the latest level. State resets on a gap or interval change.
- `seasonal`: average counts at each future phase from up to `season-cycles`
  historical cycles. `season-buckets` is explicit, chosen from training data/domain
  knowledge. Each phase needs two matching complete observations, otherwise it uses
  SMA. Future horizons can differ, so a quiet current phase can precede a busy one.

All models update separately by function, reject duplicate/out-of-order observations,
skip incomplete buckets and reset across missing intervals. They do not read a
future trace. Seasonal history is bounded to the configured cycles. EWMA carries
older information in its level; `smoothing_state` identifies the start/count of that
contiguous training segment. `input_buckets_used` is its recent bucket audit, not a
claim that EWMA forgets older observations within the segment.

The CLI supports `--model`, `--alpha`, `--season-buckets`, `--season-cycles`,
`--horizon-buckets`, and per-function `--model-config`. Without new flags, SMA batch
output and two-horizon live behavior remain compatible with the previous version.
Non-SMA/configured or explicit `--shared-format` batch output uses the shared contract
with simulated publication time; live `--follow` always uses actual publication time.
`--expected-interval` rejects accidental resolution mismatches.

`configs/development-30s.json` is a development configuration for the existing
synthetic bucket generator: its 20-bucket period is 600 seconds at 30-second
resolution. It is NOT a suitable seasonal period for the 30-second replay cycle.
It keeps SMA for sparse demand rather than claiming sparse bursts became predictable.

## Reproduce the forecast-only comparison

Use fresh output paths. Commands run from repository root except the generator's
explicit subshell. The datasets below are generated, not measured cloud traffic.

```bash
(cd telemetry && python3 -m tools.synthetic_buckets --seed 101 --hours 6 \
  --out ../results/forecast-development/tuning-101)
(cd telemetry && python3 -m tools.synthetic_buckets --seed 202 --hours 6 \
  --out ../results/forecast-development/heldout-202)

python3 -m predictor.backtest \
  --input results/forecast-development/tuning-101/demand_buckets.jsonl \
  --output results/forecast-development/tuning-101/comparison.json \
  --season-buckets 20 --season-cycles 3 --window-size 3 --alpha 0.3 \
  --warmup-buckets 60 --horizon-buckets 3 --availability-delay 10 --lead-time 5

python3 -m predictor.backtest \
  --input results/forecast-development/heldout-202/demand_buckets.jsonl \
  --output results/forecast-development/heldout-202/comparison.json \
  --season-buckets 20 --season-cycles 3 --window-size 3 --alpha 0.3 \
  --warmup-buckets 60 --horizon-buckets 3 --availability-delay 10 --lead-time 5
```

The parameters above were fixed before comparing the two seeds. Both synthetic
datasets use the same generator family, so this is a held-out-seed check, not
evidence of generalisation to other workload families or real deployments.

Each model ingests rows chronologically. A target is scored only using a forecast
published at least five seconds BEFORE the target bucket starts. Publication is
simulated as history end plus ten seconds; partly elapsed buckets do not count as
advance forecasts. Missing forecasts remain in the score as zero/reactive fallback.
After the declared 60-bucket training prefix, complete actual buckets are scored
including zero-demand buckets. Reports include MAE, active-bucket MAE, demand recall,
false-positive buckets and coverage. The positive-demand threshold is 0.5, matching
the existing scheduler's binary demand threshold. Lower MAE alone is insufficient.

The existing policy evaluation forecast-MAE matcher matches bucket timestamps but
does not select forecasts against this advance-publication deadline. This tool's
scores are deliberately separate. Hasan should reconcile that distinction before
quoting policy-report MAE as evidence of anticipation; his code is unchanged here.

Results from six synthetic hours per seed:

| Function / model | Seed 101 MAE | Seed 202 MAE |
| --- | ---: | ---: |
| Periodic / SMA | 4.3062 | 4.4928 |
| Periodic / seasonal | 1.3507 | 1.4327 |
| Steady / SMA | 1.9405 | 1.8933 |
| Steady / EWMA | 1.8452 | 1.8209 |
| Sparse / SMA | 0.4566 | 0.3359 |
| Sparse / EWMA | 0.4497 | 0.3325 |

On held-out periodic demand, recall rose from 0.5096 to 0.6015 and false-positive
buckets fell from 167 to 112. Active-bucket MAE fell from 6.9302 to 2.8066. Most
sparse active buckets were still missed. Do not describe this as improved cold-start
latency, cost savings, or superiority to Lambda. The JSON reports are ignored raw
development output; these tables are reproducible summaries.

## Run a model on measured historical telemetry

Do not filter away incomplete rows before a resolution/history audit; models skip
them and reset when the next complete bucket reveals a gap. Specify a warmup small
enough for the available data and disclose it. The three-minute pilots are too short
to establish a 20-bucket repeating pattern.

For batch forecasts, a separate new output file can be selected:

```bash
python3 -m predictor.main --run-id forecast-dev-1 \
  --input results/forecast-development/tuning-101/demand_buckets.jsonl \
  --model-config predictor/configs/development-30s.json \
  --expected-interval 30 --horizon-buckets 3
```

These historical timestamps must not be shifted onto a live clock. Batch shared
records declare an assumed availability delay; they are not evidence that the
predictor published those records in the past.

## Live experiment handoff

Before any real actuation, agree the following with Isaac and Hasan; this PR does
not change their code or silently change existing experiments:

1. Choose a workload whose repeating quiet intervals actually let Knative scale to
   zero. A 30-second cycle may keep instances warm, while the unknown initial burst
   remains cold. Keep unseen irregular bursts as a separate limitation test.
2. Collect enough history before assessing recurring patterns. History must be
   observed traffic, not a future request schedule. Record/exclude training time
   consistently across compared policies and disclose startup behavior separately.
3. For the existing 30-second cycle, a possible development resolution is five-second
   buckets and a six-bucket seasonal period. Preserve the collector's current
   ten-second grace initially; shortening it requires evidence about log lateness.
   Longer forecast horizons are needed: six five-second horizons cover 30 seconds.
   Publication delay + scheduler tick + startup lead must fit inside the horizon.
4. Isaac must agree matching scheduler interval/lead/tick settings. Its existing
   shared contract already accepts distinct horizon counts. Hasan must align
   workload, observation/training windows and repeated policy comparisons. MAE
   between five- and thirty-second buckets cannot be compared directly.
5. Use a fresh run ID, dry-run first, check rejected forecasts and temporal ordering,
   then validate real actuation and repeated latency/resource results separately.

Example predictor-only command once a compatible five-second collector is running:

```bash
python3 -m predictor.main --run-id forecast-fine-pilot \
  --input telemetry/data/forecast-fine-pilot/demand_buckets.jsonl \
  --model seasonal --season-buckets 6 --season-cycles 3 \
  --expected-interval 5 --horizon-buckets 6 --follow
```

This example applies the seasonal model to all input functions; use a per-function
configuration for a mixed workload after tuning it on training data.

## Mixed-model compatibility and live shadow scoring

Jun Kai reproduced both synthetic comparisons on his Mac. The mixed development
profile produced 2,136 forecasts from seed 101 and the unchanged scheduler accepted
all 2,136 with zero rejects in a historical dry-run. That verifies the shared data
interface, not real pre-warming performance.

`configs/shadow-5s.json` is a separate shadow profile for the replay's 30-second
cycle: six five-second phases for periodic demand, EWMA for steady, SMA for sparse.
Use it with `--expected-interval 5 --horizon-buckets 6 --follow`. Keep the collector's
existing ten-second grace initially. Leave the scheduler stopped: the first test
observes predictions while Knative remains reactive. No image rebuild is needed.

After a fresh ten-minute live replay, stop the predictor and finalize the collector
normally. Preserve the replay's real environment metadata and validate request/log
matching. The following forecast-only checker reads the replay window but never
changes the evaluation run or computes policy latency/resource comparisons:

```bash
python3 -m predictor.score_live \
  --actual telemetry/data/forecast-shadow-317/demand_buckets.jsonl \
  --forecasts predictor/data/forecast-shadow-317/forecasts.jsonl \
  --run-dir results/forecast-shadow-317 \
  --warmup-seconds 90 --lead-time 5 \
  --output results/forecast-development/shadow-317-after-warmup.json
```

The 90-second training prefix is measured from client replay start, not collector
startup, so a long idle setup interval cannot stand in for training traffic. Score
again with `--warmup-seconds 0` and a new filename to disclose startup behavior too.
Only complete buckets fully inside the scoring window qualify. Each target uses
the newest forecast published at least five seconds before bucket start; late
forecasts cannot overwrite an eligible earlier forecast. Missing forecasts count
as zero/reactive fallback, and report input hashes preserve provenance. This uses
actual publication timestamps rather than the offline backtest's assumed delay.

The shadow profile and dataset must still be checked on live traffic. Do not merge
or actuate merely because the synthetic comparison or contract test passes.

## References

The seasonal approach is a mean across matching past phases, extending the seasonal
naive baseline; EWMA is simple exponential smoothing. Chronological backtesting
uses the rolling-origin principle, with the additional pre-warming lead deadline.

- Hyndman and Athanasopoulos, *Forecasting: Principles and Practice*,
  [simple methods](https://otexts.com/fpp3/simple-methods.html),
  [exponential smoothing](https://otexts.com/fpp3/ses.html), and
  [time series cross-validation](https://otexts.com/fpp3/tscv.html).

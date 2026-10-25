# predictor
Lead: Sook Yee.

Forecast future per-function bucket counts using only historical data. Moving average first; seasonal/ARIMA models later.

## Hard Guarantee
**No future data look-ahead**: predictions only use historical, finalised buckets
with `bucket_end < current prediction time`. No peeking at future events.

---
## Input Contract (from telemetry module)
Reads `demand_buckets.jsonl` from telemetry output directory. Each line is a
finalised demand bucket record:
| Field | Type | Description |
|---|---|---|
| `function_id` | string | Unique ID of the serverless function |
| `bucket_start` | ISO8601 UTC | Start time of the 30s bucket |
| `bucket_end` | ISO8601 UTC | End time of the 30s bucket |
| `request_count` | int | Total valid user requests in this bucket |
| `complete` | bool | `false` if this bucket overlaps a data gap (unreliable) |

Rules for input processing:
1. Buckets are processed in ascending `bucket_start` order (event time)
2. Skip buckets where `complete = false` (data gap, not trustworthy)
3. Only use buckets that have been finalised by telemetry

---
## Forecasting Logic
### Simple Moving Average (SMA)
For each function:
1. Maintain a sliding window of the last `N` valid (complete=true) historical buckets
2. Predicted request count for next bucket = average of `request_count` in the window

### Configuration
| Parameter | Default | Description |
|---|---|---|
| `WINDOW_SIZE` | 3 | Number of historical buckets to average |
| `BUCKET_INTERVAL_SEC` | 30 | Fixed bucket length (matches telemetry) |

### Edge Cases
- **Cold start (no historical buckets)**: return prediction = 0
- **Fewer than N historical buckets**: average over all available valid buckets
- **Gap buckets (`complete=false`)**: excluded from window, do not break sliding window

---
## Output Contract (for policy module)
Outputs `forecasts.jsonl` to the run output directory. Each line is a prediction:
| Field | Type | Description |
|---|---|---|
| `function_id` | string | Function the prediction is for |
| `forecast_bucket_start` | ISO8601 UTC | Start time of the predicted bucket |
| `forecast_request_count` | float | Predicted average request count |
| `window_size` | int | Window size used for this prediction |
| `input_buckets_used` | list | List of historical buckets used (for audit: proves no future data) |

Example output:
```json
{
  "function_id": "periodic",
  "forecast_bucket_start": "2026-10-25T10:01:30Z",
  "forecast_request_count": 3.33,
  "window_size": 3,
  "input_buckets_used": [
    {"bucket_start": "2026-10-25T10:00:00Z", "request_count": 2},
    {"bucket_start": "2026-10-25T10:00:30Z", "request_count": 5},
    {"bucket_start": "2026-10-25T10:01:00Z", "request_count": 3}
  ]
}


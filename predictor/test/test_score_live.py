import unittest
from datetime import datetime, timedelta, timezone

from predictor.score_live import score

T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)


def at(seconds):
    return T0 + timedelta(seconds=seconds)


def actual(seconds=30, count=4, complete=True):
    return {"function_id": "periodic", "bucket_start": at(seconds).isoformat(),
            "interval_seconds": 5, "invocation_count": count, "complete": complete}


def forecast(generated=20, target=30, count=4):
    return {"function_id": "periodic", "generated_at": at(generated).isoformat(),
            "history_end": at(15).isoformat(), "valid_until": at(60).isoformat(),
            "interval_seconds": 5,
            "predictions": [{"bucket_start": at(target).isoformat(), "invocation_count": count}]}


class LiveScoreTests(unittest.TestCase):
    def test_newest_forecast_before_lead_deadline_wins_not_latest_line(self):
        records = [forecast(20, count=3), forecast(24, count=4), forecast(29, count=999)]
        for order in (records, list(reversed(records))):
            report = score([actual()], order, at(0), at(60), lead_time=5)
            self.assertEqual(report["metrics"]["periodic"]["mae"], 0)
            self.assertEqual(report["metrics"]["periodic"]["advance_forecasts"], 1)
            self.assertEqual(report["late_predictions_for_scored_targets"], 1)

    def test_elapsed_bucket_forecast_cannot_hide_missing_advance_forecast(self):
        report = score([actual()], [forecast(31, count=4)], at(0), at(60), lead_time=5)
        self.assertEqual(report["metrics"]["periodic"]["fallback_buckets"], 1)
        self.assertEqual(report["metrics"]["periodic"]["mae"], 4)

    def test_only_complete_buckets_fully_inside_window(self):
        rows = [actual(0, 999), actual(5, 999, False), actual(10, 4), actual(15, 999)]
        report = score(rows, [], at(3), at(18))
        self.assertEqual(report["metrics"]["periodic"]["scored_buckets"], 1)
        self.assertEqual(report["metrics"]["periodic"]["mae"], 4)

    def test_bad_timing_and_future_training_data_rejected(self):
        invalid = forecast(generated=10)
        with self.assertRaisesRegex(ValueError, "timing"):
            score([actual()], [invalid], at(0), at(60))
        invalid = forecast()
        invalid["input_buckets_used"] = [{"bucket_start": at(14).isoformat()}]
        with self.assertRaisesRegex(ValueError, "history_end"):
            score([actual()], [invalid], at(0), at(60))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            score([actual(), actual()], [], at(0), at(60))

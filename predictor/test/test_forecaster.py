import unittest
from predictor.main import SMAPredictor


class TestSMAPredictor(unittest.TestCase):
    def test_cold_start_no_prediction(self):
        f = SMAPredictor(3)
        self.assertIsNone(f.predict_next("periodic"))

    def test_sma_average_calc(self):
        f = SMAPredictor(3)
        f.ingest({
            "function_id": "periodic",
            "bucket_start": "2026-10-01T00:00:00Z",
            "interval_seconds": 30,
            "invocation_count": 2,
            "complete": True
        })
        f.ingest({
            "function_id": "periodic",
            "bucket_start": "2026-10-01T00:00:30Z",
            "interval_seconds": 30,
            "invocation_count": 5,
            "complete": True
        })
        f.ingest({
            "function_id": "periodic",
            "bucket_start": "2026-10-01T00:01:00Z",
            "interval_seconds": 30,
            "invocation_count": 3,
            "complete": True
        })
        pred = f.predict_next("periodic")
        self.assertAlmostEqual(pred["forecast_invocation_count"], (2+5+3)/3, places=2)
        self.assertEqual(pred["forecast_bucket_start"], "2026-10-01T00:01:30Z")

    def test_skip_incomplete_bucket(self):
        f = SMAPredictor(3)
        f.ingest({
            "function_id": "steady",
            "bucket_start": "2026-10-01T00:00:00Z",
            "interval_seconds": 30,
            "invocation_count": 999,
            "complete": False
        })
        self.assertIsNone(f.predict_next("steady"))

    def test_sliding_window_limit(self):
        f = SMAPredictor(window_size=2)
        base_ts = "2026-10-01T00:00:00Z"
        from datetime import datetime, timedelta
        base_dt = datetime.fromisoformat(base_ts.replace("Z","+00:00"))
        for i, cnt in enumerate([1, 2, 3, 4]):
            current_dt = base_dt + timedelta(seconds=i*30)
            bucket_start = current_dt.isoformat().replace("+00:00","Z")
            f.ingest({
                "function_id": "burst",
                "bucket_start": bucket_start,
                "interval_seconds":30,
                "invocation_count": cnt,
                "complete": True
            })
        pred = f.predict_next("burst")
        self.assertEqual(len(pred["input_buckets_used"]), 2)
        self.assertAlmostEqual(pred["forecast_invocation_count"], 3.5)

if __name__ == "__main__":
    unittest.main()

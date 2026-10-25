import unittest
from predictor.main import SMAForecaster

class TestSMAForecaster(unittest.TestCase):
    def test_cold_start_no_prediction(self):
        f = SMAForecaster(3)
        self.assertIsNone(f.predict_next("periodic"))

    def test_sma_average_calc(self):
        f = SMAForecaster(3)
        f.ingest_bucket({
            "function_id": "periodic",
            "bucket_start": "2026-10-25T10:00:00Z",
            "bucket_end": "2026-10-25T10:00:30Z",
            "request_count": 2,
            "complete": True
        })
        f.ingest_bucket({
            "function_id": "periodic",
            "bucket_start": "2026-10-25T10:00:30Z",
            "bucket_end": "2026-10-25T10:01:00Z",
            "request_count": 5,
            "complete": True
        })
        f.ingest_bucket({
            "function_id": "periodic",
            "bucket_start": "2026-10-25T10:01:00Z",
            "bucket_end": "2026-10-25T10:01:30Z",
            "request_count": 3,
            "complete": True
        })
        pred = f.predict_next("periodic")
        self.assertAlmostEqual(pred["forecast_request_count"], 10/3)

    def test_skip_incomplete_bucket(self):
        f = SMAForecaster(3)
        f.ingest_bucket({
            "function_id": "steady",
            "bucket_start": "2026-10-01T00:00:00Z",
            "bucket_end": "2026-10-01T00:00:30Z",
            "request_count": 999,
            "complete": False
        })
        self.assertIsNone(f.predict_next("steady"))

if __name__ == "__main__":
    unittest.main()

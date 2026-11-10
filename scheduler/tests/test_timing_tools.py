import json
import os
import pathlib
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from prewarm import main as cli
from prewarm.contracts import format_ts
from prewarm.policy import Policy, PolicyConfig
from prewarm.source import ForecastBook
from tools import readiness_timeline as rt
from tools import timing_check as tc

ROOT = pathlib.Path(__file__).resolve().parents[2]
T0 = datetime(2026, 11, 10, 3, 0, 0, tzinfo=timezone.utc)


def at(s):
    return rt.stamp(T0 + timedelta(seconds=s))


class BudgetTests(unittest.TestCase):
    def test_long_gap_settings_leave_slack(self):
        b = tc.budget(tc.Settings(cooldown=20))
        self.assertTrue(b["ok"])
        self.assertAlmostEqual(b["coverage_slack_s"], 3.5)          # 25 - 10 - 1.5 - 5 - 5
        self.assertAlmostEqual(b["worst_ready_margin_s"], 2.968)
        self.assertEqual(b["max_extra_hold_vs_knative_s"], 0.0)

    def test_too_short_horizon_or_lead_fails(self):
        self.assertFalse(tc.budget(tc.Settings(horizons=4))["ok"])
        self.assertFalse(tc.budget(tc.Settings(lead_time_ms=2000))["ok"])


class SimulationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.trace, cls.duration = tc.load_trace("long-gap", 401)
        cls.config = json.loads((ROOT / "predictor/configs/long-gap-5s.json").read_text())

    def test_bursts_after_two_cycles_are_prewarmed_and_released(self):
        run = tc.simulate(self.trace, self.config, tc.Settings(cooldown=20), duration=self.duration)
        outcomes = [b["outcome"] for b in run["bursts"]]
        self.assertEqual(outcomes[:2], ["missed", "missed"])     # seasonal model still learning
        self.assertEqual(outcomes[2:], ["prewarmed"] * 4)
        for burst in run["bursts"][2:]:
            self.assertGreater(burst["ready_margin_s"], 2.0)
            self.assertLess(burst["min_scale_patch_s"], burst["first_request_s"])
            self.assertLess(burst["forecast_published_s"], burst["min_scale_patch_s"])
        for burst in run["bursts"][:-1]:
            self.assertGreater(burst["zero_ready_s_before_next"], 30)   # back to zero between bursts

    def test_slow_pod_start_is_late(self):
        run = tc.simulate(self.trace, self.config, tc.Settings(cooldown=20, ready_s=8.0),
                          duration=self.duration)
        self.assertEqual(run["outcomes"]["prewarmed"], 0)
        self.assertEqual(run["outcomes"]["late"], 4)


def lifecycle(pod, fn, seen, ready=None, terminating=None, gone=None, created=None):
    base = {"pod": pod, "function_id": fn, "created_at": at(created if created is not None else seen)}
    rows = [{**base, "event": "pod_seen", "observed_at": at(seen)}]
    if ready is not None:
        rows.append({**base, "event": "pod_ready", "observed_at": at(ready + 1), "ready_at": at(ready)})
    if terminating is not None:
        rows.append({**base, "event": "pod_terminating", "observed_at": at(terminating)})
    if gone is not None:
        rows.append({**base, "event": "pod_gone", "observed_at": at(gone)})
    return rows


def requests(fn, *times):
    return [{"function_id": fn, "sent_at": at(t), "completed_at": at(t + 0.1), "request_kind": "user"}
            for t in times]


def tick(fn, t, target, forecast_at=None, outcome=None, reason=""):
    decision = {"function_id": fn, "decided_at": at(t), "forecast_generated_at": forecast_at and at(forecast_at),
                "target_warm_replicas": target, "lead_time_ms": 5000, "reason": reason, "dry_run": False}
    audit = {"function_id": fn, "decided_at": at(t),
             "outcome": outcome or ("forecast" if target else "forecast")}
    return decision, audit


class TimelineTests(unittest.TestCase):
    def build(self, extra_decisions=(), extra_actuation=(), final=True, drop_final_restore=False):
        fn = "periodic"
        # burst 1 at 0 s: nothing prepared -> missed; reactive Pod a ready at 2 s, gone at 120 s.
        # burst 2 at 200 s: patch at 194 s, Pod b created 194 s, ready 196 s -> prewarmed.
        # burst 3 at 260 s: Pod b still Ready when patched again at 255 s -> retained.
        # burst 4 at 600 s: patch at 599 s, Pod c ready 602 s -> late.
        life = (lifecycle("a", fn, 0, ready=2, terminating=90, gone=120)
                + lifecycle("b", fn, 194, ready=196, terminating=330, gone=360)
                + lifecycle("c", fn, 599, ready=602))
        reqs = requests(fn, 0, 1, 3, 200, 201, 260, 600, 601, 603)
        ticks = [tick(fn, 190, 0), tick(fn, 194, 1, forecast_at=185), tick(fn, 199, 1, forecast_at=190),
                 tick(fn, 215, 0), tick(fn, 255, 1, forecast_at=250), tick(fn, 265, 0),
                 tick(fn, 599, 1, forecast_at=590)] + list(extra_decisions)
        actuation = [{"function_id": fn, "at": at(194), "status": "applied", "applied_min_scale": "1"},
                     {"function_id": fn, "at": at(199), "status": "noop"},
                     {"function_id": fn, "at": at(215), "status": "restored"},
                     {"function_id": fn, "at": at(255), "status": "applied"},
                     {"function_id": fn, "at": at(265), "status": "restored"},
                     {"function_id": fn, "at": at(599), "status": "applied"},
                     {"function_id": fn, "at": at(700), "status": "restored"}] + list(extra_actuation)
        forecasts = [{"function_id": fn, "generated_at": at(185),
                      "predictions": [{"bucket_start": at(200), "invocation_count": 3}]}]
        if drop_final_restore:
            actuation = [a for a in actuation if a["at"] != at(700)]
        manifest = {"run_id": "t", "actuator": "min-scale/revision", "final": final, "ended_at": at(700),
                    "stop_reason": "x appeared (replay finished)",
                    "policy": {"interval_seconds": 5, "demand_threshold": 0.5, "dry_run": False}}
        return rt.build(reqs, life, [d for d, _ in ticks], [a for _, a in ticks], actuation, manifest,
                        forecasts, ("periodic",), 30.0)

    def test_outcomes_and_timeline(self):
        report = self.build()
        outcomes = [b["outcome"] for b in report["bursts"]]
        self.assertEqual(outcomes, ["missed", "prewarmed", "retained", "late"])
        pre = report["bursts"][1]
        self.assertEqual(pre["forecast_first_saw_burst_at"], at(185))
        self.assertEqual(pre["positive_decision_at"], at(194))
        self.assertEqual(pre["min_scale_patch_at"], at(194))
        self.assertEqual(pre["ready_pods_at_patch"], 0)
        self.assertEqual(pre["pod_ready_at"], at(196))
        self.assertEqual(pre["ready_margin_s"], 4.0)
        self.assertEqual(pre["exposed_requests"], 0)
        self.assertEqual(report["bursts"][0]["exposed_requests"], 2)     # 0 s and 1 s before Ready at 2 s
        self.assertEqual(report["bursts"][3]["exposed_requests"], 2)

    def test_gaps_show_release(self):
        gaps = {g["after_burst"]: g for g in self.build()["gaps"]}
        self.assertTrue(gaps[1]["released"])
        self.assertEqual(gaps[1]["no_ready_pod_after_s"], 87.0)          # Pod a terminating at 90 s
        self.assertFalse(gaps[2]["released"])                            # Pod b Ready through 200-260 s
        self.assertTrue(gaps[3]["released"])
        self.assertEqual(gaps[3]["min_scale_restored_after_s"], 5.0)

    def test_fallback_safe_and_violation_detected(self):
        ok = self.build(extra_decisions=[tick("periodic", 650, 0, outcome="fallback",
                                              reason="stale forecast (expired x); reactive fallback")])
        self.assertTrue(ok["fallback"]["safe"])
        self.assertEqual(ok["fallback"]["reasons"], {"stale forecast": 1})
        bad = self.build(extra_decisions=[tick("periodic", 650, 0, outcome="fallback", reason="no forecast")],
                         extra_actuation=[{"function_id": "periodic", "at": at(650), "status": "applied"}])
        self.assertFalse(bad["fallback"]["safe"])

    def test_clean_run_is_safe(self):
        report = self.build()
        self.assertTrue(report["safe"])
        self.assertEqual(report["shutdown"]["patched_functions"], ["periodic"])
        self.assertEqual(report["shutdown"]["not_restored"], [])
        self.assertEqual(report["shutdown"]["stop_reason"], "x appeared (replay finished)")

    def test_actuation_failures_are_listed_and_unsafe(self):
        report = self.build(extra_actuation=[
            {"function_id": "periodic", "at": at(400), "status": "error", "detail": "timeout"},
            {"function_id": "periodic", "at": at(405), "status": "error_suspended", "detail": "timeout",
             "restored_after_failures": "periodic-00002"},
            {"function_id": "periodic", "at": at(410), "status": "suspended"}])
        self.assertFalse(report["actuation"]["safe"])
        self.assertFalse(report["safe"])
        self.assertEqual([p["status"] for p in report["actuation"]["problems"]], ["error", "error_suspended"])
        self.assertEqual(report["actuation"]["suspended_ticks"], {"periodic": 1})
        self.assertIn("Actuation failures: 2", rt.markdown(report))

    def test_platform_revert_is_unsafe(self):
        report = self.build(extra_actuation=[{"function_id": "periodic", "at": at(420), "status": "applied",
                                              "reverted_by_platform": True, "previous_min_scale": "0"},
                                             {"function_id": "periodic", "at": at(430), "status": "restored"}])
        self.assertFalse(report["actuation"]["safe"])

    def test_shutdown_without_restore_or_final_manifest_is_unsafe(self):
        missing = self.build(drop_final_restore=True)
        self.assertEqual(missing["shutdown"]["not_restored"], ["periodic"])
        self.assertFalse(missing["safe"])
        self.assertFalse(self.build(final=False)["shutdown"]["safe"])
        failed = self.build(extra_actuation=[{"function_id": "periodic", "at": at(701),
                                              "status": "restore_failed", "detail": "forbidden"}])
        self.assertFalse(failed["shutdown"]["safe"])

    def test_cli_reactive_run_writes_json_and_markdown(self):
        tmp = tempfile.mkdtemp()
        try:
            run = pathlib.Path(tmp, "reactive-1")
            run.mkdir()
            run.joinpath("client_measurements.jsonl").write_text(
                "".join(json.dumps(r) + "\n" for r in requests("periodic", 0, 1, 200)))
            run.joinpath("pod_lifecycle.jsonl").write_text(
                "".join(json.dumps(r) + "\n" for r in lifecycle("a", "periodic", 0, ready=2,
                                                                terminating=90, gone=120)))
            out = run / "readiness_timeline.json"
            self.assertEqual(rt.main(["--run-dir", str(run), "--output", str(out)]), 0)
            report = json.loads(out.read_text())
            self.assertEqual([b["outcome"] for b in report["bursts"]], ["missed", "missed"])
            self.assertTrue(report["gaps"][0]["released"])
            # Pod was still starting at the last request (1 s): release counts from when it left Ready.
            self.assertEqual(report["gaps"][0]["no_ready_pod_after_s"], 89.0)
            self.assertTrue(out.with_suffix(".md").is_file())
            with self.assertRaises(SystemExit):           # never overwrite a report
                rt.main(["--run-dir", str(run), "--output", str(out)])
        finally:
            shutil.rmtree(tmp)

    def test_dry_run_and_missing_lifecycle_are_labelled(self):
        report = rt.build(requests("periodic", 0), [], [], [], [], {"actuator": "dry-run",
                          "policy": {"dry_run": True}}, (), ("periodic",))
        self.assertEqual(report["bursts"][0]["outcome"], "unknown")
        report = rt.build(requests("periodic", 5), lifecycle("a", "periodic", 0, ready=1), [], [], [],
                          {"actuator": "dry-run", "policy": {"dry_run": True}}, (), ("periodic",))
        self.assertEqual(report["bursts"][0]["outcome"], "dry_run")
        self.assertIn("Readiness timeline", rt.markdown(report))


def live(fn, history_end, counts, delay=11.5):
    """A live predictor record: 5 s buckets, published `delay` s after its history ends."""
    return {"function_id": fn, "generated_at": format_ts(history_end + timedelta(seconds=delay)),
            "history_end": format_ts(history_end), "interval_seconds": 5, "model": "seasonal-p36-c3",
            "predictions": [{"bucket_start": format_ts(history_end + timedelta(seconds=5 * i)),
                             "invocation_count": c} for i, c in enumerate(counts)],
            "valid_until": format_ts(history_end + timedelta(seconds=5 * len(counts)))}


LONG_GAP = json.loads((ROOT / "scheduler/configs/long-gap-5s.json").read_text())


def long_gap_config(**overrides):
    return PolicyConfig(**{**LONG_GAP, **overrides})


class LiveFiveSecondTests(unittest.TestCase):
    def stream(self, delay, seconds=120):
        book = ForecastBook(["periodic"], 5)
        for i in range(seconds // 5):
            book.add_line(i, json.dumps(live("periodic", T0 + timedelta(seconds=5 * i), (1,) * 6, delay)))
        return book

    def covered_through(self, book, now):
        forecast = book.latest("periodic", now)
        return forecast.predictions[-1][0] + timedelta(seconds=5)

    def test_six_horizons_cover_lead_plus_tick_with_ten_second_grace(self):
        book, policy = self.stream(11.5), Policy(long_gap_config(), ["periodic"])
        for phase in range(5):
            now = T0 + timedelta(seconds=20 + phase)
            while now < T0 + timedelta(seconds=100):
                _, audits = policy.decide(now, book.snapshot(now))
                self.assertEqual(audits[0]["outcome"], "forecast")
                self.assertGreaterEqual(self.covered_through(book, now), now + timedelta(seconds=10))
                now += timedelta(seconds=5)

    def test_publication_later_than_fifteen_seconds_leaves_window_uncovered(self):
        book = self.stream(16.5)
        short = [p for p in range(5)
                 if self.covered_through(book, T0 + timedelta(seconds=60 + p)) <
                 T0 + timedelta(seconds=70 + p)]
        self.assertTrue(short)

    def test_predictor_stopping_ends_a_hold_immediately(self):
        book = ForecastBook(["periodic"], 5)
        book.add_line(1, json.dumps(live("periodic", T0, (0, 0, 3, 3, 0, 0))))
        book.add_line(2, json.dumps(live("periodic", T0 + timedelta(seconds=5), (0,) * 6)))
        policy = Policy(long_gap_config(cooldown_seconds=60), ["periodic"])
        now = T0 + timedelta(seconds=12)
        decisions, _ = policy.decide(now, book.snapshot(now))
        self.assertEqual(decisions[0]["target_warm_replicas"], 1)
        now = T0 + timedelta(seconds=17)
        decisions, audits = policy.decide(now, book.snapshot(now))
        self.assertEqual(audits[0]["outcome"], "hold")
        now = T0 + timedelta(seconds=35)          # second record expired; predictor has stopped
        decisions, audits = policy.decide(now, book.snapshot(now))
        self.assertEqual(audits[0]["outcome"], "fallback")
        self.assertEqual(decisions[0]["target_warm_replicas"], 0)
        self.assertIn("stale", decisions[0]["reason"])

    def test_missing_forecast_file_never_actuates(self):
        tmp = tempfile.mkdtemp()
        try:
            argv = ["--run-id", "nofile", "--forecasts", os.path.join(tmp, "absent.jsonl"),
                    "--out-dir", tmp, "--config", str(ROOT / "scheduler/configs/long-gap-5s.json"),
                    "--replay", format_ts(T0), format_ts(T0 + timedelta(seconds=30))]
            self.assertEqual(cli.main(argv), 0)
            with open(os.path.join(tmp, "nofile", "decisions.jsonl")) as fh:
                rows = [json.loads(l) for l in fh]
            self.assertTrue(rows)
            self.assertTrue(all(r["target_warm_replicas"] == 0 and "no forecast" in r["reason"] for r in rows))
        finally:
            shutil.rmtree(tmp)


class StopTests(unittest.TestCase):
    """Live (wall-clock) runs stop by themselves when the replay ends, and still restore."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.forecasts = os.path.join(self.tmp, "f.jsonl")
        open(self.forecasts, "w").close()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def argv(self, run_id, *extra):
        return ["--run-id", run_id, "--forecasts", self.forecasts, "--out-dir", self.tmp,
                "--actuator", "dry-run", "--backend", "none", "--tick", "0.2", *extra]

    def manifest(self, run_id):
        with open(os.path.join(self.tmp, run_id, "scheduler_manifest.json")) as fh:
            return json.load(fh)

    def test_stops_when_replay_manifest_appears(self):
        import threading
        marker = os.path.join(self.tmp, "run_manifest.json")
        timer = threading.Timer(0.6, lambda: open(marker, "w").close())
        timer.start()
        try:
            self.assertEqual(cli.main(self.argv("stopfile", "--stop-when-exists", marker)), 0)
        finally:
            timer.cancel()
        manifest = self.manifest("stopfile")
        self.assertTrue(manifest["final"])
        self.assertIn("appeared", manifest["stop_reason"])
        self.assertEqual(manifest["stop_conditions"]["stop_when_exists"], marker)
        self.assertGreaterEqual(manifest["totals"]["ticks"], 1)

    def test_stop_at_in_the_past_stops_after_one_tick(self):
        self.assertEqual(cli.main(self.argv("stopat", "--stop-at", "2020-01-01T00:00:00Z")), 0)
        manifest = self.manifest("stopat")
        self.assertIn("stop time", manifest["stop_reason"])
        self.assertEqual(manifest["totals"]["ticks"], 1)

    def test_existing_stop_file_is_refused(self):
        marker = os.path.join(self.tmp, "old_run_manifest.json")
        open(marker, "w").close()
        with self.assertRaises(SystemExit):
            cli.main(self.argv("old", "--stop-when-exists", marker))

    def test_should_stop_reasons(self):
        args = cli.build_parser().parse_args(["--duration", "10"])
        self.assertIsNone(cli.should_stop(args, None, 100.0, monotonic=50.0))
        self.assertIn("duration", cli.should_stop(args, None, 100.0, monotonic=100.0))
        self.assertIn("stop time", cli.should_stop(args, T0, None, now=T0))


class ConfigFileTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_config_sets_policy_and_flags_override(self):
        path = os.path.join(self.tmp, "f.jsonl")
        open(path, "w").close()
        argv = ["--run-id", "cfg", "--forecasts", path, "--out-dir", self.tmp,
                "--config", str(ROOT / "scheduler/configs/long-gap-5s.json"), "--lead-time-ms", "6000",
                "--replay", format_ts(T0), format_ts(T0 + timedelta(seconds=10))]
        self.assertEqual(cli.main(argv), 0)
        with open(os.path.join(self.tmp, "cfg", "scheduler_manifest.json")) as fh:
            manifest = json.load(fh)
        self.assertEqual(manifest["policy"]["interval_seconds"], 5)
        self.assertEqual(manifest["policy"]["cooldown_seconds"], 20.0)
        self.assertEqual(manifest["policy"]["lead_time_ms"], 6000)
        self.assertEqual(len(manifest["policy_config_file"]["sha256"]), 64)

    def test_unknown_setting_rejected(self):
        bad = os.path.join(self.tmp, "bad.json")
        with open(bad, "w") as fh:
            json.dump({"interval_seconds": 5, "dry_run": False}, fh)
        with self.assertRaises(SystemExit):
            cli.parse_args(["--config", bad])


if __name__ == "__main__":
    unittest.main()

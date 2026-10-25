import unittest
from datetime import datetime, timezone

from prewarm.adapter import (ABSENT, MARKER, MAX_SCALE, MIN_SCALE, DryRunActuator,
                             MinScaleActuator, WarmupActuator, restore_namespace)
from prewarm.kube import KubeError, KubectlClient
from tests.fakes import FakeKube

NOW = datetime(2026, 10, 1, tzinfo=timezone.utc)


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class MinScaleTests(unittest.TestCase):
    def setUp(self):
        self.kube = FakeKube()
        self.kube.add_function("periodic", "periodic-00001", {MIN_SCALE: "0", MAX_SCALE: "4"})
        self.clock = Clock()
        self.act = MinScaleActuator(self.kube, "revision", max_failures=2, backoff_s=30, clock=self.clock)

    def ann(self, kind="revision", name="periodic-00001"):
        return self.kube.annotations(kind, name)

    def test_apply_records_original_and_is_idempotent(self):
        r = self.act.apply("periodic", 1, NOW)
        self.assertEqual(r["status"], "applied")
        self.assertEqual(self.ann()[MIN_SCALE], "1")
        self.assertEqual(self.ann()[MARKER], "0")
        writes = len(self.kube.patches)
        r = self.act.apply("periodic", 1, NOW)
        self.assertEqual(r["status"], "noop")
        self.assertEqual(len(self.kube.patches), writes)

    def test_never_touches_service_or_creates_revisions(self):
        self.act.apply("periodic", 2, NOW)
        self.act.apply("periodic", 0, NOW)
        self.assertTrue(all(kind == "revision" for kind, _n, _b in self.kube.patches))

    def test_release_restores_original(self):
        self.act.apply("periodic", 2, NOW)
        r = self.act.apply("periodic", 0, NOW)
        self.assertEqual(r["status"], "restored")
        self.assertEqual(self.ann()[MIN_SCALE], "0")
        self.assertNotIn(MARKER, self.ann())

    def test_absent_original_is_removed_on_restore(self):
        kube = FakeKube()
        kube.add_function("steady", "steady-00001", {})
        act = MinScaleActuator(kube)
        act.apply("steady", 1, NOW)
        self.assertEqual(kube.annotations("revision", "steady-00001")[MARKER], ABSENT)
        act.restore_all(NOW)
        self.assertEqual(kube.annotations("revision", "steady-00001"), {})

    def test_zero_without_prior_change_is_noop(self):
        r = self.act.apply("periodic", 0, NOW)
        self.assertEqual(r["status"], "noop")
        self.assertEqual(self.kube.patches, [])

    def test_never_below_fixed_baseline_minimum(self):
        kube = FakeKube()
        kube.add_function("periodic", "periodic-00001", {MIN_SCALE: "1"})
        act = MinScaleActuator(kube)
        self.assertEqual(act.apply("periodic", 0, NOW)["status"], "noop")
        self.assertEqual(act.apply("periodic", 1, NOW)["status"], "noop")
        self.assertEqual(kube.annotations("revision", "periodic-00001")[MIN_SCALE], "1")

    def test_capped_by_max_scale(self):
        kube = FakeKube()
        kube.add_function("periodic", "periodic-00001", {MAX_SCALE: "1"})
        r = MinScaleActuator(kube).apply("periodic", 2, NOW)
        self.assertEqual(r["applied_min_scale"], "1")
        self.assertEqual(r["capped_by_max_scale"], 1)

    def test_pa_target(self):
        act = MinScaleActuator(self.kube, "pa")
        act.apply("periodic", 1, NOW)
        self.assertEqual(self.ann("pa")[MIN_SCALE], "1")
        self.assertEqual(self.ann("revision")[MIN_SCALE], "0")

    def test_detects_platform_revert(self):
        self.act.apply("periodic", 1, NOW)
        self.kube.objects[("revision", "periodic-00001")]["metadata"]["annotations"][MIN_SCALE] = "0"
        r = self.act.apply("periodic", 1, NOW)
        self.assertTrue(r["reverted_by_platform"])
        self.assertEqual(r["status"], "applied")
        self.assertEqual(self.act.reverts, 1)

    def test_new_revision_restores_old_first(self):
        self.act.apply("periodic", 1, NOW)
        self.kube.objects[("revision", "periodic-00002")] = {"metadata": {
            "name": "periodic-00002", "annotations": {MIN_SCALE: "0"}}}
        self.kube.objects[("ksvc", "periodic")]["status"]["latestReadyRevisionName"] = "periodic-00002"
        r = self.act.apply("periodic", 1, NOW)
        self.assertEqual(r["restored_previous_revision"], "periodic-00001")
        self.assertNotIn(MARKER, self.ann())
        self.assertEqual(self.ann(name="periodic-00002")[MIN_SCALE], "1")

    def test_failures_suspend_restore_and_recover(self):
        self.act.apply("periodic", 1, NOW)
        self.kube.fail_next = 1
        self.assertEqual(self.act.apply("periodic", 1, NOW)["status"], "error")
        self.kube.fail_next = 1
        r = self.act.apply("periodic", 1, NOW)
        self.assertEqual(r["status"], "error_suspended")
        self.assertEqual(r["restored_after_failures"], "periodic-00001")
        self.assertEqual(self.ann()[MIN_SCALE], "0")
        self.assertEqual(self.act.apply("periodic", 1, NOW)["status"], "suspended")
        self.clock.t = 31
        self.assertEqual(self.act.apply("periodic", 1, NOW)["status"], "applied")

    def test_restart_recovers_original_from_marker(self):
        self.act.apply("periodic", 1, NOW)
        fresh = MinScaleActuator(self.kube)          # simulated scheduler restart
        r = fresh.apply("periodic", 0, NOW)
        self.assertEqual(r["status"], "restored")
        self.assertEqual(self.ann()[MIN_SCALE], "0")

    def test_restore_namespace(self):
        self.act.apply("periodic", 2, NOW)
        restored = restore_namespace(self.kube, "revision")
        self.assertEqual(restored, [("periodic-00001", "0")])
        self.assertEqual(self.ann()[MIN_SCALE], "0")

    def test_readiness_reported(self):
        self.kube.set_pods("periodic", ready=1, not_ready=1)
        r = self.act.apply("periodic", 1, NOW)
        self.assertEqual(r["readiness"], {"ready_pods": 1, "pods": 2})


class OtherActuatorTests(unittest.TestCase):
    def test_dry_run_changes_nothing(self):
        kube = FakeKube()
        kube.add_function("periodic", "periodic-00001")
        r = DryRunActuator(kube).apply("periodic", 2, NOW)
        self.assertEqual(r["status"], "dry_run")
        self.assertEqual(kube.patches, [])

    def test_warmup_sends_marked_requests(self):
        seen = []

        class Resp:
            status = 200

            def read(self):
                return b"{}"

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def opener(req, timeout):
            seen.append(dict(req.header_items()))
            return Resp()

        act = WarmupActuator(opener=opener)
        self.assertEqual(act.apply("periodic", 0, NOW)["status"], "noop")
        r = act.apply("periodic", 2, NOW)
        self.assertEqual(r["status"], "applied")
        self.assertEqual(len(seen), 2)
        for headers in seen:
            self.assertEqual(headers["X-request-kind"], "warmup")
            self.assertEqual(headers["Host"], "periodic.csc3104.example.com")
        self.assertEqual(len({h["X-request-id"] for h in seen}), 2)


class KubectlClientTests(unittest.TestCase):
    def test_command_shape_and_errors(self):
        calls = []

        class Proc:
            def __init__(self, rc, out="", err=""):
                self.returncode, self.stdout, self.stderr = rc, out, err

        def runner(cmd, **kw):
            calls.append(cmd)
            if "patch" in cmd:
                return Proc(1, err='Error from server (NotFound): revisions "x" not found')
            return Proc(0, '{"items": [1]}')

        client = KubectlClient("csc3104", context="kind-csc3104", runner=runner)
        self.assertEqual(client.list("pa", "a=b"), [1])
        self.assertEqual(calls[0][:6], ["kubectl", "--context", "kind-csc3104", "get",
                                        "podautoscalers.autoscaling.internal.knative.dev", "-l"])
        with self.assertRaises(KubeError) as ctx:
            client.merge_patch("revision", "x", {"metadata": {}})
        self.assertTrue(ctx.exception.not_found)
        self.assertIn("--type", calls[1])
        self.assertEqual(client.calls, 2)


if __name__ == "__main__":
    unittest.main()

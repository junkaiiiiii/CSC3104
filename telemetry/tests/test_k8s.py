import json
import os
import queue
import stat
import sys
import tempfile
import unittest
from pathlib import Path

from collector.collector import Collector
from collector.k8s import (RECORD_FIELDS, KubeSource, PodTracker, parse_cpu_m, parse_mem_mi,
                           parse_pod, split_stamp)
from tests.test_telemetry import ROOT, line, ts

STAMP = "2026-10-25T06:11:05.123456789Z"


def pod_item(name="p1", function="periodic", ready=False, terminating=False, started=True,
             terminated=False, requests=True):
    state = {"terminated": {}} if terminated else ({"running": {}} if started else {"waiting": {}})
    meta = {"name": name, "creationTimestamp": "2026-10-25T06:11:01Z",
            "labels": {"serving.knative.dev/service": function, "serving.knative.dev/revision": function + "-00001"}}
    if terminating:
        meta["deletionTimestamp"] = "2026-10-25T06:15:00Z"
        meta["deletionGracePeriodSeconds"] = 300
    resources = {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "256Mi"}}
    return {"metadata": meta, "spec": {"containers": [
        {"name": "user-container", "resources": resources if requests else {}},
        {"name": "queue-proxy", "resources": {"requests": {"cpu": "25m"}}}]},
        "status": {"phase": "Running", "containerStatuses": [{"name": "user-container", "state": state}],
                   "conditions": [{"type": "Ready", "status": "True" if ready else "False",
                                   "lastTransitionTime": "2026-10-25T06:11:04Z"}]}}


def infos(*items):
    return [parse_pod(i) for i in items]


class FakeProc:
    def __init__(self, lines, rc=0, on_start=None):
        self.stdout, self._rc = lines, rc
        if on_start:
            on_start()

    def wait(self):
        return self._rc

    def terminate(self):
        pass


class ParsingTests(unittest.TestCase):
    def test_quantities(self):
        self.assertEqual(parse_cpu_m("100m"), 100)
        self.assertEqual(parse_cpu_m("1"), 1000)
        self.assertEqual(parse_cpu_m("0.5"), 500)
        self.assertEqual(parse_mem_mi("128Mi"), 128)
        self.assertEqual(parse_mem_mi("1Gi"), 1024)
        self.assertEqual(parse_mem_mi("268435456"), 256)
        self.assertIsNone(parse_cpu_m(None))
        self.assertIsNone(parse_mem_mi("lots"))

    def test_pod_info_sums_all_containers_and_flags_state(self):
        info = parse_pod(pod_item(ready=True))
        self.assertEqual((info["cpu_request_m"], info["mem_request_mi"]), (125, 128))
        self.assertEqual((info["cpu_limit_m"], info["mem_limit_mi"]), (1000, 256))
        self.assertTrue(info["ready"] and info["started"] and not info["terminating"])
        self.assertEqual(info["ready_at"], "2026-10-25T06:11:04Z")
        self.assertEqual(parse_pod(pod_item(terminating=True))["deletion_grace_period_s"], 300)
        self.assertIsNone(info["deletion_grace_period_s"])
        self.assertIsNone(parse_pod({"metadata": {"name": "x", "labels": {}}}))
        unset = parse_pod(pod_item(requests=False))
        self.assertIsNone(unset["mem_request_mi"])  # unknown stays None, not 0

    def test_split_stamp(self):
        self.assertEqual(split_stamp(STAMP + ' {"a": 1}\n'), (STAMP, '{"a": 1}'))
        self.assertEqual(split_stamp("no timestamp here"), (None, "no timestamp here"))


class TrackerTests(unittest.TestCase):
    def test_lifecycle_events_in_order(self):
        t, seen = PodTracker(), []
        steps = [infos(pod_item(started=False)), infos(pod_item(ready=True)), infos(pod_item(ready=True)),
                 infos(pod_item(ready=True, terminating=True)), []]
        for n, step in enumerate(steps):
            seen.append([r["event"] for r in t.update(step, "t%d" % n)])
        self.assertEqual(seen, [["pod_seen"], ["pod_ready"], [], ["pod_terminating"], ["pod_gone"]])

    def test_unready_and_first_seen_already_ready(self):
        t = PodTracker()
        self.assertEqual([r["event"] for r in t.update(infos(pod_item(ready=True)), "a")], ["pod_seen", "pod_ready"])
        self.assertEqual([r["event"] for r in t.update(infos(pod_item(ready=False)), "b")], ["pod_unready"])

    def test_records_match_contract_sample(self):
        sample = json.loads((ROOT / "contracts/pod_lifecycle.example.json").read_text())
        record = PodTracker().update(infos(pod_item(ready=True)), "t")[0]
        self.assertEqual(set(record), set(sample))
        self.assertEqual(tuple(record), RECORD_FIELDS)


class FollowTests(unittest.TestCase):
    def source(self, spawn, live):
        s = KubeSource("csc3104", ["periodic"], spawn=spawn, retry_s=0, now=lambda: 1792908700.0)
        s._live = live
        return s

    def drain(self, s):
        items = []
        while True:
            try:
                items.append(s.out.get_nowait())
            except queue.Empty:
                return items

    def test_clean_drain_strips_timestamps(self):
        live = {"p1": parse_pod(pod_item(ready=True, terminating=True))}
        s = self.source(lambda cmd: FakeProc([STAMP + " " + line(request_id="a") + "\n"]), live)
        s._follow("p1", "periodic", "2026-10-25T06:11:01Z")
        items = self.drain(s)
        self.assertEqual([i[0] for i in items], ["line"])
        self.assertEqual(json.loads(items[0][2])["request_id"], "a")

    def test_reconnect_uses_since_time_and_ends_cleanly(self):
        live = {"p1": parse_pod(pod_item(ready=True))}
        calls = []

        def spawn(cmd):
            calls.append(cmd)
            if len(calls) == 1:
                return FakeProc([STAMP + " first\n"])
            return FakeProc([], on_start=lambda: live.update(p1=parse_pod(pod_item(ready=True, terminating=True))))

        s = self.source(spawn, live)
        s._follow("p1", "periodic", "2026-10-25T06:11:01Z")
        self.assertNotIn("--since-time", calls[0])
        self.assertEqual(calls[1][calls[1].index("--since-time") + 1], STAMP)
        self.assertEqual([i[0] for i in self.drain(s)], ["line"])  # no gap

    def test_pod_vanishing_without_clean_drain_is_a_gap(self):
        s = self.source(lambda cmd: FakeProc([STAMP + " x\n"], rc=1), {})  # pod already gone
        s._follow("p1", "periodic", "2026-10-25T06:11:01Z")
        kinds = [i[0] for i in self.drain(s)]
        self.assertEqual(kinds, ["line", "gap"])

    def test_pod_that_never_started_leaves_no_gap(self):
        s = self.source(lambda cmd: FakeProc([], rc=1), {})
        s._follow("p1", "periodic", "2026-10-25T06:11:01Z")
        self.assertEqual(self.drain(s), [])

    def test_kubectl_outage_is_recorded_as_gap_on_recovery(self):
        clock = [1000.0]
        answers = [json.dumps({"items": []}), OSError("boom"), OSError("boom"), json.dumps({"items": []})]

        def run(cmd):
            answer = answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer

        s = KubeSource("csc3104", ["periodic"], run=run, now=lambda: clock[0], poll_interval_s=2)
        s.poll_once(False)
        for t in (1010.0, 1020.0, 1030.0):
            clock[0] = t
            s.poll_once(False)
        kinds = [i[0] for i in self.drain(s)]
        self.assertEqual(kinds, ["error", "gap"])


class FakeKubectlIntegration(unittest.TestCase):
    """Runs the real subprocess paths against a fake kubectl executable."""

    def test_end_to_end_with_fake_kubectl(self):
        work = Path(tempfile.mkdtemp())
        script = work / "kubectl"
        script.write_text("#!%s\nimport os, sys\nd = os.environ['FAKE_KUBECTL_DIR']\na = sys.argv[1:]\n"
                          "if 'get' in a:\n    sys.stdout.write(open(d + '/pods.json').read())\n"
                          "elif 'logs' in a:\n    pod = a[a.index('-n') + 2]\n"
                          "    sys.stdout.write(open(d + '/' + pod + '.log').read())\n" % sys.executable)
        script.chmod(script.stat().st_mode | stat.S_IEXEC)
        (work / "pods.json").write_text(json.dumps({"items": [pod_item(ready=True, terminating=True)]}))
        (work / "p1.log").write_text("".join("%s %s\n" % (STAMP, line(request_id="r-%d" % i, at=i)) for i in range(3)))
        os.environ["FAKE_KUBECTL_DIR"] = str(work)
        self.addCleanup(os.environ.pop, "FAKE_KUBECTL_DIR", None)

        source = KubeSource("csc3104", ["periodic", "steady"], kubectl=str(script), poll_interval_s=0.2)
        source.poll_once()
        got = []
        try:
            while sum(1 for g in got if g[0] == "line") < 3:
                got.append(source.out.get(timeout=5))
        finally:
            source.stop()
        self.assertEqual([g[0] for g in got if g[0] != "line"][:2], ["lifecycle", "lifecycle"])
        self.assertTrue(all(g[1] == "p1" for g in got if g[0] == "line"))

        collector = Collector(str(work / "run"), ["periodic", "steady"], "k", clock="wall", source="k8s:csc3104")
        for g in got:
            if g[0] == "line":
                collector.process_line(g[2], pod=g[1])
            elif g[0] == "lifecycle":
                collector.record_lifecycle(g[1])
        self.assertTrue(collector.close(flush=True))
        raw = [json.loads(x) for x in (work / "run/raw_events.jsonl").read_text().splitlines()]
        self.assertEqual({r["pod"] for r in raw}, {"p1"})
        self.assertEqual(len((work / "run/pod_lifecycle.jsonl").read_text().splitlines()), 3)


class FlushTests(unittest.TestCase):
    def test_flush_finalises_current_bucket_as_incomplete(self):
        base = 1792908000.0  # 30s-aligned
        c = Collector(tempfile.mkdtemp(), ["periodic"], "f", clock="wall")
        c._started = base
        c._agg = c._new_agg(base)
        c.process_line(line(request_id="a", at=0), wall_now=base + 5)  # started_at far away: late
        c.close(wall_now=base + 40, flush=True)
        out = [json.loads(x) for x in (Path(c.writer.directory) / "demand_buckets.jsonl").read_text().splitlines()]
        self.assertTrue(out)
        self.assertFalse(out[-1]["complete"])


if __name__ == "__main__":
    unittest.main()

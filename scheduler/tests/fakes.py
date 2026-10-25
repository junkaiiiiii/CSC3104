"""In-memory stand-in for KubectlClient / InClusterClient."""
import copy

from prewarm.kube import KubeError


def merge(target, patch):
    """RFC 7386 JSON merge patch (null deletes)."""
    for key, value in patch.items():
        if value is None:
            target.pop(key, None)
        elif isinstance(value, dict):
            node = target.get(key)
            if not isinstance(node, dict):
                node = target[key] = {}
            merge(node, value)
        else:
            target[key] = value


class FakeKube:
    def __init__(self):
        self.objects = {}        # (kind, name) -> object
        self.calls = 0
        self.patches = []        # (kind, name, body)
        self.fail_next = 0       # raise KubeError on the next N calls

    def add_function(self, fn, revision, annotations=None, pa_annotations=None, ready_pods=0):
        self.objects[("ksvc", fn)] = {"metadata": {"name": fn}, "status": {
            "latestReadyRevisionName": revision,
            "traffic": [{"revisionName": revision, "percent": 100}]}}
        self.objects[("revision", revision)] = {"metadata": {
            "name": revision, "annotations": dict(annotations or {}),
            "labels": {"serving.knative.dev/service": fn}}}
        self.objects[("pa", revision)] = {"metadata": {
            "name": revision, "annotations": dict(pa_annotations or annotations or {})}}
        self.set_pods(fn, ready_pods)

    def set_pods(self, fn, ready, not_ready=0):
        for key in [k for k in self.objects if k[0] == "pod" and k[1].startswith(fn + "-pod")]:
            del self.objects[key]
        for i in range(ready + not_ready):
            self.objects[("pod", "%s-pod-%d" % (fn, i))] = {
                "metadata": {"name": "%s-pod-%d" % (fn, i),
                             "labels": {"serving.knative.dev/service": fn}},
                "status": {"conditions": [{"type": "Ready", "status": "True" if i < ready else "False"}]}}

    def _tick(self):
        self.calls += 1
        if self.fail_next:
            self.fail_next -= 1
            raise KubeError("injected failure")

    def get(self, kind, name):
        self._tick()
        if (kind, name) not in self.objects:
            raise KubeError("%s %s not found" % (kind, name), not_found=True)
        return copy.deepcopy(self.objects[(kind, name)])

    def list(self, kind, selector=None):
        self._tick()
        items = [copy.deepcopy(o) for (k, _n), o in sorted(self.objects.items()) if k == kind]
        if selector:
            key, value = selector.split("=", 1)
            items = [o for o in items if (o["metadata"].get("labels") or {}).get(key) == value]
        return items

    def merge_patch(self, kind, name, body):
        self._tick()
        if (kind, name) not in self.objects:
            raise KubeError("%s %s not found" % (kind, name), not_found=True)
        self.patches.append((kind, name, copy.deepcopy(body)))
        merge(self.objects[(kind, name)], body)
        return copy.deepcopy(self.objects[(kind, name)])

    def annotations(self, kind, name):
        return self.objects[(kind, name)]["metadata"].get("annotations") or {}

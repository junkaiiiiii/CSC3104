"""Minimal Kubernetes access with two interchangeable backends.

KubectlClient   shells out to kubectl (laptop / kind; uses your kubeconfig).
InClusterClient talks to the API server with the pod's service account
                (scheduler/deploy/rbac.yaml lists the exact permissions).

Only get / list / JSON merge-patch are needed. Standard library only.
"""
from __future__ import annotations

import json
import os
import ssl
import subprocess
import urllib.error
import urllib.parse
import urllib.request

# kind -> (API group, version, plural)
RESOURCES = {
    "ksvc": ("serving.knative.dev", "v1", "services"),
    "revision": ("serving.knative.dev", "v1", "revisions"),
    "pa": ("autoscaling.internal.knative.dev", "v1alpha1", "podautoscalers"),
    "pod": ("", "v1", "pods"),
}


class KubeError(RuntimeError):
    def __init__(self, message, not_found=False, conflict=False):
        super().__init__(message)
        self.not_found = not_found
        self.conflict = conflict


def _kubectl_name(kind: str) -> str:
    group, _version, plural = RESOURCES[kind]
    return plural + ("." + group if group else "")


class KubectlClient:
    def __init__(self, namespace: str, context: str = None, kubectl: str = "kubectl",
                 timeout_s: float = 15.0, runner=subprocess.run):
        self.namespace = namespace
        self._base = [kubectl] + (["--context", context] if context else [])
        self._timeout = timeout_s
        self._run = runner
        self.calls = 0

    def _exec(self, args):
        self.calls += 1
        cmd = self._base + args + ["-n", self.namespace, "-o", "json"]
        try:
            proc = self._run(cmd, capture_output=True, text=True, timeout=self._timeout)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise KubeError("kubectl failed: %s" % exc)
        if proc.returncode != 0:
            err = (proc.stderr or "").strip()
            raise KubeError("kubectl %s: %s" % (" ".join(args[:3]), err[:300]),
                            not_found="NotFound" in err or "not found" in err,
                            conflict="Conflict" in err or "the object has been modified" in err)
        try:
            return json.loads(proc.stdout or "{}")
        except ValueError:
            raise KubeError("kubectl returned non-JSON output")

    def get(self, kind: str, name: str) -> dict:
        return self._exec(["get", _kubectl_name(kind), name])

    def list(self, kind: str, selector: str = None) -> list:
        args = ["get", _kubectl_name(kind)] + (["-l", selector] if selector else [])
        return self._exec(args).get("items", [])

    def merge_patch(self, kind: str, name: str, body: dict) -> dict:
        return self._exec(["patch", _kubectl_name(kind), name, "--type", "merge",
                           "-p", json.dumps(body, separators=(",", ":"))])


class InClusterClient:
    SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"

    def __init__(self, namespace: str = None, timeout_s: float = 10.0, opener=None):
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        if not host:
            raise KubeError("not running inside a cluster (KUBERNETES_SERVICE_HOST unset)")
        self._server = "https://%s:%s" % (host, port)
        with open(os.path.join(self.SA_DIR, "token")) as fh:
            self._token = fh.read().strip()
        if namespace is None:
            with open(os.path.join(self.SA_DIR, "namespace")) as fh:
                namespace = fh.read().strip()
        self.namespace = namespace
        self._ctx = ssl.create_default_context(cafile=os.path.join(self.SA_DIR, "ca.crt"))
        self._timeout = timeout_s
        self._open = opener or urllib.request.urlopen
        self.calls = 0

    def _path(self, kind, name=None):
        group, version, plural = RESOURCES[kind]
        prefix = "/api/%s" % version if not group else "/apis/%s/%s" % (group, version)
        path = "%s/namespaces/%s/%s" % (prefix, self.namespace, plural)
        return path + ("/" + urllib.parse.quote(name) if name else "")

    def _request(self, method, path, body=None, content_type=None):
        self.calls += 1
        req = urllib.request.Request(self._server + path, method=method,
                                     data=json.dumps(body).encode() if body is not None else None)
        req.add_header("Authorization", "Bearer " + self._token)
        req.add_header("Accept", "application/json")
        if content_type:
            req.add_header("Content-Type", content_type)
        try:
            with self._open(req, timeout=self._timeout, context=self._ctx) as resp:
                return json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as exc:
            detail = exc.read()[:300].decode(errors="replace")
            raise KubeError("%s %s: HTTP %d %s" % (method, path, exc.code, detail),
                            not_found=exc.code == 404, conflict=exc.code == 409)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            raise KubeError("%s %s: %s" % (method, path, exc))

    def get(self, kind, name):
        return self._request("GET", self._path(kind, name))

    def list(self, kind, selector=None):
        query = "?" + urllib.parse.urlencode({"labelSelector": selector}) if selector else ""
        return self._request("GET", self._path(kind) + query).get("items", [])

    def merge_patch(self, kind, name, body):
        return self._request("PATCH", self._path(kind, name), body, "application/merge-patch+json")

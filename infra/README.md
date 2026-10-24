# infra: platform for the pre-warming scheduler (CORE-01)

Lead: Vania. 

This folder builds the platform every other component runs on: a single-node Kubernetes cluster on your own computer (kind), Knative Serving with the Kourier ingress, and three copies of the benchmark function (`periodic`, `steady`, `sparse`) that scale to zero when idle.

Exact versions are in [VERSIONS.md](VERSIONS.md). If something breaks, see [RECOVERY.md](RECOVERY.md).

## What you get

- A cluster named `csc3104` running Kubernetes v1.35.8, built from a pinned node image.
- Knative Serving v1.23.0 and Kourier v1.23.0, installed from the plain release YAML (not the operator).
- Namespace `csc3104` with three Knative Services: `periodic`, `steady`, `sparse`. Each runs the same benchmark image under a different name, so the evaluation can send each one a different traffic pattern.
- Initial experiment settings, not tuned values: autoscaler class KPA, min-scale 0, max-scale 4, one request per copy at a time.

## What you need first

- Docker Desktop, running, with at least 4 CPUs and 8 GB of memory.
- `kind` and `kubectl` installed.
- A bash terminal. On Windows use Git Bash. PowerShell will break the quote marks in some commands.
- Port 8080 free on your computer.
- Permission to pull the benchmark image (see "Known limits" below).
- Internet access for the downloads.

## Quick start

From the top folder of this repo:

```bash
bash infra/scripts/up.sh
```

This creates the cluster, installs Knative and Kourier, and deploys the three functions. The first run downloads images and can take several minutes.

Check it:

```bash
kubectl get nodes
kubectl get pods -n knative-serving
kubectl get ksvc -n csc3104
```

The node should be `Ready`, every Knative pod `Running`, and all three services `READY True`.

Send a request:

```bash
curl -H "Host: periodic.csc3104.example.com" -H "X-Request-ID: first-1" http://127.0.0.1:8080/
```

On PowerShell type `curl.exe`, not `curl`. The reply should show `"function_id": "periodic"`, `"revision": "periodic-00001"` and `"first_user_request": true`. The `Host` label is how Kourier chooses the function: `<name>.csc3104.example.com`. That name is never looked up on the internet.

Tear it down:

```bash
bash infra/scripts/down.sh
```

This deletes the whole cluster.

## Files

| Path | Purpose |
| --- | --- |
| `kind/cluster.yaml` | Cluster description: pinned node image and the port mapping |
| `knative/namespace.yaml` | Creates the `csc3104` namespace |
| `knative/periodic.yaml`, `steady.yaml`, `sparse.yaml` | The three functions. Identical except for name and `FUNCTION_ID` |
| `knative/benchmark.yaml` | The original template with a placeholder image. Do not apply it as is |
| `knative/patches/*.json` | Three small settings changes that `up.sh` applies: ingress class, Kourier port, domain |
| `scripts/up.sh`, `scripts/down.sh` | Build and delete the platform |
| `tools/watch_pods.py` | Records pod state once a second to a file |
| `tools/send.py` | Sends numbered requests and prints one JSON line per reply |

Apply manifests by name. Do not run `kubectl apply -f infra/knative/` on the whole folder, because it would try to apply the placeholder template.

## Measuring scale to zero

Run these from the top folder, in two terminals. Create a `results` folder first. Git ignores it on purpose.

```bash
# Terminal 1: leave running
python infra/tools/watch_pods.py csc3104 results/run1-pods.jsonl

# Terminal 2, a few seconds later
python infra/tools/send.py periodic.csc3104.example.com run1 6 | tee results/run1-send.jsonl
```

The watcher never ends by itself. Press Ctrl+C once `kubectl get pods -n csc3104` reports `No resources found`. Wait for zero pods before the next round. Large raw files are shared outside git, with a link and a hash.

## Security

The cluster port is bound to `127.0.0.1`, so only your own computer can reach it. Never commit kubeconfig files, tokens or pull secrets. `.gitignore` already blocks `kubeconfig*`.

## Known limits

- **Image access.** The benchmark image package is private. A cluster cannot pull it without either a public package or a pull secret. Agree this with Jun Kai. A public package cannot be made private again.
- **RBAC not written yet.** Scoped permissions for the scheduler (issue #11) depend on the result of the actuation spike (#4).
- **No performance claims.** These are local single-node runs with a cached image. They show the platform works, not that pre-warming helps.
- **Do not patch the function template in a loop** until the actuation spike shows what that does on this Knative version.
- **Fixed-minimum baseline.** A min-scale 1 variant is not included yet. Deploy it fresh when the baseline run needs it.
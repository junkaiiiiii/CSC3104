# Platform recovery guide (CORE-11)

Use this when the local cluster misbehaves. The cluster holds no data you need: manifests are in git, and measurement files are saved in `results/`. A full rebuild is always safe, so if two quick fixes do not work, rebuild.

Run commands from the top folder of the repo, in Git Bash. Replace `<pod>` with a pod name from `kubectl get pods`.

## 1. Quick health check

```bash
docker version
kubectl config current-context
kubectl get nodes
kubectl get pods -n knative-serving
kubectl get pods -n kourier-system
kubectl get ksvc -n csc3104
```

Healthy means: Docker shows a Server block, the context is `kind-csc3104`, the node is `Ready`, every pod in the two system namespaces is `Running` with all containers ready (`1/1` or `2/2`), and all three services say `READY True`.

## 2. Common problems

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| "Cannot connect to the Docker daemon" | Docker Desktop is not running | Start Docker Desktop, wait until it settles, retry |
| `kubectl` shows the wrong cluster, or connection refused | Wrong context, or Docker is off | `kubectl config use-context kind-csc3104`. If still refused, check Docker, then rebuild |
| Cluster gone after a restart, or node not `Ready` for several minutes | The node container did not come back cleanly | Rebuild (section 3) |
| A Knative or Kourier pod is `Pending` or restarting | Not enough memory given to Docker | Raise Docker to at least 8 GB, then `kubectl describe pod <pod> -n knative-serving`. Rebuild if it does not recover |
| A service shows `READY False` | Revision failed to start | `kubectl describe ksvc <name> -n csc3104` and `kubectl get revision -n csc3104`; read the reason lines |
| Pod is `ErrImagePull` or `ImagePullBackOff` | Cluster cannot download the image: private package, or wrong digest | `kubectl describe pod <pod> -n csc3104`. Confirm access with `docker pull <image digest from VERSIONS.md>`. Fix access with Jun Kai, or create a pull secret |
| `curl` returns 404 | Wrong Host label, or the domain patch is missing | The label must be `<name>.csc3104.example.com`. Check `kubectl get configmap config-domain -n knative-serving -o yaml` shows `example.com` |
| `curl` says connection refused | Port 8080 does not reach Kourier | `kubectl get svc kourier -n kourier-system` must show `80:31080/TCP`, and `docker ps` must show `127.0.0.1:8080->31080/tcp`. If the cluster was built without the port mapping, rebuild |
| First request after idle is slow | Normal cold start | Wait. If nothing arrives after two minutes, check `kubectl get pods -n csc3104` for a pod stuck in an error |
| Pods stay `Terminating` | Pods can take several minutes to finish shutting down (we saw over four minutes) | Wait and check again. If still there after ten minutes, `kubectl describe pod <pod> -n csc3104` |
| A request fails or hangs for a long time | Function or Knative component unhealthy | Function logs: `kubectl logs <pod> -n csc3104 -c user-container`. Knative controller logs: `kubectl logs -n knative-serving deployment/controller` |

## 3. Rebuild from nothing

```bash
bash infra/scripts/down.sh
bash infra/scripts/up.sh
```

`down.sh` deletes the cluster. `up.sh` recreates the cluster, Knative, Kourier and the three functions. Time it once and record it here: FILL IN minutes. After a rebuild the first run needs the images downloaded again, so label that run "uncached".

## 4. Scheduler left capacity raised

Not written yet. This procedure depends on how the actuation spike (#4) shows the scheduler changes capacity. When #4 is decided, record here: the original setting to restore, the exact command that restores it, and how to confirm the function returns to zero. Until then, if a function stays warm when it should not, a rebuild resets everything.

## 5. Before each experiment run

1. `kubectl get pods -n csc3104` reports `No resources found`, or the run states its starting capacity.
2. All three services are `READY True`.
3. The versions in `VERSIONS.md` still match the cluster.
4. Record whether the images are cached on the node.
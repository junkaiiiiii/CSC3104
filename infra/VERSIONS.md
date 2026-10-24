# Version lock (CORE-01)
Tested on Windows.

## Pinned by files in this repo

| Component | Value | Where it is pinned |
| --- | --- | --- |
| Kubernetes | v1.35.8 | Node image below |
| kind node image | `kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0` | `kind/cluster.yaml` |
| Knative Serving | v1.23.0 (release `knative-v1.23.0`) | `scripts/up.sh` |
| Kourier ingress | v1.23.0 (release `knative-v1.23.0`) | `scripts/up.sh` |
| Install method | Plain release YAML, not the Knative Operator | `scripts/up.sh` |
| Benchmark image | `ghcr.io/junkaiiiiii/csc3104-benchmark@sha256:b410b379a4a333380a57dfd4e35cd87d9ddc0ca57ea50c0db99bc2ba99e01bbc` | `knative/periodic.yaml`, `steady.yaml`, `sparse.yaml` (source: `docs/11-benchmark-image-handover.md`) |
| Function settings | KPA class, min-scale 0, max-scale 4, containerConcurrency 1, request 100m CPU / 128Mi, limit 1 CPU / 256Mi | `knative/*.yaml` |
| Ingress class | `kourier.ingress.networking.knative.dev` | `knative/patches/ingress-class.json` |
| Kourier exposure | NodePort 31080, mapped to `127.0.0.1:8080` on the host | `knative/patches/nodeport.json`, `kind/cluster.yaml` |
| Domain | `example.com` (requests carry a Host header; nothing resolves on the internet) | `knative/patches/domain.json` |

## Why these versions

- Knative 1.23.0's own end-to-end tests run on Kubernetes 1.34 and 1.35 ([workflow](https://github.com/knative/serving/blob/knative-v1.23.0/.github/workflows/kind-e2e.yaml)), so the node image is 1.35.
- kind v0.33.0 would default to Kubernetes 1.37, which Knative 1.23.0 does not test against. The image is pinned to avoid that default.
- The node image reference was taken from the [kind v0.33.0 release notes](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0). It belongs with that kind version; if a different kind version is installed, use that release's Kubernetes 1.35 image and update this file.
- Install commands: [Knative Serving YAML install](https://knative.dev/docs/install/yaml-install/serving/install-serving-with-yaml/).
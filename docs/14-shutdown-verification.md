# Benchmark shutdown verification

## Image to deploy

`ghcr.io/junkaiiiiii/csc3104-benchmark@sha256:08b3dc54461c0687c76e867a71b7c11955d7c1580133a89ad02ebb3fc5904c4e`

Published from source commit `6728276e90384977c89c59b47a5438342a4605f9` by
[workflow run 41681854171](https://github.com/junkaiiiiii/CSC3104/actions/runs/41681854171).
Platforms: linux/amd64 and linux/arm64. The application code in this image includes
the signal-handling fix; later commits update manifests/documentation only.

Python base index:
`python:3.12-slim@sha256:b18b261e1b4974b4f321451ad5251ded85abb22c2b8b262da527dda1d031ab30`.
The index includes both deployed architectures.

## Observations reported by Jun Kai

These values come from terminal output collected on Jun Kai's Apple Silicon Mac.
The raw files remain on his machine in ignored directories; this document is a
summary, not an independently downloadable raw-data archive.

| Check | Previous image | Fixed image |
| --- | --- | --- |
| Standalone Docker stop, 10-second deadline | 10.160 s; exit 137; OOMKilled false | local build: 0.136 s; exit 0 |
| Published image Docker stop on Mac | Not retested | 0.139 s; exit 0; passed true |
| Knative observed terminating-to-gone | Around 302 s in earlier pilots | 28.999 s and 28.987 s across two Pods |

Knative verification used `periodic-00002`, served request ID
`shutdown-check-1`, and then scaled to zero. The collector reported:
1 request counted, 0 duplicates, 0 late, 0 quarantined, 8 Pod events,
and conservation OK. No periodic Pods remained at the end.

Raw evidence locations, relative to Jun Kai's repository root:

- `results/shutdown/before.json`
- `results/shutdown/after.json`
- `results/shutdown/published-arm64.json`
- `telemetry/data/shutdown-knative-1/pod_lifecycle.jsonl`
- Full collector output under `telemetry/data/shutdown-knative-1/`

Back up these ignored artifacts outside Git, preserving their original contents.
No durable raw-evidence link has been recorded yet.

## Interpretation and limits

The standalone result confirms a clean exit in the fixed application container,
rather than waiting for Docker's force-kill deadline. Native regressions cover
idle SIGTERM/SIGINT, stopping during initialization, and completing an active
response and its invocation event after repeated SIGTERM.

The roughly 90% reduction in observed Pod termination time is a two-Pod diagnostic
comparison, not a repeated controlled benchmark. The Python base changed between
the original image and the fixed image; the new base is now pinned. Lifecycle
durations are based on polling, and the remaining roughly 29 seconds have not been
isolated by container. Do not attribute the whole residual delay to queue-proxy.

The published image passed a runner-native shutdown check in GitHub Actions and
the published ARM64 check on the Mac. Cluster validation so far covers periodic;
steady and sparse still need deployment and verification on the new digest.

## Repeating the application check

From repository root:

```bash
python3 -m unittest discover -s scripts -p 'test_benchmark_shutdown.py'
python3 scripts/smoke_benchmark.py
python3 scripts/check_container_shutdown.py \
  --image ghcr.io/junkaiiiiii/csc3104-benchmark@sha256:08b3dc54461c0687c76e867a71b7c11955d7c1580133a89ad02ebb3fc5904c4e \
  --output results/shutdown/published-arm64-repeat.json
```

The output filename must be new. Do not overwrite pre-fix evidence.

## Deployment and subsequent experiments

All three maintained function manifests now reference this image. With the
scheduler stopped, apply only those files (the directory also has a placeholder):

```bash
kubectl --context kind-csc3104 apply \
  -f infra/knative/periodic.yaml \
  -f infra/knative/steady.yaml \
  -f infra/knative/sparse.yaml
```

Confirm latest-created and latest-ready revisions match and each revision uses the
new digest. Updating steady/sparse creates new revisions; periodic may be unchanged
because it was patched during the diagnostic. Keep old run artifacts intact and
label them pre-fix. Rerun reactive, fixed-minimum and predictive policies on the
same new image with fresh run IDs before comparing performance. Shutdown time
outside the replay observation window remains separate from in-window resources.

# Benchmark image handover
Owner: Wong Jun Kai. Recorded 21 October 2026 (Asia/Singapore).

## Published artifact
Source commit: `76b2b952c04ab6a7adb7d5cff977339456556677`

[Successful publishing run](https://github.com/junkaiiiiii/CSC3104/actions/runs/39462568373)

Image tag:
```text
ghcr.io/junkaiiiiii/csc3104-benchmark:sha-76b2b952c04ab6a7adb7d5cff977339456556677
```

Pinned multi-platform image reference:
```text
ghcr.io/junkaiiiiii/csc3104-benchmark@sha256:b410b379a4a333380a57dfd4e35cd87d9ddc0ca57ea50c0db99bc2ba99e01bbc
```

The digest above was supplied from the workflow summary. The workflow builds linux/amd64 and linux/arm64. It is manual (workflow_dispatch), not automatic on every push. Tags identify the source commit; use the digest to identify this exact artifact because rebuilding the same commit can resolve a different mutable base image.

## Evidence and limits
| Check | Result | Evidence source |
|---|---|---|
| Local Docker image build | Passed | Jun Kai's terminal output |
| Original local container HTTP checks | Passed | Repeated requests retained instance ID and changed first_user_request to false |
| GitHub publishing workflow | Passed | Run linked above, success verified through GitHub |
| Authenticated registry pull by digest | Passed | Terminal reported matching digest and downloaded image |
| Registry image on ARM64 Mac | Passed | /healthz ready; / returned benchmark event |
| Requested AMD64 emulation check | HTTP checks passed | User ran the instructed test sequence; no independent architecture inspection captured |
| Kubernetes/Knative deployment | Pending | Team reports no cluster set up |
| Cluster pull access | Pending | Private registry authentication required |
| Latency/resource improvements | Not evaluated | These are functional smoke checks only |

Host reported: Apple Silicon Mac, Docker Desktop 4.60.1, Docker Engine 29.2.0. The reported dates in JSON are UTC; they correspond to 21 October in Singapore.

ARM64 registry-test instance ID: `6db6eb6f-92e0-4893-a38b-280b2bebab19`.
Requested AMD64-test instance ID: `99d44cc8-7129-47c7-9cfc-78f0fb3a6b21`.

first_user_request is a process-first-use proxy, not proof of user cold-start exposure. service_ms excludes platform provisioning and client transport; these smoke outputs are not performance evaluation results. An emulation check does not demonstrate native AMD64 server performance.

## Reproduce local image checks
Authenticate Docker to ghcr.io using your own authorised credentials. Do not put credentials in Git, issue comments or screenshots.

```bash
docker pull ghcr.io/junkaiiiiii/csc3104-benchmark@sha256:b410b379a4a333380a57dfd4e35cd87d9ddc0ca57ea50c0db99bc2ba99e01bbc
docker run --rm --name csc3104-registry-test -p 8080:8080 ghcr.io/junkaiiiiii/csc3104-benchmark@sha256:b410b379a4a333380a57dfd4e35cd87d9ddc0ca57ea50c0db99bc2ba99e01bbc
```

In another terminal:
```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/
curl http://localhost:8080/
docker stop csc3104-registry-test
```

Expected: health ready; consecutive user requests share instance_id; first_user_request true then false. To test emulation explicitly, start the same digest with --platform linux/amd64, and inspect the running container's selected image architecture if stronger architecture evidence is needed.

## Cluster handover: Vania + Jun Kai
1. Vania installs and documents compatible pinned Kubernetes, Knative Serving and networking versions (issue #1).
2. Jun Kai supplies the pinned image reference above.
3. Configure authorised pull access for the private package in the workload namespace. A Docker login on Jun Kai's Mac does not authenticate cluster nodes. Each team account needs actual package read access; a token scope alone does not grant access to someone else's package. Do not share Jun Kai's personal token.
4. Replace the image placeholder in infra/knative/benchmark.yaml with this digest. Configure appropriate imagePullSecrets through the Knative template or service account for the deployed version.
5. Deploy, verify Ready status and reach the function through Knative. Record selected architecture, versions, resources, revision, request IDs and image digest.
6. Capture scale-to-zero and cold/warm behavior for issue #3, then complete the actuation feasibility spike (#4).

## Task status
- #2: Image built, published and locally checked; cluster pull and deployment evidence remain pending. Keep open.
- #12: Starter CI and manual image publication exist; extend checks as team components are implemented. Keep open.
- #17: Full experiment entry points depend on the future evaluation harness. Keep open.

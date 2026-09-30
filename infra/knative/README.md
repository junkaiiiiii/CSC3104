# Deployment template
Vania supplies compatible pinned Kubernetes, Knative Serving and networking installation instructions. The files here do not install Knative.
Replace the placeholder image with a registry-accessible digest; arrange authenticated pulls if private. Apply namespace.yaml then benchmark.yaml and record Ready status and URL.
The template uses KPA, min-scale 0, max-scale 4 and concurrency 1 as initial experiment settings, not tuned production values.
For the fixed baseline, deploy a fresh controlled run configuration with min-scale 1, then wait for Ready capacity. Record revision changes and reset state between policies.
Do not implement a loop patching this template until the actuation spike demonstrates the behavior on the chosen version.

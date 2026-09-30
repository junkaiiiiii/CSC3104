# Proposal alignment and research evidence
The uploaded proposal is a planning input; this repository does not validate its bibliography or preserve its unsupported claims as established facts.

## Corrections to carry into the report
- Cold starts can occur on scale-out as well as after inactivity; client provisioning delay differs from application init_ms.
- Predictive/per-function pre-warming has prior work. Explain our precise implementation and evaluation rather than claiming forecasting is unused.
- Knative can retain minimum replicas; a cold-start penalty is not inevitable for every request.
- AWS provisioned concurrency can be adjusted; it is not necessarily permanently fixed.
- Compare policies on one platform first. An AWS-vs-Knative result has confounding platform differences.
- Report overall latency, cold-start exposure, and resource cost; avoided cold starts are not the same as faster intrinsic initialization.
- Proactive bounds alone do not guarantee no resource waste. Fleet scalability and self-healing behavior need tests.
- Confirm all paper titles, authors, DOIs and in-text years from publisher/proceedings records. Several entries in the supplied copy do not match the previously identified papers and need fresh checking before citation.

## Primary sources to read
- Knative scale bounds: https://knative.dev/docs/serving/autoscaling/scale-bounds/
- Knative services and revisions: https://knative.dev/docs/serving/services/
- Knative scale-to-zero: https://knative.dev/docs/serving/autoscaling/scale-to-zero/
- AWS provisioned concurrency: https://docs.aws.amazon.com/lambda/latest/dg/provisioned-concurrency.html
- Shahrad et al., Serverless in the Wild, USENIX ATC 2020: https://www.usenix.org/conference/atc20/presentation/shahrad
- Jegannathan et al., forecasting approach preprint: https://arxiv.org/abs/2206.15176

These links are reading targets, not evidence that each source or its deployment behavior has been fully reproduced here. Maintain an evidence table: claim, exact source metadata, relevant page/section, what it supports, limitations. Add verified citation exports; do not copy the proposal's references unchecked.

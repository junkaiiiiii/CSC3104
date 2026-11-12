# Lambda on-demand comparison setup

Owner: Jun Kai (benchmark and infrastructure). This adds Lambda infrastructure and
read-only verification tools. Isaac's scheduler and Hasan's evaluation code are
unchanged. Forecasting PR #28 is merged separately. No AWS resources are created by
local tests or CI; deployment requires your AWS account and local credentials.

## What gets deployed

`template.yaml` creates periodic, steady and sparse Lambda functions, a published
`benchmark` alias for each, IAM-authenticated HTTPS Function URLs, and seven-day log
groups. It creates one logging-only execution role unless `ExecutionRoleArn` names
an existing authorised role. Existing roles must already trust Lambda and permit
the required log operations; the template does not modify their policies.

Defaults: managed Python 3.12, ARM64, 256 MB, 30-second handler timeout, zero
artificial delays, no Provisioned Concurrency. Runtime updates are applied on
function update rather than automatically between updates. Capture actual runtime
versions and do not redeploy during an experiment. This is a managed-runtime ZIP
deployment, not the same container image or Linux distribution as Knative.

`ReservedConcurrency=0` means leave concurrency unreserved; positive values up to
four cap **each** function if account quotas allow. Unreserved is not scale-to-zero
disabled and is not a concurrency cap. Always record the actual setting. The
256-MB Lambda memory choice does not imply equivalent CPU to Knative's resource
requests/limits. Use only regions and services permitted by the course account.

## 1. Local verification

Commands run from repository root. Python 3.12 is the deployed runtime; SAM's
container build below also works when the Mac's installed Python version differs.

```bash
python3 -m unittest discover -s functions/lambda_benchmark/tests -v
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r infra/lambda/requirements-tools.txt
python -m unittest discover -s infra/lambda/tests -v
cfn-lint infra/lambda/template.yaml
```

Host tooling dependencies are pinned separately; the runtime handler has no
third-party dependencies. Do not put AWS credentials or tokens in the repository.
Use an existing AWS profile, SSO session or course-issued temporary session locally.
Do not paste credentials into chat. Verify tools and the intended identity:

```bash
aws --version
sam --version
aws sts get-caller-identity
```

## 2. Build and deploy

Deployment creates billable AWS resources. Review the SAM/CloudFormation change
set and use the intended course account/region. Example stack name below is short
enough for the explicit function names; keep stack names under 48 characters.
The region is an example, not a requirement; use your course's allowed region.
Append `--profile NAME` to AWS/SAM commands when using a named profile.

```bash
sam build --template-file infra/lambda/template.yaml --use-container

sam deploy --guided \
  --template-file .aws-sam/build/template.yaml \
  --stack-name csc3104-lambda \
  --region ap-southeast-1 \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides Architecture=arm64 MemoryMb=256 InitDelayMs=0 WorkDelayMs=0 CodeRevision="$(git rev-parse HEAD)"
```

If the course restricts role creation, add `ExecutionRoleArn=YOUR_AUTHORISED_ROLE_ARN`
to parameter overrides. Do not create or alter a lab role just to bypass account
restrictions. The conditional IAM resource will be omitted when using an existing
role. The caller still needs deployment permissions for Lambda, CloudFormation,
logs, packaging storage and passing that execution role.

No endpoint is anonymous/public. Function URL callers need both
`lambda:InvokeFunctionUrl` and `lambda:InvokeFunction` on the relevant benchmark
alias ARNs. The execution role is not automatically the caller's identity policy.
Use account-approved permissions; the tools do not modify caller policies.

## 3. Verify and record deployment

Save a new snapshot before each experiment; files live under ignored `results/`.

```bash
python infra/lambda/export_environment.py \
  --stack csc3104-lambda --region ap-southeast-1 \
  --output results/lambda/deployment-1.json
```

It reads stack outputs, aliases, numbered function versions, CodeSha256, runtime,
architecture, memory, timeout, concurrency, runtime management and allowed benchmark
environment values. Arbitrary environment values and credentials are not exported.
Recheck alias/version before and after experiments: a Function URL backed by an
alias can change target on a later deployment, despite its URL staying the same.

Find each URL in the stack outputs or snapshot. Run a signed smoke check using its
actual value and matching function ID; this example uses a shell variable so it
cannot accidentally invoke a made-up endpoint:

```bash
PERIODIC_URL=$(aws cloudformation describe-stacks --stack-name csc3104-lambda \
  --region ap-southeast-1 --query "Stacks[0].Outputs[?OutputKey=='PeriodicUrl'].OutputValue | [0]" --output text)

python infra/lambda/smoke_url.py \
  --url "$PERIODIC_URL" --region ap-southeast-1 --function-id periodic \
  --output results/lambda/smoke-periodic-1.json
```

Repeat for `SteadyUrl`/`steady` and `SparseUrl`/`sparse`, with fresh output filenames.
The checker uses the AWS SDK's SigV4 signer and local credential chain, refreshes
temporary credentials and refuses redirects/foreign hosts. Reports contain no
authorization headers or credentials. Health and two user checks intentionally warm
execution environments; do not use these three diagnostic times as performance
results or assume the next invocation will be cold. Do not assume two requests use
the same execution environment.

An unsigned `curl` to these URLs is expected to be denied. The existing generic
evaluation replay client does not sign AWS requests; simply substituting these URLs
into its endpoints file is not a valid Lambda replay integration.

## 4. Handoff for the final comparison

Hasan owns workload replay, platform-log correlation, analysis and cost conclusions.
Isaac owns Knative prediction/actuation. This PR supplies deployable benchmarks and
verification, not their implementations. Agree the following before formal runs:

- Reuse identical user traces and artificial delay settings; align architecture
  where available, and disclose resource/network differences. Use version snapshots
  and CodeSha256 for Lambda, pinned image digests/revisions for Knative.
- Implement IAM-signed HTTP workload calls with per-request `X-Request-ID` and
  `X-Request-Kind`. The smoke signer illustrates the SDK call; do not measure CLI
  startup as function latency. Record whether client signing time is included and
  keep latency boundaries consistent. Do not send Knative Host headers to Lambda.
- Capture unfiltered CloudWatch logs for all three snapshot log groups over each
  run window, including initialization before traffic and delayed log delivery.
  Preserve raw START/END/REPORT lines and application JSON, then join AWS request
  IDs to client IDs through application events. Local tests cannot replace this.
- Use AWS platform initialization evidence rather than first-user flags. Leaving
  a function idle for an arbitrary time does not guarantee its next request is cold.
  Report cold-start evidence/unknowns separately from Knative's readiness exposure.
- Separate user traffic from warm-ups/probes in demand. Lambda health/warm-up
  invocations can still be billable: retain their overhead when calculating costs.
- Capture actual billed-duration/memory/initialization fields and service pricing
  for the chosen date/region. Replica-seconds are not a Lambda invoice. Memory sizes
  alone do not equalise CPU; one laptop cluster is not equivalent to AWS hardware.
- Repeat reactive, fixed-minimum, predictive and Lambda on-demand workloads with
  consistent training windows, seeds and run order. Provisioned Concurrency is an
  optional later warm-capacity comparison, not implemented by this template.

No Lambda performance or superiority result is established by this setup.

## 5. Cleanup

After exporting and backing up raw evidence, remove this project's stack using the
same profile and region. Check the proposed deletion before confirming:

```bash
sam delete --stack-name csc3104-lambda --region ap-southeast-1
```

SAM packaging storage or logs retained by account policies may require separate
review. Do not delete shared/course resources. Preserve the evidence before cleanup.

## AWS references

- [Function URL event/response format and SigV4](https://docs.aws.amazon.com/lambda/latest/dg/urls-invocation.html)
- [Function URL caller permissions](https://docs.aws.amazon.com/lambda/latest/dg/urls-auth.html)
- [SAM FunctionUrlConfig and alias targeting](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/sam-property-function-functionurlconfig.html)
- [Python platform REPORT fields](https://docs.aws.amazon.com/lambda/latest/dg/python-logging.html)
- [Runtime management](https://docs.aws.amazon.com/lambda/latest/dg/runtime-management-configure-settings.html)

# AWS Lambda benchmark

This directory deploys the periodic, steady and sparse benchmark functions to AWS Lambda. The evaluation client supports signed Function URL requests and matches responses to CloudWatch REPORT records.

Jun Kai handled the Lambda application and deployment. Hasan handled workload replay, log correlation and evaluation.

## Deployment settings

The SAM template creates three functions, a published `benchmark` alias for each, IAM-authenticated Function URLs and log groups with seven-day retention.

The reported evaluation used us-east-1, Python 3.12, ARM64, 256 MB memory and zero artificial delays. The handler timeout is thirty seconds. Provisioned Concurrency is not configured. This is a managed-runtime ZIP deployment rather than the Knative container image.

`ReservedConcurrency=0` leaves concurrency unreserved in this template. Positive values, up to four, set a cap for each function. Zero here does not disable invocation.

## 1. Install the tools and run local checks

Run commands from the repository root. Docker is required for the SAM container build. Install AWS CLI and SAM CLI separately.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r infra/lambda/requirements-tools.txt
python -m unittest discover -s functions/lambda_benchmark/tests -v
python -m unittest discover -s infra/lambda/tests -v
python -m unittest evaluation.tests.test_lambda
cfn-lint infra/lambda/template.yaml
aws --version
sam --version
aws sts get-caller-identity
```

Use your own authorised AWS credentials. For a named profile, set `AWS_PROFILE` before running the commands. Learner Lab credentials expire and need refreshing when a new session starts. Local tests do not deploy AWS resources.

## 2. Build and deploy

The example below uses AWS Academy's existing LabRole. On another account, substitute an authorised Lambda execution role, or omit the role lookup and `ExecutionRoleArn` parameter to let SAM create the logging role. An existing role must trust Lambda and allow the required log operations.

```bash
export AWS_DEFAULT_REGION=us-east-1

sam build --template-file infra/lambda/template.yaml --use-container

ROLE_ARN=$(aws iam get-role --role-name LabRole --query Role.Arn --output text)

sam deploy --guided \
  --template-file .aws-sam/build/template.yaml \
  --stack-name csc3104-lambda \
  --region us-east-1 \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides Architecture=arm64 MemoryMb=256 \
    InitDelayMs=0 WorkDelayMs=0 ReservedConcurrency=0 \
    CodeRevision="$(git rev-parse HEAD)" ExecutionRoleArn="$ROLE_ARN"
```

Review the changeset before accepting it. Deployment creates AWS resources and incurs usage. Use a region allowed by the account.

Function URLs require IAM-signed requests. Callers need `lambda:InvokeFunctionUrl` and `lambda:InvokeFunction` for the benchmark aliases. The function execution role does not automatically grant invocation permission to the caller.

## 3. Record and check the deployment

```bash
python infra/lambda/export_environment.py \
  --stack csc3104-lambda --region us-east-1 \
  --output results/lambda/deployment-1.json

PERIODIC_URL=$(python -c 'import json; print(json.load(open("results/lambda/deployment-1.json"))["functions"]["periodic"]["function_url"])')

python infra/lambda/smoke_url.py \
  --url "$PERIODIC_URL" --region us-east-1 --function-id periodic \
  --output results/lambda/smoke-periodic-1.json
```

Repeat the smoke check for steady and sparse using their URLs and matching function IDs. The checker sends a health request and two user requests. These calls warm execution environments, so their timings are diagnostic checks rather than performance samples. An unsigned curl is expected to be denied.

The deployment snapshot records numbered versions, code hashes, aliases, runtime settings and log groups without exporting credentials. Capture another snapshot after the experiment and check that the versions, aliases and settings have not changed. Use new snapshot filenames each time.

## 4. Prepare endpoints and replay a trace

Create endpoint and metadata files from the saved deployment:

```bash
python - <<'PY'
import json
from pathlib import Path

snapshot = json.loads(Path("results/lambda/deployment-1.json").read_text())
functions = snapshot["functions"]
config = functions["periodic"]["configuration"]
for row in functions.values():
    actual = row["configuration"]
    assert all(actual[key] == config[key]
               for key in ("Runtime", "Architectures", "MemorySize"))
endpoints = {
    name: {"url": row["function_url"], "auth": "sigv4", "region": snapshot["region"]}
    for name, row in functions.items()
}
metadata = {
    "region": snapshot["region"],
    "runtime": config["Runtime"],
    "architecture": config["Architectures"][0],
    "memory_mb": config["MemorySize"],
    "function_versions": {
        name: {
            "version": row["version"],
            "code_sha256": row["configuration"]["CodeSha256"]
        }
        for name, row in functions.items()
    }
}
for name, value in (("endpoints", endpoints), ("metadata", metadata)):
    with Path(f"results/lambda/{name}-1.json").open("x") as stream:
        json.dump(value, stream, indent=2)
PY
```

The metadata uses the deployed runtime, architecture and memory from the snapshot. For the reported configuration these should be Python 3.12, arm64 and 256 MB.

Use the same long-gap trace as the Knative comparison. Generate it only if it does not already exist:

```bash
python scripts/prepare_long_gap.py --seed 402 --output results/traces/long-gap-402
```

Replay the saved trace:

```bash
export RUN=lambda-402-r1

python -m evaluation.replay \
  --trace results/traces/long-gap-402/trace.jsonl \
  --endpoints results/lambda/endpoints-1.json --policy lambda \
  --metadata results/lambda/metadata-1.json --run-id "$RUN"
```

For a named profile, use `AWS_PROFILE` or add `--aws-profile PROFILE` to replay. Do not include Knative Host headers in the Lambda endpoints.

The client signs requests with SigV4 and does not retry them. It records signing time separately; HTTP latency starts after signing, immediately before sending the request. Each run ID must be new.

## 5. Fetch logs and match execution reports

Allow several minutes for CloudWatch delivery after replay. Derive the log window from the run manifest:

```bash
read -r START END < <(python - <<'PY'
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

manifest = json.loads((Path("results") / os.environ["RUN"] / "run_manifest.json").read_text())
def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
start = timestamp(manifest["started_at"]) - timedelta(seconds=60)
end = timestamp(manifest["ended_at"]) + timedelta(minutes=5)
print(start.isoformat(), end.isoformat())
PY
)

python -m evaluation.lambda_correlate fetch \
  --log-group periodic=/aws/lambda/csc3104-lambda-periodic \
  --log-group steady=/aws/lambda/csc3104-lambda-steady \
  --log-group sparse=/aws/lambda/csc3104-lambda-sparse \
  --start "$START" --end "$END" --region us-east-1 \
  --output "results/$RUN/cloudwatch-1.jsonl"

python -m evaluation.lambda_correlate correlate \
  --run-dir "results/$RUN" --reports "results/$RUN/cloudwatch-1.jsonl"
```

If you used a different stack name, take the log-group names from the deployment snapshot.

The outputs are `lambda_joined.jsonl` and `lambda_correlation.json`. Check that every client request matched a REPORT and that unmatched, ambiguous and no_aws_request_id are zero. Also check HTTP success and timeout counts.

If records are missing, wait and fetch again to a new filename. Use the complete replacement export for correlation. Passing overlapping exports together can create duplicate REPORT IDs.

Repeat with seeds 403-406 and fresh run IDs. Keep each trace identical to its Knative counterpart. Do not redeploy or change aliases during the comparison. Keep raw logs, client records and deployment snapshots outside Git.

## Results and limits

The completed five-seed evaluation matched all 3,475 requests, with no unmatched or ambiguous records. Reported HTTP p95 ranged from 785.285 to 805.334 ms, while execution p95 ranged from 2.450 to 3.300 ms. Matched requests used 2.708 billed GB-seconds in total.

These measurements have different boundaries. HTTP latency includes the remote network path; execution duration comes from CloudWatch. Initialisation evidence comes from AWS Init Duration, not the application's first-user flag. Waiting for an arbitrary idle period does not guarantee a cold invocation.

Billed GB-seconds are billed duration multiplied by configured memory. They are not total monetary cost or directly comparable to Knative Ready replica-seconds. The experiment used on-demand Lambda only and does not establish an advantage over Lambda or Provisioned Concurrency.

See [the evaluation README](../../evaluation/README.md) for metric definitions and cost-estimation options.

## Cleanup

After saving the results, remove the stack created for this experiment:

```bash
sam delete --stack-name csc3104-lambda --region us-east-1
```

Check for retained logs or packaging storage separately. Do not delete shared lab resources.

## AWS documentation

- [Function URL invocation](https://docs.aws.amazon.com/lambda/latest/dg/urls-invocation.html)
- [Function URL permissions](https://docs.aws.amazon.com/lambda/latest/dg/urls-auth.html)
- [Python log and REPORT fields](https://docs.aws.amazon.com/lambda/latest/dg/python-logging.html)
- [Provisioned Concurrency](https://docs.aws.amazon.com/lambda/latest/dg/provisioned-concurrency.html)

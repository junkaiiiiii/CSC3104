# Lambda benchmark adapter

This is a standard-library Python handler for Lambda Function URL payload format
2.0. It performs the same artificial `INIT_DELAY_MS` and `WORK_DELAY_MS` work as the
HTTP benchmark and preserves its core invocation fields and first-user accounting.
The deployed Knative application/image is unchanged.

`GET /` returns and logs one invocation event. `/healthz` and Knative-style probe
headers return readiness without entering user demand. Warm-ups are logged as
`request_kind=warmup` and do not consume `first_user_request`. Headers are handled
case-insensitively. Supported GET requests have a differential regression against
the actual HTTP benchmark's handler; unsupported methods are outside the benchmark.

Lambda additions:

| Field | Meaning |
| --- | --- |
| `platform` | `aws_lambda` |
| `aws_request_id` | Join key to AWS platform invocation/REPORT logs |
| `first_invocation` | First handler entry in this module instance, including health/probe calls |
| `initialization_type` | AWS runtime environment value, or unknown locally |
| `revision` | `lambda-` followed by the invoked function version |

`first_invocation` and `first_user_request` are process-local markers, not verified
platform cold-start labels. A health check can consume the former while leaving the
latter true. Use AWS platform Init Duration/REPORT evidence joined by request ID;
missing cold-start evidence is unknown, not proof of a warm invocation.
`init_ms` measures application initialization work only, not full Lambda startup.

Run local handler tests without an AWS account:

```bash
python3 -m unittest discover -s functions/lambda_benchmark/tests -v
```

Local direct calls cannot verify AWS runtime startup, billing, IAM, URL routing or
network latency. Deployment, signed smoke checks, snapshots and the evaluation
handoff are documented in [infra/lambda/README.md](../../infra/lambda/README.md).

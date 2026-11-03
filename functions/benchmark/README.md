# Instrumented benchmark
Lead: Jun Kai.

Run `python functions/benchmark/app.py` or:
```bash
docker build -t csc3104-benchmark:dev functions/benchmark
docker run --rm -p 8080:8080 -e FUNCTION_ID=periodic csc3104-benchmark:dev
```

GET / returns request/process identifiers and emits one JSON event to stdout. GET /healthz is excluded. Knative probe headers are also excluded. Send X-Request-ID for client correlation, and X-Request-Kind: warmup for synthetic activation requests.

INIT_DELAY_MS adds artificial initialization work before the server binds; WORK_DELAY_MS adds artificial processing delay. Defaults are zero. Treat these as labelled synthetic settings, not measurements of a real application.
first_user_request remains true on the first user request even if a warmup request prepared the process. It is a first-use proxy, not an exact cold-start label. service_ms excludes transport/provisioning and response transmission. init_ms measures application startup work only.

Use one process per container; adding workers changes identity/first-use semantics and requires redesigned instrumentation. Pin the base image digest and deployed function digest before final evaluation. The starter does not publish an image or deploy a cluster.

## Shutdown

SIGTERM and SIGINT stop the listener and allow accepted request handlers to finish,
including their response and invocation log. Readiness returns 503 once stopping
begins if a probe reaches the handler before the listener closes. The process exits
with code 0 after draining. Artificial initialization delay is also interruptible.
The main thread handles signals; a separate serving thread avoids calling
`shutdown()` from the thread running `serve_forever()`. Request threads are explicitly
non-daemon so server close waits for them. Docker/Kubernetes still imposes the outer
grace deadline; a request that outlasts that deadline can be force-killed.

Run the signal/drain regressions from the repository root:

```bash
python3 -m unittest discover -s scripts -p 'test_benchmark_shutdown.py'
python3 scripts/smoke_benchmark.py
docker build -t csc3104-benchmark:shutdown-fix functions/benchmark
python3 scripts/check_container_shutdown.py \
  --image csc3104-benchmark:shutdown-fix --output results/shutdown/after.json
```

The container check runs the application as PID 1 without an added init process,
waits for readiness, times a ten-second-timeout Docker stop and saves exit status.
It removes only its own uniquely named test container. A fresh output filename is
required. This checks the application container, not Knative queue-proxy draining;
validate Pod termination separately after publishing and pinning the new image.
Rerun all policy baselines on the same new digest before comparing performance.

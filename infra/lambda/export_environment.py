"""Read-only deployment snapshot; no secrets, policy analysis or workload replay."""
import argparse
import importlib.metadata
import json
from datetime import datetime, timezone
from pathlib import Path

FIELDS = ("FunctionArn", "Runtime", "MemorySize", "Timeout", "Version", "CodeSha256",
          "RevisionId", "LastModified", "Architectures", "RuntimeVersionConfig", "Role")
ENV_FIELDS = ("FUNCTION_ID", "INIT_DELAY_MS", "WORK_DELAY_MS", "BENCHMARK_CODE_REVISION")


def safe_configuration(configuration):
    result = {name: configuration[name] for name in FIELDS if name in configuration}
    environment = configuration.get("Environment", {}).get("Variables", {})
    result["benchmark_environment"] = {name: environment[name] for name in ENV_FIELDS if name in environment}
    return result


def main():
    import boto3
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stack", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--profile")
    parser.add_argument("--output", required=True, help="new deployment snapshot file")
    args = parser.parse_args()
    target = Path(args.output)
    if target.exists():
        raise FileExistsError("use a new deployment snapshot filename")
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    cloudformation, client = session.client("cloudformation"), session.client("lambda")
    stack = cloudformation.describe_stacks(StackName=args.stack)["Stacks"][0]
    outputs = {row["OutputKey"]: row["OutputValue"] for row in stack.get("Outputs", [])}
    functions = {}
    for fn, prefix in (("periodic", "Periodic"), ("steady", "Steady"), ("sparse", "Sparse")):
        name = outputs[prefix + "FunctionName"]
        alias = client.get_alias(FunctionName=name, Name="benchmark")
        version = alias["FunctionVersion"]
        if not version.isdigit():
            raise ValueError("benchmark alias must point to a published numbered version")
        configuration = client.get_function_configuration(FunctionName=name, Qualifier=version)
        url = client.get_function_url_config(FunctionName=name, Qualifier="benchmark")
        if url["AuthType"] != "AWS_IAM" or url["FunctionUrl"] != outputs[prefix + "Url"]:
            raise ValueError("Function URL configuration differs from the intended IAM endpoint")
        functions[fn] = {"function_name": name, "alias_arn": alias["AliasArn"],
                         "alias_revision_id": alias["RevisionId"], "version": version,
                         "function_url": url["FunctionUrl"], "auth_type": url["AuthType"],
                         "configuration": safe_configuration(configuration),
                         "concurrency": client.get_function_concurrency(FunctionName=name).get("ReservedConcurrentExecutions"),
                         "runtime_management": client.get_runtime_management_config(FunctionName=name, Qualifier=version),
                         "log_group": "/aws/lambda/" + name}
        # ResponseMetadata contains response headers, not needed in the snapshot.
        functions[fn]["runtime_management"].pop("ResponseMetadata", None)
    report = {"captured_at": datetime.now(timezone.utc).isoformat(), "stack_id": stack["StackId"],
              "region": args.region, "policy": "lambda_on_demand", "functions": functions,
              "sdk_versions": {name: importlib.metadata.version(name) for name in ("boto3", "botocore")},
              "note": "Recheck alias/version before and after each experiment; this snapshot does not pin a mutable URL alias."}
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x") as stream:
        stream.write(json.dumps(report, indent=2) + "\n")
    print("Saved deployment snapshot:", target)


if __name__ == "__main__":
    main()

"""Snapshot a stopped scheduler's final artifacts into its predictive run."""
import argparse
import json
import shutil
from pathlib import Path

from .common import sha256_file


def finalize(run_dir, controller_dir, forecasts=None):
    run_dir, source = Path(run_dir), Path(controller_dir)
    path = run_dir / "run_manifest.json"
    run = json.loads(path.read_text())
    controller = json.loads((source / "scheduler_manifest.json").read_text())
    if run.get("policy") != "predictive":
        raise ValueError("only predictive runs have controller artifacts to finalize")
    if controller.get("final") is not True or not controller.get("ended_at"):
        raise ValueError("stop the scheduler cleanly before finalizing")
    if controller.get("run_id") != run.get("run_id"):
        raise ValueError("scheduler and evaluation run IDs must match")
    files = {name: source / name for name in (
        "scheduler_manifest.json", "decisions.jsonl", "actuation.jsonl")}
    if (source / "policy_audit.jsonl").exists():
        files["policy_audit.jsonl"] = source / "policy_audit.jsonl"
    if forecasts:
        files["forecasts.jsonl"] = Path(forecasts)
    for name, src in files.items():
        if not src.is_file():
            raise ValueError(f"missing artifact: {src}")
        if src.resolve() == (run_dir / name).resolve():
            raise ValueError("source artifacts must be outside the evaluation run directory")
    hashes = {}
    for name, src in files.items():
        temporary = run_dir / (name + ".tmp")
        shutil.copyfile(src, temporary)
        temporary.replace(run_dir / name)
        hashes[name] = sha256_file(run_dir / name)
    run["controller_artifacts"] = hashes
    run["controller_artifacts_final"] = True
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(run, indent=2) + "\n")
    temporary.replace(path)
    return hashes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--controller-dir", required=True)
    parser.add_argument("--forecasts", help="forecast file after stopping the predictor")
    args = parser.parse_args()
    hashes = finalize(args.run_dir, args.controller_dir, args.forecasts)
    print(f"Finalized {len(hashes)} artifacts in {args.run_dir}")


if __name__ == "__main__":
    main()

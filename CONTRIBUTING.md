# Contributing
Choose an issue; record your lead/reviewer and dependencies. Branch from current main. Keep changes focused and include commands, output, assumptions, and limitations in the PR.
Run `python -m compileall functions` and the benchmark smoke test before changing the function. Add meaningful checks for policy budgets, input failures, no future-data leakage, and resource integration when those components exist.
Never commit kubeconfigs, credentials, tokens or local experiment output. Save small anonymised fixtures in contracts/; store large raw artifacts outside git with durable links and hashes.
Pin deployed images to digests. The starter Docker base tag is a convenience and must be pinned for final measurements. Agree on contract version changes with every affected component owner.

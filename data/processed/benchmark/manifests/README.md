# processed/benchmark/manifests

Store stable benchmark manifest artifacts here.

Checked-in files under this directory should remain deterministic across reruns.
Runtime-only benchmark `*_run_manifest.json` files are still written during
command execution, but they are intentionally untracked so timestamps, git SHA,
and invocation-specific output paths do not churn the repo.

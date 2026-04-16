# GPU Container Contract

`Dockerfile.gpu` is the bootstrap path for GPU-native strict-open audit work.

This is intentionally minimal in PR2:
- NVIDIA CUDA runtime base image
- Python install
- editable install of the package
- default command wired to `scz-audit --help`

It is a contract for later ingest and training PRs, not a production container definition.

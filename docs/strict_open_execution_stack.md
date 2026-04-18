# strict-open v0 execution stack

> Superseded.
> This is archived exploratory documentation.
> Active benchmark guidance lives in [`docs/benchmark_claim.md`](benchmark_claim.md)
> and [`docs/benchmark_pivot_roadmap.md`](benchmark_pivot_roadmap.md).

## Product and spec posture

Product and scientific claims for `strict-open v0` stay vendor-neutral, even as the build is framed narrowly around cohort stability and noise auditing.

## Intended execution path

- The implementation path is GPU-native and NVIDIA-friendly.
- `PyTorch`, `MONAI`, `CUDA`, and a containerized GPU runtime are the intended execution path.

## Future-evaluation items only

- `TensorRT` is a future-evaluation item, not a current requirement.
- `Dynamo` is a future-evaluation item, not a current requirement.
- `BioNeMo` is a future-evaluation item, not a current requirement.

## Explicit boundary

The plan should not force irrelevant NVIDIA tools into `strict-open v0`.

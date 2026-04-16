# strict-open v0 execution stack

## Product and spec posture

Product and scientific claims for `strict-open v0` stay vendor-neutral.

## Intended execution path

- The implementation path is GPU-native and NVIDIA-friendly.
- `PyTorch`, `MONAI`, `CUDA`, and a containerized GPU runtime are the intended execution path.

## Future-evaluation items only

- `TensorRT` is a future-evaluation item, not a current requirement.
- `Dynamo` is a future-evaluation item, not a current requirement.
- `BioNeMo` is a future-evaluation item, not a current requirement.

## Explicit boundary

The plan should not force irrelevant NVIDIA tools into `strict-open v0`.

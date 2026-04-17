"""Benchmark scaffolding contracts and helpers."""

from .dataset_audit import run_benchmark_dataset_audit
from .dataset_registry import (
    ACCESS_LEVELS,
    LOCAL_STATUSES,
    OUTCOME_FAMILIES,
    BenchmarkDecision,
    DatasetRegistryEntry,
    build_outcome_support,
    derive_benchmark_decision,
    load_dataset_registry,
    write_dataset_registry,
)
from .paths import BenchmarkPaths, benchmark_paths
from .provenance import file_sha256, resolve_git_sha, write_json_artifact, write_text_artifact
from .run_manifest import DatasetReference, RunManifest, build_run_manifest, write_run_manifest

__all__ = [
    "ACCESS_LEVELS",
    "BenchmarkPaths",
    "BenchmarkDecision",
    "DatasetReference",
    "DatasetRegistryEntry",
    "LOCAL_STATUSES",
    "OUTCOME_FAMILIES",
    "RunManifest",
    "benchmark_paths",
    "build_outcome_support",
    "build_run_manifest",
    "derive_benchmark_decision",
    "file_sha256",
    "load_dataset_registry",
    "resolve_git_sha",
    "run_benchmark_dataset_audit",
    "write_dataset_registry",
    "write_json_artifact",
    "write_text_artifact",
    "write_run_manifest",
]

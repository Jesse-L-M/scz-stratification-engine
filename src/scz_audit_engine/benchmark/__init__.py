"""Benchmark scaffolding contracts and helpers."""

from .harmonize import run_benchmark_harmonization
from .dataset_audit import run_benchmark_dataset_audit
from .dataset_registry import (
    ACCESS_LEVELS,
    BENCHMARK_V0_ELIGIBILITY_STATES,
    CLAIM_LEVELS,
    LOCAL_STATUSES,
    OUTCOME_FAMILIES,
    OUTCOME_TEMPORAL_VALIDITY_STATES,
    REPRESENTATION_COMPARISON_SUPPORT_STATES,
    BenchmarkDecision,
    DatasetRegistryEntry,
    build_full_external_validation_support,
    build_outcome_support,
    derive_benchmark_decision,
    load_dataset_registry,
    write_dataset_registry,
)
from .paths import BenchmarkPaths, benchmark_paths
from .provenance import file_sha256, resolve_git_sha, write_json_artifact, write_text_artifact
from .run_manifest import DatasetReference, RunManifest, build_run_manifest, write_run_manifest
from .schema import (
    BenchmarkSchema,
    CANONICAL_BENCHMARK_SCHEMA,
    CANONICAL_TABLE_NAMES,
    SCHEMA_VERSION,
    TableContract,
    benchmark_schema,
)
from .schema_artifacts import run_benchmark_define_schema
from .splits import (
    ASSIGNMENT_NOTE,
    DEFAULT_SPLIT_FRACTIONS,
    SPLIT_ORDER,
    SPLIT_PROTOCOL_VERSION,
    write_benchmark_split_artifacts,
)

__all__ = [
    "ACCESS_LEVELS",
    "BenchmarkSchema",
    "BENCHMARK_V0_ELIGIBILITY_STATES",
    "CANONICAL_BENCHMARK_SCHEMA",
    "CANONICAL_TABLE_NAMES",
    "CLAIM_LEVELS",
    "BenchmarkPaths",
    "BenchmarkDecision",
    "DatasetReference",
    "DatasetRegistryEntry",
    "DEFAULT_SPLIT_FRACTIONS",
    "LOCAL_STATUSES",
    "OUTCOME_FAMILIES",
    "OUTCOME_TEMPORAL_VALIDITY_STATES",
    "REPRESENTATION_COMPARISON_SUPPORT_STATES",
    "RunManifest",
    "ASSIGNMENT_NOTE",
    "SPLIT_ORDER",
    "SPLIT_PROTOCOL_VERSION",
    "benchmark_paths",
    "build_full_external_validation_support",
    "build_outcome_support",
    "build_run_manifest",
    "benchmark_schema",
    "derive_benchmark_decision",
    "file_sha256",
    "load_dataset_registry",
    "resolve_git_sha",
    "run_benchmark_dataset_audit",
    "run_benchmark_define_schema",
    "run_benchmark_harmonization",
    "SCHEMA_VERSION",
    "TableContract",
    "write_dataset_registry",
    "write_benchmark_split_artifacts",
    "write_json_artifact",
    "write_text_artifact",
    "write_run_manifest",
]

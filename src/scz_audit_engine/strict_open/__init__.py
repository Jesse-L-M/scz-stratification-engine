"""Strict-open bootstrap contracts and helpers."""

from .audit import run_tcp_audit
from .harmonize import run_tcp_harmonization
from .paths import StrictOpenPaths, strict_open_paths
from .provenance import (
    AuditProvenance,
    ProcessedOutputRecord,
    ProvenanceMapping,
    SourceFileRecord,
    SourceManifest,
    build_audit_provenance,
    build_source_manifest,
    write_audit_provenance,
    write_source_manifest,
)
from .run_manifest import DatasetReference, RunManifest, build_run_manifest, write_run_manifest
from .schema import STRICT_OPEN_TABLE_SCHEMAS, STRICT_OPEN_TABLE_NAMES, TableSchema
from .splits import run_strict_open_split_definition

__all__ = [
    "AuditProvenance",
    "DatasetReference",
    "ProcessedOutputRecord",
    "ProvenanceMapping",
    "RunManifest",
    "SourceFileRecord",
    "SourceManifest",
    "STRICT_OPEN_TABLE_NAMES",
    "STRICT_OPEN_TABLE_SCHEMAS",
    "StrictOpenPaths",
    "TableSchema",
    "build_audit_provenance",
    "build_run_manifest",
    "build_source_manifest",
    "run_strict_open_split_definition",
    "run_tcp_harmonization",
    "run_tcp_audit",
    "strict_open_paths",
    "write_audit_provenance",
    "write_run_manifest",
    "write_source_manifest",
]

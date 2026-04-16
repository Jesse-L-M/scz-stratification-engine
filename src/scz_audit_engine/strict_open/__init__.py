"""Strict-open bootstrap contracts and helpers."""

from .paths import StrictOpenPaths, strict_open_paths
from .run_manifest import DatasetReference, RunManifest, build_run_manifest, write_run_manifest
from .schema import STRICT_OPEN_TABLE_SCHEMAS, STRICT_OPEN_TABLE_NAMES, TableSchema

__all__ = [
    "DatasetReference",
    "RunManifest",
    "STRICT_OPEN_TABLE_NAMES",
    "STRICT_OPEN_TABLE_SCHEMAS",
    "StrictOpenPaths",
    "TableSchema",
    "build_run_manifest",
    "strict_open_paths",
    "write_run_manifest",
]

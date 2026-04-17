"""Benchmark cohort source adapters."""

from __future__ import annotations

from pathlib import Path

from .base import OpenNeuroSnapshotBundle, OpenNeuroSourceAdapter, SourceAdapter
from .fep_ds003944 import FEPDS003944BenchmarkSourceAdapter
from .tcp_ds005237 import TCPDS005237BenchmarkSourceAdapter


def build_default_source_adapters(
    snapshot_roots: dict[str, str | Path] | None = None,
) -> tuple[SourceAdapter, ...]:
    roots = snapshot_roots or {}
    return (
        TCPDS005237BenchmarkSourceAdapter(snapshot_root=roots.get("tcp-ds005237")),
        FEPDS003944BenchmarkSourceAdapter(snapshot_root=roots.get("fep-ds003944")),
    )


__all__ = [
    "FEPDS003944BenchmarkSourceAdapter",
    "OpenNeuroSnapshotBundle",
    "OpenNeuroSourceAdapter",
    "SourceAdapter",
    "TCPDS005237BenchmarkSourceAdapter",
    "build_default_source_adapters",
]

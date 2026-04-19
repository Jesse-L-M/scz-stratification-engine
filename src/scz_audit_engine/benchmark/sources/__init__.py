"""Benchmark cohort source adapters."""

from __future__ import annotations

from pathlib import Path

from .base import (
    CohortHarmonizationBundle,
    HarmonizableSourceAdapter,
    OpenNeuroSnapshotBundle,
    OpenNeuroSourceAdapter,
    SourceAdapter,
)
from .ds000115 import DS000115BenchmarkSourceAdapter
from .fep_ds003944 import FEPDS003944BenchmarkSourceAdapter
from .tcp_ds005237 import TCPDS005237BenchmarkSourceAdapter
from .ucla_cnp_ds000030 import UCLACNPDS000030BenchmarkSourceAdapter


def build_default_source_adapters(
    snapshot_roots: dict[str, str | Path] | None = None,
) -> tuple[SourceAdapter, ...]:
    roots = snapshot_roots or {}
    return (
        TCPDS005237BenchmarkSourceAdapter(snapshot_root=roots.get("tcp-ds005237")),
        FEPDS003944BenchmarkSourceAdapter(snapshot_root=roots.get("fep-ds003944")),
        UCLACNPDS000030BenchmarkSourceAdapter(
            snapshot_root=roots.get("ucla-cnp-ds000030")
        ),
        DS000115BenchmarkSourceAdapter(snapshot_root=roots.get("ds000115")),
    )


__all__ = [
    "FEPDS003944BenchmarkSourceAdapter",
    "DS000115BenchmarkSourceAdapter",
    "CohortHarmonizationBundle",
    "HarmonizableSourceAdapter",
    "OpenNeuroSnapshotBundle",
    "OpenNeuroSourceAdapter",
    "SourceAdapter",
    "TCPDS005237BenchmarkSourceAdapter",
    "UCLACNPDS000030BenchmarkSourceAdapter",
    "build_default_source_adapters",
]

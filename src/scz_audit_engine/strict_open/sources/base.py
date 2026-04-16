"""Source-adapter contracts for strict-open raw ingest."""

from __future__ import annotations

import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from ..provenance import SourceFileRecord, file_sha256


@dataclass(frozen=True, slots=True)
class StageResult:
    """The result of staging or inspecting a raw source."""

    source: str
    source_identifier: str
    dataset_accession: str
    dataset_version: str | None
    raw_root: Path
    files: tuple[SourceFileRecord, ...]


class SourceAdapter(ABC):
    """Small abstraction for a public source adapter."""

    source_name: str
    source_identifier: str
    dataset_accession: str
    dataset_version: str | None

    @abstractmethod
    def stage(self, destination: str | Path, *, source_root: str | Path | None = None) -> StageResult:
        """Stage this source into the raw destination and return discovered files."""

    @abstractmethod
    def inspect(self, raw_root: str | Path) -> StageResult:
        """Inspect already-staged raw inputs and return the file inventory."""


def copy_source_tree(source_root: str | Path, destination: str | Path) -> tuple[SourceFileRecord, ...]:
    """Copy a local source tree into the raw destination and inventory the files."""

    source_path = Path(source_root)
    destination_path = Path(destination)
    records: list[SourceFileRecord] = []
    for local_path in sorted(path for path in source_path.rglob("*") if path.is_file()):
        relative_path = local_path.relative_to(source_path)
        output_path = destination_path / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, output_path)
        records.append(
            SourceFileRecord(
                relative_path=relative_path.as_posix(),
                storage="copied",
                size_bytes=output_path.stat().st_size,
                sha256=file_sha256(output_path),
                source_url=None,
                content_kind="file",
            )
        )
    return tuple(records)


def inspect_local_tree(raw_root: str | Path, *, storage: str = "staged") -> tuple[SourceFileRecord, ...]:
    """Inventory a local raw tree and return deterministic file records."""

    root = Path(raw_root)
    records: list[SourceFileRecord] = []
    for local_path in sorted(path for path in root.rglob("*") if path.is_file()):
        records.append(
            SourceFileRecord(
                relative_path=local_path.relative_to(root).as_posix(),
                storage=storage,
                size_bytes=local_path.stat().st_size,
                sha256=file_sha256(local_path),
                source_url=None,
                content_kind="file",
            )
        )
    return tuple(records)


__all__ = ["SourceAdapter", "StageResult", "copy_source_tree", "inspect_local_tree"]

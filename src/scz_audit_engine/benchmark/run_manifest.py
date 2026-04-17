"""Run-manifest contract and writer for benchmark commands."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .provenance import write_json_artifact


def utc_now_iso() -> str:
    """Return an RFC 3339 UTC timestamp."""

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class DatasetReference:
    """Dataset reference for a benchmark run."""

    source: str | None = None
    cohort: str | None = None

    def __post_init__(self) -> None:
        if self.source is None and self.cohort is None:
            raise ValueError("dataset reference requires a source or cohort identifier")

    def to_dict(self) -> dict[str, str]:
        payload: dict[str, str] = {}
        if self.source is not None:
            payload["source"] = self.source
        if self.cohort is not None:
            payload["cohort"] = self.cohort
        return payload


@dataclass(frozen=True, slots=True)
class RunManifest:
    """Lightweight contract for benchmark run provenance."""

    dataset: DatasetReference
    command: tuple[str, ...]
    git_sha: str | None
    seed: int
    output_paths: dict[str, str]
    timestamp: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset.to_dict(),
            "command": list(self.command),
            "git_sha": self.git_sha,
            "seed": self.seed,
            "output_paths": dict(self.output_paths),
            "timestamp": self.timestamp,
        }


def build_run_manifest(
    *,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
    output_paths: dict[str, str | Path],
    dataset_source: str | None = None,
    cohort_identifier: str | None = None,
    timestamp: str | None = None,
) -> RunManifest:
    """Create a benchmark run manifest from plain Python values."""

    normalized_output_paths = {name: str(path) for name, path in output_paths.items()}
    dataset = DatasetReference(source=dataset_source, cohort=cohort_identifier)
    return RunManifest(
        dataset=dataset,
        command=tuple(command),
        git_sha=git_sha,
        seed=seed,
        output_paths=normalized_output_paths,
        timestamp=timestamp or utc_now_iso(),
    )


def write_run_manifest(manifest: RunManifest, destination: str | Path) -> Path:
    """Write a manifest to disk as JSON and return the resolved path."""

    return write_json_artifact(manifest.to_dict(), destination)


__all__ = [
    "DatasetReference",
    "RunManifest",
    "build_run_manifest",
    "utc_now_iso",
    "write_run_manifest",
]

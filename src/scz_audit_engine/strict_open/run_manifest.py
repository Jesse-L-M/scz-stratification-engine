"""Run-manifest contract and writer for strict-open commands."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    """Return an RFC 3339 UTC timestamp."""

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class DatasetReference:
    """Source/version reference for a strict-open run."""

    source: str
    version: str | None = None

    def to_dict(self) -> dict[str, str]:
        payload = {"source": self.source}
        if self.version is not None:
            payload["version"] = self.version
        return payload


@dataclass(frozen=True, slots=True)
class RunManifest:
    """Lightweight contract for run-level provenance."""

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
    dataset_source: str,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
    output_paths: dict[str, str | Path],
    dataset_version: str | None = None,
    timestamp: str | None = None,
) -> RunManifest:
    """Create a run manifest from plain Python values."""

    normalized_output_paths = {name: str(path) for name, path in output_paths.items()}
    return RunManifest(
        dataset=DatasetReference(source=dataset_source, version=dataset_version),
        command=tuple(command),
        git_sha=git_sha,
        seed=seed,
        output_paths=normalized_output_paths,
        timestamp=timestamp or utc_now_iso(),
    )


def write_run_manifest(manifest: RunManifest, destination: str | Path) -> Path:
    """Write a manifest to disk as JSON and return the resolved path."""

    output_path = Path(destination)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


__all__ = [
    "DatasetReference",
    "RunManifest",
    "build_run_manifest",
    "utc_now_iso",
    "write_run_manifest",
]

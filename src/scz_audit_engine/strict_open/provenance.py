"""Source-manifest and raw-to-processed provenance helpers."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .run_manifest import utc_now_iso

GIT_ANNEX_POINTER_PREFIX = "../.git/annex/objects/"


def file_sha256(path: str | Path, chunk_size: int = 65536) -> str:
    """Return the SHA256 digest for a local file."""

    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def is_git_annex_pointer_text(text: str) -> bool:
    """Return whether a text payload is a git-annex pointer."""

    return text.startswith(GIT_ANNEX_POINTER_PREFIX)


def local_file_content_kind(path: str | Path, sample_bytes: int = 512) -> str:
    """Classify a local file payload for provenance purposes."""

    with Path(path).open("rb") as handle:
        sample = handle.read(sample_bytes)
    try:
        decoded = sample.decode("utf-8-sig")
    except UnicodeDecodeError:
        return "file"
    return "git-annex-pointer" if is_git_annex_pointer_text(decoded) else "file"


def resolve_git_sha(repo_root: str | Path) -> str | None:
    """Return the short git SHA for the repo root when available."""

    try:
        completed = subprocess.run(
            ["git", "-C", str(Path(repo_root)), "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None

    git_sha = completed.stdout.strip()
    return git_sha or None


@dataclass(frozen=True, slots=True)
class SourceFileRecord:
    """A raw source file discovered or staged during ingest."""

    relative_path: str
    storage: str
    size_bytes: int | None
    sha256: str | None = None
    source_url: str | None = None
    content_kind: str = "file"

    def to_dict(self) -> dict[str, Any]:
        return {
            "content_kind": self.content_kind,
            "relative_path": self.relative_path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "source_url": self.source_url,
            "storage": self.storage,
        }


@dataclass(frozen=True, slots=True)
class SourceManifest:
    """Source-level manifest emitted during ingest."""

    source: str
    source_identifier: str
    dataset_accession: str
    dataset_version: str | None
    ingest_timestamp: str
    command: tuple[str, ...]
    git_sha: str | None
    raw_root: str
    files: tuple[SourceFileRecord, ...]

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "command": list(self.command),
            "dataset_accession": self.dataset_accession,
            "dataset_version": self.dataset_version,
            "files": [record.to_dict() for record in self.files],
            "git_sha": self.git_sha,
            "ingest_timestamp": self.ingest_timestamp,
            "raw_root": self.raw_root,
            "source": self.source,
            "source_identifier": self.source_identifier,
        }
        return payload


@dataclass(frozen=True, slots=True)
class ProcessedOutputRecord:
    """A processed artifact written by a strict-open command."""

    output_name: str
    relative_path: str
    sha256: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "output_name": self.output_name,
            "relative_path": self.relative_path,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class ProvenanceMapping:
    """A raw-to-processed mapping entry."""

    processed_output: str
    raw_inputs: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "processed_output": self.processed_output,
            "raw_inputs": list(self.raw_inputs),
        }


@dataclass(frozen=True, slots=True)
class AuditProvenance:
    """Raw-to-processed provenance emitted during audit/profile generation."""

    source: str
    source_identifier: str
    dataset_accession: str
    dataset_version: str | None
    generated_at: str
    command: tuple[str, ...]
    git_sha: str | None
    raw_root: str
    processed_outputs: tuple[ProcessedOutputRecord, ...]
    mappings: tuple[ProvenanceMapping, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "command": list(self.command),
            "dataset_accession": self.dataset_accession,
            "dataset_version": self.dataset_version,
            "generated_at": self.generated_at,
            "git_sha": self.git_sha,
            "mappings": [mapping.to_dict() for mapping in self.mappings],
            "processed_outputs": [record.to_dict() for record in self.processed_outputs],
            "raw_root": self.raw_root,
            "source": self.source,
            "source_identifier": self.source_identifier,
        }


def build_source_manifest(
    *,
    source: str,
    source_identifier: str,
    dataset_accession: str,
    dataset_version: str | None,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    raw_root: str | Path,
    files: list[SourceFileRecord] | tuple[SourceFileRecord, ...],
    ingest_timestamp: str | None = None,
) -> SourceManifest:
    """Create a source manifest from plain Python values."""

    sorted_files = tuple(sorted(files, key=lambda record: record.relative_path))
    return SourceManifest(
        source=source,
        source_identifier=source_identifier,
        dataset_accession=dataset_accession,
        dataset_version=dataset_version,
        ingest_timestamp=ingest_timestamp or utc_now_iso(),
        command=tuple(command),
        git_sha=git_sha,
        raw_root=str(raw_root),
        files=sorted_files,
    )


def build_audit_provenance(
    *,
    source: str,
    source_identifier: str,
    dataset_accession: str,
    dataset_version: str | None,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    raw_root: str | Path,
    processed_outputs: list[ProcessedOutputRecord] | tuple[ProcessedOutputRecord, ...],
    mappings: list[ProvenanceMapping] | tuple[ProvenanceMapping, ...],
    generated_at: str | None = None,
) -> AuditProvenance:
    """Create audit provenance metadata from plain Python values."""

    return AuditProvenance(
        source=source,
        source_identifier=source_identifier,
        dataset_accession=dataset_accession,
        dataset_version=dataset_version,
        generated_at=generated_at or utc_now_iso(),
        command=tuple(command),
        git_sha=git_sha,
        raw_root=str(raw_root),
        processed_outputs=tuple(processed_outputs),
        mappings=tuple(mappings),
    )


def write_json_artifact(payload: dict[str, Any], destination: str | Path) -> Path:
    """Write a JSON artifact to disk and return the resolved path."""

    output_path = Path(destination)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def write_source_manifest(manifest: SourceManifest, destination: str | Path) -> Path:
    """Write a source manifest to disk and return the resolved path."""

    return write_json_artifact(manifest.to_dict(), destination)


def write_audit_provenance(provenance: AuditProvenance, destination: str | Path) -> Path:
    """Write audit provenance to disk and return the resolved path."""

    return write_json_artifact(provenance.to_dict(), destination)


def load_source_manifest(source: str | Path) -> SourceManifest:
    """Load a source manifest from JSON."""

    payload = json.loads(Path(source).read_text(encoding="utf-8"))
    files = tuple(SourceFileRecord(**record) for record in payload["files"])
    return SourceManifest(
        source=payload["source"],
        source_identifier=payload["source_identifier"],
        dataset_accession=payload["dataset_accession"],
        dataset_version=payload.get("dataset_version"),
        ingest_timestamp=payload["ingest_timestamp"],
        command=tuple(payload["command"]),
        git_sha=payload.get("git_sha"),
        raw_root=payload["raw_root"],
        files=files,
    )


__all__ = [
    "AuditProvenance",
    "ProcessedOutputRecord",
    "ProvenanceMapping",
    "SourceFileRecord",
    "SourceManifest",
    "build_audit_provenance",
    "build_source_manifest",
    "file_sha256",
    "is_git_annex_pointer_text",
    "local_file_content_kind",
    "load_source_manifest",
    "resolve_git_sha",
    "write_audit_provenance",
    "write_json_artifact",
    "write_source_manifest",
]

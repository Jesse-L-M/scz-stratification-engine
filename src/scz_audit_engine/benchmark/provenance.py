"""Minimal provenance helpers for benchmark artifacts."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any


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


def write_json_artifact(payload: dict[str, Any], destination: str | Path) -> Path:
    """Write a JSON artifact to disk and return the resolved path."""

    output_path = Path(destination)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


__all__ = ["file_sha256", "resolve_git_sha", "write_json_artifact"]

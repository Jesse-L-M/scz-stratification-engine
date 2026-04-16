"""TCP / ds005237 source adapter."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from urllib import error, request

from ..provenance import SourceFileRecord, file_sha256
from .base import SourceAdapter, StageResult, copy_source_tree, inspect_local_tree

DEFAULT_GITHUB_REPO = "OpenNeuroDatasets/ds005237"
DEFAULT_GITHUB_REF = "main"
DEFAULT_DATASET_VERSION = "1.1.3"
ANNEX_POINTER_PREFIX = "../.git/annex/objects/"


@dataclass(frozen=True, slots=True)
class TCPDS005237SourceAdapter(SourceAdapter):
    """Stage or inspect the public TCP / ds005237 source."""

    source_name: str = "tcp"
    source_identifier: str = "tcp-ds005237"
    dataset_accession: str = "ds005237"
    dataset_version: str | None = DEFAULT_DATASET_VERSION
    github_repo: str = DEFAULT_GITHUB_REPO
    github_ref: str = DEFAULT_GITHUB_REF

    @property
    def github_api_root(self) -> str:
        return f"https://api.github.com/repos/{self.github_repo}"

    @property
    def raw_content_root(self) -> str:
        return f"https://raw.githubusercontent.com/{self.github_repo}/{self.github_ref}"

    def stage(self, destination: str | Path, *, source_root: str | Path | None = None) -> StageResult:
        destination_path = Path(destination)
        destination_path.mkdir(parents=True, exist_ok=True)

        if source_root is not None:
            files = copy_source_tree(source_root, destination_path)
            return StageResult(
                source=self.source_name,
                source_identifier=self.source_identifier,
                dataset_accession=self.dataset_accession,
                dataset_version=self.dataset_version,
                raw_root=destination_path,
                files=files,
            )

        files = self._stage_public_metadata(destination_path)
        return StageResult(
            source=self.source_name,
            source_identifier=self.source_identifier,
            dataset_accession=self.dataset_accession,
            dataset_version=self._discover_latest_tag() or self.dataset_version,
            raw_root=destination_path,
            files=files,
        )

    def inspect(self, raw_root: str | Path) -> StageResult:
        root = Path(raw_root)
        return StageResult(
            source=self.source_name,
            source_identifier=self.source_identifier,
            dataset_accession=self.dataset_accession,
            dataset_version=self.dataset_version,
            raw_root=root,
            files=inspect_local_tree(root),
        )

    def _stage_public_metadata(self, destination: Path) -> tuple[SourceFileRecord, ...]:
        tree_items = self._fetch_tree()
        indexed_paths = tuple(sorted(item["path"] for item in tree_items if item.get("type") == "blob"))
        downloaded_records: dict[str, SourceFileRecord] = {}

        for relative_path in indexed_paths:
            if not self._should_stage_remote_path(relative_path):
                continue
            file_url = f"{self.raw_content_root}/{relative_path}"
            content = self._download_text(file_url)
            output_path = destination / relative_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(content, encoding="utf-8")
            content_kind = "git-annex-pointer" if content.startswith(ANNEX_POINTER_PREFIX) else "file"
            downloaded_records[relative_path] = SourceFileRecord(
                relative_path=relative_path,
                storage="staged",
                size_bytes=output_path.stat().st_size,
                sha256=file_sha256(output_path),
                source_url=file_url,
                content_kind=content_kind,
            )

        records: list[SourceFileRecord] = []
        for item in sorted(tree_items, key=lambda entry: entry["path"]):
            if item.get("type") != "blob":
                continue
            relative_path = item["path"]
            downloaded = downloaded_records.get(relative_path)
            if downloaded is not None:
                records.append(downloaded)
                continue
            records.append(
                SourceFileRecord(
                    relative_path=relative_path,
                    storage="remote-listed",
                    size_bytes=item.get("size"),
                    sha256=None,
                    source_url=f"{self.raw_content_root}/{relative_path}",
                    content_kind="file",
                )
            )
        return tuple(records)

    def _fetch_tree(self) -> list[dict[str, object]]:
        payload = self._fetch_json(f"{self.github_api_root}/git/trees/{self.github_ref}?recursive=1")
        tree = payload.get("tree", [])
        if not isinstance(tree, list):
            raise RuntimeError("Unexpected TCP tree payload from GitHub API.")
        return [item for item in tree if isinstance(item, dict)]

    def _discover_latest_tag(self) -> str | None:
        try:
            payload = self._fetch_json(f"{self.github_api_root}/tags?per_page=1")
        except RuntimeError:
            return self.dataset_version

        if not isinstance(payload, list) or not payload:
            return self.dataset_version
        latest = payload[0]
        if not isinstance(latest, dict):
            return self.dataset_version
        tag_name = latest.get("name")
        return tag_name if isinstance(tag_name, str) else self.dataset_version

    def _fetch_json(self, url: str) -> dict[str, object] | list[object]:
        try:
            with request.urlopen(request.Request(url, headers={"User-Agent": "scz-audit-engine"}), timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except (error.URLError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Failed to fetch TCP metadata from {url}") from exc

    def _download_text(self, url: str) -> str:
        try:
            with request.urlopen(request.Request(url, headers={"User-Agent": "scz-audit-engine"}), timeout=30) as response:
                return response.read().decode("utf-8")
        except error.URLError as exc:
            raise RuntimeError(f"Failed to download TCP file from {url}") from exc

    @staticmethod
    def _should_stage_remote_path(relative_path: str) -> bool:
        if relative_path.startswith(".datalad/"):
            return False
        if relative_path in {".bidsignore", "CHANGES", "README", "dataset_description.json", "participants.tsv"}:
            return True
        if relative_path.startswith("phenotype/"):
            return True
        if relative_path.startswith("motion_FD/"):
            return True
        return False


__all__ = ["TCPDS005237SourceAdapter"]

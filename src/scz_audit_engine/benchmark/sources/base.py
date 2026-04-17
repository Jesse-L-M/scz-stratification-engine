"""Metadata-only source adapters for benchmark cohort discovery."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib import error, request

from ..dataset_registry import DatasetRegistryEntry

OPENNEURO_GRAPHQL_URL = "https://openneuro.org/crn/graphql"


@dataclass(frozen=True, slots=True)
class OpenNeuroSnapshotBundle:
    """Minimal snapshot bundle needed to audit a cohort from OpenNeuro metadata."""

    dataset: dict[str, Any]
    root_files: tuple[dict[str, Any], ...]
    phenotype_files: tuple[str, ...]
    readme_text: str
    participants_tsv: str | None


class SourceAdapter(ABC):
    """Small abstraction for metadata-only benchmark source auditing."""

    source_identifier: str

    @abstractmethod
    def audit(self) -> DatasetRegistryEntry:
        """Normalize audited source metadata into a registry entry."""


class OpenNeuroSourceAdapter(SourceAdapter):
    """Common loader for OpenNeuro metadata snapshots."""

    dataset_accession: str
    dataset_tag: str
    github_repo: str

    def __init__(self, *, snapshot_root: str | Path | None = None) -> None:
        self.snapshot_root = Path(snapshot_root).resolve() if snapshot_root is not None else None

    @property
    def dataset_page_url(self) -> str:
        return f"https://openneuro.org/datasets/{self.dataset_accession}"

    @property
    def raw_content_root(self) -> str:
        return f"https://raw.githubusercontent.com/{self.github_repo}/{self.dataset_tag}"

    @property
    def readme_url(self) -> str:
        return f"{self.raw_content_root}/README"

    @property
    def participants_url(self) -> str:
        return f"{self.raw_content_root}/participants.tsv"

    def audit(self) -> DatasetRegistryEntry:
        return self.normalize_bundle(self.load_snapshot_bundle())

    @abstractmethod
    def normalize_bundle(self, bundle: OpenNeuroSnapshotBundle) -> DatasetRegistryEntry:
        """Map the loaded snapshot bundle into a dataset-registry row."""

    def load_snapshot_bundle(self) -> OpenNeuroSnapshotBundle:
        if self.snapshot_root is not None:
            return self._load_snapshot_bundle_from_disk(self.snapshot_root)
        return self._fetch_snapshot_bundle()

    def _fetch_snapshot_bundle(self) -> OpenNeuroSnapshotBundle:
        dataset = _graphql_query(
            """
            query DatasetMetadata($datasetId: ID!) {
              dataset(id: $datasetId) {
                id
                name
                metadata {
                  species
                  studyDesign
                  studyDomain
                  modalities
                  ages
                }
              }
            }
            """,
            {"datasetId": self.dataset_accession},
        )["dataset"]
        snapshot_payload = _graphql_query(
            """
            query SnapshotRootFiles($datasetId: ID!, $tag: String!) {
              snapshot(datasetId: $datasetId, tag: $tag) {
                tag
                description {
                  Name
                  DatasetDOI
                  License
                  Authors
                  ReferencesAndLinks
                }
                files {
                  id
                  filename
                  size
                  directory
                  annexed
                }
              }
            }
            """,
            {"datasetId": self.dataset_accession, "tag": self.dataset_tag},
        )["snapshot"]
        dataset["latestSnapshot"] = {
            "tag": snapshot_payload["tag"],
            "description": snapshot_payload["description"],
        }
        root_files = tuple(snapshot_payload["files"])
        phenotype_tree_id = next(
            (
                item["id"]
                for item in root_files
                if item.get("directory") and item.get("filename") == "phenotype"
            ),
            None,
        )
        phenotype_files: tuple[str, ...] = ()
        if phenotype_tree_id is not None:
            phenotype_files = tuple(
                item["filename"]
                for item in _graphql_query(
                    """
                    query PhenotypeFiles($datasetId: ID!, $tag: String!, $tree: String!) {
                      snapshot(datasetId: $datasetId, tag: $tag) {
                        files(tree: $tree, recursive: true) {
                          filename
                          directory
                          annexed
                        }
                      }
                    }
                    """,
                    {
                        "datasetId": self.dataset_accession,
                        "tag": self.dataset_tag,
                        "tree": phenotype_tree_id,
                    },
                )["snapshot"]["files"]
                if not item.get("directory")
            )

        return OpenNeuroSnapshotBundle(
            dataset=dataset,
            root_files=root_files,
            phenotype_files=phenotype_files,
            readme_text=_fetch_text(self.readme_url),
            participants_tsv=_fetch_text(self.participants_url, required=False),
        )

    @staticmethod
    def _load_snapshot_bundle_from_disk(snapshot_root: Path) -> OpenNeuroSnapshotBundle:
        dataset_payload = json.loads((snapshot_root / "dataset_metadata.json").read_text(encoding="utf-8"))
        root_files_payload = json.loads((snapshot_root / "root_files.json").read_text(encoding="utf-8"))
        phenotype_files_payload = json.loads((snapshot_root / "phenotype_files.json").read_text(encoding="utf-8"))

        dataset = _extract_graphql_object(dataset_payload, "dataset")
        root_files = tuple(_extract_graphql_object(root_files_payload, "files"))
        phenotype_files = tuple(_extract_graphql_object(phenotype_files_payload, "files"))
        participants_path = snapshot_root / "participants.tsv"
        participants_tsv = participants_path.read_text(encoding="utf-8") if participants_path.exists() else None
        return OpenNeuroSnapshotBundle(
            dataset=dataset,
            root_files=root_files,
            phenotype_files=phenotype_files,
            readme_text=(snapshot_root / "README.txt").read_text(encoding="utf-8"),
            participants_tsv=participants_tsv,
        )


def _extract_graphql_object(payload: dict[str, Any], key: str) -> Any:
    if key in payload:
        return payload[key]
    data = payload.get("data")
    if isinstance(data, dict) and key in data:
        return data[key]
    raise KeyError(f"expected GraphQL object '{key}' in payload")


def _graphql_query(query: str, variables: dict[str, Any]) -> dict[str, Any]:
    request_payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    request_headers = {"Content-Type": "application/json", "User-Agent": "scz-audit-engine"}
    try:
        with request.urlopen(
            request.Request(OPENNEURO_GRAPHQL_URL, data=request_payload, headers=request_headers),
            timeout=30,
        ) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (error.URLError, json.JSONDecodeError) as exc:
        raise RuntimeError("Failed to fetch OpenNeuro metadata.") from exc

    if payload.get("errors"):
        raise RuntimeError(f"OpenNeuro metadata query failed: {payload['errors']}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("Unexpected OpenNeuro GraphQL payload.")
    return data


def _fetch_text(url: str, *, required: bool = True) -> str | None:
    try:
        with request.urlopen(
            request.Request(url, headers={"User-Agent": "scz-audit-engine"}),
            timeout=30,
        ) as response:
            return response.read().decode("utf-8")
    except error.HTTPError as exc:
        if not required and exc.code == 404:
            return None
        raise RuntimeError(f"Failed to fetch remote text from {url}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Failed to fetch remote text from {url}") from exc


__all__ = [
    "OPENNEURO_GRAPHQL_URL",
    "OpenNeuroSnapshotBundle",
    "OpenNeuroSourceAdapter",
    "SourceAdapter",
]

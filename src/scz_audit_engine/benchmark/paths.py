"""Repo-relative path helpers for the benchmark namespace."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def resolve_repo_root(repo_root: str | Path | None = None) -> Path:
    """Resolve the repository root for benchmark artifacts."""

    if repo_root is not None:
        return Path(repo_root).resolve()
    return Path(__file__).resolve().parents[3]


@dataclass(frozen=True, slots=True)
class BenchmarkPaths:
    """Container for the benchmark directory contract."""

    repo_root: Path

    @property
    def dataset_registry_path(self) -> Path:
        return self.curated_root / "dataset_registry.csv"

    @property
    def raw_root(self) -> Path:
        return self.repo_root / "data" / "raw" / "benchmark"

    @property
    def processed_root(self) -> Path:
        return self.repo_root / "data" / "processed" / "benchmark"

    @property
    def curated_root(self) -> Path:
        return self.repo_root / "data" / "curated" / "benchmark"

    @property
    def schema_root(self) -> Path:
        return self.curated_root / "schema"

    @property
    def manifests_root(self) -> Path:
        return self.processed_root / "manifests"

    @property
    def reports_root(self) -> Path:
        return self.processed_root / "reports"

    @property
    def harmonized_root(self) -> Path:
        return self.processed_root / "harmonized"

    @property
    def examples_root(self) -> Path:
        return self.repo_root / "examples" / "benchmark_v0"

    @property
    def config_path(self) -> Path:
        return self.repo_root / "config" / "benchmark_v0.toml"

    def output_roots(self) -> dict[str, Path]:
        """Return the canonical benchmark roots keyed by logical name."""

        return {
            "dataset_registry": self.dataset_registry_path,
            "raw": self.raw_root,
            "processed": self.processed_root,
            "curated": self.curated_root,
            "schema": self.schema_root,
            "manifests": self.manifests_root,
            "reports": self.reports_root,
            "harmonized": self.harmonized_root,
            "examples": self.examples_root,
        }

    def default_manifest_path(self, filename: str = "run_manifest.json") -> Path:
        """Return the default destination for a benchmark run manifest."""

        return self.manifests_root / filename

    def default_report_path(self, filename: str = "dataset_audit.json") -> Path:
        """Return the default destination for a benchmark report artifact."""

        return self.reports_root / filename

    def default_harmonized_path(self, filename: str = "subjects.csv") -> Path:
        """Return the default destination for a harmonized benchmark artifact."""

        return self.harmonized_root / filename


def benchmark_paths(repo_root: str | Path | None = None) -> BenchmarkPaths:
    """Build a benchmark path contract rooted at the repository."""

    return BenchmarkPaths(repo_root=resolve_repo_root(repo_root))


__all__ = ["BenchmarkPaths", "benchmark_paths", "resolve_repo_root"]

"""Repo-relative path helpers for the strict-open namespace."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def resolve_repo_root(repo_root: str | Path | None = None) -> Path:
    """Resolve the repository root for strict-open artifacts."""

    if repo_root is not None:
        return Path(repo_root).resolve()
    return Path(__file__).resolve().parents[3]


@dataclass(frozen=True, slots=True)
class StrictOpenPaths:
    """Container for the strict-open directory contract."""

    repo_root: Path

    @property
    def raw_root(self) -> Path:
        return self.repo_root / "data" / "raw" / "strict_open"

    @property
    def processed_root(self) -> Path:
        return self.repo_root / "data" / "processed" / "strict_open"

    @property
    def curated_root(self) -> Path:
        return self.repo_root / "data" / "curated" / "strict_open"

    @property
    def manifests_root(self) -> Path:
        return self.processed_root / "manifests"

    @property
    def profiles_root(self) -> Path:
        return self.processed_root / "profiles"

    @property
    def harmonized_root(self) -> Path:
        return self.processed_root / "harmonized"

    @property
    def splits_root(self) -> Path:
        return self.processed_root / "splits"

    @property
    def features_root(self) -> Path:
        return self.processed_root / "features"

    @property
    def targets_root(self) -> Path:
        return self.processed_root / "targets"

    @property
    def models_root(self) -> Path:
        return self.processed_root / "models"

    @property
    def reports_root(self) -> Path:
        return self.processed_root / "reports"

    @property
    def examples_root(self) -> Path:
        return self.repo_root / "examples" / "strict_open_v0"

    @property
    def config_path(self) -> Path:
        return self.repo_root / "config" / "strict_open_v0.toml"

    def output_roots(self) -> dict[str, Path]:
        """Return the canonical strict-open roots keyed by logical name."""

        return {
            "raw": self.raw_root,
            "processed": self.processed_root,
            "curated": self.curated_root,
            "manifests": self.manifests_root,
            "profiles": self.profiles_root,
            "harmonized": self.harmonized_root,
            "splits": self.splits_root,
            "features": self.features_root,
            "targets": self.targets_root,
            "models": self.models_root,
            "reports": self.reports_root,
            "examples": self.examples_root,
        }

    def default_manifest_path(self, filename: str = "run_manifest.json") -> Path:
        """Return the default destination for a strict-open run manifest."""

        return self.manifests_root / filename

    def source_raw_root(self, source_name: str) -> Path:
        """Return the raw directory for a specific source."""

        return self.raw_root / source_name

    def default_profile_path(self, filename: str = "audit_profile.json") -> Path:
        """Return the default destination for a strict-open audit profile."""

        return self.profiles_root / filename

    def default_harmonized_path(self, filename: str = "subjects.csv") -> Path:
        """Return the default destination for a harmonized strict-open artifact."""

        return self.harmonized_root / filename

    def default_split_path(self, filename: str = "split_assignments.csv") -> Path:
        """Return the default destination for a split artifact."""

        return self.splits_root / filename

    def default_feature_path(self, filename: str = "visit_features.csv") -> Path:
        """Return the default destination for a feature artifact."""

        return self.features_root / filename

    def default_target_path(self, filename: str = "derived_targets.csv") -> Path:
        """Return the default destination for a target artifact."""

        return self.targets_root / filename

    def default_model_path(self, filename: str = "baselines/baseline_predictions.csv") -> Path:
        """Return the default destination for a model artifact."""

        return self.models_root / filename

    def default_report_path(self, filename: str = "baseline_summary.json") -> Path:
        """Return the default destination for a report artifact."""

        return self.reports_root / filename


def strict_open_paths(repo_root: str | Path | None = None) -> StrictOpenPaths:
    """Build a strict-open path contract rooted at the repository."""

    return StrictOpenPaths(repo_root=resolve_repo_root(repo_root))


__all__ = ["StrictOpenPaths", "resolve_repo_root", "strict_open_paths"]

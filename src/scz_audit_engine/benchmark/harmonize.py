"""Canonical benchmark harmonization plus deterministic split-contract generation."""

from __future__ import annotations

import csv
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .provenance import write_json_artifact
from .run_manifest import build_run_manifest, utc_now_iso, write_run_manifest
from .schema import CANONICAL_TABLE_NAMES, benchmark_schema
from .sources import build_default_source_adapters
from .splits import write_benchmark_split_artifacts

HARMONIZATION_MANIFEST_NAME = "harmonization_manifest.json"
RUN_MANIFEST_NAME = "benchmark_harmonize_run_manifest.json"
SPLIT_MANIFEST_NAME = "benchmark_split_manifest.json"


@dataclass(frozen=True, slots=True)
class BenchmarkHarmonizationArtifacts:
    """Paths and row counts emitted by the benchmark harmonizer."""

    harmonized_root: Path
    manifests_root: Path
    table_paths: dict[str, Path]
    harmonization_manifest_path: Path
    split_manifest_path: Path
    run_manifest_path: Path
    cohort_ids: tuple[str, ...]
    row_counts: dict[str, int]

    def to_summary(self) -> dict[str, object]:
        return {
            "cohorts_harmonized": list(self.cohort_ids),
            "harmonized_dir": str(self.harmonized_root),
            "manifests_dir": str(self.manifests_root),
            "harmonization_manifest": str(self.harmonization_manifest_path),
            "split_manifest": str(self.split_manifest_path),
            "run_manifest": str(self.run_manifest_path),
            "row_counts": dict(self.row_counts),
            **{table_name: str(path) for table_name, path in self.table_paths.items()},
        }


def run_benchmark_harmonization(
    *,
    raw_root: str | Path,
    harmonized_root: str | Path,
    manifests_root: str | Path,
    repo_root: str | Path | None,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
    adapters: tuple[object, ...] | None = None,
) -> BenchmarkHarmonizationArtifacts:
    """Harmonize staged benchmark cohort roots and freeze deterministic splits."""

    raw_path = Path(raw_root).resolve()
    harmonized_path = Path(harmonized_root).resolve()
    manifests_path = Path(manifests_root).resolve()
    schema = benchmark_schema()
    generated_at = utc_now_iso()

    cohort_bundles: list[Any] = []
    missing_expected_cohorts: list[str] = []
    explicit_snapshot_root_failures: list[str] = []
    for adapter in adapters or build_default_source_adapters():
        if not getattr(adapter, "supports_harmonization", True):
            continue
        cohort_root = _resolve_adapter_root(raw_path, adapter)
        if cohort_root is None:
            snapshot_root = getattr(adapter, "snapshot_root", None)
            if snapshot_root is not None:
                explicit_snapshot_root_failures.append(
                    _format_explicit_snapshot_root_failure(adapter, Path(snapshot_root))
                )
            missing_expected_cohorts.append(getattr(adapter, "source_identifier", "unknown"))
            continue
        cohort_bundles.append(adapter.harmonize(cohort_root))

    if explicit_snapshot_root_failures:
        raise FileNotFoundError("Invalid explicit benchmark snapshot roots: " + "; ".join(explicit_snapshot_root_failures))

    if not cohort_bundles:
        expected = ", ".join(sorted(missing_expected_cohorts)) or "known benchmark cohorts"
        raise FileNotFoundError(
            f"No staged benchmark cohort roots were discovered under {raw_path}. Expected one of: {expected}."
        )

    table_rows: dict[str, list[dict[str, str]]] = {table_name: [] for table_name in CANONICAL_TABLE_NAMES}
    row_counts_by_table_and_cohort: dict[str, dict[str, int]] = {
        table_name: {} for table_name in CANONICAL_TABLE_NAMES
    }
    input_cohort_roots = {}
    cohort_caveats = {}
    unsupported_fields_summary = {}
    cohort_support = {}

    for bundle in cohort_bundles:
        input_cohort_roots[bundle.cohort_id] = str(bundle.input_root)
        cohort_caveats[bundle.cohort_id] = list(bundle.caveats)
        unsupported_fields_summary[bundle.cohort_id] = {
            table_name: list(messages)
            for table_name, messages in sorted(bundle.unsupported_fields.items())
        }
        cohort_support[bundle.cohort_id] = {
            "benchmark_v0_eligibility": bundle.audit_entry.benchmark_v0_eligibility,
            "representation_comparison_support": bundle.audit_entry.representation_comparison_support,
            "outcome_temporal_validity": bundle.audit_entry.outcome_temporal_validity,
            "concurrent_endpoint_only": bundle.audit_entry.concurrent_endpoint_only,
        }
        for table_name in CANONICAL_TABLE_NAMES:
            rows = list(bundle.tables.get(table_name, ()))
            table_rows[table_name].extend(rows)
            row_counts_by_table_and_cohort[table_name][bundle.cohort_id] = len(rows)

    split_artifacts = write_benchmark_split_artifacts(
        subjects=table_rows["subjects"],
        visits=table_rows["visits"],
        diagnoses=table_rows["diagnoses"],
        outcomes=table_rows["outcomes"],
        assignments_path=harmonized_path / "split_assignments.csv",
        manifest_path=manifests_path / SPLIT_MANIFEST_NAME,
        command=command,
        git_sha=git_sha,
        seed=seed,
    )
    table_rows["split_assignments"] = list(split_artifacts.rows)
    split_counts_by_cohort = Counter(row["cohort_id"] for row in split_artifacts.rows)
    for bundle in cohort_bundles:
        row_counts_by_table_and_cohort["split_assignments"][bundle.cohort_id] = split_counts_by_cohort.get(
            bundle.cohort_id,
            0,
        )

    table_paths: dict[str, Path] = {}
    row_counts = {table_name: len(table_rows[table_name]) for table_name in CANONICAL_TABLE_NAMES}
    for table_contract in schema.tables:
        destination = harmonized_path / f"{table_contract.name}.csv"
        if table_contract.name == "split_assignments":
            table_paths[table_contract.name] = split_artifacts.assignments_path
            continue
        table_paths[table_contract.name] = _write_csv_table(
            table_contract.all_columns,
            table_rows[table_contract.name],
            destination,
        )

    stable_output_paths = {
        table_name: _stable_output_reference(path, anchor=harmonized_path)
        for table_name, path in table_paths.items()
    }
    harmonization_manifest_payload = {
        "seed": seed,
        "schema_version": schema.version,
        "input_cohort_roots": {
            cohort_id: _stable_input_root_reference(Path(root), raw_root=raw_path)
            for cohort_id, root in input_cohort_roots.items()
        },
        "cohorts_harmonized": [bundle.cohort_id for bundle in cohort_bundles],
        "missing_expected_cohorts": sorted(missing_expected_cohorts),
        "output_paths": {
            **stable_output_paths,
            "harmonization_manifest": HARMONIZATION_MANIFEST_NAME,
            "split_manifest": _stable_output_reference(split_artifacts.manifest_path, anchor=manifests_path),
        },
        "row_counts_by_table": row_counts,
        "row_counts_by_table_and_cohort": row_counts_by_table_and_cohort,
        "unsupported_fields_summary": unsupported_fields_summary,
        "cohort_caveats": cohort_caveats,
        "cohort_support": cohort_support,
        "claim_boundary_statement": (
            "Benchmark harmonization operationalizes the current narrow-go line only. It does not upgrade the "
            "repo to full external validation or prospective benchmarking."
        ),
        "current_limitations": [
            "Outcome rows remain concurrent-only where the public cohorts are concurrent-only.",
            "tcp-ds005237 stays explicitly limited because public labels remain broad Patient versus GenPop.",
            "Representation builders and benchmark models remain intentionally deferred.",
        ],
    }
    harmonization_manifest_path = write_json_artifact(
        harmonization_manifest_payload,
        harmonized_path / HARMONIZATION_MANIFEST_NAME,
    )

    run_manifest_path = write_run_manifest(
        build_run_manifest(
            dataset_source="benchmark",
            command=command,
            git_sha=git_sha,
            seed=seed,
            repo_root=repo_root,
            output_paths={
                **table_paths,
                "harmonization_manifest": harmonization_manifest_path,
                "split_manifest": split_artifacts.manifest_path,
            },
            timestamp=generated_at,
        ),
        manifests_path / RUN_MANIFEST_NAME,
    )

    return BenchmarkHarmonizationArtifacts(
        harmonized_root=harmonized_path,
        manifests_root=manifests_path,
        table_paths=table_paths,
        harmonization_manifest_path=harmonization_manifest_path,
        split_manifest_path=split_artifacts.manifest_path,
        run_manifest_path=run_manifest_path,
        cohort_ids=tuple(bundle.cohort_id for bundle in cohort_bundles),
        row_counts=row_counts,
    )


def _write_csv_table(
    columns: tuple[str, ...],
    rows: list[dict[str, str]],
    destination: Path,
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(columns),
            lineterminator="\n",
        )
        writer.writeheader()
        for row in _sort_rows(rows, columns):
            writer.writerow({column: row.get(column, "") for column in columns})
    return destination


def _sort_rows(rows: list[dict[str, str]], columns: tuple[str, ...]) -> list[dict[str, str]]:
    return sorted(rows, key=lambda row: tuple(row.get(column, "") for column in columns))


def _stable_output_reference(path: Path, *, anchor: Path) -> str:
    try:
        return str(path.resolve().relative_to(anchor.resolve()))
    except ValueError:
        return path.name


def _stable_input_root_reference(path: Path, *, raw_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(raw_root.resolve()))
    except ValueError:
        return path.name


def _resolve_adapter_root(raw_root: Path, adapter: object) -> Path | None:
    snapshot_root = getattr(adapter, "snapshot_root", None)
    if snapshot_root is not None:
        candidate = Path(snapshot_root).resolve()
        return candidate if _matches_adapter(candidate, adapter) else None
    return _discover_cohort_root(raw_root, adapter)


def _discover_cohort_root(raw_root: Path, adapter: object) -> Path | None:
    if _matches_adapter(raw_root, adapter):
        return raw_root

    for candidate_name in getattr(adapter, "candidate_root_names", ()):
        candidate = raw_root / candidate_name
        if _matches_adapter(candidate, adapter):
            return candidate
    return None


def _matches_adapter(path: Path, adapter: object) -> bool:
    if not path.exists() or not path.is_dir():
        return False
    if not (path / "participants.tsv").exists():
        return False
    metadata_path = path / "dataset_metadata.json"
    if not metadata_path.exists():
        return False
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False
    dataset_id = _extract_dataset_id(payload)
    return dataset_id == getattr(adapter, "dataset_accession", None)


def _extract_dataset_id(payload: dict[str, Any]) -> str:
    dataset = payload.get("dataset")
    if not isinstance(dataset, dict):
        data = payload.get("data")
        dataset = data.get("dataset") if isinstance(data, dict) else None
    if not isinstance(dataset, dict):
        return ""
    return str(dataset.get("id", "")).strip()


def _format_explicit_snapshot_root_failure(adapter: object, snapshot_root: Path) -> str:
    source_identifier = getattr(adapter, "source_identifier", "unknown")
    expected_dataset = getattr(adapter, "dataset_accession", "unknown")
    return (
        f"{source_identifier} at {snapshot_root.resolve()} "
        f"(expected a staged root matching dataset accession {expected_dataset})"
    )


__all__ = [
    "BenchmarkHarmonizationArtifacts",
    "HARMONIZATION_MANIFEST_NAME",
    "RUN_MANIFEST_NAME",
    "SPLIT_MANIFEST_NAME",
    "run_benchmark_harmonization",
]

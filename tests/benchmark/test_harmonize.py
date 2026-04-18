import csv
import json
import shutil
from pathlib import Path

import pytest

from scz_audit_engine.benchmark.harmonize import run_benchmark_harmonization
from scz_audit_engine.benchmark.schema import CANONICAL_BENCHMARK_SCHEMA, CANONICAL_TABLE_NAMES
from scz_audit_engine.benchmark.sources import (
    FEPDS003944BenchmarkSourceAdapter,
    TCPDS005237BenchmarkSourceAdapter,
)


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "benchmark_sources"


def test_benchmark_harmonize_emits_all_canonical_tables_and_manifest(tmp_path) -> None:
    harmonized_root = tmp_path / "data" / "processed" / "benchmark" / "harmonized"
    manifests_root = tmp_path / "data" / "processed" / "benchmark" / "manifests"

    results = run_benchmark_harmonization(
        raw_root=FIXTURE_ROOT,
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize", "--raw-root", str(FIXTURE_ROOT)],
        git_sha="abc1234",
        seed=1729,
    )

    assert results.cohort_ids == ("tcp-ds005237", "fep-ds003944")
    assert Path(results.harmonization_manifest_path).exists()
    assert Path(results.split_manifest_path).exists()
    assert Path(results.run_manifest_path).exists()

    for table_name in CANONICAL_TABLE_NAMES:
        assert Path(results.table_paths[table_name]).exists()
        assert _csv_header(Path(results.table_paths[table_name])) == list(
            CANONICAL_BENCHMARK_SCHEMA.table(table_name).all_columns
        )

    manifest = json.loads(Path(results.harmonization_manifest_path).read_text(encoding="utf-8"))
    assert manifest["row_counts_by_table"] == {
        "subjects": 7,
        "visits": 7,
        "diagnoses": 7,
        "symptom_scores": 10,
        "cognition_scores": 6,
        "functioning_scores": 8,
        "treatment_exposures": 2,
        "outcomes": 8,
        "modality_features": 0,
        "split_assignments": 7,
    }
    assert manifest["row_counts_by_table_and_cohort"] == {
        "subjects": {"fep-ds003944": 3, "tcp-ds005237": 4},
        "visits": {"fep-ds003944": 3, "tcp-ds005237": 4},
        "diagnoses": {"fep-ds003944": 3, "tcp-ds005237": 4},
        "symptom_scores": {"fep-ds003944": 6, "tcp-ds005237": 4},
        "cognition_scores": {"fep-ds003944": 6, "tcp-ds005237": 0},
        "functioning_scores": {"fep-ds003944": 4, "tcp-ds005237": 4},
        "treatment_exposures": {"fep-ds003944": 2, "tcp-ds005237": 0},
        "outcomes": {"fep-ds003944": 4, "tcp-ds005237": 4},
        "modality_features": {"fep-ds003944": 0, "tcp-ds005237": 0},
        "split_assignments": {"fep-ds003944": 3, "tcp-ds005237": 4},
    }
    assert "tcp-ds005237 stays explicitly limited" in " ".join(manifest["current_limitations"])
    assert "current narrow-go line only" in manifest["claim_boundary_statement"]


def test_benchmark_harmonize_preserves_column_order_and_conservative_mapping_caveats(tmp_path) -> None:
    harmonized_root = tmp_path / "harmonized"
    manifests_root = tmp_path / "manifests"

    run_benchmark_harmonization(
        raw_root=FIXTURE_ROOT,
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize", "--raw-root", str(FIXTURE_ROOT)],
        git_sha="abc1234",
        seed=1729,
    )

    diagnoses_rows = _read_csv_rows(harmonized_root / "diagnoses.csv")
    outcomes_rows = _read_csv_rows(harmonized_root / "outcomes.csv")
    cognition_rows = _read_csv_rows(harmonized_root / "cognition_scores.csv")
    modality_rows = _read_csv_rows(harmonized_root / "modality_features.csv")
    manifest = json.loads((harmonized_root / "harmonization_manifest.json").read_text(encoding="utf-8"))

    assert any(
        row["cohort_id"] == "tcp-ds005237"
        and row["diagnosis_granularity"] == "public_patient_vs_genpop_only"
        and "Patient versus GenPop" in row["mapping_caveat"]
        for row in diagnoses_rows
    )
    assert all(row["outcome_is_prospective"] == "false" for row in outcomes_rows)
    assert all(row["concurrent_endpoint_only"] == "true" for row in outcomes_rows)
    assert not any(row["cohort_id"] == "tcp-ds005237" for row in cognition_rows)
    assert manifest["row_counts_by_table_and_cohort"]["cognition_scores"]["tcp-ds005237"] == 0
    assert modality_rows == []
    assert manifest["row_counts_by_table_and_cohort"]["modality_features"] == {
        "fep-ds003944": 0,
        "tcp-ds005237": 0,
    }
    assert (
        manifest["unsupported_fields_summary"]["tcp-ds005237"]["cognition_scores"][0]
        == "No local cognition tables are staged in the current public TCP root; cognition support remains unstated in harmonized rows."
    )
    assert (
        manifest["unsupported_fields_summary"]["fep-ds003944"]["modality_features"][0]
        == "No staged subject-level EEG files were present in the current root."
    )
    assert (
        manifest["unsupported_fields_summary"]["tcp-ds005237"]["modality_features"][0]
        == "No staged subject-level MRI or fMRI files were present in the current root."
    )
    assert any(
        row["cohort_id"] == "fep-ds003944"
        and "concurrent poor functional outcome benchmark only" in row["mapping_caveat"]
        for row in outcomes_rows
    )


def test_benchmark_harmonize_honors_adapter_snapshot_roots(tmp_path) -> None:
    harmonized_root = tmp_path / "harmonized"
    manifests_root = tmp_path / "manifests"
    adapters = (
        TCPDS005237BenchmarkSourceAdapter(snapshot_root=FIXTURE_ROOT / "tcp_ds005237"),
        FEPDS003944BenchmarkSourceAdapter(snapshot_root=FIXTURE_ROOT / "fep_ds003944"),
    )

    results = run_benchmark_harmonization(
        raw_root=tmp_path / "empty-raw-root",
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize"],
        git_sha="abc1234",
        seed=1729,
        adapters=adapters,
    )

    assert results.cohort_ids == ("tcp-ds005237", "fep-ds003944")
    assert results.row_counts["subjects"] == 7


def test_benchmark_harmonize_does_not_fallback_from_explicit_snapshot_roots(tmp_path) -> None:
    adapters = (
        TCPDS005237BenchmarkSourceAdapter(snapshot_root=tmp_path / "missing-tcp"),
        FEPDS003944BenchmarkSourceAdapter(snapshot_root=tmp_path / "missing-fep"),
    )

    with pytest.raises(FileNotFoundError, match="Invalid explicit benchmark snapshot roots"):
        run_benchmark_harmonization(
            raw_root=FIXTURE_ROOT,
            harmonized_root=tmp_path / "harmonized",
            manifests_root=tmp_path / "manifests",
            repo_root=Path(__file__).resolve().parents[2],
            command=["scz-audit", "benchmark", "harmonize"],
            git_sha="abc1234",
            seed=1729,
            adapters=adapters,
        )


def test_benchmark_harmonize_fails_if_any_explicit_snapshot_root_is_invalid(tmp_path) -> None:
    adapters = (
        TCPDS005237BenchmarkSourceAdapter(snapshot_root=FIXTURE_ROOT / "tcp_ds005237"),
        FEPDS003944BenchmarkSourceAdapter(snapshot_root=tmp_path / "missing-fep"),
    )

    with pytest.raises(FileNotFoundError, match="Invalid explicit benchmark snapshot roots"):
        run_benchmark_harmonization(
            raw_root=FIXTURE_ROOT,
            harmonized_root=tmp_path / "harmonized",
            manifests_root=tmp_path / "manifests",
            repo_root=Path(__file__).resolve().parents[2],
            command=["scz-audit", "benchmark", "harmonize"],
            git_sha="abc1234",
            seed=1729,
            adapters=adapters,
        )


def test_benchmark_harmonize_discovers_wrapped_dataset_metadata_payloads(tmp_path) -> None:
    raw_root = tmp_path / "raw"
    shutil.copytree(FIXTURE_ROOT / "fep_ds003944", raw_root / "fep-ds003944")
    shutil.copytree(FIXTURE_ROOT / "tcp_ds005237", raw_root / "tcp-ds005237")

    fep_metadata_path = raw_root / "fep-ds003944" / "dataset_metadata.json"
    fep_metadata_payload = json.loads(fep_metadata_path.read_text(encoding="utf-8"))
    fep_metadata_path.write_text(
        json.dumps({"data": {"dataset": fep_metadata_payload["dataset"]}}, indent=2),
        encoding="utf-8",
    )

    results = run_benchmark_harmonization(
        raw_root=raw_root,
        harmonized_root=tmp_path / "harmonized",
        manifests_root=tmp_path / "manifests",
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize"],
        git_sha="abc1234",
        seed=1729,
    )

    assert results.cohort_ids == ("tcp-ds005237", "fep-ds003944")
    manifest = json.loads((tmp_path / "harmonized" / "harmonization_manifest.json").read_text(encoding="utf-8"))
    assert manifest["input_cohort_roots"]["fep-ds003944"].endswith("/fep-ds003944")


def _csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]

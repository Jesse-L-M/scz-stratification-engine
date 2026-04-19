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

    assert results.cohort_ids == (
        "tcp-ds005237",
        "fep-ds003944",
        "ucla-cnp-ds000030",
        "ds000115",
    )
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
        "subjects": 15,
        "visits": 15,
        "diagnoses": 15,
        "symptom_scores": 29,
        "cognition_scores": 70,
        "functioning_scores": 8,
        "treatment_exposures": 9,
        "outcomes": 8,
        "modality_features": 14,
        "split_assignments": 15,
    }
    assert manifest["row_counts_by_table_and_cohort"] == {
        "subjects": {
            "ds000115": 4,
            "fep-ds003944": 3,
            "tcp-ds005237": 4,
            "ucla-cnp-ds000030": 4,
        },
        "visits": {
            "ds000115": 4,
            "fep-ds003944": 3,
            "tcp-ds005237": 4,
            "ucla-cnp-ds000030": 4,
        },
        "diagnoses": {
            "ds000115": 4,
            "fep-ds003944": 3,
            "tcp-ds005237": 4,
            "ucla-cnp-ds000030": 4,
        },
        "symptom_scores": {
            "ds000115": 12,
            "fep-ds003944": 6,
            "tcp-ds005237": 4,
            "ucla-cnp-ds000030": 7,
        },
        "cognition_scores": {
            "ds000115": 36,
            "fep-ds003944": 6,
            "tcp-ds005237": 0,
            "ucla-cnp-ds000030": 28,
        },
        "functioning_scores": {
            "ds000115": 0,
            "fep-ds003944": 4,
            "tcp-ds005237": 4,
            "ucla-cnp-ds000030": 0,
        },
        "treatment_exposures": {
            "ds000115": 0,
            "fep-ds003944": 2,
            "tcp-ds005237": 0,
            "ucla-cnp-ds000030": 7,
        },
        "outcomes": {
            "ds000115": 0,
            "fep-ds003944": 4,
            "tcp-ds005237": 4,
            "ucla-cnp-ds000030": 0,
        },
        "modality_features": {
            "ds000115": 0,
            "fep-ds003944": 0,
            "tcp-ds005237": 0,
            "ucla-cnp-ds000030": 14,
        },
        "split_assignments": {
            "ds000115": 4,
            "fep-ds003944": 3,
            "tcp-ds005237": 4,
            "ucla-cnp-ds000030": 4,
        },
    }
    assert "generated_at" not in manifest
    assert "git_sha" not in manifest
    assert "command" not in manifest
    assert manifest["output_paths"]["subjects"] == "subjects.csv"
    assert manifest["output_paths"]["split_manifest"] == "benchmark_split_manifest.json"
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
    assert not any(row["cohort_id"] in {"ucla-cnp-ds000030", "ds000115"} for row in outcomes_rows)
    assert any(
        row["cohort_id"] == "ucla-cnp-ds000030"
        and row["feature_name"] == "rest_available"
        and row["modality_type"] == "fMRI"
        for row in modality_rows
    )
    assert manifest["row_counts_by_table_and_cohort"]["modality_features"] == {
        "ds000115": 0,
        "fep-ds003944": 0,
        "tcp-ds005237": 0,
        "ucla-cnp-ds000030": 14,
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
    assert (
        manifest["unsupported_fields_summary"]["ucla-cnp-ds000030"]["outcomes"][0]
        == "ucla-cnp-ds000030 remains a cross-sectional representation cohort only; no benchmarkable outcome rows are emitted."
    )
    assert (
        manifest["unsupported_fields_summary"]["ds000115"]["outcomes"][0]
        == "ds000115 remains a low-weight cross-sectional representation cohort only; no benchmarkable outcome rows are emitted."
    )
    assert any(
        row["cohort_id"] == "fep-ds003944"
        and "concurrent poor functional outcome benchmark only" in row["mapping_caveat"]
        for row in outcomes_rows
    )


def test_benchmark_harmonize_writes_lf_only_csv_artifacts(tmp_path) -> None:
    harmonized_root = tmp_path / "harmonized"
    manifests_root = tmp_path / "manifests"

    results = run_benchmark_harmonization(
        raw_root=FIXTURE_ROOT,
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize", "--raw-root", str(FIXTURE_ROOT)],
        git_sha="abc1234",
        seed=1729,
    )

    for table_path in results.table_paths.values():
        assert b"\r\n" not in Path(table_path).read_bytes()


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
    assert manifest["input_cohort_roots"]["fep-ds003944"] == "fep-ds003944"


def test_benchmark_harmonization_manifest_is_location_independent_and_deterministic(tmp_path) -> None:
    first_raw_root = tmp_path / "first-raw"
    second_raw_root = tmp_path / "second-raw"
    shutil.copytree(FIXTURE_ROOT / "fep_ds003944", first_raw_root / "fep-ds003944")
    shutil.copytree(FIXTURE_ROOT / "tcp_ds005237", first_raw_root / "tcp-ds005237")
    shutil.copytree(FIXTURE_ROOT / "fep_ds003944", second_raw_root / "fep-ds003944")
    shutil.copytree(FIXTURE_ROOT / "tcp_ds005237", second_raw_root / "tcp-ds005237")

    first = run_benchmark_harmonization(
        raw_root=first_raw_root,
        harmonized_root=tmp_path / "first-harmonized",
        manifests_root=tmp_path / "first-manifests",
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize", "--raw-root", str(first_raw_root)],
        git_sha="abc1234",
        seed=1729,
    )
    second = run_benchmark_harmonization(
        raw_root=second_raw_root,
        harmonized_root=tmp_path / "second-harmonized",
        manifests_root=tmp_path / "second-manifests",
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize", "--raw-root", str(second_raw_root)],
        git_sha="deadbeef",
        seed=1729,
    )

    first_manifest_text = first.harmonization_manifest_path.read_text(encoding="utf-8")
    second_manifest_text = second.harmonization_manifest_path.read_text(encoding="utf-8")

    assert first_manifest_text == second_manifest_text
    assert str(tmp_path) not in first_manifest_text

    first_split_manifest = json.loads(first.split_manifest_path.read_text(encoding="utf-8"))
    second_split_manifest = json.loads(second.split_manifest_path.read_text(encoding="utf-8"))
    assert first_split_manifest == second_split_manifest
    assert "git_sha" not in first_split_manifest
    assert "command" not in first_split_manifest


def _csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]

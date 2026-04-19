import csv
import json
import shutil
from pathlib import Path

from scz_audit_engine.benchmark.harmonize import run_benchmark_harmonization
from scz_audit_engine.benchmark.representations import (
    CLINICAL_SNAPSHOT_COLUMNS,
    COGNITION_PROFILE_COLUMNS,
    DIAGNOSIS_ANCHOR_COLUMNS,
    SYMPTOM_PROFILE_COLUMNS,
    run_benchmark_representation_build,
)

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "benchmark_sources"


def test_build_representations_emits_family_artifacts_and_manifest(tmp_path) -> None:
    harmonized_root, manifests_root = _prepare_harmonized_fixture(tmp_path)
    representations_root = tmp_path / "representations"

    results = run_benchmark_representation_build(
        harmonized_root=harmonized_root,
        representations_root=representations_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "build-representations"],
        git_sha="abc1234",
        seed=1729,
    )

    assert Path(results.representation_manifest_path).exists()
    assert Path(results.run_manifest_path).exists()
    assert results.family_row_counts == {
        "diagnosis_anchor": 15,
        "symptom_profile": 15,
        "cognition_profile": 15,
        "clinical_snapshot": 15,
    }
    assert _csv_header(results.family_paths["diagnosis_anchor"]) == list(DIAGNOSIS_ANCHOR_COLUMNS)
    assert _csv_header(results.family_paths["symptom_profile"]) == list(SYMPTOM_PROFILE_COLUMNS)
    assert _csv_header(results.family_paths["cognition_profile"]) == list(COGNITION_PROFILE_COLUMNS)
    assert _csv_header(results.family_paths["clinical_snapshot"]) == list(CLINICAL_SNAPSHOT_COLUMNS)

    manifest = json.loads(results.representation_manifest_path.read_text(encoding="utf-8"))
    assert manifest["row_counts_by_family"] == results.family_row_counts
    assert manifest["output_paths"]["diagnosis_anchor"] == "diagnosis_anchor.csv"
    assert manifest["output_paths"]["representation_manifest"] == "representation_manifest.json"
    assert "generated_at" not in manifest
    assert "git_sha" not in manifest
    assert "command" not in manifest

    run_manifest = json.loads(results.run_manifest_path.read_text(encoding="utf-8"))
    assert run_manifest["command"] == ["scz-audit", "benchmark", "build-representations"]


def test_build_representations_preserves_cross_sectional_limits_and_available_support(tmp_path) -> None:
    harmonized_root, manifests_root = _prepare_harmonized_fixture(tmp_path)
    representations_root = tmp_path / "representations"

    results = run_benchmark_representation_build(
        harmonized_root=harmonized_root,
        representations_root=representations_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "build-representations"],
        git_sha="abc1234",
        seed=1729,
    )

    diagnosis_rows = _read_csv_rows(results.family_paths["diagnosis_anchor"])
    symptom_rows = _read_csv_rows(results.family_paths["symptom_profile"])
    cognition_rows = _read_csv_rows(results.family_paths["cognition_profile"])
    clinical_rows = _read_csv_rows(results.family_paths["clinical_snapshot"])
    manifest = json.loads(results.representation_manifest_path.read_text(encoding="utf-8"))

    assert all(
        row["outcome_row_available"] == "false"
        for row in diagnosis_rows
        if row["cohort_id"] in {"ucla-cnp-ds000030", "ds000115"}
    )
    assert any(
        row["subject_id"] == "ucla-cnp-ds000030:sub-50005"
        and row["diagnosis_schizophrenia_flag"] == "1"
        and row["diagnosis_is_case"] == "1"
        for row in diagnosis_rows
    )
    assert any(
        row["subject_id"] == "ucla-cnp-ds000030:sub-60005"
        and row["diagnosis_bipolar_flag"] == "1"
        and row["diagnosis_is_case"] == "1"
        for row in diagnosis_rows
    )
    assert any(
        row["subject_id"] == "ucla-cnp-ds000030:sub-10159"
        and row["available_feature_count"] == "0"
        and row["symptom_burden_mean_z"] == ""
        for row in symptom_rows
    )
    assert any(
        row["subject_id"] == "tcp-ds005237:sub-NDARINVTEST001"
        and row["available_feature_count"] == "0"
        and row["cognition_performance_mean_z"] == ""
        for row in cognition_rows
    )
    assert any(
        row["subject_id"] == "ucla-cnp-ds000030:sub-50005"
        and row["treatment_exposure_count"] == "6"
        and row["modality_availability_count"] == "4"
        and row["outcome_row_count"] == "0"
        for row in clinical_rows
    )
    assert any(
        row["subject_id"] == "fep-ds003944:sub-1824"
        and row["outcome_row_available"] == "true"
        and row["functioning_status_mean_z"] != ""
        for row in clinical_rows
    )
    assert manifest["cross_sectional_only_cohorts"] == ["ds000115", "ucla-cnp-ds000030"]
    assert manifest["outcome_bearing_cohorts"] == ["fep-ds003944", "tcp-ds005237"]


def test_build_representations_are_deterministic_and_location_independent(tmp_path) -> None:
    first_raw = tmp_path / "first-raw"
    second_raw = tmp_path / "second-raw"
    shutil.copytree(FIXTURE_ROOT, first_raw)
    shutil.copytree(FIXTURE_ROOT, second_raw)

    first_harmonized_root = tmp_path / "first-harmonized"
    first_manifests_root = tmp_path / "first-manifests"
    second_harmonized_root = tmp_path / "second-harmonized"
    second_manifests_root = tmp_path / "second-manifests"

    run_benchmark_harmonization(
        raw_root=first_raw,
        harmonized_root=first_harmonized_root,
        manifests_root=first_manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize", "--raw-root", str(first_raw)],
        git_sha="abc1234",
        seed=1729,
    )
    run_benchmark_harmonization(
        raw_root=second_raw,
        harmonized_root=second_harmonized_root,
        manifests_root=second_manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize", "--raw-root", str(second_raw)],
        git_sha="deadbeef",
        seed=1729,
    )

    first = run_benchmark_representation_build(
        harmonized_root=first_harmonized_root,
        representations_root=tmp_path / "first-representations",
        manifests_root=first_manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "build-representations"],
        git_sha="abc1234",
        seed=1729,
    )
    second = run_benchmark_representation_build(
        harmonized_root=second_harmonized_root,
        representations_root=tmp_path / "second-representations",
        manifests_root=second_manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "build-representations"],
        git_sha="deadbeef",
        seed=1729,
    )

    assert first.representation_manifest_path.read_text(encoding="utf-8") == second.representation_manifest_path.read_text(
        encoding="utf-8"
    )
    assert first.family_paths["diagnosis_anchor"].read_text(encoding="utf-8") == second.family_paths[
        "diagnosis_anchor"
    ].read_text(encoding="utf-8")
    assert str(tmp_path) not in first.representation_manifest_path.read_text(encoding="utf-8")


def test_build_representations_write_lf_only_csv_artifacts(tmp_path) -> None:
    harmonized_root, manifests_root = _prepare_harmonized_fixture(tmp_path)
    representations_root = tmp_path / "representations"

    results = run_benchmark_representation_build(
        harmonized_root=harmonized_root,
        representations_root=representations_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "build-representations"],
        git_sha="abc1234",
        seed=1729,
    )

    for artifact_path in results.family_paths.values():
        assert b"\r\n" not in artifact_path.read_bytes()


def _prepare_harmonized_fixture(tmp_path: Path) -> tuple[Path, Path]:
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
    return harmonized_root, manifests_root


def _csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]

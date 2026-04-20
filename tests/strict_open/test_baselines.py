import csv
import json
from pathlib import Path

import pytest

from scz_audit_engine.strict_open.baseline_eval import run_strict_open_baseline_training
from scz_audit_engine.strict_open.baselines import BASELINE_FAMILY_NAMES
from scz_audit_engine.strict_open.features import run_strict_open_feature_build
from scz_audit_engine.strict_open.harmonize import run_tcp_harmonization
from scz_audit_engine.strict_open.provenance import build_source_manifest, write_source_manifest
from scz_audit_engine.strict_open.sources import TCPDS005237SourceAdapter
from scz_audit_engine.strict_open.splits import run_strict_open_split_definition
from scz_audit_engine.strict_open.targets import run_strict_open_target_build


FIXTURE_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "tcp_raw" / "source_input"
POINTER_PAYLOAD = "../.git/annex/objects/example-pointer"


def test_train_baselines_emits_required_families_predictions_and_reports(tmp_path) -> None:
    manifests_root, splits_root, features_root, targets_root = _prepare_target_fixture(tmp_path)
    models_root = tmp_path / "data" / "processed" / "strict_open" / "models" / "baselines"
    reports_root = tmp_path / "data" / "processed" / "strict_open" / "reports"

    results = run_strict_open_baseline_training(
        features_root=features_root,
        targets_root=targets_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        models_root=models_root,
        reports_root=reports_root,
        command=["scz-audit", "strict-open", "train-baselines"],
        git_sha="abc1234",
        seed=1729,
    )

    prediction_rows = _read_csv_rows(Path(results["baseline_predictions"]))
    registry_payload = json.loads(Path(results["baseline_registry"]).read_text(encoding="utf-8"))
    summary_payload = json.loads(Path(results["baseline_summary_json"]).read_text(encoding="utf-8"))

    assert Path(results["baseline_predictions"]).exists()
    assert Path(results["baseline_registry"]).exists()
    assert Path(results["baseline_summary_json"]).exists()
    assert Path(results["baseline_summary_md"]).exists()
    assert Path(results["run_manifest"]).exists()

    assert {row["baseline_name"] for row in registry_payload["baseline_families"]} == set(BASELINE_FAMILY_NAMES)
    assert prediction_rows
    assert {row["split"] for row in prediction_rows} == {"train", "validation", "test"}
    assert {row["split"] for row in summary_payload["metrics_table"]} == {"train", "validation", "test"}
    assert any(row["status"] == "skipped" for row in summary_payload["metrics_table"])
    assert any(
        row["baseline_name"] == "cognition_only_snapshot" and row["target_name"] == "state_noise_score"
        for row in summary_payload["skipped_baseline_targets"]
    )
    assert any(
        row["baseline_name"] == "mri_only_snapshot" and row["target_name"] == "state_noise_score"
        for row in summary_payload["comparison_table"]
    )
    assert summary_payload["confounds"]["coverage_by_diagnosis"]
    assert summary_payload["confounds"]["coverage_by_site"]
    assert summary_payload["confounds"]["missingness_support"]


def test_train_baselines_keep_sparse_public_inputs_honest(tmp_path) -> None:
    manifests_root, splits_root, features_root, targets_root = _prepare_target_fixture(
        tmp_path,
        pointer_relative_paths=(
            "phenotype/cogfq01.tsv",
            "phenotype/panss01.tsv",
            "phenotype/qids01.tsv",
        ),
    )
    models_root = tmp_path / "data" / "processed" / "strict_open" / "models" / "baselines"
    reports_root = tmp_path / "data" / "processed" / "strict_open" / "reports"

    results = run_strict_open_baseline_training(
        features_root=features_root,
        targets_root=targets_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        models_root=models_root,
        reports_root=reports_root,
        command=["scz-audit", "strict-open", "train-baselines"],
        git_sha="abc1234",
        seed=1729,
    )

    prediction_rows = _read_csv_rows(Path(results["baseline_predictions"]))
    summary_payload = json.loads(Path(results["baseline_summary_json"]).read_text(encoding="utf-8"))

    assert {row["target_name"] for row in prediction_rows} == {"state_noise_score"}
    assert {row["baseline_name"] for row in prediction_rows} == {"mri_only_snapshot"}
    assert all(
        row["available_target_rows"] == 0
        for row in summary_payload["coverage_table"]
        if row["target_name"] in {"global_cognition_dev", "stable_cognitive_burden_proxy"}
    )
    assert all(
        row["target_name"] == "state_noise_score"
        for row in summary_payload["comparison_table"]
    )
    assert any(
        "No evaluable global_cognition_dev rows" in row["reason"]
        for row in summary_payload["skipped_baseline_targets"]
    )
    assert any(
        "No evaluable stable_cognitive_burden_proxy rows" in row["reason"]
        for row in summary_payload["skipped_baseline_targets"]
    )


def test_train_baselines_validate_frozen_split_contract(tmp_path) -> None:
    manifests_root, splits_root, features_root, targets_root = _prepare_target_fixture(tmp_path)
    models_root = tmp_path / "data" / "processed" / "strict_open" / "models" / "baselines"
    reports_root = tmp_path / "data" / "processed" / "strict_open" / "reports"
    split_rows = _read_csv_rows(splits_root / "split_assignments.csv")

    for row in split_rows:
        if row["subject_id"] != "tcp-ds005237:sub-TCP002":
            continue
        row["split"] = "test"
        break
    _write_csv_rows(splits_root / "split_assignments.csv", split_rows)

    with pytest.raises(ValueError, match="frozen split contract"):
        run_strict_open_baseline_training(
            features_root=features_root,
            targets_root=targets_root,
            splits_root=splits_root,
            manifests_root=manifests_root,
            models_root=models_root,
            reports_root=reports_root,
            command=["scz-audit", "strict-open", "train-baselines"],
            git_sha="abc1234",
            seed=1729,
        )


def test_train_baselines_skip_mri_rows_without_motion_qc(tmp_path) -> None:
    manifests_root, splits_root, features_root, targets_root = _prepare_target_fixture(tmp_path)
    models_root = tmp_path / "data" / "processed" / "strict_open" / "models" / "baselines"
    reports_root = tmp_path / "data" / "processed" / "strict_open" / "reports"
    feature_rows = _read_csv_rows(features_root / "visit_features.csv")

    for row in feature_rows:
        if row["visit_id"] != "tcp-ds005237:sub-TCP003:visit-00-2026-01-03":
            continue
        row["mri_mean_fd_mean"] = ""
        row["mri_qc_measure_count"] = "0"
        row["mri_qc_missing_indicator"] = "1"
        break
    _write_csv_rows(features_root / "visit_features.csv", feature_rows)

    results = run_strict_open_baseline_training(
        features_root=features_root,
        targets_root=targets_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        models_root=models_root,
        reports_root=reports_root,
        command=["scz-audit", "strict-open", "train-baselines"],
        git_sha="abc1234",
        seed=1729,
    )

    prediction_rows = _read_csv_rows(Path(results["baseline_predictions"]))
    summary_payload = json.loads(Path(results["baseline_summary_json"]).read_text(encoding="utf-8"))

    assert not any(
        row["baseline_name"] == "mri_only_snapshot"
        and row["target_name"] == "state_noise_score"
        and row["visit_id"] == "tcp-ds005237:sub-TCP003:visit-00-2026-01-03"
        for row in prediction_rows
    )
    assert {
        row["reason"]
        for row in summary_payload["support_gap_reasons"]
        if row["baseline_name"] == "mri_only_snapshot"
        and row["target_name"] == "state_noise_score"
        and row["split"] == "test"
    } == {"Current-visit MRI motion QC is unavailable for this row."}


@pytest.mark.parametrize(
    "relative_manifest_path",
    (
        ("feature_manifest.json"),
        ("target_manifest.json"),
        ("split_manifest.json"),
    ),
)
def test_train_baselines_require_upstream_manifests(tmp_path, relative_manifest_path: str) -> None:
    manifests_root, splits_root, features_root, targets_root = _prepare_target_fixture(tmp_path)
    models_root = tmp_path / "data" / "processed" / "strict_open" / "models" / "baselines"
    reports_root = tmp_path / "data" / "processed" / "strict_open" / "reports"

    if relative_manifest_path == "feature_manifest.json":
        manifest_path = features_root / relative_manifest_path
    elif relative_manifest_path == "target_manifest.json":
        manifest_path = targets_root / relative_manifest_path
    else:
        manifest_path = splits_root / relative_manifest_path
    manifest_path.unlink()

    with pytest.raises(FileNotFoundError, match=str(manifest_path)):
        run_strict_open_baseline_training(
            features_root=features_root,
            targets_root=targets_root,
            splits_root=splits_root,
            manifests_root=manifests_root,
            models_root=models_root,
            reports_root=reports_root,
            command=["scz-audit", "strict-open", "train-baselines"],
            git_sha="abc1234",
            seed=1729,
        )


def _prepare_target_fixture(
    tmp_path: Path,
    *,
    pointer_relative_paths: tuple[str, ...] = (),
) -> tuple[Path, Path, Path, Path]:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits"
    features_root = tmp_path / "data" / "processed" / "strict_open" / "features"
    targets_root = tmp_path / "data" / "processed" / "strict_open" / "targets"
    adapter = TCPDS005237SourceAdapter()
    staged = adapter.stage(raw_root, source_root=FIXTURE_SOURCE_ROOT)

    for relative_path in pointer_relative_paths:
        (raw_root / relative_path).write_text(POINTER_PAYLOAD, encoding="utf-8")

    source_manifest = build_source_manifest(
        source=staged.source,
        source_identifier=staged.source_identifier,
        dataset_accession=staged.dataset_accession,
        dataset_version=staged.dataset_version,
        command=["scz-audit", "strict-open", "ingest", "--source", "tcp"],
        git_sha="abc1234",
        raw_root=staged.raw_root,
        files=staged.files,
        ingest_timestamp="2026-04-16T12:00:00Z",
    )
    source_manifest_path = manifests_root / "tcp_source_manifest.json"
    write_source_manifest(source_manifest, source_manifest_path)

    run_tcp_harmonization(
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        command=["scz-audit", "strict-open", "harmonize"],
        git_sha="abc1234",
        seed=1729,
        dataset_version="1.1.3",
        source_manifest_path=source_manifest_path,
    )
    run_strict_open_split_definition(
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        splits_root=splits_root,
        command=["scz-audit", "strict-open", "define-splits"],
        git_sha="abc1234",
        seed=1729,
    )
    run_strict_open_feature_build(
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        features_root=features_root,
        command=["scz-audit", "strict-open", "build-features"],
        git_sha="abc1234",
        seed=1729,
    )
    run_strict_open_target_build(
        features_root=features_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        targets_root=targets_root,
        command=["scz-audit", "strict-open", "build-targets"],
        git_sha="abc1234",
        seed=1729,
    )
    return manifests_root, splits_root, features_root, targets_root


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _write_csv_rows(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        raise ValueError("Expected at least one row to write.")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

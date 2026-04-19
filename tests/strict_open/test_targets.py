import csv
import json
import shutil
from pathlib import Path

from scz_audit_engine.strict_open.features import run_strict_open_feature_build
from scz_audit_engine.strict_open.harmonize import run_tcp_harmonization
from scz_audit_engine.strict_open.provenance import build_source_manifest, write_source_manifest
from scz_audit_engine.strict_open.sources import TCPDS005237SourceAdapter
from scz_audit_engine.strict_open.splits import run_strict_open_split_definition
from scz_audit_engine.strict_open.targets import run_strict_open_target_build


FIXTURE_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "tcp_raw" / "source_input"
POINTER_PAYLOAD = "../.git/annex/objects/example-pointer"


def test_build_targets_emits_required_targets_and_manifest(tmp_path) -> None:
    harmonized_root, manifests_root, splits_root, features_root = _prepare_feature_fixture(tmp_path)
    targets_root = tmp_path / "data" / "processed" / "strict_open" / "targets"

    results = run_strict_open_target_build(
        features_root=features_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        targets_root=targets_root,
        command=["scz-audit", "strict-open", "build-targets"],
        git_sha="abc1234",
        seed=1729,
    )

    derived_targets_path = Path(results["derived_targets"])
    target_manifest_path = Path(results["target_manifest"])
    run_manifest_path = Path(results["run_manifest"])

    assert derived_targets_path.exists()
    assert target_manifest_path.exists()
    assert run_manifest_path.exists()

    target_rows = _read_csv_rows(derived_targets_path)
    target_manifest = json.loads(target_manifest_path.read_text(encoding="utf-8"))

    assert {row["target_name"] for row in target_rows} == {
        "global_cognition_dev",
        "state_noise_score",
        "stable_cognitive_burden_proxy",
    }
    state_noise_values = [float(row["target_value"]) for row in target_rows if row["target_name"] == "state_noise_score"]
    assert any(value not in {0.0, 1.0} for value in state_noise_values)
    assert target_manifest["counts_by_target_name"] == {
        "global_cognition_dev": 2,
        "stable_cognitive_burden_proxy": 2,
        "state_noise_score": 4,
    }
    assert target_manifest["target_coverage_by_split"]["state_noise_score"]["train"]["emitted"] == 1
    assert target_manifest["target_coverage_by_split"]["global_cognition_dev"]["test"]["emitted"] == 0
    assert target_manifest["reasons_targets_were_unavailable"]["global_cognition_dev"] == {
        "insufficient_cognition_evidence": 2
    }
    assert target_manifest["public_path_limit_note"] is not None
    assert target_manifest["split_contract_validation"]["mismatched_rows"] == 0


def test_build_targets_omits_stable_burden_when_support_is_insufficient(tmp_path) -> None:
    harmonized_root, manifests_root, splits_root, features_root = _prepare_feature_fixture(
        tmp_path,
        pointer_relative_paths=(
            "phenotype/panss01.tsv",
            "phenotype/qids01.tsv",
        ),
    )
    targets_root = tmp_path / "data" / "processed" / "strict_open" / "targets"

    results = run_strict_open_target_build(
        features_root=features_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        targets_root=targets_root,
        command=["scz-audit", "strict-open", "build-targets"],
        git_sha="abc1234",
        seed=1729,
    )

    target_rows = _read_csv_rows(Path(results["derived_targets"]))
    target_manifest = json.loads(Path(results["target_manifest"]).read_text(encoding="utf-8"))

    assert {row["target_name"] for row in target_rows} == {
        "global_cognition_dev",
        "state_noise_score",
    }
    assert not any(row["target_name"] == "stable_cognitive_burden_proxy" for row in target_rows)
    assert target_manifest["counts_by_target_name"] == {
        "global_cognition_dev": 2,
        "stable_cognitive_burden_proxy": 0,
        "state_noise_score": 3,
    }
    assert target_manifest["reasons_targets_were_unavailable"]["stable_cognitive_burden_proxy"] == {
        "insufficient_cognition_evidence": 1,
        "insufficient_supporting_symptom_proxy": 2,
    }
    assert target_manifest["public_path_limit_note"] is not None


def test_build_targets_omit_stable_burden_without_mri_support(tmp_path) -> None:
    harmonized_root, manifests_root, splits_root, features_root = _prepare_feature_fixture(tmp_path)
    targets_root = tmp_path / "data" / "processed" / "strict_open" / "targets"

    feature_rows = _read_csv_rows(features_root / "visit_features.csv")
    for row in feature_rows:
        if row["visit_id"] != "tcp-ds005237:sub-TCP002:visit-00-2026-01-02":
            continue
        row["mri_available_modality_count"] = "0"
        row["mri_present_fraction"] = "0"
        row["stable_support_family_count"] = "1"
        row["feature_family_available_count"] = "2"
        break
    _write_csv_rows(features_root / "visit_features.csv", feature_rows)

    results = run_strict_open_target_build(
        features_root=features_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        targets_root=targets_root,
        command=["scz-audit", "strict-open", "build-targets"],
        git_sha="abc1234",
        seed=1729,
    )

    target_rows = _read_csv_rows(Path(results["derived_targets"]))
    target_manifest = json.loads(Path(results["target_manifest"]).read_text(encoding="utf-8"))

    assert _target_value(
        target_rows,
        visit_id="tcp-ds005237:sub-TCP001:visit-00-2026-01-01",
        target_name="stable_cognitive_burden_proxy",
    )
    assert not any(
        row["visit_id"] == "tcp-ds005237:sub-TCP002:visit-00-2026-01-02"
        and row["target_name"] == "stable_cognitive_burden_proxy"
        for row in target_rows
    )
    assert target_manifest["reasons_targets_were_unavailable"]["stable_cognitive_burden_proxy"] == {
        "insufficient_cognition_evidence": 2,
        "insufficient_supporting_evidence": 1,
    }


def test_build_targets_do_not_shift_train_targets_when_held_out_scores_change(tmp_path) -> None:
    harmonized_root, manifests_root, splits_root, features_root = _prepare_feature_fixture(tmp_path)
    first_targets_root = tmp_path / "data" / "processed" / "strict_open" / "targets_first"
    second_features_root = tmp_path / "data" / "processed" / "strict_open" / "features_shifted"
    second_targets_root = tmp_path / "data" / "processed" / "strict_open" / "targets_second"

    first_results = run_strict_open_target_build(
        features_root=features_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        targets_root=first_targets_root,
        command=["scz-audit", "strict-open", "build-targets"],
        git_sha="abc1234",
        seed=1729,
    )

    shutil.copytree(features_root, second_features_root)
    shifted_feature_rows = _read_csv_rows(second_features_root / "visit_features.csv")
    for row in shifted_feature_rows:
        if row["visit_id"] != "tcp-ds005237:sub-TCP001:visit-00-2026-01-01":
            continue
        row["cognition_score_mean"] = "100"
        row["symptom_score_mean"] = "100"
        break
    _write_csv_rows(second_features_root / "visit_features.csv", shifted_feature_rows)

    second_results = run_strict_open_target_build(
        features_root=second_features_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        targets_root=second_targets_root,
        command=["scz-audit", "strict-open", "build-targets"],
        git_sha="abc1234",
        seed=1729,
    )

    first_target_rows = _read_csv_rows(Path(first_results["derived_targets"]))
    second_target_rows = _read_csv_rows(Path(second_results["derived_targets"]))

    assert _target_value(
        first_target_rows,
        visit_id="tcp-ds005237:sub-TCP002:visit-00-2026-01-02",
        target_name="global_cognition_dev",
    ) == _target_value(
        second_target_rows,
        visit_id="tcp-ds005237:sub-TCP002:visit-00-2026-01-02",
        target_name="global_cognition_dev",
    )
    assert _target_value(
        first_target_rows,
        visit_id="tcp-ds005237:sub-TCP002:visit-00-2026-01-02",
        target_name="stable_cognitive_burden_proxy",
    ) == _target_value(
        second_target_rows,
        visit_id="tcp-ds005237:sub-TCP002:visit-00-2026-01-02",
        target_name="stable_cognitive_burden_proxy",
    )


def test_build_targets_writes_lf_only_csv_artifacts(tmp_path) -> None:
    harmonized_root, manifests_root, splits_root, features_root = _prepare_feature_fixture(tmp_path)
    targets_root = tmp_path / "data" / "processed" / "strict_open" / "targets"

    results = run_strict_open_target_build(
        features_root=features_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        targets_root=targets_root,
        command=["scz-audit", "strict-open", "build-targets"],
        git_sha="abc1234",
        seed=1729,
    )

    assert b"\r\n" not in Path(results["derived_targets"]).read_bytes()


def _prepare_feature_fixture(
    tmp_path: Path,
    *,
    pointer_relative_paths: tuple[str, ...] = (),
) -> tuple[Path, Path, Path, Path]:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits"
    features_root = tmp_path / "data" / "processed" / "strict_open" / "features"
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
    return harmonized_root, manifests_root, splits_root, features_root


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
        writer.writerows(rows)


def _target_value(rows: list[dict[str, str]], *, visit_id: str, target_name: str) -> str:
    for row in rows:
        if row["visit_id"] == visit_id and row["target_name"] == target_name:
            return row["target_value"]
    raise AssertionError(f"Missing target row for {visit_id} / {target_name}")

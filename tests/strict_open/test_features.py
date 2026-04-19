import csv
import json
from pathlib import Path

from scz_audit_engine.strict_open.features import FEATURE_COLUMNS, run_strict_open_feature_build
from scz_audit_engine.strict_open.harmonize import run_tcp_harmonization
from scz_audit_engine.strict_open.provenance import build_source_manifest, write_source_manifest
from scz_audit_engine.strict_open.sources import TCPDS005237SourceAdapter
from scz_audit_engine.strict_open.splits import run_strict_open_split_definition


FIXTURE_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "tcp_raw" / "source_input"
POINTER_PAYLOAD = "../.git/annex/objects/example-pointer"


def test_build_features_emits_visit_features_and_manifest(tmp_path) -> None:
    harmonized_root, manifests_root, splits_root = _prepare_harmonized_and_split_fixture(tmp_path)
    features_root = tmp_path / "data" / "processed" / "strict_open" / "features"

    results = run_strict_open_feature_build(
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        features_root=features_root,
        command=["scz-audit", "strict-open", "build-features"],
        git_sha="abc1234",
        seed=1729,
    )

    visit_features_path = Path(results["visit_features"])
    feature_manifest_path = Path(results["feature_manifest"])
    run_manifest_path = Path(results["run_manifest"])

    assert visit_features_path.exists()
    assert feature_manifest_path.exists()
    assert run_manifest_path.exists()
    assert _csv_header(visit_features_path) == list(FEATURE_COLUMNS)

    feature_rows = _read_csv_rows(visit_features_path)
    feature_manifest = json.loads(feature_manifest_path.read_text(encoding="utf-8"))

    assert len(feature_rows) == 4
    assert {row["split"] for row in feature_rows} == {"train", "validation", "test"}
    assert all(row["subject_id"] and row["visit_id"] for row in feature_rows)

    follow_up_row = next(row for row in feature_rows if row["visit_id"].endswith("2026-02-15"))
    assert follow_up_row["split"] == "validation"
    assert follow_up_row["cognition_score_count"] == "0"
    assert follow_up_row["cognition_score_mean"] == ""
    assert follow_up_row["cognition_missing_indicator"] == "1"
    assert follow_up_row["mri_available_modality_count"] == "0"
    assert float(follow_up_row["state_noise_proxy_input"]) > 0.5
    assert follow_up_row["visit_ambiguity_proxy_input"] == "0.5"

    assert feature_manifest["row_counts"] == {"visit_features": 4}
    assert feature_manifest["feature_coverage_summary"] == {
        "total_visits": 4,
        "visits_with_cognition": 2,
        "visits_with_symptoms": 4,
        "visits_with_any_mri": 3,
        "visits_with_motion_qc": 3,
    }
    assert feature_manifest["missingness_summary"]["visits_missing_cognition"] == 2
    assert feature_manifest["split_contract_validation"]["visits_missing_split"] == 0
    assert str(splits_root / "split_assignments.csv") in feature_manifest["input_paths"]
    assert follow_up_row["stable_support_family_count"] == "1"


def test_build_features_keeps_sparse_public_inputs_honest(tmp_path) -> None:
    harmonized_root, manifests_root, splits_root = _prepare_harmonized_and_split_fixture(
        tmp_path,
        pointer_relative_paths=(
            "phenotype/cogfq01.tsv",
            "phenotype/panss01.tsv",
            "phenotype/qids01.tsv",
        ),
    )
    features_root = tmp_path / "data" / "processed" / "strict_open" / "features"

    results = run_strict_open_feature_build(
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        features_root=features_root,
        command=["scz-audit", "strict-open", "build-features"],
        git_sha="abc1234",
        seed=1729,
    )

    feature_rows = _read_csv_rows(Path(results["visit_features"]))
    feature_manifest = json.loads(Path(results["feature_manifest"]).read_text(encoding="utf-8"))

    assert len(feature_rows) == 3
    assert all(row["cognition_score_count"] == "0" for row in feature_rows)
    assert all(row["cognition_score_mean"] == "" for row in feature_rows)
    assert all(row["symptom_score_count"] == "0" for row in feature_rows)
    assert all(row["symptom_score_mean"] == "" for row in feature_rows)
    assert all(row["state_noise_proxy_input"] != "" for row in feature_rows)
    assert feature_manifest["feature_coverage_summary"]["visits_with_cognition"] == 0
    assert feature_manifest["feature_coverage_summary"]["visits_with_symptoms"] == 0
    assert feature_manifest["unavailable_feature_families"] == ["cognition", "symptom"]


def test_build_features_writes_lf_only_csv_artifacts(tmp_path) -> None:
    harmonized_root, manifests_root, splits_root = _prepare_harmonized_and_split_fixture(tmp_path)
    features_root = tmp_path / "data" / "processed" / "strict_open" / "features"

    results = run_strict_open_feature_build(
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        manifests_root=manifests_root,
        features_root=features_root,
        command=["scz-audit", "strict-open", "build-features"],
        git_sha="abc1234",
        seed=1729,
    )

    assert b"\r\n" not in Path(results["visit_features"]).read_bytes()


def _prepare_harmonized_and_split_fixture(
    tmp_path: Path,
    *,
    pointer_relative_paths: tuple[str, ...] = (),
) -> tuple[Path, Path, Path]:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits"
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
    return harmonized_root, manifests_root, splits_root


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader)

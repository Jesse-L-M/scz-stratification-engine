import csv
import json
from pathlib import Path

from scz_audit_engine.strict_open.harmonize import run_tcp_harmonization
from scz_audit_engine.strict_open.provenance import SourceFileRecord, build_source_manifest, write_source_manifest
from scz_audit_engine.strict_open.schema import (
    COGNITION_SCORES,
    MRI_FEATURES,
    SUBJECTS,
    SYMPTOM_BEHAVIOR_SCORES,
    VISITS,
)
from scz_audit_engine.strict_open.sources import TCPDS005237SourceAdapter


FIXTURE_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "tcp_raw" / "source_input"


def test_tcp_harmonize_emits_canonical_tables_and_manifest(tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    adapter = TCPDS005237SourceAdapter()
    staged = adapter.stage(raw_root, source_root=FIXTURE_SOURCE_ROOT)

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

    results = run_tcp_harmonization(
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        command=["scz-audit", "strict-open", "harmonize"],
        git_sha="abc1234",
        seed=1729,
        dataset_version="1.1.3",
        source_manifest_path=source_manifest_path,
    )

    assert Path(results["subjects"]).exists()
    assert Path(results["visits"]).exists()
    assert Path(results["cognition_scores"]).exists()
    assert Path(results["symptom_behavior_scores"]).exists()
    assert Path(results["mri_features"]).exists()
    assert Path(results["harmonization_manifest"]).exists()
    assert Path(results["run_manifest"]).exists()

    assert _csv_header(Path(results["subjects"])) == list(SUBJECTS.columns)
    assert _csv_header(Path(results["visits"])) == list(VISITS.columns)
    assert _csv_header(Path(results["cognition_scores"])) == list(COGNITION_SCORES.columns)
    assert _csv_header(Path(results["symptom_behavior_scores"])) == list(SYMPTOM_BEHAVIOR_SCORES.columns)
    assert _csv_header(Path(results["mri_features"])) == list(MRI_FEATURES.columns)

    subjects_rows = _read_csv_rows(Path(results["subjects"]))
    visits_rows = _read_csv_rows(Path(results["visits"]))
    cognition_rows = _read_csv_rows(Path(results["cognition_scores"]))
    symptom_rows = _read_csv_rows(Path(results["symptom_behavior_scores"]))
    mri_rows = _read_csv_rows(Path(results["mri_features"]))
    harmonization_manifest = json.loads(Path(results["harmonization_manifest"]).read_text(encoding="utf-8"))

    assert len(subjects_rows) == 3
    assert len(visits_rows) == 4
    assert len(cognition_rows) == 2
    assert len(symptom_rows) == 7
    assert len(mri_rows) == 32

    assert subjects_rows[0] == {
        "subject_id": "tcp-ds005237:sub-TCP001",
        "source_dataset": "tcp-ds005237",
        "source_subject_id": "sub-TCP001",
        "diagnosis": "Patient",
        "site_id": "1",
        "sex": "F",
        "age_years": "25.0",
    }
    assert any(
        row["subject_id"] == "tcp-ds005237:sub-TCP001"
        and row["visit_label"] == "2026-02-15"
        and row["days_from_baseline"] == "45"
        for row in visits_rows
    )
    assert any(
        row["instrument"] == "qids01"
        and row["visit_id"].endswith("2026-02-15")
        and row["measure"] == "qids_1"
        and row["score"] == "3"
        for row in symptom_rows
    )
    assert any(
        row["subject_id"] == "tcp-ds005237:sub-TCP002"
        and row["modality"] == "T2w"
        and row["feature_name"] == "available"
        and row["feature_value"] == "0"
        and row["qc_status"] == "missing"
        for row in mri_rows
    )
    assert sum(1 for row in mri_rows if row["feature_name"] == "mean_fd") == 5

    assert harmonization_manifest["row_counts"] == {
        "subjects": 3,
        "visits": 4,
        "cognition_scores": 2,
        "symptom_behavior_scores": 7,
        "mri_features": 32,
    }
    assert harmonization_manifest["score_summary"]["accessible_instruments"] == {
        "cognition": ["cogfq01"],
        "symptom": ["panss01", "qids01"],
    }
    assert harmonization_manifest["unmapped_fields"] == {}
    assert harmonization_manifest["subject_issues"] == {
        "duplicate_subject_rows": [],
        "missing_subject_rows": 0,
    }


def test_tcp_harmonize_prefers_accessible_local_files_over_stale_source_manifest_flags(tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    adapter = TCPDS005237SourceAdapter()
    adapter.stage(raw_root, source_root=FIXTURE_SOURCE_ROOT)

    source_manifest = build_source_manifest(
        source="tcp",
        source_identifier="tcp-ds005237",
        dataset_accession="ds005237",
        dataset_version="1.1.3",
        command=["scz-audit", "strict-open", "ingest", "--source", "tcp"],
        git_sha="abc1234",
        raw_root=raw_root,
        files=(
            SourceFileRecord(
                relative_path="phenotype/qids01.tsv",
                storage="staged",
                size_bytes=123,
                sha256=None,
                source_url=None,
                content_kind="git-annex-pointer",
            ),
            SourceFileRecord(
                relative_path="motion_FD/TCP_FD_rest_AP_1.csv",
                storage="staged",
                size_bytes=123,
                sha256=None,
                source_url=None,
                content_kind="git-annex-pointer",
            ),
        ),
        ingest_timestamp="2026-04-16T12:00:00Z",
    )
    source_manifest_path = manifests_root / "tcp_source_manifest.json"
    write_source_manifest(source_manifest, source_manifest_path)

    results = run_tcp_harmonization(
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        command=["scz-audit", "strict-open", "harmonize"],
        git_sha="abc1234",
        seed=1729,
        dataset_version="1.1.3",
        source_manifest_path=source_manifest_path,
    )
    visits_rows = _read_csv_rows(Path(results["visits"]))
    symptom_rows = _read_csv_rows(Path(results["symptom_behavior_scores"]))
    harmonization_manifest = json.loads(Path(results["harmonization_manifest"]).read_text(encoding="utf-8"))

    assert len(visits_rows) == 4
    assert {row["instrument"] for row in symptom_rows} == {"panss01", "qids01"}
    assert harmonization_manifest["inaccessible_inputs"] == []
    assert harmonization_manifest["score_summary"]["inaccessible_instruments"] == {}
    assert harmonization_manifest["source_manifest_path"] == str(source_manifest_path)


def test_tcp_harmonize_excludes_annex_backed_scores_qc_and_mri_payloads(tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    adapter = TCPDS005237SourceAdapter()
    staged = adapter.stage(raw_root, source_root=FIXTURE_SOURCE_ROOT)

    pointer_payload = "../.git/annex/objects/example-pointer"
    (raw_root / "phenotype" / "qids01.tsv").write_text(pointer_payload, encoding="utf-8")
    (raw_root / "motion_FD" / "TCP_FD_rest_AP_1.csv").write_text(pointer_payload, encoding="utf-8")
    (raw_root / "sub-TCP001" / "anat" / "sub-TCP001_run-01_T1w.nii.gz").write_text(pointer_payload, encoding="utf-8")

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

    results = run_tcp_harmonization(
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        command=["scz-audit", "strict-open", "harmonize"],
        git_sha="abc1234",
        seed=1729,
        dataset_version="1.1.3",
        source_manifest_path=source_manifest_path,
    )
    visits_rows = _read_csv_rows(Path(results["visits"]))
    symptom_rows = _read_csv_rows(Path(results["symptom_behavior_scores"]))
    mri_rows = _read_csv_rows(Path(results["mri_features"]))
    harmonization_manifest = json.loads(Path(results["harmonization_manifest"]).read_text(encoding="utf-8"))

    assert len(visits_rows) == 3
    assert any(
        row["subject_id"] == "tcp-ds005237:sub-TCP003" and row["visit_label"] == "baseline"
        for row in visits_rows
    )
    assert {row["instrument"] for row in symptom_rows} == {"panss01"}
    assert not any(
        row["modality"] == "restAP" and row["feature_name"] == "mean_fd"
        for row in mri_rows
    )
    assert any(
        row["subject_id"] == "tcp-ds005237:sub-TCP001"
        and row["modality"] == "T1w"
        and row["feature_name"] == "available"
        and row["feature_value"] == "0"
        and row["qc_status"] == "missing"
        for row in mri_rows
    )
    assert sorted(harmonization_manifest["inaccessible_inputs"]) == [
        "motion_FD/TCP_FD_rest_AP_1.csv",
        "phenotype/qids01.tsv",
        "sub-TCP001/anat/sub-TCP001_run-01_T1w.nii.gz",
    ]
    assert harmonization_manifest["row_counts"] == {
        "subjects": 3,
        "visits": 3,
        "cognition_scores": 2,
        "symptom_behavior_scores": 3,
        "mri_features": 29,
    }
    assert harmonization_manifest["score_summary"]["inaccessible_instruments"] == {
        "symptom": ["qids01"]
    }


def test_tcp_harmonize_does_not_claim_manifest_only_mri_payloads_are_available(tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    adapter = TCPDS005237SourceAdapter()
    staged = adapter.stage(raw_root, source_root=FIXTURE_SOURCE_ROOT)

    for payload_path in raw_root.rglob("*.nii.gz"):
        payload_path.unlink()

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

    results = run_tcp_harmonization(
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        command=["scz-audit", "strict-open", "harmonize"],
        git_sha="abc1234",
        seed=1729,
        dataset_version="1.1.3",
        source_manifest_path=source_manifest_path,
    )
    mri_rows = _read_csv_rows(Path(results["mri_features"]))
    harmonization_manifest = json.loads(Path(results["harmonization_manifest"]).read_text(encoding="utf-8"))

    assert any(
        row["subject_id"] == "tcp-ds005237:sub-TCP001"
        and row["modality"] == "T1w"
        and row["feature_name"] == "available"
        and row["feature_value"] == "0"
        and row["qc_status"] == "missing"
        for row in mri_rows
    )
    assert "sub-TCP001/anat/sub-TCP001_run-01_T1w.nii.gz" in harmonization_manifest["inaccessible_inputs"]


def test_tcp_harmonize_flags_manifest_only_motion_qc_files_as_inaccessible(tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    adapter = TCPDS005237SourceAdapter()
    staged = adapter.stage(raw_root, source_root=FIXTURE_SOURCE_ROOT)

    (raw_root / "motion_FD" / "TCP_FD_rest_AP_1.csv").unlink()

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

    results = run_tcp_harmonization(
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        command=["scz-audit", "strict-open", "harmonize"],
        git_sha="abc1234",
        seed=1729,
        dataset_version="1.1.3",
        source_manifest_path=source_manifest_path,
    )
    mri_rows = _read_csv_rows(Path(results["mri_features"]))
    harmonization_manifest = json.loads(Path(results["harmonization_manifest"]).read_text(encoding="utf-8"))

    assert not any(
        row["modality"] == "restAP" and row["feature_name"] == "mean_fd"
        for row in mri_rows
    )
    assert "motion_FD/TCP_FD_rest_AP_1.csv" in harmonization_manifest["inaccessible_inputs"]
    assert "restAP" not in harmonization_manifest["mri_summary"]["qc_rows_by_modality"]


def _csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]

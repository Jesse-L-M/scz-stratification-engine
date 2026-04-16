import json
from pathlib import Path

from scz_audit_engine.strict_open.audit import run_tcp_audit
from scz_audit_engine.strict_open.provenance import build_source_manifest, write_source_manifest
from scz_audit_engine.strict_open.sources import TCPDS005237SourceAdapter


FIXTURE_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "tcp_raw" / "source_input"


def test_tcp_audit_writes_expected_profile_sections(tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    profiles_root = tmp_path / "data" / "processed" / "strict_open" / "profiles"
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

    results = run_tcp_audit(
        raw_root=raw_root,
        manifests_root=manifests_root,
        profiles_root=profiles_root,
        command=["scz-audit", "strict-open", "audit"],
        git_sha="abc1234",
        seed=1729,
        dataset_version="1.1.3",
        source_manifest_path=source_manifest_path,
    )
    profile_payload = json.loads(Path(results["audit_profile"]).read_text(encoding="utf-8"))

    assert Path(results["audit_profile"]).exists()
    assert Path(results["audit_provenance"]).exists()
    assert Path(results["run_manifest"]).exists()
    assert profile_payload["subject_counts"] == {
        "participant_rows": 3,
        "participant_subjects": 3,
        "subject_directories": 3,
    }
    assert profile_payload["diagnosis_breakdown"] == {"GenPop": 1, "Patient": 2}
    assert profile_payload["repeat_visit_availability"]["subjects_with_repeat_visits"] == 1
    assert profile_payload["cognition_instrument_inventory"]["available_instruments"] == 1
    assert profile_payload["symptom_instrument_inventory"]["available_instruments"] == 2
    assert profile_payload["mri_modality_qc_inventory"]["modality_subject_counts"]["T1w"] == 3
    assert profile_payload["mri_modality_qc_inventory"]["missing_subjects_by_modality"]["T2w"] == 1
    assert profile_payload["mri_modality_qc_inventory"]["missing_subjects_by_modality"]["stroopPA"] == 2
    assert profile_payload["missingness_summary"]["participant_missing_by_column"]["age"] == 1
    assert profile_payload["missingness_summary"]["phenotype_missing_cells"] == 5
    assert profile_payload["missingness_summary"]["phenotype_unresolved_tables"] == 0

import json
from pathlib import Path

from scz_audit_engine.strict_open.provenance import (
    ProcessedOutputRecord,
    ProvenanceMapping,
    build_audit_provenance,
    build_source_manifest,
    file_sha256,
    load_source_manifest,
    write_audit_provenance,
    write_source_manifest,
)
from scz_audit_engine.strict_open.sources import TCPDS005237SourceAdapter


FIXTURE_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "tcp_raw" / "source_input"


def test_source_manifest_has_required_tcp_fields_and_hashes(tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    adapter = TCPDS005237SourceAdapter()
    staged = adapter.stage(raw_root, source_root=FIXTURE_SOURCE_ROOT)

    manifest = build_source_manifest(
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
    output_path = manifests_root / "tcp_source_manifest.json"

    written_path = write_source_manifest(manifest, output_path)
    payload = json.loads(written_path.read_text(encoding="utf-8"))

    assert payload["dataset_accession"] == "ds005237"
    assert payload["dataset_version"] == "1.1.3"
    assert payload["source"] == "tcp"
    assert payload["source_identifier"] == "tcp-ds005237"
    assert payload["command"] == ["scz-audit", "strict-open", "ingest", "--source", "tcp"]
    participants_entry = next(
        record for record in payload["files"] if record["relative_path"] == "participants.tsv"
    )
    assert participants_entry["sha256"] == file_sha256(raw_root / "participants.tsv")
    assert participants_entry["storage"] == "copied"
    reloaded = load_source_manifest(written_path)
    assert reloaded.dataset_version == "1.1.3"


def test_audit_provenance_serializes_processed_output_mappings(tmp_path) -> None:
    profile_path = tmp_path / "profiles" / "tcp_audit_profile.json"
    profile_path.parent.mkdir(parents=True, exist_ok=True)
    profile_path.write_text('{"status":"ok"}\n', encoding="utf-8")

    provenance = build_audit_provenance(
        source="tcp",
        source_identifier="tcp-ds005237",
        dataset_accession="ds005237",
        dataset_version="1.1.3",
        command=["scz-audit", "strict-open", "audit"],
        git_sha="abc1234",
        raw_root=tmp_path / "raw" / "tcp",
        processed_outputs=(
            ProcessedOutputRecord(
                output_name="audit_profile",
                relative_path="tcp_audit_profile.json",
                sha256=file_sha256(profile_path),
            ),
        ),
        mappings=(
            ProvenanceMapping(
                processed_output="tcp_audit_profile.json",
                raw_inputs=("participants.tsv", "phenotype/cogfq01.tsv"),
            ),
        ),
        generated_at="2026-04-16T12:00:00Z",
    )
    output_path = tmp_path / "manifests" / "tcp_audit_provenance.json"

    written_path = write_audit_provenance(provenance, output_path)
    payload = json.loads(written_path.read_text(encoding="utf-8"))

    assert payload["dataset_accession"] == "ds005237"
    assert payload["generated_at"] == "2026-04-16T12:00:00Z"
    assert payload["processed_outputs"] == [
        {
            "output_name": "audit_profile",
            "relative_path": "tcp_audit_profile.json",
            "sha256": file_sha256(profile_path),
        }
    ]
    assert payload["mappings"] == [
        {
            "processed_output": "tcp_audit_profile.json",
            "raw_inputs": ["participants.tsv", "phenotype/cogfq01.tsv"],
        }
    ]

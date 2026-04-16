import json

from scz_stratification_engine.strict_open.run_manifest import build_run_manifest, write_run_manifest


def test_run_manifest_has_required_fields() -> None:
    manifest = build_run_manifest(
        dataset_source="tcp",
        dataset_version="ds005237",
        command=["scz-stratification", "strict-open", "ingest"],
        git_sha="abc1234",
        seed=1729,
        output_paths={"manifest": "data/processed/strict_open/manifests/run_manifest.json"},
        timestamp="2026-04-16T12:00:00Z",
    )

    assert manifest.to_dict() == {
        "dataset": {"source": "tcp", "version": "ds005237"},
        "command": ["scz-stratification", "strict-open", "ingest"],
        "git_sha": "abc1234",
        "seed": 1729,
        "output_paths": {"manifest": "data/processed/strict_open/manifests/run_manifest.json"},
        "timestamp": "2026-04-16T12:00:00Z",
    }


def test_write_run_manifest_serializes_json(tmp_path) -> None:
    manifest = build_run_manifest(
        dataset_source="tcp",
        command=["scz-stratification", "strict-open", "audit"],
        git_sha=None,
        seed=1729,
        output_paths={"profile": tmp_path / "data/processed/strict_open/profiles/audit.json"},
        timestamp="2026-04-16T12:00:00Z",
    )
    destination = tmp_path / "manifests" / "audit_manifest.json"

    written_path = write_run_manifest(manifest, destination)

    assert written_path == destination
    assert json.loads(destination.read_text(encoding="utf-8")) == manifest.to_dict()

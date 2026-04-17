import json

import pytest

from scz_audit_engine.benchmark.run_manifest import build_run_manifest, write_run_manifest


def test_run_manifest_has_required_fields() -> None:
    manifest = build_run_manifest(
        cohort_identifier="pronia",
        command=["scz-audit", "benchmark", "run-benchmark"],
        git_sha="abc1234",
        seed=1729,
        output_paths={"manifest": "data/processed/benchmark/manifests/run_manifest.json"},
        timestamp="2026-04-17T12:00:00Z",
    )

    assert manifest.to_dict() == {
        "dataset": {"cohort": "pronia"},
        "command": ["scz-audit", "benchmark", "run-benchmark"],
        "git_sha": "abc1234",
        "seed": 1729,
        "output_paths": {"manifest": "data/processed/benchmark/manifests/run_manifest.json"},
        "timestamp": "2026-04-17T12:00:00Z",
    }


def test_write_run_manifest_serializes_json(tmp_path) -> None:
    manifest = build_run_manifest(
        dataset_source="openneuro",
        command=["scz-audit", "benchmark", "audit-datasets"],
        git_sha=None,
        seed=1729,
        output_paths={"report": tmp_path / "data/processed/benchmark/reports/dataset_audit.json"},
        timestamp="2026-04-17T12:00:00Z",
    )
    destination = tmp_path / "manifests" / "benchmark_run_manifest.json"

    written_path = write_run_manifest(manifest, destination)

    assert written_path == destination
    assert json.loads(destination.read_text(encoding="utf-8")) == manifest.to_dict()


def test_run_manifest_requires_a_dataset_source_or_cohort_identifier() -> None:
    with pytest.raises(ValueError, match="source or cohort identifier"):
        build_run_manifest(
            command=["scz-audit", "benchmark", "run-benchmark"],
            git_sha="abc1234",
            seed=1729,
            output_paths={"manifest": "data/processed/benchmark/manifests/run_manifest.json"},
        )

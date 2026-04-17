import json

from scz_audit_engine.benchmark.provenance import file_sha256, write_json_artifact


def test_file_sha256_matches_known_payload(tmp_path) -> None:
    payload_path = tmp_path / "artifact.txt"
    payload_path.write_text("benchmark\n", encoding="utf-8")

    assert file_sha256(payload_path) == "8f8dbecfd77ab2386b49d723c6b2474f2c22c246805fa0f677bbaf6e4f7bbbfe"


def test_write_json_artifact_serializes_sorted_json(tmp_path) -> None:
    destination = tmp_path / "manifests" / "benchmark_run_manifest.json"

    written_path = write_json_artifact({"z": 1, "a": {"value": 2}}, destination)

    assert written_path == destination
    assert json.loads(destination.read_text(encoding="utf-8")) == {"a": {"value": 2}, "z": 1}
    assert destination.read_text(encoding="utf-8").splitlines()[1].strip().startswith('"a"')

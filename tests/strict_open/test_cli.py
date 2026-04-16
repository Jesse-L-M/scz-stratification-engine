import json
from pathlib import Path

import pytest

from scz_audit_engine.cli import STRICT_OPEN_COMMANDS, main

IMPLEMENTED_COMMANDS = {"ingest", "audit"}
STUB_COMMANDS = tuple(command_name for command_name in STRICT_OPEN_COMMANDS if command_name not in IMPLEMENTED_COMMANDS)
FIXTURE_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "tcp_raw" / "source_input"


def test_top_level_help_works(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["--help"])

    assert excinfo.value.code == 0
    assert "strict-open" in capsys.readouterr().out


def test_strict_open_help_works(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["strict-open", "--help"])

    assert excinfo.value.code == 0
    output = capsys.readouterr().out
    assert "Bootstrap commands for the strict-open v0" in output
    assert "cohort stability and noise audit" in output
    assert "ingest" in output


@pytest.mark.parametrize("command_name", STRICT_OPEN_COMMANDS)
def test_stub_command_help_is_registered(
    command_name: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["strict-open", command_name, "--help"])

    assert excinfo.value.code == 0
    assert command_name in capsys.readouterr().out


@pytest.mark.parametrize("command_name", STUB_COMMANDS)
def test_stub_commands_exit_with_not_implemented_message(
    command_name: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(["strict-open", command_name])

    assert exit_code == 1
    assert "not implemented yet" in capsys.readouterr().err


def test_ingest_command_stages_tcp_fixture(capsys: pytest.CaptureFixture[str], tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"

    exit_code = main(
        [
            "strict-open",
            "ingest",
            "--source",
            "tcp",
            "--source-root",
            str(FIXTURE_SOURCE_ROOT),
            "--raw-root",
            str(raw_root),
            "--manifest-dir",
            str(manifests_root),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["source"] == "tcp"
    assert payload["raw_root"] == str(raw_root)
    assert (raw_root / "participants.tsv").exists()
    assert (manifests_root / "tcp_source_manifest.json").exists()


def test_audit_command_writes_tcp_profile(capsys: pytest.CaptureFixture[str], tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    profiles_root = tmp_path / "data" / "processed" / "strict_open" / "profiles"

    ingest_exit_code = main(
        [
            "strict-open",
            "ingest",
            "--source",
            "tcp",
            "--source-root",
            str(FIXTURE_SOURCE_ROOT),
            "--raw-root",
            str(raw_root),
            "--manifest-dir",
            str(manifests_root),
        ]
    )
    assert ingest_exit_code == 0
    capsys.readouterr()

    audit_exit_code = main(
        [
            "strict-open",
            "audit",
            "--raw-root",
            str(raw_root),
            "--manifest-dir",
            str(manifests_root),
            "--profile-dir",
            str(profiles_root),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert audit_exit_code == 0
    assert Path(payload["audit_profile"]).exists()
    assert Path(payload["audit_provenance"]).exists()
    assert Path(payload["run_manifest"]).exists()

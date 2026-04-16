import pytest

from scz_audit_engine.cli import STRICT_OPEN_COMMANDS, main


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


@pytest.mark.parametrize("command_name", STRICT_OPEN_COMMANDS)
def test_stub_commands_exit_with_not_implemented_message(
    command_name: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(["strict-open", command_name])

    assert exit_code == 1
    assert "not implemented yet" in capsys.readouterr().err

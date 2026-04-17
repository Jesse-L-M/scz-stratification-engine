import pytest

from scz_audit_engine.cli import BENCHMARK_COMMANDS, main


def test_top_level_help_lists_benchmark_and_strict_open(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["--help"])

    assert excinfo.value.code == 0
    output = capsys.readouterr().out
    assert "benchmark" in output
    assert "strict-open" in output


def test_benchmark_help_works(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["benchmark", "--help"])

    assert excinfo.value.code == 0
    output = capsys.readouterr().out
    assert "Mainline commands for the multi-cohort psychosis benchmark scaffold." in output
    assert "run-benchmark" in output


@pytest.mark.parametrize("command_name", BENCHMARK_COMMANDS)
def test_benchmark_subcommand_help_is_registered(
    command_name: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["benchmark", command_name, "--help"])

    assert excinfo.value.code == 0
    assert command_name in capsys.readouterr().out


@pytest.mark.parametrize("command_name", BENCHMARK_COMMANDS)
def test_benchmark_stub_commands_exit_with_not_implemented_message(
    command_name: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(["benchmark", command_name])

    assert exit_code == 1
    assert "not implemented yet" in capsys.readouterr().err


def test_strict_open_help_still_works(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["strict-open", "--help"])

    assert excinfo.value.code == 0
    output = capsys.readouterr().out
    assert "Bootstrap commands for the strict-open v0" in output
    assert "cohort stability and noise audit" in output

import json
from pathlib import Path

import pytest

from scz_audit_engine.benchmark.sources import build_default_source_adapters
from scz_audit_engine.cli import BENCHMARK_COMMANDS, main

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "benchmark_sources"


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


@pytest.mark.parametrize(
    "command_name",
    tuple(command_name for command_name in BENCHMARK_COMMANDS if command_name != "audit-datasets"),
)
def test_benchmark_stub_commands_exit_with_not_implemented_message(
    command_name: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = main(["benchmark", command_name])

    assert exit_code == 1
    assert "not implemented yet" in capsys.readouterr().err


def test_benchmark_audit_datasets_runs_end_to_end_with_fixture_adapters(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import scz_audit_engine.benchmark.dataset_audit as dataset_audit_module

    monkeypatch.setattr(
        dataset_audit_module,
        "build_default_source_adapters",
        lambda: build_default_source_adapters(
            {
                "tcp-ds005237": FIXTURE_ROOT / "tcp_ds005237",
                "fep-ds003944": FIXTURE_ROOT / "fep_ds003944",
            }
        ),
    )

    registry_path = tmp_path / "dataset_registry.csv"
    reports_dir = tmp_path / "reports"
    manifests_dir = tmp_path / "manifests"

    exit_code = main(
        [
            "benchmark",
            "audit-datasets",
            "--registry-path",
            str(registry_path),
            "--reports-dir",
            str(reports_dir),
            "--manifest-dir",
            str(manifests_dir),
        ]
    )

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["decision"] == "narrow-go"
    assert Path(output["dataset_registry"]).exists()
    assert Path(output["json_report"]).exists()
    assert Path(output["markdown_report"]).exists()
    manifest = json.loads(Path(output["run_manifest"]).read_text(encoding="utf-8"))
    assert manifest["command"] == [
        "scz-audit",
        "benchmark",
        "audit-datasets",
        "--registry-path",
        str(registry_path),
        "--reports-dir",
        str(reports_dir),
        "--manifest-dir",
        str(manifests_dir),
    ]


def test_strict_open_help_still_works(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["strict-open", "--help"])

    assert excinfo.value.code == 0
    output = capsys.readouterr().out
    assert "Bootstrap commands for the strict-open v0" in output
    assert "cohort stability and noise audit" in output

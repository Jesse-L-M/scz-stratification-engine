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
    assert "Commands for the benchmark dataset and outcome feasibility gate." in output
    assert "define-schema" in output
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
    tuple(
        command_name
        for command_name in BENCHMARK_COMMANDS
        if command_name not in {"audit-datasets", "define-schema"}
    ),
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
    assert output["claim_level"] == "narrow_outcome_benchmark"
    assert Path(output["dataset_registry"]).exists()
    assert Path(output["json_report"]).exists()
    assert Path(output["markdown_report"]).exists()
    json_report = json.loads(Path(output["json_report"]).read_text(encoding="utf-8"))
    assert json_report["decision"]["claim_level"] == "narrow_outcome_benchmark"
    assert json_report["decision"]["full_external_validation_cohorts"] == []
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


def test_benchmark_define_schema_runs_and_writes_artifacts(
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    schema_dir = tmp_path / "schema"
    manifests_dir = tmp_path / "manifests"

    exit_code = main(
        [
            "benchmark",
            "define-schema",
            "--output-dir",
            str(schema_dir),
            "--manifest-dir",
            str(manifests_dir),
        ]
    )

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["schema_version"] == "benchmark_v0"
    assert output["tables"] == [
        "subjects",
        "visits",
        "diagnoses",
        "symptom_scores",
        "cognition_scores",
        "functioning_scores",
        "treatment_exposures",
        "outcomes",
        "modality_features",
        "split_assignments",
    ]
    assert Path(output["json_schema"]).exists()
    assert Path(output["markdown_schema"]).exists()
    assert Path(output["run_manifest"]).exists()

    json_schema = json.loads(Path(output["json_schema"]).read_text(encoding="utf-8"))
    assert json_schema["schema_version"] == "benchmark_v0"
    assert json_schema["table_names"] == output["tables"]
    assert "generated_at" not in json_schema
    assert "outcomes" in json_schema["table_names"]
    markdown_schema = Path(output["markdown_schema"]).read_text(encoding="utf-8")
    assert "Generated at:" not in markdown_schema

    manifest = json.loads(Path(output["run_manifest"]).read_text(encoding="utf-8"))
    assert manifest["command"] == [
        "scz-audit",
        "benchmark",
        "define-schema",
        "--output-dir",
        str(schema_dir),
        "--manifest-dir",
        str(manifests_dir),
    ]
    assert manifest["output_paths"] == {
        "json_schema": str(schema_dir / "benchmark_schema.json"),
        "markdown_schema": str(schema_dir / "benchmark_schema.md"),
    }


def test_benchmark_define_schema_is_deterministic_for_schema_artifacts(
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    schema_dir = tmp_path / "schema"
    manifests_dir = tmp_path / "manifests"

    first_exit_code = main(
        [
            "benchmark",
            "define-schema",
            "--output-dir",
            str(schema_dir),
            "--manifest-dir",
            str(manifests_dir),
        ]
    )
    assert first_exit_code == 0
    capsys.readouterr()
    first_json = (schema_dir / "benchmark_schema.json").read_text(encoding="utf-8")
    first_markdown = (schema_dir / "benchmark_schema.md").read_text(encoding="utf-8")

    second_exit_code = main(
        [
            "benchmark",
            "define-schema",
            "--output-dir",
            str(schema_dir),
            "--manifest-dir",
            str(manifests_dir),
        ]
    )
    assert second_exit_code == 0
    capsys.readouterr()

    assert (schema_dir / "benchmark_schema.json").read_text(encoding="utf-8") == first_json
    assert (schema_dir / "benchmark_schema.md").read_text(encoding="utf-8") == first_markdown


def test_strict_open_help_still_works(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["strict-open", "--help"])

    assert excinfo.value.code == 0
    output = capsys.readouterr().out
    assert "Bootstrap commands for the strict-open v0" in output
    assert "cohort stability and noise audit" in output

import json
from pathlib import Path

import pytest

from scz_audit_engine.cli import STRICT_OPEN_COMMANDS, main

IMPLEMENTED_COMMANDS = {
    "ingest",
    "audit",
    "harmonize",
    "define-splits",
    "build-features",
    "build-targets",
    "train-baselines",
}
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
    config_path = tmp_path / "strict_open_test.toml"
    config_path.write_text(Path("config/strict_open_v0.toml").read_text(encoding="utf-8"), encoding="utf-8")

    exit_code = main(
        [
            "strict-open",
            "ingest",
            "--config",
            str(config_path),
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
    source_manifest = json.loads((manifests_root / "tcp_source_manifest.json").read_text(encoding="utf-8"))
    run_manifest = json.loads((manifests_root / "tcp_ingest_run_manifest.json").read_text(encoding="utf-8"))
    expected_command = [
        "scz-audit",
        "strict-open",
        "ingest",
        "--config",
        str(config_path),
        "--source",
        "tcp",
        "--source-root",
        str(FIXTURE_SOURCE_ROOT),
        "--raw-root",
        str(raw_root),
        "--manifest-dir",
        str(manifests_root),
    ]
    assert source_manifest["command"] == expected_command
    assert run_manifest["command"] == expected_command


def test_audit_command_writes_tcp_profile(capsys: pytest.CaptureFixture[str], tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    profiles_root = tmp_path / "data" / "processed" / "strict_open" / "profiles"
    config_path = tmp_path / "strict_open_test.toml"
    config_path.write_text(Path("config/strict_open_v0.toml").read_text(encoding="utf-8"), encoding="utf-8")

    ingest_exit_code = main(
        [
            "strict-open",
            "ingest",
            "--config",
            str(config_path),
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
            "--config",
            str(config_path),
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
    audit_run_manifest = json.loads((manifests_root / "tcp_audit_run_manifest.json").read_text(encoding="utf-8"))
    audit_provenance = json.loads((manifests_root / "tcp_audit_provenance.json").read_text(encoding="utf-8"))
    expected_command = [
        "scz-audit",
        "strict-open",
        "audit",
        "--config",
        str(config_path),
        "--raw-root",
        str(raw_root),
        "--manifest-dir",
        str(manifests_root),
        "--profile-dir",
        str(profiles_root),
    ]
    assert audit_run_manifest["command"] == expected_command
    assert audit_provenance["command"] == expected_command


def test_harmonize_command_writes_canonical_outputs(capsys: pytest.CaptureFixture[str], tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    config_path = tmp_path / "strict_open_test.toml"
    config_path.write_text(Path("config/strict_open_v0.toml").read_text(encoding="utf-8"), encoding="utf-8")

    ingest_exit_code = main(
        [
            "strict-open",
            "ingest",
            "--config",
            str(config_path),
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

    harmonize_exit_code = main(
        [
            "strict-open",
            "harmonize",
            "--config",
            str(config_path),
            "--raw-root",
            str(raw_root),
            "--manifest-dir",
            str(manifests_root),
            "--output-dir",
            str(harmonized_root),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    harmonization_manifest = json.loads((harmonized_root / "harmonization_manifest.json").read_text(encoding="utf-8"))
    run_manifest = json.loads((manifests_root / "tcp_harmonize_run_manifest.json").read_text(encoding="utf-8"))
    expected_command = [
        "scz-audit",
        "strict-open",
        "harmonize",
        "--config",
        str(config_path),
        "--raw-root",
        str(raw_root),
        "--manifest-dir",
        str(manifests_root),
        "--output-dir",
        str(harmonized_root),
    ]

    assert harmonize_exit_code == 0
    assert Path(payload["subjects"]).exists()
    assert Path(payload["visits"]).exists()
    assert Path(payload["cognition_scores"]).exists()
    assert Path(payload["symptom_behavior_scores"]).exists()
    assert Path(payload["mri_features"]).exists()
    assert Path(payload["harmonization_manifest"]).exists()
    assert harmonization_manifest["command"] == expected_command
    assert run_manifest["command"] == expected_command


def test_define_splits_command_writes_split_outputs(capsys: pytest.CaptureFixture[str], tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits"
    config_path = tmp_path / "strict_open_test.toml"
    config_path.write_text(Path("config/strict_open_v0.toml").read_text(encoding="utf-8"), encoding="utf-8")

    ingest_exit_code = main(
        [
            "strict-open",
            "ingest",
            "--config",
            str(config_path),
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

    harmonize_exit_code = main(
        [
            "strict-open",
            "harmonize",
            "--config",
            str(config_path),
            "--raw-root",
            str(raw_root),
            "--manifest-dir",
            str(manifests_root),
            "--output-dir",
            str(harmonized_root),
        ]
    )
    assert harmonize_exit_code == 0
    capsys.readouterr()

    define_splits_exit_code = main(
        [
            "strict-open",
            "define-splits",
            "--config",
            str(config_path),
            "--harmonized-dir",
            str(harmonized_root),
            "--manifest-dir",
            str(manifests_root),
            "--output-dir",
            str(splits_root),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    split_manifest = json.loads((splits_root / "split_manifest.json").read_text(encoding="utf-8"))
    run_manifest = json.loads((manifests_root / "tcp_define_splits_run_manifest.json").read_text(encoding="utf-8"))
    expected_command = [
        "scz-audit",
        "strict-open",
        "define-splits",
        "--config",
        str(config_path),
        "--harmonized-dir",
        str(harmonized_root),
        "--manifest-dir",
        str(manifests_root),
        "--output-dir",
        str(splits_root),
    ]

    assert define_splits_exit_code == 0
    assert Path(payload["split_assignments"]).exists()
    assert Path(payload["split_manifest"]).exists()
    assert split_manifest["command"] == expected_command
    assert run_manifest["command"] == expected_command


def test_build_features_command_writes_feature_outputs(capsys: pytest.CaptureFixture[str], tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits"
    features_root = tmp_path / "data" / "processed" / "strict_open" / "features"
    config_path = tmp_path / "strict_open_test.toml"
    config_path.write_text(Path("config/strict_open_v0.toml").read_text(encoding="utf-8"), encoding="utf-8")

    _run_cli_pipeline_to_splits(
        capsys=capsys,
        config_path=config_path,
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
    )

    build_features_exit_code = main(
        [
            "strict-open",
            "build-features",
            "--config",
            str(config_path),
            "--harmonized-dir",
            str(harmonized_root),
            "--splits-dir",
            str(splits_root),
            "--manifest-dir",
            str(manifests_root),
            "--output-dir",
            str(features_root),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    feature_manifest = json.loads((features_root / "feature_manifest.json").read_text(encoding="utf-8"))
    run_manifest = json.loads((manifests_root / "tcp_build_features_run_manifest.json").read_text(encoding="utf-8"))
    expected_command = [
        "scz-audit",
        "strict-open",
        "build-features",
        "--config",
        str(config_path),
        "--harmonized-dir",
        str(harmonized_root),
        "--splits-dir",
        str(splits_root),
        "--manifest-dir",
        str(manifests_root),
        "--output-dir",
        str(features_root),
    ]

    assert build_features_exit_code == 0
    assert Path(payload["visit_features"]).exists()
    assert Path(payload["feature_manifest"]).exists()
    assert feature_manifest["command"] == expected_command
    assert run_manifest["command"] == expected_command


def test_build_targets_command_writes_target_outputs(capsys: pytest.CaptureFixture[str], tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits"
    features_root = tmp_path / "data" / "processed" / "strict_open" / "features"
    targets_root = tmp_path / "data" / "processed" / "strict_open" / "targets"
    config_path = tmp_path / "strict_open_test.toml"
    config_path.write_text(Path("config/strict_open_v0.toml").read_text(encoding="utf-8"), encoding="utf-8")

    _run_cli_pipeline_to_splits(
        capsys=capsys,
        config_path=config_path,
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
    )

    build_features_exit_code = main(
        [
            "strict-open",
            "build-features",
            "--config",
            str(config_path),
            "--harmonized-dir",
            str(harmonized_root),
            "--splits-dir",
            str(splits_root),
            "--manifest-dir",
            str(manifests_root),
            "--output-dir",
            str(features_root),
        ]
    )
    assert build_features_exit_code == 0
    capsys.readouterr()

    build_targets_exit_code = main(
        [
            "strict-open",
            "build-targets",
            "--config",
            str(config_path),
            "--features-dir",
            str(features_root),
            "--harmonized-dir",
            str(harmonized_root),
            "--splits-dir",
            str(splits_root),
            "--manifest-dir",
            str(manifests_root),
            "--output-dir",
            str(targets_root),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    target_manifest = json.loads((targets_root / "target_manifest.json").read_text(encoding="utf-8"))
    run_manifest = json.loads((manifests_root / "tcp_build_targets_run_manifest.json").read_text(encoding="utf-8"))
    expected_command = [
        "scz-audit",
        "strict-open",
        "build-targets",
        "--config",
        str(config_path),
        "--features-dir",
        str(features_root),
        "--harmonized-dir",
        str(harmonized_root),
        "--splits-dir",
        str(splits_root),
        "--manifest-dir",
        str(manifests_root),
        "--output-dir",
        str(targets_root),
    ]

    assert build_targets_exit_code == 0
    assert Path(payload["derived_targets"]).exists()
    assert Path(payload["target_manifest"]).exists()
    assert target_manifest["command"] == expected_command
    assert run_manifest["command"] == expected_command


def test_build_features_manifest_keeps_explicit_default_config_flag(capsys: pytest.CaptureFixture[str], tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits"
    features_root = tmp_path / "data" / "processed" / "strict_open" / "features"

    _run_cli_pipeline_to_splits(
        capsys=capsys,
        config_path=Path("config/strict_open_v0.toml"),
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
    )

    build_features_exit_code = main(
        [
            "strict-open",
            "build-features",
            "--config",
            "config/strict_open_v0.toml",
            "--harmonized-dir",
            str(harmonized_root),
            "--splits-dir",
            str(splits_root),
            "--manifest-dir",
            str(manifests_root),
            "--output-dir",
            str(features_root),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    feature_manifest = json.loads((features_root / "feature_manifest.json").read_text(encoding="utf-8"))

    assert build_features_exit_code == 0
    assert Path(payload["feature_manifest"]).exists()
    assert feature_manifest["command"][:5] == [
        "scz-audit",
        "strict-open",
        "build-features",
        "--config",
        "config/strict_open_v0.toml",
    ]


def test_train_baselines_command_writes_baseline_outputs(capsys: pytest.CaptureFixture[str], tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits"
    features_root = tmp_path / "data" / "processed" / "strict_open" / "features"
    targets_root = tmp_path / "data" / "processed" / "strict_open" / "targets"
    models_root = tmp_path / "data" / "processed" / "strict_open" / "models" / "baselines"
    reports_root = tmp_path / "data" / "processed" / "strict_open" / "reports"
    config_path = tmp_path / "strict_open_test.toml"
    config_path.write_text(Path("config/strict_open_v0.toml").read_text(encoding="utf-8"), encoding="utf-8")

    _run_cli_pipeline_to_targets(
        capsys=capsys,
        config_path=config_path,
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
        features_root=features_root,
        targets_root=targets_root,
    )

    train_baselines_exit_code = main(
        [
            "strict-open",
            "train-baselines",
            "--config",
            str(config_path),
            "--features-dir",
            str(features_root),
            "--targets-dir",
            str(targets_root),
            "--splits-dir",
            str(splits_root),
            "--manifest-dir",
            str(manifests_root),
            "--models-dir",
            str(models_root),
            "--reports-dir",
            str(reports_root),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    registry_payload = json.loads((models_root / "baseline_registry.json").read_text(encoding="utf-8"))
    run_manifest = json.loads((manifests_root / "tcp_train_baselines_run_manifest.json").read_text(encoding="utf-8"))
    expected_command = [
        "scz-audit",
        "strict-open",
        "train-baselines",
        "--config",
        str(config_path),
        "--features-dir",
        str(features_root),
        "--targets-dir",
        str(targets_root),
        "--splits-dir",
        str(splits_root),
        "--manifest-dir",
        str(manifests_root),
        "--models-dir",
        str(models_root),
        "--reports-dir",
        str(reports_root),
    ]

    assert train_baselines_exit_code == 0
    assert Path(payload["baseline_predictions"]).exists()
    assert Path(payload["baseline_registry"]).exists()
    assert Path(payload["baseline_summary_json"]).exists()
    assert Path(payload["baseline_summary_md"]).exists()
    assert registry_payload["command"] == expected_command
    assert run_manifest["command"] == expected_command


def _run_cli_pipeline_to_splits(
    *,
    capsys: pytest.CaptureFixture[str],
    config_path: Path,
    raw_root: Path,
    manifests_root: Path,
    harmonized_root: Path,
    splits_root: Path,
) -> None:
    ingest_exit_code = main(
        [
            "strict-open",
            "ingest",
            "--config",
            str(config_path),
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

    harmonize_exit_code = main(
        [
            "strict-open",
            "harmonize",
            "--config",
            str(config_path),
            "--raw-root",
            str(raw_root),
            "--manifest-dir",
            str(manifests_root),
            "--output-dir",
            str(harmonized_root),
        ]
    )
    assert harmonize_exit_code == 0
    capsys.readouterr()

    define_splits_exit_code = main(
        [
            "strict-open",
            "define-splits",
            "--config",
            str(config_path),
            "--harmonized-dir",
            str(harmonized_root),
            "--manifest-dir",
            str(manifests_root),
            "--output-dir",
            str(splits_root),
        ]
    )
    assert define_splits_exit_code == 0
    capsys.readouterr()


def _run_cli_pipeline_to_targets(
    *,
    capsys: pytest.CaptureFixture[str],
    config_path: Path,
    raw_root: Path,
    manifests_root: Path,
    harmonized_root: Path,
    splits_root: Path,
    features_root: Path,
    targets_root: Path,
) -> None:
    _run_cli_pipeline_to_splits(
        capsys=capsys,
        config_path=config_path,
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        splits_root=splits_root,
    )

    build_features_exit_code = main(
        [
            "strict-open",
            "build-features",
            "--config",
            str(config_path),
            "--harmonized-dir",
            str(harmonized_root),
            "--splits-dir",
            str(splits_root),
            "--manifest-dir",
            str(manifests_root),
            "--output-dir",
            str(features_root),
        ]
    )
    assert build_features_exit_code == 0
    capsys.readouterr()

    build_targets_exit_code = main(
        [
            "strict-open",
            "build-targets",
            "--config",
            str(config_path),
            "--features-dir",
            str(features_root),
            "--harmonized-dir",
            str(harmonized_root),
            "--splits-dir",
            str(splits_root),
            "--manifest-dir",
            str(manifests_root),
            "--output-dir",
            str(targets_root),
        ]
    )
    assert build_targets_exit_code == 0
    capsys.readouterr()

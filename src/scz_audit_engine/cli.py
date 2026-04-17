"""Command-line interface for the scz audit engine."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
import tomllib

from . import __version__
from .strict_open import (
    BASELINE_FAMILY_NAMES,
    build_source_manifest,
    build_run_manifest,
    run_strict_open_baseline_training,
    run_strict_open_feature_build,
    run_strict_open_split_definition,
    run_strict_open_target_build,
    run_tcp_harmonization,
    run_tcp_audit,
    strict_open_paths,
    write_run_manifest,
    write_source_manifest,
)
from .strict_open.provenance import resolve_git_sha
from .strict_open.sources import TCPDS005237SourceAdapter

STRICT_OPEN_COMMANDS = (
    "ingest",
    "audit",
    "harmonize",
    "define-splits",
    "build-features",
    "build-targets",
    "train-baselines",
    "train",
    "eval",
    "report",
)

DEFAULT_CONFIG_PATH = "config/strict_open_v0.toml"
DEFAULT_TCP_SOURCE = "tcp"


def _build_help_handler(parser: argparse.ArgumentParser) -> Callable[[argparse.Namespace], int]:
    def handler(_args: argparse.Namespace) -> int:
        parser.print_help()
        return 0

    return handler


def _build_stub_handler(command_name: str) -> Callable[[argparse.Namespace], int]:
    def handler(_args: argparse.Namespace) -> int:
        print(
            f"strict-open {command_name} is not implemented yet in PR2; this command is a bootstrap stub.",
            file=sys.stderr,
        )
        return 1

    return handler


def _load_strict_open_config(config_path: str | Path) -> dict[str, object]:
    with Path(config_path).open("rb") as handle:
        return tomllib.load(handle)


def _resolve_path(value: str | None, *, repo_root: Path, fallback: Path) -> Path:
    if not value:
        return fallback
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate
    return (repo_root / candidate).resolve()


def _build_tcp_adapter(config: dict[str, object]) -> TCPDS005237SourceAdapter:
    source_config = config.get("sources", {})
    tcp_config = source_config.get("tcp", {}) if isinstance(source_config, dict) else {}
    if not isinstance(tcp_config, dict):
        tcp_config = {}
    return TCPDS005237SourceAdapter(
        dataset_version=tcp_config.get("dataset_version", TCPDS005237SourceAdapter.dataset_version),
        github_repo=tcp_config.get("github_repo", TCPDS005237SourceAdapter.github_repo),
        github_ref=tcp_config.get("github_ref", TCPDS005237SourceAdapter.github_ref),
    )


def _append_flag(command: list[str], flag: str, value: str | None) -> None:
    if value is None:
        return
    command.extend([flag, value])


def _build_invoked_command(command_name: str, args: argparse.Namespace) -> list[str]:
    command = ["scz-audit", "strict-open", command_name]
    config_path = getattr(args, "config", None)
    config_explicit = bool(getattr(args, "_config_explicit", False))
    if config_path is not None and (config_explicit or config_path != DEFAULT_CONFIG_PATH):
        _append_flag(command, "--config", str(config_path))

    if command_name == "ingest":
        _append_flag(command, "--source", str(getattr(args, "source", DEFAULT_TCP_SOURCE)))
        _append_flag(command, "--source-root", getattr(args, "source_root", None))
        _append_flag(command, "--raw-root", getattr(args, "raw_root", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
        return command

    if command_name == "audit":
        _append_flag(command, "--raw-root", getattr(args, "raw_root", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
        _append_flag(command, "--profile-dir", getattr(args, "profile_dir", None))
        return command

    if command_name == "harmonize":
        _append_flag(command, "--raw-root", getattr(args, "raw_root", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
        _append_flag(command, "--output-dir", getattr(args, "output_dir", None))
        return command

    if command_name == "define-splits":
        _append_flag(command, "--harmonized-dir", getattr(args, "harmonized_dir", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
        _append_flag(command, "--output-dir", getattr(args, "output_dir", None))
        return command

    if command_name == "build-features":
        _append_flag(command, "--harmonized-dir", getattr(args, "harmonized_dir", None))
        _append_flag(command, "--splits-dir", getattr(args, "splits_dir", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
        _append_flag(command, "--output-dir", getattr(args, "output_dir", None))
        return command

    if command_name == "build-targets":
        _append_flag(command, "--features-dir", getattr(args, "features_dir", None))
        _append_flag(command, "--harmonized-dir", getattr(args, "harmonized_dir", None))
        _append_flag(command, "--splits-dir", getattr(args, "splits_dir", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
        _append_flag(command, "--output-dir", getattr(args, "output_dir", None))
        return command

    if command_name == "train-baselines":
        _append_flag(command, "--features-dir", getattr(args, "features_dir", None))
        _append_flag(command, "--targets-dir", getattr(args, "targets_dir", None))
        _append_flag(command, "--splits-dir", getattr(args, "splits_dir", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
        _append_flag(command, "--models-dir", getattr(args, "models_dir", None))
        _append_flag(command, "--reports-dir", getattr(args, "reports_dir", None))
        return command

    return command


def _build_ingest_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        if args.source != DEFAULT_TCP_SOURCE:
            print("Only --source tcp is implemented in PR3.", file=sys.stderr)
            return 2

        path_contract = strict_open_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_strict_open_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}

        raw_base_root = _resolve_path(
            paths_config.get("raw_root"),
            repo_root=repo_root,
            fallback=path_contract.raw_root,
        )
        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        raw_root = Path(args.raw_root).resolve() if args.raw_root else raw_base_root / args.source
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()

        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        adapter = _build_tcp_adapter(config)
        stage_result = adapter.stage(raw_root, source_root=args.source_root)

        command = _build_invoked_command("ingest", args)
        source_manifest_path = manifests_root / "tcp_source_manifest.json"
        source_manifest = build_source_manifest(
            source=stage_result.source,
            source_identifier=stage_result.source_identifier,
            dataset_accession=stage_result.dataset_accession,
            dataset_version=stage_result.dataset_version,
            command=command,
            git_sha=git_sha,
            raw_root=stage_result.raw_root,
            files=stage_result.files,
        )
        write_source_manifest(source_manifest, source_manifest_path)

        run_manifest_path = manifests_root / "tcp_ingest_run_manifest.json"
        run_manifest = build_run_manifest(
            dataset_source=stage_result.source,
            dataset_version=stage_result.dataset_version,
            command=command,
            git_sha=git_sha,
            seed=seed,
            output_paths={
                "raw_root": stage_result.raw_root,
                "source_manifest": source_manifest_path,
            },
        )
        write_run_manifest(run_manifest, run_manifest_path)

        print(
            json.dumps(
                {
                    "files_discovered": len(stage_result.files),
                    "raw_root": str(stage_result.raw_root),
                    "run_manifest": str(run_manifest_path),
                    "source_manifest": str(source_manifest_path),
                    "source": args.source,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    return handler


def _build_audit_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        path_contract = strict_open_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_strict_open_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}

        raw_base_root = _resolve_path(
            paths_config.get("raw_root"),
            repo_root=repo_root,
            fallback=path_contract.raw_root,
        )
        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        profiles_root = _resolve_path(
            paths_config.get("profiles_root"),
            repo_root=repo_root,
            fallback=path_contract.profiles_root,
        )
        raw_root = Path(args.raw_root).resolve() if args.raw_root else raw_base_root / DEFAULT_TCP_SOURCE
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()
        if args.profile_dir:
            profiles_root = Path(args.profile_dir).resolve()

        adapter = _build_tcp_adapter(config)
        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        command = _build_invoked_command("audit", args)
        results = run_tcp_audit(
            raw_root=raw_root,
            manifests_root=manifests_root,
            profiles_root=profiles_root,
            command=command,
            git_sha=git_sha,
            seed=seed,
            dataset_version=adapter.dataset_version,
            source_manifest_path=manifests_root / "tcp_source_manifest.json",
        )
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    return handler


def _build_harmonize_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        path_contract = strict_open_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_strict_open_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}

        raw_base_root = _resolve_path(
            paths_config.get("raw_root"),
            repo_root=repo_root,
            fallback=path_contract.raw_root,
        )
        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        harmonized_root = _resolve_path(
            paths_config.get("harmonized_root"),
            repo_root=repo_root,
            fallback=path_contract.harmonized_root,
        )
        raw_root = Path(args.raw_root).resolve() if args.raw_root else raw_base_root / DEFAULT_TCP_SOURCE
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()
        if args.output_dir:
            harmonized_root = Path(args.output_dir).resolve()

        adapter = _build_tcp_adapter(config)
        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        command = _build_invoked_command("harmonize", args)
        results = run_tcp_harmonization(
            raw_root=raw_root,
            manifests_root=manifests_root,
            harmonized_root=harmonized_root,
            command=command,
            git_sha=git_sha,
            seed=seed,
            dataset_version=adapter.dataset_version,
            source_manifest_path=manifests_root / "tcp_source_manifest.json",
        )
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    return handler


def _build_define_splits_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        path_contract = strict_open_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_strict_open_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}
        split_config = config.get("splits", {})
        if not isinstance(split_config, dict):
            split_config = {}

        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        harmonized_root = _resolve_path(
            paths_config.get("harmonized_root"),
            repo_root=repo_root,
            fallback=path_contract.harmonized_root,
        )
        splits_root = _resolve_path(
            paths_config.get("splits_root"),
            repo_root=repo_root,
            fallback=path_contract.splits_root,
        )
        if args.harmonized_dir:
            harmonized_root = Path(args.harmonized_dir).resolve()
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()
        if args.output_dir:
            splits_root = Path(args.output_dir).resolve()

        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        command = _build_invoked_command("define-splits", args)
        split_fractions = {
            "train": float(split_config.get("train_fraction", 0.6)),
            "validation": float(split_config.get("validation_fraction", 0.2)),
            "test": float(split_config.get("test_fraction", 0.2)),
        }
        results = run_strict_open_split_definition(
            harmonized_root=harmonized_root,
            manifests_root=manifests_root,
            splits_root=splits_root,
            command=command,
            git_sha=git_sha,
            seed=seed,
            split_fractions=split_fractions,
        )
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    return handler


def _build_build_features_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        path_contract = strict_open_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_strict_open_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}

        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        harmonized_root = _resolve_path(
            paths_config.get("harmonized_root"),
            repo_root=repo_root,
            fallback=path_contract.harmonized_root,
        )
        splits_root = _resolve_path(
            paths_config.get("splits_root"),
            repo_root=repo_root,
            fallback=path_contract.splits_root,
        )
        features_root = _resolve_path(
            paths_config.get("features_root"),
            repo_root=repo_root,
            fallback=path_contract.features_root,
        )
        if args.harmonized_dir:
            harmonized_root = Path(args.harmonized_dir).resolve()
        if args.splits_dir:
            splits_root = Path(args.splits_dir).resolve()
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()
        if args.output_dir:
            features_root = Path(args.output_dir).resolve()

        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        command = _build_invoked_command("build-features", args)
        results = run_strict_open_feature_build(
            harmonized_root=harmonized_root,
            splits_root=splits_root,
            manifests_root=manifests_root,
            features_root=features_root,
            command=command,
            git_sha=git_sha,
            seed=seed,
        )
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    return handler


def _build_build_targets_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        path_contract = strict_open_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_strict_open_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}

        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        harmonized_root = _resolve_path(
            paths_config.get("harmonized_root"),
            repo_root=repo_root,
            fallback=path_contract.harmonized_root,
        )
        splits_root = _resolve_path(
            paths_config.get("splits_root"),
            repo_root=repo_root,
            fallback=path_contract.splits_root,
        )
        features_root = _resolve_path(
            paths_config.get("features_root"),
            repo_root=repo_root,
            fallback=path_contract.features_root,
        )
        targets_root = _resolve_path(
            paths_config.get("targets_root"),
            repo_root=repo_root,
            fallback=path_contract.targets_root,
        )
        if args.features_dir:
            features_root = Path(args.features_dir).resolve()
        if args.harmonized_dir:
            harmonized_root = Path(args.harmonized_dir).resolve()
        if args.splits_dir:
            splits_root = Path(args.splits_dir).resolve()
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()
        if args.output_dir:
            targets_root = Path(args.output_dir).resolve()

        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        command = _build_invoked_command("build-targets", args)
        results = run_strict_open_target_build(
            features_root=features_root,
            harmonized_root=harmonized_root,
            splits_root=splits_root,
            manifests_root=manifests_root,
            targets_root=targets_root,
            command=command,
            git_sha=git_sha,
            seed=seed,
        )
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    return handler


def _build_train_baselines_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        path_contract = strict_open_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_strict_open_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}

        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        features_root = _resolve_path(
            paths_config.get("features_root"),
            repo_root=repo_root,
            fallback=path_contract.features_root,
        )
        targets_root = _resolve_path(
            paths_config.get("targets_root"),
            repo_root=repo_root,
            fallback=path_contract.targets_root,
        )
        splits_root = _resolve_path(
            paths_config.get("splits_root"),
            repo_root=repo_root,
            fallback=path_contract.splits_root,
        )
        models_root = _resolve_path(
            paths_config.get("models_root"),
            repo_root=repo_root,
            fallback=path_contract.models_root,
        )
        reports_root = _resolve_path(
            paths_config.get("reports_root"),
            repo_root=repo_root,
            fallback=path_contract.reports_root,
        )
        if args.features_dir:
            features_root = Path(args.features_dir).resolve()
        if args.targets_dir:
            targets_root = Path(args.targets_dir).resolve()
        if args.splits_dir:
            splits_root = Path(args.splits_dir).resolve()
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()
        models_dir = Path(args.models_dir).resolve() if args.models_dir else models_root / "baselines"
        reports_dir = Path(args.reports_dir).resolve() if args.reports_dir else reports_root

        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        command = _build_invoked_command("train-baselines", args)
        results = run_strict_open_baseline_training(
            features_root=features_root,
            targets_root=targets_root,
            splits_root=splits_root,
            manifests_root=manifests_root,
            models_root=models_dir,
            reports_root=reports_dir,
            command=command,
            git_sha=git_sha,
            seed=seed,
        )
        print(json.dumps(results, indent=2, sort_keys=True))
        return 0

    return handler


def build_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser."""

    parser = argparse.ArgumentParser(
        prog="scz-audit",
        description="Bootstrap CLI for the strict-open v0 cognitive stability and trial-noise audit engine.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.set_defaults(handler=_build_help_handler(parser))

    subparsers = parser.add_subparsers(dest="command")
    strict_open_parser = subparsers.add_parser(
        "strict-open",
        help="Commands for the strict-open v0 namespace.",
        description="Bootstrap commands for the strict-open v0 cohort stability and noise audit engine.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    strict_open_parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Path to the strict-open v0 config file.",
    )
    strict_open_parser.set_defaults(handler=_build_help_handler(strict_open_parser))

    strict_open_subparsers = strict_open_parser.add_subparsers(dest="strict_open_command")
    for command_name in STRICT_OPEN_COMMANDS:
        command_parser = strict_open_subparsers.add_parser(
            command_name,
            help=f"Stub command for strict-open {command_name}.",
            description=f"Stub command for strict-open {command_name}.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        command_parser.add_argument(
            "--config",
            default=DEFAULT_CONFIG_PATH,
            help="Path to the strict-open v0 config file.",
        )
        if command_name == "ingest":
            command_parser.add_argument(
                "--source",
                default=DEFAULT_TCP_SOURCE,
                choices=[DEFAULT_TCP_SOURCE],
                help="Public source to ingest.",
            )
            command_parser.add_argument(
                "--source-root",
                help="Optional local TCP source directory to stage instead of fetching public metadata.",
            )
            command_parser.add_argument(
                "--raw-root",
                help="Destination raw root for the staged source.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for source and run manifests.",
            )
            command_parser.set_defaults(handler=_build_ingest_handler())
        elif command_name == "audit":
            command_parser.add_argument(
                "--raw-root",
                help="Raw TCP root to audit.",
            )
            command_parser.add_argument(
                "--profile-dir",
                help="Destination directory for audit profiles.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for audit manifests and provenance.",
            )
            command_parser.set_defaults(handler=_build_audit_handler())
        elif command_name == "harmonize":
            command_parser.add_argument(
                "--raw-root",
                help="Raw TCP root to harmonize.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for run manifests.",
            )
            command_parser.add_argument(
                "--output-dir",
                help="Destination directory for harmonized outputs.",
            )
            command_parser.set_defaults(handler=_build_harmonize_handler())
        elif command_name == "define-splits":
            command_parser.add_argument(
                "--harmonized-dir",
                help="Directory containing harmonized strict-open tables.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for run manifests.",
            )
            command_parser.add_argument(
                "--output-dir",
                help="Destination directory for split outputs.",
            )
            command_parser.set_defaults(handler=_build_define_splits_handler())
        elif command_name == "build-features":
            command_parser.add_argument(
                "--harmonized-dir",
                help="Directory containing harmonized strict-open tables.",
            )
            command_parser.add_argument(
                "--splits-dir",
                help="Directory containing frozen strict-open split assignments.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for run manifests.",
            )
            command_parser.add_argument(
                "--output-dir",
                help="Destination directory for feature outputs.",
            )
            command_parser.set_defaults(handler=_build_build_features_handler())
        elif command_name == "build-targets":
            command_parser.add_argument(
                "--features-dir",
                help="Directory containing strict-open feature outputs.",
            )
            command_parser.add_argument(
                "--harmonized-dir",
                help="Directory containing harmonized strict-open tables.",
            )
            command_parser.add_argument(
                "--splits-dir",
                help="Directory containing frozen strict-open split assignments.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for run manifests.",
            )
            command_parser.add_argument(
                "--output-dir",
                help="Destination directory for derived target outputs.",
            )
            command_parser.set_defaults(handler=_build_build_targets_handler())
        elif command_name == "train-baselines":
            command_parser.description = (
                "Train deterministic strict-open baseline families and evaluate them by frozen split."
            )
            command_parser.add_argument(
                "--features-dir",
                help="Directory containing strict-open feature outputs.",
            )
            command_parser.add_argument(
                "--targets-dir",
                help="Directory containing strict-open derived target outputs.",
            )
            command_parser.add_argument(
                "--splits-dir",
                help="Directory containing frozen strict-open split assignments.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for run manifests.",
            )
            command_parser.add_argument(
                "--models-dir",
                help="Destination directory for baseline model artifacts.",
            )
            command_parser.add_argument(
                "--reports-dir",
                help="Destination directory for baseline summary artifacts.",
            )
            command_parser.epilog = (
                "Required baseline families: " + ", ".join(BASELINE_FAMILY_NAMES) + "."
            )
            command_parser.set_defaults(handler=_build_train_baselines_handler())
        else:
            command_parser.set_defaults(handler=_build_stub_handler(command_name))

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return an exit code."""

    parser = build_parser()
    argv_list = list(argv) if argv is not None else None
    args = parser.parse_args(argv_list)
    tokens = argv_list or []
    args._config_explicit = any(
        token == "--config" or token.startswith("--config=")
        for token in tokens
    )
    handler: Callable[[argparse.Namespace], int] = args.handler
    return handler(args)


if __name__ == "__main__":
    raise SystemExit(main())

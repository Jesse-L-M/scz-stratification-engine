"""Command-line interface for the scz audit engine."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
import tomllib

from . import __version__
from .benchmark import (
    benchmark_paths,
    run_benchmark_dataset_audit,
    run_benchmark_define_schema,
    run_benchmark_harmonization,
    run_benchmark_representation_build,
    run_cross_sectional_benchmark,
)
from .strict_open import (
    build_source_manifest,
    build_run_manifest,
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

BENCHMARK_COMMANDS = (
    "audit-datasets",
    "define-schema",
    "harmonize",
    "build-representations",
    "run-benchmark",
    "report",
)
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

DEFAULT_BENCHMARK_CONFIG_PATH = "config/benchmark_v0.toml"
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


def _build_benchmark_stub_handler(command_name: str) -> Callable[[argparse.Namespace], int]:
    def handler(_args: argparse.Namespace) -> int:
        print(
            f"benchmark {command_name} is not implemented yet; this command remains a roadmap stub.",
            file=sys.stderr,
        )
        return 1

    return handler


def _load_toml_config(config_path: str | Path) -> dict[str, object]:
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

    return command


def _build_benchmark_invoked_command(command_name: str, args: argparse.Namespace) -> list[str]:
    command = ["scz-audit", "benchmark", command_name]
    config_path = getattr(args, "config", None)
    config_explicit = bool(getattr(args, "_config_explicit", False))
    if config_path is not None and (config_explicit or config_path != DEFAULT_BENCHMARK_CONFIG_PATH):
        _append_flag(command, "--config", str(config_path))

    if command_name == "audit-datasets":
        _append_flag(command, "--registry-path", getattr(args, "registry_path", None))
        _append_flag(command, "--reports-dir", getattr(args, "reports_dir", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
        return command

    if command_name == "define-schema":
        _append_flag(command, "--output-dir", getattr(args, "output_dir", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
        return command

    if command_name == "harmonize":
        _append_flag(command, "--raw-root", getattr(args, "raw_root", None))
        _append_flag(command, "--output-dir", getattr(args, "output_dir", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
        return command

    if command_name == "build-representations":
        _append_flag(command, "--harmonized-dir", getattr(args, "harmonized_dir", None))
        _append_flag(command, "--output-dir", getattr(args, "output_dir", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
        return command

    if command_name == "run-benchmark":
        _append_flag(command, "--representations-dir", getattr(args, "representations_dir", None))
        _append_flag(command, "--harmonized-dir", getattr(args, "harmonized_dir", None))
        _append_flag(command, "--output-dir", getattr(args, "output_dir", None))
        _append_flag(command, "--manifest-dir", getattr(args, "manifest_dir", None))
    return command


def _build_benchmark_audit_datasets_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        path_contract = benchmark_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_toml_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}

        registry_fallback = _resolve_path(
            config.get("dataset_registry_path"),
            repo_root=repo_root,
            fallback=path_contract.dataset_registry_path,
        )
        reports_root = _resolve_path(
            paths_config.get("reports_root"),
            repo_root=repo_root,
            fallback=path_contract.reports_root,
        )
        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        registry_path = Path(args.registry_path).resolve() if args.registry_path else registry_fallback
        if args.reports_dir:
            reports_root = Path(args.reports_dir).resolve()
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()

        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        artifacts = run_benchmark_dataset_audit(
            registry_path=registry_path,
            reports_root=reports_root,
            manifests_root=manifests_root,
            repo_root=repo_root,
            command=_build_benchmark_invoked_command("audit-datasets", args),
            git_sha=git_sha,
            seed=seed,
        )
        print(json.dumps(artifacts.to_summary(), indent=2, sort_keys=True))
        return 0

    return handler


def _build_benchmark_define_schema_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        path_contract = benchmark_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_toml_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}

        schema_root = _resolve_path(
            paths_config.get("schema_root"),
            repo_root=repo_root,
            fallback=path_contract.schema_root,
        )
        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        if args.output_dir:
            schema_root = Path(args.output_dir).resolve()
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()

        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        artifacts = run_benchmark_define_schema(
            schema_root=schema_root,
            manifests_root=manifests_root,
            repo_root=repo_root,
            command=_build_benchmark_invoked_command("define-schema", args),
            git_sha=git_sha,
            seed=seed,
        )
        print(json.dumps(artifacts.to_summary(), indent=2, sort_keys=True))
        return 0

    return handler


def _build_benchmark_harmonize_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        path_contract = benchmark_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_toml_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}

        raw_root = _resolve_path(
            paths_config.get("raw_root"),
            repo_root=repo_root,
            fallback=path_contract.raw_root,
        )
        harmonized_root = _resolve_path(
            paths_config.get("harmonized_root"),
            repo_root=repo_root,
            fallback=path_contract.harmonized_root,
        )
        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        if args.raw_root:
            raw_root = Path(args.raw_root).resolve()
        if args.output_dir:
            harmonized_root = Path(args.output_dir).resolve()
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()

        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        artifacts = run_benchmark_harmonization(
            raw_root=raw_root,
            harmonized_root=harmonized_root,
            manifests_root=manifests_root,
            repo_root=repo_root,
            command=_build_benchmark_invoked_command("harmonize", args),
            git_sha=git_sha,
            seed=seed,
        )
        print(json.dumps(artifacts.to_summary(), indent=2, sort_keys=True))
        return 0

    return handler


def _build_benchmark_build_representations_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        path_contract = benchmark_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_toml_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}

        harmonized_root = _resolve_path(
            paths_config.get("harmonized_root"),
            repo_root=repo_root,
            fallback=path_contract.harmonized_root,
        )
        representations_root = _resolve_path(
            paths_config.get("representations_root"),
            repo_root=repo_root,
            fallback=path_contract.representations_root,
        )
        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        if args.harmonized_dir:
            harmonized_root = Path(args.harmonized_dir).resolve()
        if args.output_dir:
            representations_root = Path(args.output_dir).resolve()
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()

        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        artifacts = run_benchmark_representation_build(
            harmonized_root=harmonized_root,
            representations_root=representations_root,
            manifests_root=manifests_root,
            repo_root=repo_root,
            command=_build_benchmark_invoked_command("build-representations", args),
            git_sha=git_sha,
            seed=seed,
        )
        print(json.dumps(artifacts.to_summary(), indent=2, sort_keys=True))
        return 0

    return handler


def _build_benchmark_run_benchmark_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        path_contract = benchmark_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_toml_config(config_path)
        paths_config = config.get("paths", {})
        if not isinstance(paths_config, dict):
            paths_config = {}

        harmonized_root = _resolve_path(
            paths_config.get("harmonized_root"),
            repo_root=repo_root,
            fallback=path_contract.harmonized_root,
        )
        representations_root = _resolve_path(
            paths_config.get("representations_root"),
            repo_root=repo_root,
            fallback=path_contract.representations_root,
        )
        benchmarks_root = _resolve_path(
            paths_config.get("benchmarks_root"),
            repo_root=repo_root,
            fallback=path_contract.benchmarks_root,
        )
        manifests_root = _resolve_path(
            paths_config.get("manifests_root"),
            repo_root=repo_root,
            fallback=path_contract.manifests_root,
        )
        if args.harmonized_dir:
            harmonized_root = Path(args.harmonized_dir).resolve()
        if args.representations_dir:
            representations_root = Path(args.representations_dir).resolve()
        if args.output_dir:
            benchmarks_root = Path(args.output_dir).resolve()
        if args.manifest_dir:
            manifests_root = Path(args.manifest_dir).resolve()

        seed = int(config.get("seed", 1729))
        git_sha = resolve_git_sha(repo_root)
        artifacts = run_cross_sectional_benchmark(
            harmonized_root=harmonized_root,
            representations_root=representations_root,
            benchmarks_root=benchmarks_root,
            manifests_root=manifests_root,
            repo_root=repo_root,
            command=_build_benchmark_invoked_command("run-benchmark", args),
            git_sha=git_sha,
            seed=seed,
        )
        print(json.dumps(artifacts.to_summary(), indent=2, sort_keys=True))
        return 0

    return handler


def _build_ingest_handler() -> Callable[[argparse.Namespace], int]:
    def handler(args: argparse.Namespace) -> int:
        if args.source != DEFAULT_TCP_SOURCE:
            print("Only --source tcp is implemented in PR3.", file=sys.stderr)
            return 2

        path_contract = strict_open_paths()
        repo_root = path_contract.repo_root
        config_path = _resolve_path(args.config, repo_root=repo_root, fallback=path_contract.config_path)
        config = _load_toml_config(config_path)
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
        config = _load_toml_config(config_path)
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
        config = _load_toml_config(config_path)
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
        config = _load_toml_config(config_path)
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
        config = _load_toml_config(config_path)
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
        config = _load_toml_config(config_path)
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


def build_parser() -> argparse.ArgumentParser:
    """Create the top-level CLI parser."""

    parser = argparse.ArgumentParser(
        prog="scz-audit",
        description=(
            "CLI for the benchmark mainline, with strict-open preserved as exploratory infrastructure."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.set_defaults(handler=_build_help_handler(parser))

    subparsers = parser.add_subparsers(dest="command")
    benchmark_parser = subparsers.add_parser(
        "benchmark",
        help="Mainline benchmark commands.",
        description=(
            "Commands for the benchmark feasibility gate, schema, harmonization contract, "
            "and cross-sectional representation artifacts."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    benchmark_parser.add_argument(
        "--config",
        default=DEFAULT_BENCHMARK_CONFIG_PATH,
        help="Path to the benchmark v0 config file.",
    )
    benchmark_parser.set_defaults(handler=_build_help_handler(benchmark_parser))

    benchmark_subparsers = benchmark_parser.add_subparsers(dest="benchmark_command")
    for command_name in BENCHMARK_COMMANDS:
        command_parser = benchmark_subparsers.add_parser(
            command_name,
            help=f"Command for benchmark {command_name}.",
            description=f"Command for benchmark {command_name}.",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        command_parser.add_argument(
            "--config",
            default=DEFAULT_BENCHMARK_CONFIG_PATH,
            help="Path to the benchmark v0 config file.",
        )
        if command_name == "audit-datasets":
            command_parser.add_argument(
                "--registry-path",
                help="Destination path for the checked-in benchmark dataset registry CSV.",
            )
            command_parser.add_argument(
                "--reports-dir",
                help="Destination directory for dataset-audit report artifacts.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for benchmark run manifests.",
            )
            command_parser.set_defaults(handler=_build_benchmark_audit_datasets_handler())
        elif command_name == "define-schema":
            command_parser.add_argument(
                "--output-dir",
                help="Destination directory for benchmark schema artifacts.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for benchmark run manifests.",
            )
            command_parser.set_defaults(handler=_build_benchmark_define_schema_handler())
        elif command_name == "harmonize":
            command_parser.add_argument(
                "--raw-root",
                help="Root containing staged benchmark cohort directories or fixture roots.",
            )
            command_parser.add_argument(
                "--output-dir",
                help="Destination directory for harmonized benchmark tables.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for benchmark run manifests.",
            )
            command_parser.set_defaults(handler=_build_benchmark_harmonize_handler())
        elif command_name == "build-representations":
            command_parser.add_argument(
                "--harmonized-dir",
                help="Directory containing harmonized benchmark tables.",
            )
            command_parser.add_argument(
                "--output-dir",
                help="Destination directory for benchmark representation artifacts.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for benchmark run manifests.",
            )
            command_parser.set_defaults(handler=_build_benchmark_build_representations_handler())
        elif command_name == "run-benchmark":
            command_parser.add_argument(
                "--representations-dir",
                help="Directory containing benchmark representation artifacts.",
            )
            command_parser.add_argument(
                "--harmonized-dir",
                help="Directory containing harmonized benchmark tables.",
            )
            command_parser.add_argument(
                "--output-dir",
                help="Destination directory for cross-sectional benchmark result artifacts.",
            )
            command_parser.add_argument(
                "--manifest-dir",
                help="Destination directory for benchmark run manifests.",
            )
            command_parser.set_defaults(handler=_build_benchmark_run_benchmark_handler())
        else:
            command_parser.set_defaults(handler=_build_benchmark_stub_handler(command_name))

    strict_open_parser = subparsers.add_parser(
        "strict-open",
        help="Commands for the strict-open v0 namespace.",
        description=(
            "Bootstrap commands for the strict-open v0 cohort stability and noise audit engine. "
            "This namespace remains exploratory infrastructure."
        ),
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

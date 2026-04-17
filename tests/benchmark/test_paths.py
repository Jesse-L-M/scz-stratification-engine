from pathlib import Path

from scz_audit_engine.benchmark.paths import BenchmarkPaths, benchmark_paths


def test_default_path_helpers_resolve_under_repo_root() -> None:
    paths = benchmark_paths()
    repo_root = Path(__file__).resolve().parents[2]

    assert paths.repo_root == repo_root
    assert paths.raw_root == repo_root / "data" / "raw" / "benchmark"
    assert paths.processed_root == repo_root / "data" / "processed" / "benchmark"
    assert paths.curated_root == repo_root / "data" / "curated" / "benchmark"
    assert paths.manifests_root == repo_root / "data" / "processed" / "benchmark" / "manifests"
    assert paths.examples_root == repo_root / "examples" / "benchmark_v0"
    assert paths.config_path == repo_root / "config" / "benchmark_v0.toml"
    assert paths.default_manifest_path() == repo_root / "data" / "processed" / "benchmark" / "manifests" / "run_manifest.json"


def test_explicit_repo_root_keeps_paths_deterministic() -> None:
    repo_root = Path("/tmp/benchmark-repo")
    paths = BenchmarkPaths(repo_root=repo_root)

    assert paths.output_roots() == {
        "raw": repo_root / "data" / "raw" / "benchmark",
        "processed": repo_root / "data" / "processed" / "benchmark",
        "curated": repo_root / "data" / "curated" / "benchmark",
        "manifests": repo_root / "data" / "processed" / "benchmark" / "manifests",
        "examples": repo_root / "examples" / "benchmark_v0",
    }
    assert paths.default_manifest_path("cohort_a_manifest.json") == (
        repo_root / "data" / "processed" / "benchmark" / "manifests" / "cohort_a_manifest.json"
    )

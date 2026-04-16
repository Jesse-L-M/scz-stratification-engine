from pathlib import Path

from scz_audit_engine.strict_open.paths import StrictOpenPaths, strict_open_paths


def test_default_path_helpers_resolve_under_repo_root() -> None:
    paths = strict_open_paths()
    repo_root = Path(__file__).resolve().parents[2]

    assert paths.repo_root == repo_root
    assert paths.raw_root == repo_root / "data" / "raw" / "strict_open"
    assert paths.processed_root == repo_root / "data" / "processed" / "strict_open"
    assert paths.curated_root == repo_root / "data" / "curated" / "strict_open"
    assert paths.manifests_root == repo_root / "data" / "processed" / "strict_open" / "manifests"
    assert paths.profiles_root == repo_root / "data" / "processed" / "strict_open" / "profiles"
    assert paths.examples_root == repo_root / "examples" / "strict_open_v0"
    assert paths.config_path == repo_root / "config" / "strict_open_v0.toml"
    assert paths.source_raw_root("tcp") == repo_root / "data" / "raw" / "strict_open" / "tcp"
    assert paths.default_profile_path() == repo_root / "data" / "processed" / "strict_open" / "profiles" / "audit_profile.json"


def test_explicit_repo_root_keeps_paths_deterministic() -> None:
    repo_root = Path("/tmp/strict-open-repo")
    paths = StrictOpenPaths(repo_root=repo_root)

    assert paths.output_roots() == {
        "raw": repo_root / "data" / "raw" / "strict_open",
        "processed": repo_root / "data" / "processed" / "strict_open",
        "curated": repo_root / "data" / "curated" / "strict_open",
        "manifests": repo_root / "data" / "processed" / "strict_open" / "manifests",
        "profiles": repo_root / "data" / "processed" / "strict_open" / "profiles",
        "examples": repo_root / "examples" / "strict_open_v0",
    }
    assert paths.default_manifest_path() == repo_root / "data" / "processed" / "strict_open" / "manifests" / "run_manifest.json"
    assert paths.default_profile_path("tcp_audit_profile.json") == repo_root / "data" / "processed" / "strict_open" / "profiles" / "tcp_audit_profile.json"

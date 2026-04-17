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
    assert paths.harmonized_root == repo_root / "data" / "processed" / "strict_open" / "harmonized"
    assert paths.splits_root == repo_root / "data" / "processed" / "strict_open" / "splits"
    assert paths.features_root == repo_root / "data" / "processed" / "strict_open" / "features"
    assert paths.targets_root == repo_root / "data" / "processed" / "strict_open" / "targets"
    assert paths.examples_root == repo_root / "examples" / "strict_open_v0"
    assert paths.config_path == repo_root / "config" / "strict_open_v0.toml"
    assert paths.source_raw_root("tcp") == repo_root / "data" / "raw" / "strict_open" / "tcp"
    assert paths.default_profile_path() == repo_root / "data" / "processed" / "strict_open" / "profiles" / "audit_profile.json"
    assert paths.default_harmonized_path() == repo_root / "data" / "processed" / "strict_open" / "harmonized" / "subjects.csv"
    assert paths.default_split_path() == repo_root / "data" / "processed" / "strict_open" / "splits" / "split_assignments.csv"
    assert paths.default_feature_path() == repo_root / "data" / "processed" / "strict_open" / "features" / "visit_features.csv"
    assert paths.default_target_path() == repo_root / "data" / "processed" / "strict_open" / "targets" / "derived_targets.csv"


def test_explicit_repo_root_keeps_paths_deterministic() -> None:
    repo_root = Path("/tmp/strict-open-repo")
    paths = StrictOpenPaths(repo_root=repo_root)

    assert paths.output_roots() == {
        "raw": repo_root / "data" / "raw" / "strict_open",
        "processed": repo_root / "data" / "processed" / "strict_open",
        "curated": repo_root / "data" / "curated" / "strict_open",
        "manifests": repo_root / "data" / "processed" / "strict_open" / "manifests",
        "profiles": repo_root / "data" / "processed" / "strict_open" / "profiles",
        "harmonized": repo_root / "data" / "processed" / "strict_open" / "harmonized",
        "splits": repo_root / "data" / "processed" / "strict_open" / "splits",
        "features": repo_root / "data" / "processed" / "strict_open" / "features",
        "targets": repo_root / "data" / "processed" / "strict_open" / "targets",
        "examples": repo_root / "examples" / "strict_open_v0",
    }
    assert paths.default_manifest_path() == repo_root / "data" / "processed" / "strict_open" / "manifests" / "run_manifest.json"
    assert paths.default_profile_path("tcp_audit_profile.json") == repo_root / "data" / "processed" / "strict_open" / "profiles" / "tcp_audit_profile.json"
    assert paths.default_harmonized_path("visits.csv") == repo_root / "data" / "processed" / "strict_open" / "harmonized" / "visits.csv"
    assert paths.default_split_path("split_manifest.json") == repo_root / "data" / "processed" / "strict_open" / "splits" / "split_manifest.json"
    assert paths.default_feature_path("feature_manifest.json") == repo_root / "data" / "processed" / "strict_open" / "features" / "feature_manifest.json"
    assert paths.default_target_path("target_manifest.json") == repo_root / "data" / "processed" / "strict_open" / "targets" / "target_manifest.json"

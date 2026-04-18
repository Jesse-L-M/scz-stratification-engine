from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = [
    "README.md",
    "config/benchmark_v0.toml",
    "docs/benchmark_claim.md",
    "docs/benchmark_claim_levels.md",
    "docs/benchmark_eval_protocol.md",
    "docs/benchmark_pivot_roadmap.md",
    "docs/dataset_matrix.md",
    "docs/strict_open_claim.md",
    "docs/strict_open_eval_protocol.md",
    "docs/strict_open_pr_roadmap.md",
    "docs/strict_open_scope.md",
    "docs/strict_open_sources.md",
    "docs/strict_open_execution_stack.md",
    "docs/target_outcomes.md",
    "data/raw/benchmark/README.md",
    "data/processed/benchmark/README.md",
    "data/processed/benchmark/manifests/README.md",
    "data/curated/benchmark/README.md",
    "examples/benchmark_v0/README.md",
    "pyproject.toml",
]


@pytest.mark.parametrize("relative_path", REQUIRED_FILES)
def test_required_files_exist(relative_path: str) -> None:
    assert (ROOT / relative_path).is_file()


SUPERSEDED_STRICT_OPEN_DOCS = [
    "docs/strict_open_claim.md",
    "docs/strict_open_scope.md",
    "docs/strict_open_sources.md",
    "docs/strict_open_eval_protocol.md",
    "docs/strict_open_execution_stack.md",
    "docs/strict_open_pr_roadmap.md",
]


@pytest.mark.parametrize("relative_path", SUPERSEDED_STRICT_OPEN_DOCS)
def test_superseded_strict_open_docs_have_banner(relative_path: str) -> None:
    text = (ROOT / relative_path).read_text(encoding="utf-8")
    assert "Superseded" in text

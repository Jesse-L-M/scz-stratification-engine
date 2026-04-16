from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = [
    "README.md",
    "docs/strict_open_claim.md",
    "docs/strict_open_scope.md",
    "docs/strict_open_sources.md",
    "docs/strict_open_execution_stack.md",
    "pyproject.toml",
]


@pytest.mark.parametrize("relative_path", REQUIRED_FILES)
def test_required_files_exist(relative_path: str) -> None:
    assert (ROOT / relative_path).is_file()

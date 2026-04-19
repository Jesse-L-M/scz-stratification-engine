import csv
import json
from pathlib import Path

from scz_audit_engine.strict_open.harmonize import run_tcp_harmonization
from scz_audit_engine.strict_open.provenance import build_source_manifest, write_source_manifest
from scz_audit_engine.strict_open.sources import TCPDS005237SourceAdapter
from scz_audit_engine.strict_open.splits import run_strict_open_split_definition


FIXTURE_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "tcp_raw" / "source_input"


def test_define_splits_is_deterministic_and_subject_level(tmp_path) -> None:
    harmonized_root, manifests_root = _prepare_harmonized_fixture(tmp_path)
    first_splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits_first"
    second_splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits_second"

    first_results = run_strict_open_split_definition(
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        splits_root=first_splits_root,
        command=["scz-audit", "strict-open", "define-splits"],
        git_sha="abc1234",
        seed=1729,
    )
    second_results = run_strict_open_split_definition(
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        splits_root=second_splits_root,
        command=["scz-audit", "strict-open", "define-splits"],
        git_sha="abc1234",
        seed=1729,
    )

    first_assignments = _read_csv_rows(Path(first_results["split_assignments"]))
    second_assignments = _read_csv_rows(Path(second_results["split_assignments"]))
    visits_rows = _read_csv_rows(harmonized_root / "visits.csv")

    assert first_assignments == second_assignments
    assert len(first_assignments) == 3
    assert len({row["subject_id"] for row in first_assignments}) == 3
    assert {row["split"] for row in first_assignments} == {"train", "validation", "test"}

    split_by_subject = {row["subject_id"]: row["split"] for row in first_assignments}
    for visit_row in visits_rows:
        assert visit_row["subject_id"] in split_by_subject
    assert {
        row["subject_id"]: int(row["visit_count"])
        for row in first_assignments
    } == {
        "tcp-ds005237:sub-TCP001": 2,
        "tcp-ds005237:sub-TCP002": 1,
        "tcp-ds005237:sub-TCP003": 1,
    }


def test_split_manifest_reports_site_and_diagnosis_summaries(tmp_path) -> None:
    harmonized_root, manifests_root = _prepare_harmonized_fixture(tmp_path)
    splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits"

    results = run_strict_open_split_definition(
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        splits_root=splits_root,
        command=["scz-audit", "strict-open", "define-splits"],
        git_sha="abc1234",
        seed=1729,
    )
    split_manifest = json.loads(Path(results["split_manifest"]).read_text(encoding="utf-8"))

    assert split_manifest["counts_by_split"] == {
        "train": 1,
        "validation": 1,
        "test": 1,
    }
    assert split_manifest["counts_by_split_and_diagnosis"] == {
        "train": {"GenPop": 1},
        "validation": {"Patient": 1},
        "test": {"Patient": 1},
    }
    assert split_manifest["counts_by_split_and_site"] == {
        "train": {"2": 1},
        "validation": {"1": 1},
        "test": {"1": 1},
    }
    assert "subject inherit that subject's split assignment" in split_manifest["repeat_visit_policy"]
    assert split_manifest["seed"] == 1729
    assert len(split_manifest["caveats"]) >= 2


def test_define_splits_writes_lf_only_csv_artifacts(tmp_path) -> None:
    harmonized_root, manifests_root = _prepare_harmonized_fixture(tmp_path)
    splits_root = tmp_path / "data" / "processed" / "strict_open" / "splits"

    results = run_strict_open_split_definition(
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        splits_root=splits_root,
        command=["scz-audit", "strict-open", "define-splits"],
        git_sha="abc1234",
        seed=1729,
    )

    assert b"\r\n" not in Path(results["split_assignments"]).read_bytes()


def _prepare_harmonized_fixture(tmp_path: Path) -> tuple[Path, Path]:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    manifests_root = tmp_path / "data" / "processed" / "strict_open" / "manifests"
    harmonized_root = tmp_path / "data" / "processed" / "strict_open" / "harmonized"
    adapter = TCPDS005237SourceAdapter()
    staged = adapter.stage(raw_root, source_root=FIXTURE_SOURCE_ROOT)

    source_manifest = build_source_manifest(
        source=staged.source,
        source_identifier=staged.source_identifier,
        dataset_accession=staged.dataset_accession,
        dataset_version=staged.dataset_version,
        command=["scz-audit", "strict-open", "ingest", "--source", "tcp"],
        git_sha="abc1234",
        raw_root=staged.raw_root,
        files=staged.files,
        ingest_timestamp="2026-04-16T12:00:00Z",
    )
    source_manifest_path = manifests_root / "tcp_source_manifest.json"
    write_source_manifest(source_manifest, source_manifest_path)

    run_tcp_harmonization(
        raw_root=raw_root,
        manifests_root=manifests_root,
        harmonized_root=harmonized_root,
        command=["scz-audit", "strict-open", "harmonize"],
        git_sha="abc1234",
        seed=1729,
        dataset_version="1.1.3",
        source_manifest_path=source_manifest_path,
    )
    return harmonized_root, manifests_root


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]

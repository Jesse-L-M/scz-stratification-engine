import csv
import json
from pathlib import Path

from scz_audit_engine.benchmark.harmonize import run_benchmark_harmonization
from scz_audit_engine.benchmark.splits import write_benchmark_split_artifacts


FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "benchmark_sources"


def test_benchmark_split_assignments_are_deterministic_and_subject_level(tmp_path) -> None:
    subjects, visits, diagnoses, outcomes = _prepare_harmonized_rows(tmp_path)
    first_root = tmp_path / "splits_first"
    second_root = tmp_path / "splits_second"

    first = write_benchmark_split_artifacts(
        subjects=subjects,
        visits=visits,
        diagnoses=diagnoses,
        outcomes=outcomes,
        assignments_path=first_root / "split_assignments.csv",
        manifest_path=first_root / "benchmark_split_manifest.json",
        command=["scz-audit", "benchmark", "harmonize"],
        git_sha="abc1234",
        seed=1729,
    )
    second = write_benchmark_split_artifacts(
        subjects=subjects,
        visits=visits,
        diagnoses=diagnoses,
        outcomes=outcomes,
        assignments_path=second_root / "split_assignments.csv",
        manifest_path=second_root / "benchmark_split_manifest.json",
        command=["scz-audit", "benchmark", "harmonize"],
        git_sha="abc1234",
        seed=1729,
    )

    assert list(first.rows) == list(second.rows)
    assert len(first.rows) == len(subjects) == 7
    assert len({row["subject_id"] for row in first.rows}) == 7
    assert {row["split_name"] for row in first.rows} == {"train", "validation", "test"}
    assert all(row["split_level"] == "subject" for row in first.rows)
    assert all(row["leakage_group_id"] == row["subject_id"] for row in first.rows)


def test_benchmark_split_manifest_reports_cohort_site_and_diagnosis_summaries_without_visit_leakage(tmp_path) -> None:
    subjects, visits, diagnoses, outcomes = _prepare_harmonized_rows(tmp_path)
    repeated_visit = {
        **visits[0],
        "visit_id": f"{visits[0]['visit_id']}:followup",
        "source_visit_id": "followup",
        "visit_order": "2",
        "visit_timepoint_label": "followup",
        "days_from_baseline": "30",
        "is_baseline": "false",
    }
    split_root = tmp_path / "splits"

    results = write_benchmark_split_artifacts(
        subjects=subjects,
        visits=visits + [repeated_visit],
        diagnoses=diagnoses,
        outcomes=outcomes,
        assignments_path=split_root / "split_assignments.csv",
        manifest_path=split_root / "benchmark_split_manifest.json",
        command=["scz-audit", "benchmark", "harmonize"],
        git_sha="abc1234",
        seed=1729,
    )
    manifest = json.loads(Path(results.manifest_path).read_text(encoding="utf-8"))
    assignments = _read_csv_rows(Path(results.assignments_path))

    assert len(assignments) == 7
    assert sum(manifest["counts_by_split"].values()) == 7
    assert sum(manifest["visit_counts_by_split"].values()) == len(visits) + 1
    assert set(manifest["counts_by_split_and_cohort"]["train"]) | set(
        manifest["counts_by_split_and_cohort"]["validation"]
    ) | set(manifest["counts_by_split_and_cohort"]["test"]) == {
        "fep-ds003944",
        "tcp-ds005237",
    }
    diagnosis_groups = set(manifest["counts_by_split_and_diagnosis_group"]["train"]) | set(
        manifest["counts_by_split_and_diagnosis_group"]["validation"]
    ) | set(manifest["counts_by_split_and_diagnosis_group"]["test"])
    assert "psychosis" in diagnosis_groups
    assert "broad_psychiatric_patient" in diagnosis_groups
    site_ids = set(manifest["counts_by_split_and_site"]["train"]) | set(
        manifest["counts_by_split_and_site"]["validation"]
    ) | set(manifest["counts_by_split_and_site"]["test"])
    assert "single_site_public_accession" in site_ids
    assert "1" in site_ids or "2" in site_ids
    assert "do not constitute a full external-validation claim" in manifest["claim_boundary_statement"]
    assert "subject's split assignment" in manifest["visit_leakage_policy"]
    assert manifest["counts_by_split_and_outcome_support"]["train"]["has_outcome"] >= 1
    assert "training split receives labeled rows" in manifest["label_support_policy"]

    has_outcome = {row["subject_id"] for row in outcomes}
    subject_by_id = {row["subject_id"]: row for row in subjects}
    train_labels_by_cohort = {
        cohort_id: 0
        for cohort_id in {row["cohort_id"] for row in subjects}
    }
    for row in assignments:
        if row["split_name"] != "train" or row["subject_id"] not in has_outcome:
            continue
        train_labels_by_cohort[subject_by_id[row["subject_id"]]["cohort_id"]] += 1
    assert train_labels_by_cohort["fep-ds003944"] >= 1
    assert train_labels_by_cohort["tcp-ds005237"] >= 1


def test_benchmark_split_rejects_negative_fractions(tmp_path) -> None:
    subjects, visits, diagnoses, outcomes = _prepare_harmonized_rows(tmp_path)

    import pytest

    with pytest.raises(ValueError, match="non-negative"):
        write_benchmark_split_artifacts(
            subjects=subjects,
            visits=visits,
            diagnoses=diagnoses,
            outcomes=outcomes,
            assignments_path=tmp_path / "split_assignments.csv",
            manifest_path=tmp_path / "benchmark_split_manifest.json",
            command=["scz-audit", "benchmark", "harmonize"],
            git_sha="abc1234",
            seed=1729,
            split_fractions={"train": 0.8, "validation": -0.1, "test": 0.3},
        )


def _prepare_harmonized_rows(
    tmp_path: Path,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    harmonized_root = tmp_path / "harmonized"
    manifests_root = tmp_path / "manifests"
    run_benchmark_harmonization(
        raw_root=FIXTURE_ROOT,
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize", "--raw-root", str(FIXTURE_ROOT)],
        git_sha="abc1234",
        seed=1729,
    )
    return (
        _read_csv_rows(harmonized_root / "subjects.csv"),
        _read_csv_rows(harmonized_root / "visits.csv"),
        _read_csv_rows(harmonized_root / "diagnoses.csv"),
        _read_csv_rows(harmonized_root / "outcomes.csv"),
    )


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]

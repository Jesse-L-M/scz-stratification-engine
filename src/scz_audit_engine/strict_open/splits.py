"""Deterministic subject-level split assignment for strict-open."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict, deque
from pathlib import Path

from .provenance import write_json_artifact
from .run_manifest import build_run_manifest, write_run_manifest

DEFAULT_SPLIT_FRACTIONS = {
    "train": 0.6,
    "validation": 0.2,
    "test": 0.2,
}
SPLIT_ORDER = ("train", "validation", "test")


def run_strict_open_split_definition(
    *,
    harmonized_root: str | Path,
    manifests_root: str | Path,
    splits_root: str | Path,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
    split_fractions: dict[str, float] | None = None,
) -> dict[str, str]:
    """Assign deterministic subject-level strict-open splits."""

    harmonized_path = Path(harmonized_root)
    manifests_path = Path(manifests_root)
    splits_path = Path(splits_root)
    subjects_path = harmonized_path / "subjects.csv"
    visits_path = harmonized_path / "visits.csv"
    if not subjects_path.exists():
        raise FileNotFoundError(f"Missing harmonized subjects table at {subjects_path}")
    if not visits_path.exists():
        raise FileNotFoundError(f"Missing harmonized visits table at {visits_path}")

    subjects = _read_csv_rows(subjects_path)
    visits = _read_csv_rows(visits_path)
    split_fractions = _normalize_split_fractions(split_fractions)
    assignments = _assign_subject_splits(subjects, seed=seed, split_fractions=split_fractions)
    visit_counts = Counter(row["subject_id"] for row in visits)
    assignment_rows = [
        {
            "subject_id": row["subject_id"],
            "source_subject_id": row["source_subject_id"],
            "diagnosis": row["diagnosis"],
            "site_id": row["site_id"],
            "visit_count": str(visit_counts.get(row["subject_id"], 0)),
            "split": assignments[row["subject_id"]],
        }
        for row in sorted(subjects, key=lambda subject: subject["subject_id"])
    ]

    split_assignments_path = _write_csv_rows(
        assignment_rows,
        splits_path / "split_assignments.csv",
        fieldnames=("subject_id", "source_subject_id", "diagnosis", "site_id", "visit_count", "split"),
    )
    harmonization_manifest_path = harmonized_path / "harmonization_manifest.json"
    harmonization_manifest = {}
    input_paths = [str(subjects_path), str(visits_path)]
    if harmonization_manifest_path.exists():
        harmonization_manifest = json.loads(harmonization_manifest_path.read_text(encoding="utf-8"))
        input_paths.append(str(harmonization_manifest_path))

    split_manifest = {
        "command": list(command),
        "counts_by_split": _counts_by_split(assignment_rows),
        "counts_by_split_and_diagnosis": _counts_by_split_and_field(assignment_rows, "diagnosis"),
        "counts_by_split_and_site": _counts_by_split_and_field(assignment_rows, "site_id"),
        "caveats": _build_caveats(assignment_rows),
        "git_sha": git_sha,
        "harmonized_dir": str(harmonized_path),
        "input_paths": input_paths,
        "repeat_visit_policy": "All visits for a subject inherit that subject's split assignment; no visit-level reassignment is allowed.",
        "seed": seed,
        "source": harmonization_manifest.get("source", "tcp"),
        "source_identifier": harmonization_manifest.get("source_identifier", "tcp-ds005237"),
        "dataset_version": harmonization_manifest.get("dataset_version"),
        "split_method": (
            "Deterministic subject-level assignment using diagnosis-stratified subject ordering, "
            "an interleaved subject queue, and a fixed train/validation/test quota sequence."
        ),
        "split_ratios": split_fractions,
        "site_holdout_policy": (
            "Site composition is reported for every split, but strict site holdout is not enforced for strict-open v0 "
            "because the current TCP public coverage is too small to guarantee a stable site-isolated test set."
        ),
    }
    split_manifest_path = splits_path / "split_manifest.json"
    write_json_artifact(split_manifest, split_manifest_path)

    run_manifest_path = manifests_path / "tcp_define_splits_run_manifest.json"
    run_manifest = build_run_manifest(
        dataset_source=str(split_manifest["source"]),
        dataset_version=split_manifest.get("dataset_version"),
        command=command,
        git_sha=git_sha,
        seed=seed,
        output_paths={
            "split_assignments": split_assignments_path,
            "split_manifest": split_manifest_path,
        },
    )
    write_run_manifest(run_manifest, run_manifest_path)

    return {
        "run_manifest": str(run_manifest_path),
        "split_assignments": str(split_assignments_path),
        "split_manifest": str(split_manifest_path),
        "splits_dir": str(splits_path),
    }


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _write_csv_rows(rows: list[dict[str, str]], destination: Path, *, fieldnames: tuple[str, ...]) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({fieldname: row.get(fieldname, "") for fieldname in fieldnames})
    return destination


def _normalize_split_fractions(split_fractions: dict[str, float] | None) -> dict[str, float]:
    fractions = dict(DEFAULT_SPLIT_FRACTIONS)
    if split_fractions is not None:
        fractions.update({name: float(value) for name, value in split_fractions.items() if name in SPLIT_ORDER})
    total = sum(fractions[name] for name in SPLIT_ORDER)
    if total <= 0:
        raise ValueError("Split fractions must sum to a positive value.")
    return {name: fractions[name] / total for name in SPLIT_ORDER}


def _assign_subject_splits(
    subjects: list[dict[str, str]],
    *,
    seed: int,
    split_fractions: dict[str, float],
) -> dict[str, str]:
    diagnosis_buckets: dict[str, list[dict[str, str]]] = defaultdict(list)
    for subject in subjects:
        diagnosis_buckets[(subject.get("diagnosis") or "unknown").strip() or "unknown"].append(subject)

    for diagnosis, bucket in diagnosis_buckets.items():
        diagnosis_buckets[diagnosis] = sorted(
            bucket,
            key=lambda row: _stable_subject_rank(seed, row["subject_id"]),
        )

    ordered_subjects = _interleave_diagnosis_buckets(diagnosis_buckets)
    split_sequence = _expanded_split_sequence(_target_split_counts(len(ordered_subjects), split_fractions))
    return {
        subject["subject_id"]: split_name
        for subject, split_name in zip(ordered_subjects, split_sequence, strict=True)
    }


def _interleave_diagnosis_buckets(
    diagnosis_buckets: dict[str, list[dict[str, str]]],
) -> list[dict[str, str]]:
    ordered_subjects: list[dict[str, str]] = []
    queues = {
        diagnosis: deque(bucket)
        for diagnosis, bucket in sorted(diagnosis_buckets.items())
    }
    while any(queues.values()):
        for diagnosis in sorted(queues):
            if queues[diagnosis]:
                ordered_subjects.append(queues[diagnosis].popleft())
    return ordered_subjects


def _stable_subject_rank(seed: int, subject_id: str) -> str:
    return hashlib.sha256(f"{seed}:{subject_id}".encode("utf-8")).hexdigest()


def _target_split_counts(total_subjects: int, split_fractions: dict[str, float]) -> dict[str, int]:
    counts = {split_name: 0 for split_name in SPLIT_ORDER}
    if total_subjects == 0:
        return counts

    raw_targets = {split_name: total_subjects * split_fractions[split_name] for split_name in SPLIT_ORDER}
    counts = {split_name: int(raw_targets[split_name]) for split_name in SPLIT_ORDER}
    assigned = sum(counts.values())
    remainders = sorted(
        SPLIT_ORDER,
        key=lambda split_name: (raw_targets[split_name] - counts[split_name], -SPLIT_ORDER.index(split_name)),
        reverse=True,
    )
    for split_name in remainders[: total_subjects - assigned]:
        counts[split_name] += 1

    if total_subjects >= len(SPLIT_ORDER):
        for split_name in SPLIT_ORDER:
            if counts[split_name] > 0:
                continue
            donor = max(SPLIT_ORDER, key=lambda candidate: counts[candidate])
            if counts[donor] > 1:
                counts[donor] -= 1
                counts[split_name] += 1
    return counts


def _expanded_split_sequence(target_counts: dict[str, int]) -> list[str]:
    remaining = dict(target_counts)
    sequence: list[str] = []
    while any(remaining.values()):
        for split_name in SPLIT_ORDER:
            if remaining[split_name] <= 0:
                continue
            sequence.append(split_name)
            remaining[split_name] -= 1
    return sequence


def _counts_by_split(rows: list[dict[str, str]]) -> dict[str, int]:
    counts = {split_name: 0 for split_name in SPLIT_ORDER}
    counts.update(Counter(row["split"] for row in rows))
    return counts


def _counts_by_split_and_field(rows: list[dict[str, str]], field_name: str) -> dict[str, dict[str, int]]:
    payload = {split_name: {} for split_name in SPLIT_ORDER}
    grouped: dict[str, Counter[str]] = {split_name: Counter() for split_name in SPLIT_ORDER}
    for row in rows:
        grouped[row["split"]][(row.get(field_name) or "unknown").strip() or "unknown"] += 1
    for split_name, counter in grouped.items():
        payload[split_name] = dict(sorted(counter.items()))
    return payload


def _build_caveats(rows: list[dict[str, str]]) -> list[str]:
    caveats = [
        "Repeat visits remain grouped by subject, so subject-level splits prevent longitudinal leakage.",
    ]
    sites = {(row.get("site_id") or "unknown").strip() or "unknown" for row in rows}
    if len(sites) < 3:
        caveats.append(
            "Current TCP public site coverage is too small for a reliable strict site-holdout protocol; site composition is reported instead."
        )
    if any(int(row.get("visit_count") or "0") > 1 for row in rows):
        caveats.append(
            "Some subjects have repeat visits, so downstream baselines must reuse these frozen subject-level assignments exactly."
        )
    return caveats


__all__ = ["run_strict_open_split_definition"]

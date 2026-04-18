"""Deterministic within-cohort split contracts for the benchmark namespace."""

from __future__ import annotations

import csv
import hashlib
import math
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .provenance import write_json_artifact

DEFAULT_SPLIT_FRACTIONS = {
    "train": 0.6,
    "validation": 0.2,
    "test": 0.2,
}
SPLIT_ORDER = ("train", "validation", "test")
SPLIT_PROTOCOL_VERSION = "benchmark_within_cohort_subject_v1"
ASSIGNMENT_NOTE = (
    "Within-cohort deterministic subject split; this freezes leakage-safe assignments only and does not "
    "claim full external validation."
)


@dataclass(frozen=True, slots=True)
class BenchmarkSplitArtifacts:
    """Split-assignment outputs emitted during benchmark harmonization."""

    assignments_path: Path
    manifest_path: Path
    rows: tuple[dict[str, str], ...]
    manifest: dict[str, Any]


def write_benchmark_split_artifacts(
    *,
    subjects: list[dict[str, str]],
    visits: list[dict[str, str]],
    diagnoses: list[dict[str, str]],
    outcomes: list[dict[str, str]] | None = None,
    assignments_path: str | Path,
    manifest_path: str | Path,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
    split_fractions: dict[str, float] | None = None,
) -> BenchmarkSplitArtifacts:
    """Write deterministic benchmark split assignments plus a split manifest."""

    normalized_fractions = _normalize_split_fractions(split_fractions)
    primary_diagnosis = _primary_diagnosis_by_subject(diagnoses)
    subjects_with_outcomes = {
        row["subject_id"]
        for row in outcomes or []
        if row.get("outcome_value", "").strip()
    }
    assignments = _assign_subject_splits(
        subjects,
        primary_diagnosis,
        subjects_with_outcomes,
        seed=seed,
        split_fractions=normalized_fractions,
    )
    visit_counts = Counter(row["subject_id"] for row in visits)
    rows = tuple(
        {
            "cohort_id": subject["cohort_id"],
            "subject_id": subject["subject_id"],
            "split_name": assignments[subject["subject_id"]],
            "split_level": "subject",
            "split_protocol_version": SPLIT_PROTOCOL_VERSION,
            "leakage_group_id": subject["subject_id"],
            "fold_index": "",
            "split_label": "default_within_cohort",
            "assignment_note": ASSIGNMENT_NOTE,
        }
        for subject in sorted(subjects, key=lambda row: (row["cohort_id"], row["subject_id"]))
    )
    destination = _write_csv_rows(
        rows,
        Path(assignments_path),
        fieldnames=(
            "cohort_id",
            "subject_id",
            "split_name",
            "split_level",
            "split_protocol_version",
            "leakage_group_id",
            "fold_index",
            "split_label",
            "assignment_note",
        ),
    )

    subject_details = {
        row["subject_id"]: {
            "cohort_id": row["cohort_id"],
            "site_id": row.get("site_id", "").strip() or "unknown",
            "diagnosis_group": primary_diagnosis.get(row["subject_id"], "unknown"),
            "visit_count": visit_counts.get(row["subject_id"], 0),
            "outcome_support": (
                "has_outcome" if row["subject_id"] in subjects_with_outcomes else "no_outcome"
            ),
        }
        for row in subjects
    }
    manifest = {
        "command": list(command),
        "git_sha": git_sha,
        "seed": seed,
        "split_protocol_version": SPLIT_PROTOCOL_VERSION,
        "split_method": (
            "Deterministic within-cohort subject-level assignment. Subjects are ordered by cohort, then "
            "interleaved across diagnosis groups within each cohort using a seed-stable subject hash before "
            "fixed train/validation/test quotas are applied."
        ),
        "split_ratios": normalized_fractions,
        "counts_by_split": _counts_by_split(rows),
        "counts_by_split_and_cohort": _counts_by_split_and_field(rows, subject_details, "cohort_id"),
        "counts_by_split_and_diagnosis_group": _counts_by_split_and_field(rows, subject_details, "diagnosis_group"),
        "counts_by_split_and_site": _counts_by_split_and_field(rows, subject_details, "site_id"),
        "counts_by_split_and_outcome_support": _counts_by_split_and_field(
            rows,
            subject_details,
            "outcome_support",
        ),
        "visit_counts_by_split": _visit_counts_by_split(rows, subject_details),
        "visit_leakage_policy": (
            "All visits for a subject inherit that subject's split assignment; no visit-level reassignment is allowed."
        ),
        "label_support_policy": (
            "Within each cohort, subjects with benchmark outcome rows are ordered ahead of unlabeled subjects so "
            "the training split receives labeled rows whenever the cohort exposes any labels at all."
        ),
        "claim_boundary_statement": (
            "These frozen splits stay within the current narrow benchmark lane and do not constitute a full "
            "external-validation claim."
        ),
    }
    manifest_output = write_json_artifact(manifest, manifest_path)
    return BenchmarkSplitArtifacts(
        assignments_path=destination,
        manifest_path=manifest_output,
        rows=rows,
        manifest=manifest,
    )


def _write_csv_rows(
    rows: tuple[dict[str, str], ...],
    destination: Path,
    *,
    fieldnames: tuple[str, ...],
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({fieldname: row.get(fieldname, "") for fieldname in fieldnames})
    return destination


def _primary_diagnosis_by_subject(diagnoses: list[dict[str, str]]) -> dict[str, str]:
    primary: dict[str, str] = {}
    for row in sorted(diagnoses, key=lambda item: (item["subject_id"], item["visit_id"], item["diagnosis_group"])):
        if row["subject_id"] in primary:
            continue
        if row.get("is_primary_diagnosis", "").strip().lower() in {"true", "1", "yes"}:
            primary[row["subject_id"]] = row.get("diagnosis_group", "").strip() or "unknown"
    for row in diagnoses:
        primary.setdefault(row["subject_id"], row.get("diagnosis_group", "").strip() or "unknown")
    return primary


def _normalize_split_fractions(split_fractions: dict[str, float] | None) -> dict[str, float]:
    fractions = dict(DEFAULT_SPLIT_FRACTIONS)
    if split_fractions is not None:
        fractions.update(
            {
                split_name: float(value)
                for split_name, value in split_fractions.items()
                if split_name in SPLIT_ORDER
            }
        )
    for split_name, value in fractions.items():
        if not math.isfinite(value):
            raise ValueError(f"Split fraction for {split_name} must be finite.")
        if value < 0:
            raise ValueError(f"Split fraction for {split_name} must be non-negative.")
    total = sum(fractions[split_name] for split_name in SPLIT_ORDER)
    if total <= 0:
        raise ValueError("Split fractions must sum to a positive value.")
    return {split_name: fractions[split_name] / total for split_name in SPLIT_ORDER}


def _assign_subject_splits(
    subjects: list[dict[str, str]],
    diagnosis_by_subject: dict[str, str],
    subjects_with_outcomes: set[str],
    *,
    seed: int,
    split_fractions: dict[str, float],
) -> dict[str, str]:
    assignments: dict[str, str] = {}
    cohort_buckets: dict[str, list[dict[str, str]]] = defaultdict(list)
    for subject in subjects:
        cohort_buckets[subject["cohort_id"]].append(subject)

    for cohort_id, cohort_subjects in sorted(cohort_buckets.items()):
        labeled_subjects = [
            subject for subject in cohort_subjects if subject["subject_id"] in subjects_with_outcomes
        ]
        unlabeled_subjects = [
            subject for subject in cohort_subjects if subject["subject_id"] not in subjects_with_outcomes
        ]
        ordered_subjects = _order_subjects_within_cohort(
            labeled_subjects,
            diagnosis_by_subject,
            seed=seed,
            cohort_id=cohort_id,
        ) + _order_subjects_within_cohort(
            unlabeled_subjects,
            diagnosis_by_subject,
            seed=seed,
            cohort_id=cohort_id,
        )
        split_sequence = _expanded_split_sequence(
            _target_split_counts(len(ordered_subjects), split_fractions)
        )
        for subject, split_name in zip(ordered_subjects, split_sequence, strict=True):
            assignments[subject["subject_id"]] = split_name
    return assignments


def _interleave_diagnosis_buckets(
    diagnosis_buckets: dict[str, list[dict[str, str]]],
) -> list[dict[str, str]]:
    ordered_subjects: list[dict[str, str]] = []
    queues = {
        diagnosis_group: deque(bucket)
        for diagnosis_group, bucket in sorted(diagnosis_buckets.items())
    }
    while any(queues.values()):
        for diagnosis_group in sorted(queues):
            if queues[diagnosis_group]:
                ordered_subjects.append(queues[diagnosis_group].popleft())
    return ordered_subjects


def _order_subjects_within_cohort(
    subjects: list[dict[str, str]],
    diagnosis_by_subject: dict[str, str],
    *,
    seed: int,
    cohort_id: str,
) -> list[dict[str, str]]:
    diagnosis_buckets: dict[str, list[dict[str, str]]] = defaultdict(list)
    for subject in subjects:
        diagnosis_buckets[diagnosis_by_subject.get(subject["subject_id"], "unknown")].append(subject)
    for diagnosis_group, bucket in diagnosis_buckets.items():
        diagnosis_buckets[diagnosis_group] = sorted(
            bucket,
            key=lambda row: _stable_subject_rank(seed, cohort_id, row["subject_id"]),
        )
    return _interleave_diagnosis_buckets(diagnosis_buckets)


def _stable_subject_rank(seed: int, cohort_id: str, subject_id: str) -> str:
    return hashlib.sha256(f"{seed}:{cohort_id}:{subject_id}".encode("utf-8")).hexdigest()


def _target_split_counts(total_subjects: int, split_fractions: dict[str, float]) -> dict[str, int]:
    counts = {split_name: 0 for split_name in SPLIT_ORDER}
    if total_subjects == 0:
        return counts

    raw_targets = {
        split_name: total_subjects * split_fractions[split_name]
        for split_name in SPLIT_ORDER
    }
    counts = {split_name: int(raw_targets[split_name]) for split_name in SPLIT_ORDER}
    assigned = sum(counts.values())
    remainders = sorted(
        SPLIT_ORDER,
        key=lambda split_name: (
            raw_targets[split_name] - counts[split_name],
            -SPLIT_ORDER.index(split_name),
        ),
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
    if any(count < 0 for count in target_counts.values()):
        raise ValueError("Split target counts must be non-negative.")
    remaining = dict(target_counts)
    sequence: list[str] = []
    while any(remaining.values()):
        for split_name in SPLIT_ORDER:
            if remaining[split_name] <= 0:
                continue
            sequence.append(split_name)
            remaining[split_name] -= 1
    return sequence


def _counts_by_split(rows: tuple[dict[str, str], ...]) -> dict[str, int]:
    counts = {split_name: 0 for split_name in SPLIT_ORDER}
    counts.update(Counter(row["split_name"] for row in rows))
    return counts


def _counts_by_split_and_field(
    rows: tuple[dict[str, str], ...],
    subject_details: dict[str, dict[str, Any]],
    field_name: str,
) -> dict[str, dict[str, int]]:
    grouped: dict[str, Counter[str]] = {split_name: Counter() for split_name in SPLIT_ORDER}
    for row in rows:
        detail = subject_details[row["subject_id"]]
        grouped[row["split_name"]][str(detail.get(field_name) or "unknown")] += 1
    return {
        split_name: dict(sorted(counter.items()))
        for split_name, counter in grouped.items()
    }


def _visit_counts_by_split(
    rows: tuple[dict[str, str], ...],
    subject_details: dict[str, dict[str, Any]],
) -> dict[str, int]:
    counts = {split_name: 0 for split_name in SPLIT_ORDER}
    for row in rows:
        counts[row["split_name"]] += int(subject_details[row["subject_id"]]["visit_count"])
    return counts


__all__ = [
    "ASSIGNMENT_NOTE",
    "BenchmarkSplitArtifacts",
    "DEFAULT_SPLIT_FRACTIONS",
    "SPLIT_ORDER",
    "SPLIT_PROTOCOL_VERSION",
    "write_benchmark_split_artifacts",
]

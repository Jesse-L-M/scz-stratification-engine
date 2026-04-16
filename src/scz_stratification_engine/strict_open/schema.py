"""Canonical table contracts for the strict-open namespace."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TableSchema:
    """A lightweight contract for a canonical table."""

    name: str
    columns: tuple[str, ...]


SUBJECTS = TableSchema(
    name="subjects",
    columns=(
        "subject_id",
        "source_dataset",
        "source_subject_id",
        "diagnosis",
        "site_id",
        "sex",
        "age_years",
    ),
)

VISITS = TableSchema(
    name="visits",
    columns=(
        "visit_id",
        "subject_id",
        "visit_label",
        "visit_index",
        "days_from_baseline",
    ),
)

COGNITION_SCORES = TableSchema(
    name="cognition_scores",
    columns=(
        "subject_id",
        "visit_id",
        "instrument",
        "measure",
        "score",
    ),
)

SYMPTOM_BEHAVIOR_SCORES = TableSchema(
    name="symptom_behavior_scores",
    columns=(
        "subject_id",
        "visit_id",
        "instrument",
        "measure",
        "score",
    ),
)

MRI_FEATURES = TableSchema(
    name="mri_features",
    columns=(
        "subject_id",
        "visit_id",
        "modality",
        "feature_name",
        "feature_value",
        "qc_status",
    ),
)

DERIVED_TARGETS = TableSchema(
    name="derived_targets",
    columns=(
        "subject_id",
        "visit_id",
        "target_name",
        "target_label",
        "target_value",
    ),
)

BIOLOGY_PRIORS = TableSchema(
    name="biology_priors",
    columns=(
        "feature_name",
        "target_name",
        "evidence_source",
        "evidence_score",
    ),
)

STRICT_OPEN_TABLE_SCHEMAS = {
    schema.name: schema
    for schema in (
        SUBJECTS,
        VISITS,
        COGNITION_SCORES,
        SYMPTOM_BEHAVIOR_SCORES,
        MRI_FEATURES,
        DERIVED_TARGETS,
        BIOLOGY_PRIORS,
    )
}
STRICT_OPEN_TABLE_NAMES = tuple(STRICT_OPEN_TABLE_SCHEMAS)

__all__ = [
    "BIOLOGY_PRIORS",
    "COGNITION_SCORES",
    "DERIVED_TARGETS",
    "MRI_FEATURES",
    "STRICT_OPEN_TABLE_NAMES",
    "STRICT_OPEN_TABLE_SCHEMAS",
    "SUBJECTS",
    "SYMPTOM_BEHAVIOR_SCORES",
    "TableSchema",
    "VISITS",
]

"""Canonical schema contracts for the benchmark namespace."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

SCHEMA_VERSION = "benchmark_v0"


@dataclass(frozen=True, slots=True)
class TableContract:
    """Explicit contract for one canonical benchmark table."""

    name: str
    purpose: str
    row_grain: str
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("table name must not be empty")
        if not self.purpose.strip():
            raise ValueError(f"{self.name} purpose must not be empty")
        if not self.row_grain.strip():
            raise ValueError(f"{self.name} row grain must not be empty")
        if not self.required_columns:
            raise ValueError(f"{self.name} must declare required columns")

        for field_name, values in (
            ("required_columns", self.required_columns),
            ("optional_columns", self.optional_columns),
        ):
            seen: set[str] = set()
            for column in values:
                if not column.strip():
                    raise ValueError(f"{self.name} {field_name} cannot contain blank columns")
                if column in seen:
                    raise ValueError(f"{self.name} {field_name} cannot repeat column {column!r}")
                seen.add(column)

        overlap = set(self.required_columns).intersection(self.optional_columns)
        if overlap:
            raise ValueError(f"{self.name} columns cannot be both required and optional: {overlap}")

    @property
    def all_columns(self) -> tuple[str, ...]:
        """Return the full ordered column contract for the table."""

        return self.required_columns + self.optional_columns

    def to_dict(self) -> dict[str, Any]:
        """Serialize the table contract for machine-readable artifacts."""

        return {
            "name": self.name,
            "purpose": self.purpose,
            "row_grain": self.row_grain,
            "required_columns": list(self.required_columns),
            "optional_columns": list(self.optional_columns),
        }


@dataclass(frozen=True, slots=True)
class BenchmarkSchema:
    """Container for the canonical benchmark tables."""

    version: str
    tables: tuple[TableContract, ...]

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise ValueError("schema version must not be empty")
        if not self.tables:
            raise ValueError("schema must define at least one table")

        seen: set[str] = set()
        for table in self.tables:
            if table.name in seen:
                raise ValueError(f"schema cannot repeat table {table.name!r}")
            seen.add(table.name)

    @property
    def table_names(self) -> tuple[str, ...]:
        """Return the canonical table order."""

        return tuple(table.name for table in self.tables)

    def table(self, name: str) -> TableContract:
        """Look up a table contract by name."""

        for table in self.tables:
            if table.name == name:
                return table
        raise KeyError(f"unknown benchmark schema table: {name}")

    def to_artifact_dict(self) -> dict[str, Any]:
        """Serialize the schema for JSON artifacts."""

        return {
            "schema_version": self.version,
            "table_count": len(self.tables),
            "table_names": list(self.table_names),
            "tables": [table.to_dict() for table in self.tables],
        }


CANONICAL_TABLE_NAMES = (
    "subjects",
    "visits",
    "diagnoses",
    "symptom_scores",
    "cognition_scores",
    "functioning_scores",
    "treatment_exposures",
    "outcomes",
    "modality_features",
    "split_assignments",
)

CANONICAL_BENCHMARK_SCHEMA = BenchmarkSchema(
    version=SCHEMA_VERSION,
    tables=(
        TableContract(
            name="subjects",
            purpose=(
                "Subject-level cohort membership and baseline metadata needed to state who is in "
                "scope for the benchmark."
            ),
            row_grain="One row per subject enrolled in one cohort.",
            required_columns=(
                "cohort_id",
                "subject_id",
                "source_subject_id",
                "population_scope",
                "site_id",
                "sex",
                "baseline_age",
                "enrollment_group",
                "has_longitudinal_followup",
                "representation_comparison_support",
            ),
            optional_columns=(
                "ancestry_group",
                "race_ethnicity",
                "education_years",
                "mapping_note",
            ),
        ),
        TableContract(
            name="visits",
            purpose="Visit-level timing metadata for baseline and follow-up rows.",
            row_grain="One row per subject visit within one cohort.",
            required_columns=(
                "cohort_id",
                "subject_id",
                "visit_id",
                "source_visit_id",
                "visit_order",
                "visit_timepoint_label",
                "visit_age",
                "days_from_baseline",
                "is_baseline",
            ),
            optional_columns=(
                "visit_window_label",
                "visit_status",
                "visit_note",
            ),
        ),
        TableContract(
            name="diagnoses",
            purpose="Diagnosis labels and grouping fields, including granularity and mapping caveats.",
            row_grain="One row per diagnosis assertion for one subject at one visit.",
            required_columns=(
                "cohort_id",
                "subject_id",
                "visit_id",
                "diagnosis_system",
                "diagnosis_label",
                "diagnosis_group",
                "diagnosis_granularity",
                "is_primary_diagnosis",
                "mapping_caveat",
            ),
            optional_columns=(
                "diagnosis_code",
                "source_diagnosis_label",
                "diagnosis_note",
            ),
        ),
        TableContract(
            name="symptom_scores",
            purpose="Symptom severity measurements and any harmonized symptom-domain scores.",
            row_grain="One row per symptom measure for one subject at one visit.",
            required_columns=(
                "cohort_id",
                "subject_id",
                "visit_id",
                "instrument",
                "domain",
                "measure",
                "score",
                "score_direction",
                "is_harmonized_domain_score",
                "mapping_caveat",
            ),
            optional_columns=(
                "score_unit",
                "instrument_version",
                "source_score_label",
            ),
        ),
        TableContract(
            name="cognition_scores",
            purpose="Cognitive task or scale measurements aligned to explicit domains.",
            row_grain="One row per cognition measure for one subject at one visit.",
            required_columns=(
                "cohort_id",
                "subject_id",
                "visit_id",
                "instrument",
                "domain",
                "measure",
                "score",
                "score_direction",
                "mapping_caveat",
            ),
            optional_columns=(
                "score_unit",
                "task_name",
                "source_score_label",
            ),
        ),
        TableContract(
            name="functioning_scores",
            purpose="Functioning and recovery-relevant measurements used for outcome definition.",
            row_grain="One row per functioning measure for one subject at one visit.",
            required_columns=(
                "cohort_id",
                "subject_id",
                "visit_id",
                "instrument",
                "domain",
                "measure",
                "score",
                "score_direction",
                "mapping_caveat",
            ),
            optional_columns=(
                "score_unit",
                "source_score_label",
                "recovery_domain",
            ),
        ),
        TableContract(
            name="treatment_exposures",
            purpose="Treatment exposure rows that keep medication or intervention context explicit.",
            row_grain="One row per treatment exposure record for one subject at one visit.",
            required_columns=(
                "cohort_id",
                "subject_id",
                "visit_id",
                "treatment_type",
                "treatment_name",
                "exposure_value",
                "exposure_unit",
                "exposure_window",
                "is_current_exposure",
                "mapping_caveat",
            ),
            optional_columns=(
                "source_treatment_label",
                "exposure_route",
                "adherence_note",
            ),
        ),
        TableContract(
            name="outcomes",
            purpose=(
                "Outcome rows that keep predictor timing, outcome timing, and prospective versus "
                "concurrent validity explicit."
            ),
            row_grain="One row per benchmark outcome definition for one subject at one visit.",
            required_columns=(
                "cohort_id",
                "subject_id",
                "visit_id",
                "outcome_family",
                "outcome_name",
                "outcome_value",
                "outcome_type",
                "predictor_timepoint",
                "outcome_timepoint",
                "outcome_window",
                "outcome_is_prospective",
                "concurrent_endpoint_only",
                "outcome_definition_version",
                "mapping_caveat",
            ),
            optional_columns=(
                "outcome_unit",
                "outcome_direction",
                "outcome_threshold_label",
            ),
        ),
        TableContract(
            name="modality_features",
            purpose="Feature rows emitted from one modality without assuming cross-cohort harmonization yet.",
            row_grain="One row per modality feature for one subject at one visit.",
            required_columns=(
                "cohort_id",
                "subject_id",
                "visit_id",
                "modality_type",
                "feature_name",
                "feature_value",
                "feature_unit",
                "feature_source",
                "mapping_caveat",
            ),
            optional_columns=(
                "feature_group",
                "preprocessing_version",
                "feature_quality_flag",
            ),
        ),
        TableContract(
            name="split_assignments",
            purpose="Future split-assignment rows that can carry leakage controls without generating splits yet.",
            row_grain="One row per subject assignment to one benchmark split protocol.",
            required_columns=(
                "cohort_id",
                "subject_id",
                "split_name",
                "split_level",
                "split_protocol_version",
                "leakage_group_id",
            ),
            optional_columns=(
                "fold_index",
                "split_label",
                "assignment_note",
            ),
        ),
    ),
)


def benchmark_schema() -> BenchmarkSchema:
    """Return the canonical benchmark schema contract."""

    return CANONICAL_BENCHMARK_SCHEMA


__all__ = [
    "BenchmarkSchema",
    "CANONICAL_BENCHMARK_SCHEMA",
    "CANONICAL_TABLE_NAMES",
    "SCHEMA_VERSION",
    "TableContract",
    "benchmark_schema",
]

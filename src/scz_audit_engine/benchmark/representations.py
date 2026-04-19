"""Cross-sectional benchmark representation artifacts built from harmonized tables."""

from __future__ import annotations

import csv
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .provenance import write_json_artifact
from .run_manifest import build_run_manifest, utc_now_iso, write_run_manifest

COMMON_REPRESENTATION_COLUMNS = (
    "cohort_id",
    "subject_id",
    "visit_id",
    "split_name",
    "site_id",
    "population_scope",
    "enrollment_group",
    "diagnosis_group",
    "diagnosis_granularity",
    "representation_comparison_support",
    "has_longitudinal_followup",
    "outcome_row_available",
)

DIAGNOSIS_ANCHOR_COLUMNS = COMMON_REPRESENTATION_COLUMNS + (
    "available_feature_count",
    "diagnosis_is_case",
    "diagnosis_is_control",
    "diagnosis_psychosis_flag",
    "diagnosis_schizophrenia_flag",
    "diagnosis_bipolar_flag",
    "diagnosis_adhd_flag",
    "diagnosis_broad_psychiatric_flag",
    "diagnosis_general_population_flag",
    "diagnosis_family_context_flag",
)

SYMPTOM_DOMAIN_COLUMNS = (
    "symptom_depressive_symptoms_z",
    "symptom_disorganization_z",
    "symptom_general_psychopathology_z",
    "symptom_mania_symptoms_z",
    "symptom_negative_symptoms_z",
    "symptom_positive_symptoms_z",
    "symptom_psychosis_symptoms_z",
)

COGNITION_DOMAIN_COLUMNS = (
    "cognition_executive_function_z",
    "cognition_general_intellectual_ability_z",
    "cognition_global_cognition_z",
    "cognition_processing_speed_z",
    "cognition_set_shifting_z",
    "cognition_verbal_ability_z",
    "cognition_verbal_memory_z",
    "cognition_visual_memory_z",
    "cognition_working_memory_z",
)

SYMPTOM_PROFILE_COLUMNS = COMMON_REPRESENTATION_COLUMNS + (
    "available_feature_count",
    "available_domain_count",
    "symptom_burden_mean_z",
) + SYMPTOM_DOMAIN_COLUMNS

COGNITION_PROFILE_COLUMNS = COMMON_REPRESENTATION_COLUMNS + (
    "available_feature_count",
    "available_domain_count",
    "cognition_performance_mean_z",
) + COGNITION_DOMAIN_COLUMNS

CLINICAL_SNAPSHOT_COLUMNS = COMMON_REPRESENTATION_COLUMNS + (
    "available_feature_count",
    "available_family_count",
    "symptom_burden_mean_z",
    "cognition_performance_mean_z",
    "functioning_status_mean_z",
    "treatment_exposure_count",
    "current_treatment_count",
    "modality_availability_count",
    "modality_type_count",
    "outcome_row_count",
)

REPRESENTATION_FAMILY_COLUMNS = {
    "diagnosis_anchor": DIAGNOSIS_ANCHOR_COLUMNS,
    "symptom_profile": SYMPTOM_PROFILE_COLUMNS,
    "cognition_profile": COGNITION_PROFILE_COLUMNS,
    "clinical_snapshot": CLINICAL_SNAPSHOT_COLUMNS,
}

REPRESENTATION_FAMILY_FILES = {
    "diagnosis_anchor": "diagnosis_anchor.csv",
    "symptom_profile": "symptom_profile.csv",
    "cognition_profile": "cognition_profile.csv",
    "clinical_snapshot": "clinical_snapshot.csv",
}

REPRESENTATION_MANIFEST_NAME = "representation_manifest.json"
RUN_MANIFEST_NAME = "benchmark_build_representations_run_manifest.json"

_DOMAIN_TO_COLUMN = {
    "depressive_symptoms": "symptom_depressive_symptoms_z",
    "disorganization": "symptom_disorganization_z",
    "general_psychopathology": "symptom_general_psychopathology_z",
    "mania_symptoms": "symptom_mania_symptoms_z",
    "negative_symptoms": "symptom_negative_symptoms_z",
    "positive_symptoms": "symptom_positive_symptoms_z",
    "psychosis_symptoms": "symptom_psychosis_symptoms_z",
    "executive_function": "cognition_executive_function_z",
    "general_intellectual_ability": "cognition_general_intellectual_ability_z",
    "global_cognition": "cognition_global_cognition_z",
    "processing_speed": "cognition_processing_speed_z",
    "set_shifting": "cognition_set_shifting_z",
    "verbal_ability": "cognition_verbal_ability_z",
    "verbal_memory": "cognition_verbal_memory_z",
    "visual_memory": "cognition_visual_memory_z",
    "working_memory": "cognition_working_memory_z",
}


@dataclass(frozen=True, slots=True)
class BenchmarkRepresentationArtifacts:
    """Paths and counts emitted by the benchmark representation builder."""

    harmonized_root: Path
    representations_root: Path
    manifests_root: Path
    family_paths: dict[str, Path]
    representation_manifest_path: Path
    run_manifest_path: Path
    family_row_counts: dict[str, int]

    def to_summary(self) -> dict[str, object]:
        return {
            "harmonized_dir": str(self.harmonized_root),
            "representations_dir": str(self.representations_root),
            "manifests_dir": str(self.manifests_root),
            "representation_manifest": str(self.representation_manifest_path),
            "run_manifest": str(self.run_manifest_path),
            "row_counts_by_family": dict(self.family_row_counts),
            **{family_name: str(path) for family_name, path in self.family_paths.items()},
        }


def run_benchmark_representation_build(
    *,
    harmonized_root: str | Path,
    representations_root: str | Path,
    manifests_root: str | Path,
    repo_root: str | Path | None,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
) -> BenchmarkRepresentationArtifacts:
    """Build deterministic cross-sectional representation artifacts from harmonized benchmark tables."""

    harmonized_path = Path(harmonized_root).resolve()
    representations_path = Path(representations_root).resolve()
    manifests_path = Path(manifests_root).resolve()
    generated_at = utc_now_iso()

    required_inputs = {
        "subjects": harmonized_path / "subjects.csv",
        "visits": harmonized_path / "visits.csv",
        "diagnoses": harmonized_path / "diagnoses.csv",
        "symptom_scores": harmonized_path / "symptom_scores.csv",
        "cognition_scores": harmonized_path / "cognition_scores.csv",
        "functioning_scores": harmonized_path / "functioning_scores.csv",
        "treatment_exposures": harmonized_path / "treatment_exposures.csv",
        "modality_features": harmonized_path / "modality_features.csv",
        "outcomes": harmonized_path / "outcomes.csv",
        "split_assignments": harmonized_path / "split_assignments.csv",
    }
    for input_path in required_inputs.values():
        if not input_path.exists():
            raise FileNotFoundError(f"Missing benchmark representation input at {input_path}")

    subjects = _read_csv_rows(required_inputs["subjects"])
    visits = _read_csv_rows(required_inputs["visits"])
    diagnoses = _read_csv_rows(required_inputs["diagnoses"])
    symptom_scores = _read_csv_rows(required_inputs["symptom_scores"])
    cognition_scores = _read_csv_rows(required_inputs["cognition_scores"])
    functioning_scores = _read_csv_rows(required_inputs["functioning_scores"])
    treatment_exposures = _read_csv_rows(required_inputs["treatment_exposures"])
    modality_features = _read_csv_rows(required_inputs["modality_features"])
    outcomes = _read_csv_rows(required_inputs["outcomes"])
    split_assignments = _read_csv_rows(required_inputs["split_assignments"])

    visit_contexts = _build_visit_contexts(
        subjects=subjects,
        visits=visits,
        diagnoses=diagnoses,
        split_assignments=split_assignments,
        outcomes=outcomes,
    )
    visit_keys = sorted(visit_contexts)

    symptom_domain_values = _aggregate_domain_values(
        rows=symptom_scores,
        positive_meaning="worse",
    )
    cognition_domain_values = _aggregate_domain_values(
        rows=cognition_scores,
        positive_meaning="better",
    )
    functioning_domain_values = _aggregate_domain_values(
        rows=functioning_scores,
        positive_meaning="better",
    )
    functioning_mean_by_visit = _aggregate_mean_values(
        rows=functioning_scores,
        positive_meaning="better",
    )

    treatment_rows_by_visit = _group_rows_by_visit(treatment_exposures)
    modality_rows_by_visit = _group_rows_by_visit(modality_features)
    outcome_rows_by_visit = _group_rows_by_visit(outcomes)

    family_rows = {
        "diagnosis_anchor": _build_diagnosis_anchor_rows(visit_keys, visit_contexts),
        "symptom_profile": _build_profile_rows(
            visit_keys,
            visit_contexts,
            domain_values_by_visit=symptom_domain_values,
            value_columns=SYMPTOM_DOMAIN_COLUMNS,
            mean_column="symptom_burden_mean_z",
        ),
        "cognition_profile": _build_profile_rows(
            visit_keys,
            visit_contexts,
            domain_values_by_visit=cognition_domain_values,
            value_columns=COGNITION_DOMAIN_COLUMNS,
            mean_column="cognition_performance_mean_z",
        ),
        "clinical_snapshot": _build_clinical_snapshot_rows(
            visit_keys=visit_keys,
            visit_contexts=visit_contexts,
            symptom_values_by_visit=symptom_domain_values,
            cognition_values_by_visit=cognition_domain_values,
            functioning_values_by_visit=functioning_domain_values,
            functioning_mean_by_visit=functioning_mean_by_visit,
            treatment_rows_by_visit=treatment_rows_by_visit,
            modality_rows_by_visit=modality_rows_by_visit,
            outcome_rows_by_visit=outcome_rows_by_visit,
        ),
    }

    family_paths: dict[str, Path] = {}
    family_row_counts: dict[str, int] = {}
    for family_name, rows in family_rows.items():
        destination = representations_path / REPRESENTATION_FAMILY_FILES[family_name]
        family_paths[family_name] = _write_csv_rows(
            rows,
            destination,
            fieldnames=REPRESENTATION_FAMILY_COLUMNS[family_name],
        )
        family_row_counts[family_name] = len(rows)

    representation_manifest_payload = {
        "seed": seed,
        "input_paths": {
            input_name: _stable_input_reference(path, anchor=harmonized_path)
            for input_name, path in required_inputs.items()
        },
        "output_paths": {
            family_name: _stable_output_reference(path, anchor=representations_path)
            for family_name, path in family_paths.items()
        }
        | {"representation_manifest": REPRESENTATION_MANIFEST_NAME},
        "row_counts_by_family": family_row_counts,
        "family_summaries": {
            family_name: _family_summary(
                rows,
                fieldnames=REPRESENTATION_FAMILY_COLUMNS[family_name],
            )
            for family_name, rows in family_rows.items()
        },
        "cross_sectional_only_cohorts": [
            cohort_id
            for cohort_id, context in _cohort_support_summary(visit_contexts).items()
            if not context["outcome_row_supported"]
        ],
        "outcome_bearing_cohorts": [
            cohort_id
            for cohort_id, context in _cohort_support_summary(visit_contexts).items()
            if context["outcome_row_supported"]
        ],
        "claim_boundary_statement": (
            "Representation artifacts extend cross-sectional comparison support only. They do not upgrade the repo "
            "beyond narrow-go or narrow_outcome_benchmark, and they do not constitute model benchmarking."
        ),
        "current_limitations": [
            "ucla-cnp-ds000030 and ds000115 remain cross-sectional-only cohorts with zero outcome rows.",
            "tcp-ds005237 remains label-limited and does not support psychosis-specific equivalence.",
            "These artifacts standardize public measures conservatively for representation comparison only; no predictive claim is implied.",
        ],
    }
    representation_manifest_path = write_json_artifact(
        representation_manifest_payload,
        representations_path / REPRESENTATION_MANIFEST_NAME,
    )

    run_manifest_path = write_run_manifest(
        build_run_manifest(
            dataset_source="benchmark",
            command=command,
            git_sha=git_sha,
            seed=seed,
            repo_root=repo_root,
            output_paths={
                **family_paths,
                "representation_manifest": representation_manifest_path,
            },
            timestamp=generated_at,
        ),
        manifests_path / RUN_MANIFEST_NAME,
    )

    return BenchmarkRepresentationArtifacts(
        harmonized_root=harmonized_path,
        representations_root=representations_path,
        manifests_root=manifests_path,
        family_paths=family_paths,
        representation_manifest_path=representation_manifest_path,
        run_manifest_path=run_manifest_path,
        family_row_counts=family_row_counts,
    )


def _build_visit_contexts(
    *,
    subjects: list[dict[str, str]],
    visits: list[dict[str, str]],
    diagnoses: list[dict[str, str]],
    split_assignments: list[dict[str, str]],
    outcomes: list[dict[str, str]],
) -> dict[tuple[str, str, str], dict[str, str]]:
    subjects_by_id = {row["subject_id"]: row for row in subjects}
    split_by_subject = {row["subject_id"]: row["split_name"] for row in split_assignments}
    primary_diagnosis_by_visit = _primary_diagnosis_by_visit(diagnoses)
    outcome_visit_ids = {row["visit_id"] for row in outcomes if row.get("outcome_value", "").strip()}

    contexts: dict[tuple[str, str, str], dict[str, str]] = {}
    for visit in visits:
        subject = subjects_by_id[visit["subject_id"]]
        diagnosis = primary_diagnosis_by_visit.get(
            visit["visit_id"],
            {
                "diagnosis_group": "",
                "diagnosis_granularity": "",
            },
        )
        visit_key = (visit["cohort_id"], visit["subject_id"], visit["visit_id"])
        contexts[visit_key] = {
            "cohort_id": visit["cohort_id"],
            "subject_id": visit["subject_id"],
            "visit_id": visit["visit_id"],
            "split_name": split_by_subject.get(visit["subject_id"], ""),
            "site_id": subject.get("site_id", ""),
            "population_scope": subject.get("population_scope", ""),
            "enrollment_group": subject.get("enrollment_group", ""),
            "diagnosis_group": diagnosis.get("diagnosis_group", ""),
            "diagnosis_granularity": diagnosis.get("diagnosis_granularity", ""),
            "representation_comparison_support": subject.get("representation_comparison_support", ""),
            "has_longitudinal_followup": subject.get("has_longitudinal_followup", ""),
            "outcome_row_available": "true" if visit["visit_id"] in outcome_visit_ids else "false",
        }
    return contexts


def _primary_diagnosis_by_visit(diagnoses: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    primary: dict[str, dict[str, str]] = {}
    sorted_rows = sorted(
        diagnoses,
        key=lambda row: (
            row["visit_id"],
            row.get("is_primary_diagnosis", ""),
            row.get("diagnosis_group", ""),
        ),
    )
    for row in sorted_rows:
        visit_id = row["visit_id"]
        if visit_id in primary:
            continue
        if row.get("is_primary_diagnosis", "").strip().lower() in {"true", "1", "yes"}:
            primary[visit_id] = row
    for row in sorted_rows:
        primary.setdefault(row["visit_id"], row)
    return primary


def _build_diagnosis_anchor_rows(
    visit_keys: list[tuple[str, str, str]],
    visit_contexts: dict[tuple[str, str, str], dict[str, str]],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for visit_key in visit_keys:
        context = dict(visit_contexts[visit_key])
        diagnosis_group = context["diagnosis_group"]
        flags = _diagnosis_flags(diagnosis_group)
        rows.append(
            {
                **context,
                "available_feature_count": str(sum(flags.values())),
                **{column_name: str(value) for column_name, value in flags.items()},
            }
        )
    return rows


def _diagnosis_flags(diagnosis_group: str) -> dict[str, int]:
    return {
        "diagnosis_is_case": int(
            diagnosis_group
            in {
                "psychosis",
                "schizophrenia",
                "bipolar_disorder",
                "adhd",
                "broad_psychiatric_patient",
            }
        ),
        "diagnosis_is_control": int(
            diagnosis_group in {"control", "general_population_control"}
        ),
        "diagnosis_psychosis_flag": int(diagnosis_group in {"psychosis", "schizophrenia"}),
        "diagnosis_schizophrenia_flag": int(diagnosis_group == "schizophrenia"),
        "diagnosis_bipolar_flag": int(diagnosis_group == "bipolar_disorder"),
        "diagnosis_adhd_flag": int(diagnosis_group == "adhd"),
        "diagnosis_broad_psychiatric_flag": int(diagnosis_group == "broad_psychiatric_patient"),
        "diagnosis_general_population_flag": int(diagnosis_group == "general_population_control"),
        "diagnosis_family_context_flag": int(
            diagnosis_group in {"schizophrenia_sibling", "control_sibling"}
        ),
    }


def _aggregate_domain_values(
    *,
    rows: list[dict[str, str]],
    positive_meaning: str,
) -> dict[str, dict[str, float]]:
    standardized_by_measure: dict[
        tuple[str, str, str, str],
        dict[tuple[str, str, str], float],
    ] = {}
    measure_groups: dict[tuple[str, str, str, str], list[tuple[tuple[str, str, str], float]]] = defaultdict(list)
    for row in rows:
        score = _parse_float(row.get("score"))
        if score is None:
            continue
        visit_key = (row["cohort_id"], row["subject_id"], row["visit_id"])
        measure_key = (
            row["cohort_id"],
            row.get("domain", ""),
            row.get("instrument", ""),
            row.get("measure", ""),
        )
        oriented_score = _orient_score(score, row.get("score_direction", ""), positive_meaning=positive_meaning)
        measure_groups[measure_key].append((visit_key, oriented_score))

    for measure_key, visit_values in measure_groups.items():
        standardized_by_measure[measure_key] = _zscore_visit_values(visit_values)

    domain_values_by_visit: dict[str, dict[str, float]] = defaultdict(dict)
    for measure_key, visit_scores in standardized_by_measure.items():
        _, domain, _, _ = measure_key
        target_column = _DOMAIN_TO_COLUMN.get(domain)
        if target_column is None:
            continue
        visit_domain_scores: dict[tuple[str, str, str], list[float]] = defaultdict(list)
        for visit_key, standardized_score in visit_scores.items():
            visit_domain_scores[visit_key].append(standardized_score)
        for visit_key, standardized_scores in visit_domain_scores.items():
            domain_values_by_visit[visit_key][target_column] = sum(standardized_scores) / len(standardized_scores)
    return domain_values_by_visit


def _aggregate_mean_values(
    *,
    rows: list[dict[str, str]],
    positive_meaning: str,
) -> dict[tuple[str, str, str], float]:
    measure_groups: dict[tuple[str, str, str, str], list[tuple[tuple[str, str, str], float]]] = defaultdict(list)
    for row in rows:
        score = _parse_float(row.get("score"))
        if score is None:
            continue
        visit_key = (row["cohort_id"], row["subject_id"], row["visit_id"])
        measure_key = (
            row["cohort_id"],
            row.get("domain", ""),
            row.get("instrument", ""),
            row.get("measure", ""),
        )
        oriented_score = _orient_score(score, row.get("score_direction", ""), positive_meaning=positive_meaning)
        measure_groups[measure_key].append((visit_key, oriented_score))

    visit_level_values: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for visit_scores in (_zscore_visit_values(group_rows) for group_rows in measure_groups.values()):
        for visit_key, standardized_score in visit_scores.items():
            visit_level_values[visit_key].append(standardized_score)

    return {
        visit_key: sum(values) / len(values)
        for visit_key, values in visit_level_values.items()
        if values
    }


def _orient_score(score: float, score_direction: str, *, positive_meaning: str) -> float:
    direction = score_direction.strip().lower()
    if positive_meaning == "better":
        if direction in {"higher_better", ""}:
            return score
        return -score
    if direction in {"higher_worse", ""}:
        return score
    return -score


def _zscore_visit_values(
    visit_values: list[tuple[tuple[str, str, str], float]],
) -> dict[tuple[str, str, str], float]:
    if not visit_values:
        return {}
    values = [value for _, value in visit_values]
    mean_value = sum(values) / len(values)
    variance = sum((value - mean_value) ** 2 for value in values) / len(values)
    stddev = math.sqrt(variance)
    if stddev == 0:
        return {visit_key: 0.0 for visit_key, _ in visit_values}
    return {
        visit_key: (value - mean_value) / stddev
        for visit_key, value in visit_values
    }


def _build_profile_rows(
    visit_keys: list[tuple[str, str, str]],
    visit_contexts: dict[tuple[str, str, str], dict[str, str]],
    *,
    domain_values_by_visit: dict[tuple[str, str, str], dict[str, float]],
    value_columns: tuple[str, ...],
    mean_column: str,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for visit_key in visit_keys:
        context = dict(visit_contexts[visit_key])
        domain_values = domain_values_by_visit.get(visit_key, {})
        feature_values = {
            column_name: _format_optional_float(domain_values.get(column_name))
            for column_name in value_columns
        }
        available_values = [value for value in domain_values.values() if value is not None]
        rows.append(
            {
                **context,
                "available_feature_count": str(len(available_values)),
                "available_domain_count": str(len(available_values)),
                mean_column: _format_optional_float(
                    sum(available_values) / len(available_values) if available_values else None
                ),
                **feature_values,
            }
        )
    return rows


def _build_clinical_snapshot_rows(
    *,
    visit_keys: list[tuple[str, str, str]],
    visit_contexts: dict[tuple[str, str, str], dict[str, str]],
    symptom_values_by_visit: dict[tuple[str, str, str], dict[str, float]],
    cognition_values_by_visit: dict[tuple[str, str, str], dict[str, float]],
    functioning_values_by_visit: dict[tuple[str, str, str], dict[str, float]],
    functioning_mean_by_visit: dict[tuple[str, str, str], float],
    treatment_rows_by_visit: dict[tuple[str, str, str], list[dict[str, str]]],
    modality_rows_by_visit: dict[tuple[str, str, str], list[dict[str, str]]],
    outcome_rows_by_visit: dict[tuple[str, str, str], list[dict[str, str]]],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for visit_key in visit_keys:
        context = dict(visit_contexts[visit_key])
        symptom_values = list(symptom_values_by_visit.get(visit_key, {}).values())
        cognition_values = list(cognition_values_by_visit.get(visit_key, {}).values())
        functioning_values = list(functioning_values_by_visit.get(visit_key, {}).values())
        functioning_mean = functioning_mean_by_visit.get(visit_key)
        treatment_rows = treatment_rows_by_visit.get(visit_key, [])
        modality_rows = modality_rows_by_visit.get(visit_key, [])
        outcome_rows = outcome_rows_by_visit.get(visit_key, [])
        modality_availability_count = sum(
            1 for row in modality_rows if _parse_float(row.get("feature_value")) not in {None, 0.0}
        )
        modality_type_count = len(
            {
                row.get("modality_type", "")
                for row in modality_rows
                if _parse_float(row.get("feature_value")) not in {None, 0.0}
            }
        )
        current_treatment_count = sum(
            1 for row in treatment_rows if row.get("is_current_exposure", "").strip().lower() in {"true", "1", "yes"}
        )
        available_family_count = sum(
            1
            for available in (
                bool(symptom_values),
                bool(cognition_values),
                bool(functioning_values),
                bool(treatment_rows),
                bool(modality_availability_count),
            )
            if available
        )
        rows.append(
            {
                **context,
                "available_feature_count": str(
                    sum(
                        1
                        for value in (
                            _mean_or_none(symptom_values),
                            _mean_or_none(cognition_values),
                            functioning_mean if functioning_mean is not None else _mean_or_none(functioning_values),
                        )
                        if value is not None
                    )
                    + int(bool(treatment_rows))
                    + int(bool(modality_availability_count))
                    + int(bool(outcome_rows))
                ),
                "available_family_count": str(available_family_count),
                "symptom_burden_mean_z": _format_optional_float(_mean_or_none(symptom_values)),
                "cognition_performance_mean_z": _format_optional_float(_mean_or_none(cognition_values)),
                "functioning_status_mean_z": _format_optional_float(
                    functioning_mean if functioning_mean is not None else _mean_or_none(functioning_values)
                ),
                "treatment_exposure_count": str(len(treatment_rows)),
                "current_treatment_count": str(current_treatment_count),
                "modality_availability_count": str(modality_availability_count),
                "modality_type_count": str(modality_type_count),
                "outcome_row_count": str(len(outcome_rows)),
            }
        )
    return rows


def _family_summary(rows: list[dict[str, str]], *, fieldnames: tuple[str, ...]) -> dict[str, Any]:
    numeric_counts_by_cohort = Counter(row["cohort_id"] for row in rows)
    numeric_counts_by_split = Counter(row["split_name"] for row in rows)
    rows_with_any_features_by_cohort: Counter[str] = Counter()
    rows_with_any_features_by_split: Counter[str] = Counter()
    for row in rows:
        feature_count = _parse_int(row.get("available_feature_count")) or 0
        if feature_count > 0:
            rows_with_any_features_by_cohort[row["cohort_id"]] += 1
            rows_with_any_features_by_split[row["split_name"]] += 1
    return {
        "feature_columns": [
            column_name
            for column_name in fieldnames
            if column_name not in COMMON_REPRESENTATION_COLUMNS
        ],
        "row_count": len(rows),
        "counts_by_cohort": dict(sorted(numeric_counts_by_cohort.items())),
        "counts_by_split": dict(sorted(numeric_counts_by_split.items())),
        "rows_with_any_features_by_cohort": dict(sorted(rows_with_any_features_by_cohort.items())),
        "rows_with_any_features_by_split": dict(sorted(rows_with_any_features_by_split.items())),
    }


def _cohort_support_summary(
    visit_contexts: dict[tuple[str, str, str], dict[str, str]],
) -> dict[str, dict[str, bool]]:
    summary: dict[str, dict[str, bool]] = {}
    for context in visit_contexts.values():
        cohort_id = context["cohort_id"]
        support = summary.setdefault(cohort_id, {"outcome_row_supported": False})
        if context["outcome_row_available"] == "true":
            support["outcome_row_supported"] = True
    return summary


def _group_rows_by_visit(rows: list[dict[str, str]]) -> dict[tuple[str, str, str], list[dict[str, str]]]:
    grouped: dict[tuple[str, str, str], list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[(row["cohort_id"], row["subject_id"], row["visit_id"])].append(row)
    return grouped


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv_rows(
    rows: list[dict[str, str]],
    destination: Path,
    *,
    fieldnames: tuple[str, ...],
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(fieldnames),
            lineterminator="\n",
        )
        writer.writeheader()
        for row in sorted(rows, key=lambda item: tuple(item.get(fieldname, "") for fieldname in fieldnames)):
            writer.writerow({fieldname: row.get(fieldname, "") for fieldname in fieldnames})
    return destination


def _stable_input_reference(path: Path, *, anchor: Path) -> str:
    try:
        return str(path.resolve().relative_to(anchor.resolve()))
    except ValueError:
        return path.name


def _stable_output_reference(path: Path, *, anchor: Path) -> str:
    try:
        return str(path.resolve().relative_to(anchor.resolve()))
    except ValueError:
        return path.name


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return float(stripped)
    except ValueError:
        return None


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return int(stripped)
    except ValueError:
        return None


def _mean_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _format_optional_float(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.6f}".rstrip("0").rstrip(".")


__all__ = [
    "BenchmarkRepresentationArtifacts",
    "CLINICAL_SNAPSHOT_COLUMNS",
    "COGNITION_DOMAIN_COLUMNS",
    "COGNITION_PROFILE_COLUMNS",
    "COMMON_REPRESENTATION_COLUMNS",
    "DIAGNOSIS_ANCHOR_COLUMNS",
    "REPRESENTATION_FAMILY_COLUMNS",
    "REPRESENTATION_FAMILY_FILES",
    "REPRESENTATION_MANIFEST_NAME",
    "RUN_MANIFEST_NAME",
    "SYMPTOM_DOMAIN_COLUMNS",
    "SYMPTOM_PROFILE_COLUMNS",
    "run_benchmark_representation_build",
]

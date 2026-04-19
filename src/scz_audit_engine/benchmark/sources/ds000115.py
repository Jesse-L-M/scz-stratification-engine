"""Metadata and harmonization adapter for the ds000115 working-memory cohort."""

from __future__ import annotations

import csv
import io
from pathlib import Path

from ..dataset_registry import DatasetRegistryEntry
from .base import CohortHarmonizationBundle, OpenNeuroSnapshotBundle, OpenNeuroSourceAdapter

DS000115_DIAGNOSIS_CAVEAT = (
    "This public cohort mixes schizophrenia, siblings, and controls in a family-structured design; labels are "
    "preserved explicitly without upgrading the benchmark claim beyond cross-sectional representation coverage."
)
DS000115_SYMPTOM_CAVEAT = (
    "Public symptom rows are derived within-release summary z-scores only and do not imply a benchmarkable "
    "outcome family."
)
DS000115_COGNITION_CAVEAT = (
    "Public cognition rows remain baseline cross-sectional measures only and are carried through without "
    "cross-cohort renorming."
)
TSV_NULL_VALUES = frozenset({"", "n/a", "na", "nan", "null", "none", "."})


def _count_tabulated_values(table_text: str | None, column_name: str) -> dict[str, int]:
    if not table_text:
        return {}
    counts: dict[str, int] = {}
    rows = csv.DictReader(io.StringIO(table_text), delimiter="\t")
    for row in rows:
        value = row.get(column_name, "").strip()
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def _has_any_non_null_value(table_text: str | None, *, prefixes: tuple[str, ...]) -> bool:
    if not table_text:
        return False
    rows = csv.DictReader(io.StringIO(table_text), delimiter="\t")
    for row in rows:
        for field_name, value in row.items():
            if not field_name.startswith(prefixes):
                continue
            if str(value or "").strip().lower() not in TSV_NULL_VALUES:
                return True
    return False


def _read_tsv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle, delimiter="\t")]


def _canonical_subject_id(cohort_id: str, source_subject_id: str) -> str:
    return f"{cohort_id}:{source_subject_id}"


def _canonical_visit_id(subject_id: str, visit_label: str = "baseline") -> str:
    return f"{subject_id}:{visit_label}"


def _bool_string(value: bool) -> str:
    return "true" if value else "false"


def _clean_tsv_value(value: str | None) -> str:
    text = str(value or "").strip()
    if text.lower() in TSV_NULL_VALUES:
        return ""
    return text


def _append_unsupported_field(
    unsupported_fields: dict[str, tuple[str, ...]],
    table_name: str,
    message: str,
) -> None:
    unsupported_fields[table_name] = unsupported_fields.get(table_name, ()) + (message,)


def _diagnosis_group(enrollment_group: str) -> str:
    if enrollment_group == "SCZ":
        return "schizophrenia"
    if enrollment_group == "SCZ-SIB":
        return "schizophrenia_sibling"
    if enrollment_group == "CON":
        return "control"
    if enrollment_group == "CON-SIB":
        return "control_sibling"
    return "unknown"


def _subject_mapping_note(enrollment_group: str) -> str:
    if enrollment_group == "SCZ":
        return "Schizophrenia rows are retained as low-weight cross-sectional representation support only."
    if enrollment_group == "SCZ-SIB":
        return "Sibling rows are preserved explicitly to avoid collapsing a family-structured cohort into a case-control claim."
    if enrollment_group == "CON-SIB":
        return "Control-sibling rows remain explicit family-context comparison subjects only."
    if enrollment_group == "CON":
        return "Control rows are retained as contextual comparison subjects only."
    return "Public participant label is missing or unmapped; diagnosis group remains unknown."


class DS000115BenchmarkSourceAdapter(OpenNeuroSourceAdapter):
    """Audit and harmonize the ds000115 public snapshot conservatively."""

    source_identifier = "ds000115"
    supports_harmonization = True
    dataset_accession = "ds000115"
    dataset_tag = "00001"
    github_repo = "OpenNeuroDatasets/ds000115"
    candidate_root_names = ("ds000115",)

    def normalize_bundle(self, bundle: OpenNeuroSnapshotBundle) -> DatasetRegistryEntry:
        dataset_name = str(bundle.dataset["latestSnapshot"]["description"]["Name"])
        participant_groups = _count_tabulated_values(bundle.participants_tsv, "condit")
        total_rows = sum(participant_groups.values())
        symptom_scales = (
            ("SAPS", "SANS")
            if _has_any_non_null_value(
                bundle.participants_tsv,
                prefixes=("saps", "sans"),
            )
            else ()
        )
        cognition_scales = (
            "D-prime",
            "Trail Making B",
            "WCST",
            "WAIS",
            "Logical Memory",
            "LNS",
            "DST",
            "n-back accuracy/RT",
        )
        return DatasetRegistryEntry(
            dataset_id=self.source_identifier,
            dataset_label=dataset_name,
            access_tier="strict_open",
            population_scope=(
                "schizophrenia, sibling, and control working-memory cohort"
            ),
            diagnosis_coverage=(
                "participants.tsv exposes SCZ, SCZ-SIB, CON, and CON-SIB groups "
                f"({participant_groups.get('SCZ', 0)} SCZ, "
                f"{participant_groups.get('SCZ-SIB', 0)} SCZ-SIB, "
                f"{participant_groups.get('CON', 0)} CON, "
                f"{participant_groups.get('CON-SIB', 0)} CON-SIB)."
            ),
            symptom_scales=symptom_scales,
            cognition_scales=cognition_scales,
            functioning_scales=(),
            treatment_variables=(),
            longitudinal_coverage=(
                "No repeated follow-up visit schedule or prospective public endpoint is "
                "described in the pinned snapshot metadata or README."
            ),
            outcome_availability=(
                "The public snapshot exposes cross-sectional symptom severity and working-"
                "memory performance only. It does not confirm a public functioning scale "
                "or any benchmarkable longitudinal outcome family."
            ),
            modality_availability=("MRI", "fMRI"),
            site_structure=(
                "single published accession; the public snapshot is not described as a "
                "multi-site external-validation cohort"
            ),
            sample_size_note=(
                f"participants.tsv lists {total_rows} rows "
                f"({participant_groups.get('SCZ', 0)} SCZ, "
                f"{participant_groups.get('SCZ-SIB', 0)} SCZ-SIB, "
                f"{participant_groups.get('CON', 0)} CON, "
                f"{participant_groups.get('CON-SIB', 0)} CON-SIB)."
            ),
            known_limitations=(
                "The strict-open snapshot is small, family-structured, and entirely "
                "cross-sectional. It lacks public functioning outcomes, relapse/remission "
                "windows, or other benchmarkable longitudinal endpoints."
            ),
            local_status="audited",
            benchmark_v0_eligibility="ineligible",
            representation_comparison_support="strong",
            predictor_timepoint="unmapped",
            outcome_timepoint="unmapped",
            outcome_window="unmapped",
            outcome_is_prospective=False,
            concurrent_endpoint_only=False,
            outcome_temporal_validity="none",
            benchmarkable_outcome_families=(),
            provenance_urls=(
                self.dataset_page_url,
                self.readme_url,
                self.participants_url,
            ),
            audit_summary=(
                "Strict-open schizophrenia working-memory cohort with explicit case, "
                "sibling, and control labels plus cross-sectional cognition. It is a "
                "low-weight representation sanity-check cohort only and does not add a "
                "public outcome benchmark family."
            ),
        )

    def harmonize(self, cohort_root: str | Path) -> CohortHarmonizationBundle:
        cohort_path = Path(cohort_root).resolve()
        audit_entry = self.__class__(snapshot_root=cohort_path).audit()
        participants = _read_tsv_rows(cohort_path / "participants.tsv")

        subject_rows: list[dict[str, str]] = []
        visit_rows: list[dict[str, str]] = []
        diagnosis_rows: list[dict[str, str]] = []
        subject_lookup: dict[str, dict[str, str]] = {}

        for participant in participants:
            source_subject_id = participant.get("participant_id", "").strip()
            if not source_subject_id:
                continue
            subject_id = _canonical_subject_id(self.source_identifier, source_subject_id)
            enrollment_group = participant.get("condit", "").strip() or "unknown"
            baseline_age = participant.get("age", "").strip()
            visit_id = _canonical_visit_id(subject_id)
            subject_row = {
                "cohort_id": self.source_identifier,
                "subject_id": subject_id,
                "source_subject_id": source_subject_id,
                "population_scope": audit_entry.population_scope,
                "site_id": "single_site_public_accession",
                "sex": participant.get("gender", "").strip(),
                "baseline_age": baseline_age,
                "enrollment_group": enrollment_group,
                "has_longitudinal_followup": _bool_string(False),
                "representation_comparison_support": audit_entry.representation_comparison_support,
                "ancestry_group": "",
                "race_ethnicity": participant.get("race", "").strip(),
                "education_years": participant.get("yrschool", "").strip(),
                "mapping_note": _subject_mapping_note(enrollment_group),
            }
            subject_rows.append(subject_row)
            subject_lookup[source_subject_id] = subject_row

            visit_rows.append(
                {
                    "cohort_id": self.source_identifier,
                    "subject_id": subject_id,
                    "visit_id": visit_id,
                    "source_visit_id": "baseline",
                    "visit_order": "1",
                    "visit_timepoint_label": "baseline",
                    "visit_age": baseline_age,
                    "days_from_baseline": "0",
                    "is_baseline": _bool_string(True),
                    "visit_window_label": "same_visit",
                    "visit_status": "observed",
                    "visit_note": "Public snapshot exposes baseline cross-sectional rows only.",
                }
            )

            diagnosis_rows.append(
                {
                    "cohort_id": self.source_identifier,
                    "subject_id": subject_id,
                    "visit_id": visit_id,
                    "diagnosis_system": "public_participants_label",
                    "diagnosis_label": enrollment_group,
                    "diagnosis_group": _diagnosis_group(enrollment_group),
                    "diagnosis_granularity": "schizophrenia_sibling_control_public_label",
                    "is_primary_diagnosis": _bool_string(True),
                    "mapping_caveat": DS000115_DIAGNOSIS_CAVEAT,
                    "diagnosis_code": "",
                    "source_diagnosis_label": enrollment_group,
                    "diagnosis_note": "Labels come directly from participants.tsv.",
                }
            )

        unsupported_fields: dict[str, tuple[str, ...]] = {
            "functioning_scores": (
                "No functioning scale is staged in the current ds000115 root.",
            ),
            "treatment_exposures": (
                "No treatment-exposure inventory is staged in the current ds000115 root.",
            ),
            "outcomes": (
                "ds000115 remains a low-weight cross-sectional representation cohort only; no benchmarkable outcome rows are emitted.",
            ),
            "modality_features": (
                "No subject-level modality availability rows are emitted for ds000115 in this PR.",
            ),
        }

        symptom_rows = self._harmonize_symptom_rows(
            participants=participants,
            subject_lookup=subject_lookup,
            unsupported_fields=unsupported_fields,
        )
        cognition_rows = self._harmonize_cognition_rows(
            participants=participants,
            subject_lookup=subject_lookup,
            unsupported_fields=unsupported_fields,
        )

        return CohortHarmonizationBundle(
            cohort_id=self.source_identifier,
            input_root=cohort_path,
            audit_entry=audit_entry,
            tables={
                "subjects": tuple(subject_rows),
                "visits": tuple(visit_rows),
                "diagnoses": tuple(diagnosis_rows),
                "symptom_scores": tuple(symptom_rows),
                "cognition_scores": tuple(cognition_rows),
                "functioning_scores": (),
                "treatment_exposures": (),
                "outcomes": (),
                "modality_features": (),
            },
            caveats=(
                "ds000115 stays a small cross-sectional representation cohort; outcome rows remain empty.",
                "Sibling rows remain explicit to preserve the family-structured public design rather than smoothing them into a stronger benchmark claim.",
            ),
            unsupported_fields=unsupported_fields,
        )

    def _harmonize_symptom_rows(
        self,
        *,
        participants: list[dict[str, str]],
        subject_lookup: dict[str, dict[str, str]],
        unsupported_fields: dict[str, tuple[str, ...]],
    ) -> list[dict[str, str]]:
        specs = (
            ("z_pos_4grp", "positive_symptoms"),
            ("z_neg_4grp", "negative_symptoms"),
            ("z_dis_4grp", "disorganization"),
        )
        rows: list[dict[str, str]] = []
        for participant in participants:
            source_subject_id = participant.get("participant_id", "").strip()
            if source_subject_id not in subject_lookup:
                continue
            subject_row = subject_lookup[source_subject_id]
            visit_id = _canonical_visit_id(subject_row["subject_id"])
            for source_score_label, domain in specs:
                score_value = _clean_tsv_value(participant.get(source_score_label))
                if not score_value:
                    continue
                rows.append(
                    {
                        "cohort_id": self.source_identifier,
                        "subject_id": subject_row["subject_id"],
                        "visit_id": visit_id,
                        "instrument": "derived_symptom_z",
                        "domain": domain,
                        "measure": "group_standardized_z_score",
                        "score": score_value,
                        "score_direction": "higher_worse",
                        "is_harmonized_domain_score": _bool_string(False),
                        "mapping_caveat": DS000115_SYMPTOM_CAVEAT,
                        "score_unit": "z_score",
                        "instrument_version": "",
                        "source_score_label": source_score_label,
                    }
                )
        if not rows:
            _append_unsupported_field(
                unsupported_fields,
                "symptom_scores",
                "No supported symptom summary z-score columns were present in the staged ds000115 participants.tsv file.",
            )
        return rows

    def _harmonize_cognition_rows(
        self,
        *,
        participants: list[dict[str, str]],
        subject_lookup: dict[str, dict[str, str]],
        unsupported_fields: dict[str, tuple[str, ...]],
    ) -> list[dict[str, str]]:
        specs = (
            ("d4prime", "D-prime", "working_memory", "discriminability_index", "higher_better", "", ""),
            ("TRAILB", "Trail Making B", "executive_function", "completion_time", "lower_better", "seconds", ""),
            ("WCSTPSVE", "WCST", "set_shifting", "perseverative_errors", "lower_better", "count", ""),
            ("WAIS_MATRICS_SCALE", "WAIS", "general_intellectual_ability", "matrics_scaled_score", "higher_better", "scaled_score", ""),
            ("LOGIALMEMORY_SCALE", "Logical Memory", "verbal_memory", "scaled_score", "higher_better", "scaled_score", ""),
            ("LNS_SCALE", "LNS", "working_memory", "scaled_score", "higher_better", "scaled_score", ""),
            ("DST_SCALE", "DST", "processing_speed", "scaled_score", "higher_better", "scaled_score", ""),
            ("nback2_targ", "n-back", "working_memory", "two_back_target_accuracy", "higher_better", "proportion", "2-back"),
            (
                "nback2_targ_medrt",
                "n-back",
                "working_memory",
                "two_back_target_median_response_time",
                "lower_better",
                "milliseconds",
                "2-back",
            ),
        )
        rows: list[dict[str, str]] = []
        for participant in participants:
            source_subject_id = participant.get("participant_id", "").strip()
            if source_subject_id not in subject_lookup:
                continue
            subject_row = subject_lookup[source_subject_id]
            visit_id = _canonical_visit_id(subject_row["subject_id"])
            for source_score_label, instrument, domain, measure, score_direction, score_unit, task_name in specs:
                score_value = _clean_tsv_value(participant.get(source_score_label))
                if not score_value:
                    continue
                rows.append(
                    {
                        "cohort_id": self.source_identifier,
                        "subject_id": subject_row["subject_id"],
                        "visit_id": visit_id,
                        "instrument": instrument,
                        "domain": domain,
                        "measure": measure,
                        "score": score_value,
                        "score_direction": score_direction,
                        "mapping_caveat": DS000115_COGNITION_CAVEAT,
                        "score_unit": score_unit,
                        "task_name": task_name,
                        "source_score_label": source_score_label,
                    }
                )
        if not rows:
            _append_unsupported_field(
                unsupported_fields,
                "cognition_scores",
                "No supported cognition columns were present in the staged ds000115 participants.tsv file.",
            )
        return rows


__all__ = ["DS000115BenchmarkSourceAdapter"]

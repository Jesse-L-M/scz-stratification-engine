"""Metadata and harmonization adapter for the public first-episode psychosis EEG cohort."""

from __future__ import annotations

import csv
import io
from pathlib import Path

from ..dataset_registry import DatasetRegistryEntry
from .base import CohortHarmonizationBundle, OpenNeuroSnapshotBundle, OpenNeuroSourceAdapter

FEP_DIAGNOSIS_CAVEAT = (
    "Public labels are psychosis versus control only; finer psychosis subtype equivalence is not claimed."
)
FEP_OUTCOME_CAVEAT = (
    "Same-visit functioning measures support a concurrent poor functional outcome benchmark only; no "
    "prospective endpoint is implied."
)
FEP_MODALITY_CAVEAT = (
    "Only modality availability is emitted here; no EEG representation features are built in this PR."
)
TSV_NULL_VALUES = frozenset({"", "n/a", "na", "nan", "null", "none"})
EEG_FILE_SUFFIXES = frozenset({".bdf", ".edf", ".eeg", ".fdt", ".set", ".vhdr", ".vmrk"})


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


def _read_tsv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle, delimiter="\t")]


def _read_tsv_table(path: Path) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    if not path.exists():
        return (), []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        rows = [dict(row) for row in reader]
        return tuple(reader.fieldnames or ()), rows


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


def _format_numeric_value(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return format(value, ".10g")


def _race_ethnicity(row: dict[str, str]) -> str:
    race = row.get("race", "").strip()
    ethnicity = row.get("ethnicity", "").strip()
    if race and ethnicity:
        return f"{race}; {ethnicity}"
    return race or ethnicity


def _subject_mapping_note(enrollment_group: str) -> str:
    if enrollment_group == "Control":
        return "Control participants are retained as contextual comparison rows, not as a stronger benchmark claim."
    if enrollment_group == "Psychosis":
        return "Public psychosis labels support the current narrow-go lane but not subtype-specific equivalence."
    return "Public participant label is missing or unmapped; diagnosis group remains unknown."


def _diagnosis_group(enrollment_group: str) -> str:
    if enrollment_group == "Psychosis":
        return "psychosis"
    if enrollment_group == "Control":
        return "control"
    return "unknown"


def _harmonize_modality_rows(
    cohort_path: Path,
    *,
    cohort_id: str,
    subject_lookup: dict[str, dict[str, str]],
    unsupported_fields: dict[str, tuple[str, ...]],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for source_subject_id, subject_row in subject_lookup.items():
        subject_root = cohort_path / source_subject_id
        if not subject_root.exists() or not _has_subject_level_eeg_files(subject_root):
            continue
        rows.append(
            {
                "cohort_id": cohort_id,
                "subject_id": subject_row["subject_id"],
                "visit_id": _canonical_visit_id(subject_row["subject_id"]),
                "modality_type": "EEG",
                "feature_name": "available",
                "feature_value": "1",
                "feature_unit": "binary",
                "feature_source": "staged_subject_tree",
                "mapping_caveat": FEP_MODALITY_CAVEAT,
                "feature_group": "availability",
                "preprocessing_version": "",
                "feature_quality_flag": "not_computed",
            }
        )
    if not rows:
        unsupported_fields["modality_features"] = unsupported_fields.get("modality_features", ()) + (
            "No staged subject-level EEG files were present in the current root.",
        )
    return rows


def _has_subject_level_eeg_files(subject_root: Path) -> bool:
    for path in subject_root.rglob("*"):
        if not path.is_file():
            continue
        relative_parts = tuple(part.lower() for part in path.relative_to(subject_root).parts)
        if "eeg" in relative_parts:
            return True
        if path.suffix.lower() in EEG_FILE_SUFFIXES:
            return True
    return False


def _resolve_score_from_aliases(
    row: dict[str, str],
    aliases: tuple[str, ...],
) -> tuple[str, str] | None:
    for alias in aliases:
        score_value = _clean_tsv_value(row.get(alias))
        if score_value:
            return score_value, alias
    return None


def _headers_support_measure(
    fieldnames: tuple[str, ...],
    *,
    aliases: tuple[str, ...],
) -> bool:
    header_names = set(fieldnames)
    return any(alias in header_names for alias in aliases)


def _append_unsupported_field(
    unsupported_fields: dict[str, tuple[str, ...]],
    table_name: str,
    message: str,
) -> None:
    unsupported_fields[table_name] = unsupported_fields.get(table_name, ()) + (message,)


class FEPDS003944BenchmarkSourceAdapter(OpenNeuroSourceAdapter):
    """Audit and harmonize the public ds003944 first-episode psychosis EEG cohort."""

    source_identifier = "fep-ds003944"
    dataset_accession = "ds003944"
    dataset_tag = "1.0.1"
    github_repo = "OpenNeuroDatasets/ds003944"
    candidate_root_names = ("fep-ds003944", "fep_ds003944", "ds003944")

    def normalize_bundle(self, bundle: OpenNeuroSnapshotBundle) -> DatasetRegistryEntry:
        dataset_name = str(bundle.dataset["latestSnapshot"]["description"]["Name"])
        participant_groups = _count_tabulated_values(bundle.participants_tsv, "type")
        total_rows = sum(participant_groups.values())
        return DatasetRegistryEntry(
            dataset_id=self.source_identifier,
            dataset_label=dataset_name,
            access_level="public",
            population_scope="first-episode psychosis case-control cohort",
            diagnosis_coverage=(
                f"participants.tsv lists {participant_groups.get('Psychosis', 0)} Psychosis and "
                f"{participant_groups.get('Control', 0)} Control participants."
            ),
            symptom_scales=("BPRS", "SAPS", "SANS"),
            cognition_scales=("MATRICS", "WASI"),
            functioning_scales=("GAF/GAS", "SFS"),
            treatment_variables=("chlorpromazine-equivalent medication at scan",),
            longitudinal_coverage="No repeated follow-up visits are described in the accession metadata or README.",
            outcome_availability=(
                "Poor functional outcome is potentially benchmarkable via GAF/GAS and SFS; public metadata does "
                "not confirm one-year nonremission, persistent negative symptoms, or relapse follow-up."
            ),
            modality_availability=("EEG",),
            site_structure=(
                "single public accession; README notes Task 1 includes a subset of a second acquisition but does "
                "not claim a multi-site cohort"
            ),
            sample_size_note=(
                f"participants.tsv lists {total_rows} rows ({participant_groups.get('Psychosis', 0)} Psychosis, "
                f"{participant_groups.get('Control', 0)} Control)."
            ),
            known_limitations=(
                "Public metadata describes cross-sectional assessments at scan time, so remission, persistence, "
                "and relapse endpoints are not currently supported."
            ),
            local_status="audited",
            benchmark_v0_eligibility="eligible",
            representation_comparison_support="strong",
            predictor_timepoint="scan/baseline",
            outcome_timepoint="same_visit_functioning_assessment",
            outcome_window="same_visit",
            outcome_is_prospective=False,
            concurrent_endpoint_only=True,
            outcome_temporal_validity="concurrent_only",
            benchmarkable_outcome_families=("poor_functional_outcome",),
            provenance_urls=(
                self.dataset_page_url,
                self.readme_url,
                self.participants_url,
            ),
            audit_summary=(
                "Public first-episode psychosis EEG cohort with symptom, cognition, and same-visit functioning "
                "measures. It supports a narrow concurrent outcome benchmark, but not a prospective outcome "
                "claim."
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
            enrollment_group = participant.get("type", "").strip() or "unknown"
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
                "race_ethnicity": _race_ethnicity(participant),
                "education_years": "",
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
                    "visit_note": "Public snapshot exposes baseline scan-time rows only.",
                }
            )

            diagnosis_group = _diagnosis_group(enrollment_group)
            diagnosis_rows.append(
                {
                    "cohort_id": self.source_identifier,
                    "subject_id": subject_id,
                    "visit_id": visit_id,
                    "diagnosis_system": "public_participants_label",
                    "diagnosis_label": enrollment_group,
                    "diagnosis_group": diagnosis_group,
                    "diagnosis_granularity": "first_episode_psychosis_vs_control_public_label",
                    "is_primary_diagnosis": _bool_string(True),
                    "mapping_caveat": FEP_DIAGNOSIS_CAVEAT,
                    "diagnosis_code": "",
                    "source_diagnosis_label": enrollment_group,
                    "diagnosis_note": "Labels come directly from participants.tsv.",
                }
            )

        functioning_rows: list[dict[str, str]] = []
        outcome_rows: list[dict[str, str]] = []
        unsupported_fields: dict[str, tuple[str, ...]] = {}
        modality_rows = _harmonize_modality_rows(
            cohort_path,
            cohort_id=self.source_identifier,
            subject_lookup=subject_lookup,
            unsupported_fields=unsupported_fields,
        )

        symptom_rows = self._harmonize_measure_table(
            cohort_path=cohort_path,
            source_filename="bprs.tsv",
            table_name="symptom_scores",
            subject_lookup=subject_lookup,
            instrument="BPRS",
            domain="general_psychopathology",
            measure="total_score",
            score_direction="higher_worse",
            mapping_caveat="BPRS total is carried through as a same-visit symptom score from the public phenotype table.",
            source_score_aliases=("BPRST18", "BPRST19", "bprs_total"),
            unsupported_column_message=(
                "Staged phenotype file bprs.tsv does not expose a supported BPRS total column "
                "(expected one of: BPRST18, BPRST19, bprs_total)."
            ),
            unsupported_fields=unsupported_fields,
        )
        symptom_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="saps.tsv",
                table_name="symptom_scores",
                subject_lookup=subject_lookup,
                instrument="SAPS",
                domain="positive_symptoms",
                measure="total_score",
                score_direction="higher_worse",
                mapping_caveat="SAPS total is carried through without inferring any prospective symptom endpoint.",
                source_score_aliases=("saps_total",),
                unsupported_column_message=(
                    "Staged phenotype file saps.tsv does not expose a supported staged SAPS total "
                    "column (expected: saps_total)."
                ),
                unsupported_fields=unsupported_fields,
            )
        )
        symptom_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="sans.tsv",
                table_name="symptom_scores",
                subject_lookup=subject_lookup,
                instrument="SANS",
                domain="negative_symptoms",
                measure="total_score",
                score_direction="higher_worse",
                mapping_caveat="SANS total is retained as a same-visit symptom score only.",
                source_score_aliases=("sans_total",),
                unsupported_column_message=(
                    "Staged phenotype file sans.tsv does not expose a supported staged SANS total "
                    "column (expected: sans_total)."
                ),
                unsupported_fields=unsupported_fields,
            )
        )

        cognition_rows = self._harmonize_measure_table(
            cohort_path=cohort_path,
            source_filename="matrics.tsv",
            table_name="cognition_scores",
            subject_lookup=subject_lookup,
            instrument="MATRICS",
            domain="global_cognition",
            measure="composite_score",
            score_direction="higher_better",
            mapping_caveat="MATRICS composite is passed through from the public staged phenotype table.",
            source_score_aliases=("OVERALLTSCR", "matrics_composite"),
            unsupported_column_message=(
                "Staged phenotype file matrics.tsv does not expose a supported MATRICS composite "
                "column (expected one of: OVERALLTSCR, matrics_composite)."
            ),
            unsupported_fields=unsupported_fields,
        )
        cognition_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="wasi.tsv",
                table_name="cognition_scores",
                subject_lookup=subject_lookup,
                instrument="WASI",
                domain="general_intellectual_ability",
                measure="full_scale_iq",
                score_direction="higher_better",
                mapping_caveat="WASI full-scale IQ is retained as staged without cross-cohort renorming.",
                source_score_aliases=("FULL2IQ", "wasi_fsiq"),
                unsupported_column_message=(
                    "Staged phenotype file wasi.tsv does not expose a supported WASI full-scale IQ "
                    "column (expected one of: FULL2IQ, wasi_fsiq)."
                ),
                unsupported_fields=unsupported_fields,
            )
        )

        functioning_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="gafgas.tsv",
                table_name="functioning_scores",
                subject_lookup=subject_lookup,
                instrument="GAF/GAS",
                domain="global_functioning",
                measure="total_score",
                score_direction="higher_better",
                mapping_caveat=FEP_OUTCOME_CAVEAT,
                source_score_aliases=("GAS", "gafgas_total"),
                unsupported_column_message=(
                    "Staged phenotype file gafgas.tsv does not expose a supported GAF/GAS score "
                    "column (expected one of: GAS, gafgas_total)."
                ),
                unsupported_fields=unsupported_fields,
            )
        )
        functioning_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="sfs.tsv",
                table_name="functioning_scores",
                subject_lookup=subject_lookup,
                instrument="SFS",
                domain="social_functioning",
                measure="total_score",
                score_direction="higher_better",
                mapping_caveat=FEP_OUTCOME_CAVEAT,
                source_score_aliases=("sfs_total",),
                unsupported_column_message=(
                    "Staged phenotype file sfs.tsv does not expose a supported staged SFS total "
                    "column (expected: sfs_total)."
                ),
                unsupported_fields=unsupported_fields,
            )
        )

        treatment_rows = self._harmonize_treatment_rows(cohort_path, subject_lookup, unsupported_fields)

        psychosis_subject_ids = {
            row["subject_id"]
            for row in diagnosis_rows
            if row["diagnosis_group"] == "psychosis"
        }
        for functioning_row in functioning_rows:
            if functioning_row["subject_id"] not in psychosis_subject_ids:
                continue
            outcome_direction = "lower_worse" if functioning_row["score_direction"] == "higher_better" else "higher_worse"
            outcome_rows.append(
                {
                    "cohort_id": self.source_identifier,
                    "subject_id": functioning_row["subject_id"],
                    "visit_id": functioning_row["visit_id"],
                    "outcome_family": "poor_functional_outcome",
                    "outcome_name": f"{functioning_row['instrument'].lower().replace('/', '_').replace(' ', '_')}_same_visit",
                    "outcome_value": functioning_row["score"],
                    "outcome_type": "continuous",
                    "predictor_timepoint": audit_entry.predictor_timepoint,
                    "outcome_timepoint": audit_entry.outcome_timepoint,
                    "outcome_window": audit_entry.outcome_window,
                    "outcome_is_prospective": _bool_string(False),
                    "concurrent_endpoint_only": _bool_string(True),
                    "outcome_definition_version": "benchmark_v0_concurrent_functioning",
                    "mapping_caveat": FEP_OUTCOME_CAVEAT,
                    "outcome_unit": functioning_row.get("score_unit", ""),
                    "outcome_direction": outcome_direction,
                    "outcome_threshold_label": "",
                }
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
                "functioning_scores": tuple(functioning_rows),
                "treatment_exposures": tuple(treatment_rows),
                "outcomes": tuple(outcome_rows),
                "modality_features": tuple(modality_rows),
            },
            caveats=(
                "fep-ds003944 remains a concurrent-only public cohort; no prospective outcome window is claimed.",
                "Psychosis labels are strong enough for the current narrow benchmark lane, but subtype equivalence remains out of scope.",
            ),
            unsupported_fields=unsupported_fields,
        )

    def _harmonize_measure_table(
        self,
        *,
        cohort_path: Path,
        source_filename: str,
        table_name: str,
        subject_lookup: dict[str, dict[str, str]],
        instrument: str,
        domain: str,
        measure: str,
        score_direction: str,
        mapping_caveat: str,
        source_score_aliases: tuple[str, ...],
        unsupported_column_message: str,
        unsupported_fields: dict[str, tuple[str, ...]],
    ) -> list[dict[str, str]]:
        file_path = cohort_path / "phenotype" / source_filename
        if not file_path.exists():
            _append_unsupported_field(unsupported_fields, table_name, f"Missing staged phenotype file: {source_filename}")
            return []

        fieldnames, source_rows = _read_tsv_table(file_path)
        headers_supported = _headers_support_measure(
            fieldnames,
            aliases=source_score_aliases,
        )
        rows: list[dict[str, str]] = []
        for source_row in source_rows:
            source_subject_id = source_row.get("participant_id", "").strip()
            if source_subject_id not in subject_lookup:
                continue
            resolved_score = _resolve_score_from_aliases(source_row, source_score_aliases)
            if resolved_score is None:
                continue
            score_value, source_score_label = resolved_score
            subject_row = subject_lookup[source_subject_id]
            visit_id = _canonical_visit_id(subject_row["subject_id"])
            canonical_row = {
                "cohort_id": self.source_identifier,
                "subject_id": subject_row["subject_id"],
                "visit_id": visit_id,
                "instrument": instrument,
                "domain": domain,
                "measure": measure,
                "score": score_value,
                "score_direction": score_direction,
                "mapping_caveat": mapping_caveat,
                "score_unit": "",
                "source_score_label": source_score_label,
            }
            if table_name == "symptom_scores":
                canonical_row["is_harmonized_domain_score"] = _bool_string(False)
                canonical_row["instrument_version"] = ""
            elif table_name == "cognition_scores":
                canonical_row["task_name"] = ""
            elif table_name == "functioning_scores":
                canonical_row["recovery_domain"] = ""
            rows.append(canonical_row)
        if not rows and not headers_supported:
            _append_unsupported_field(unsupported_fields, table_name, unsupported_column_message)
        return rows

    def _harmonize_treatment_rows(
        self,
        cohort_path: Path,
        subject_lookup: dict[str, dict[str, str]],
        unsupported_fields: dict[str, tuple[str, ...]],
    ) -> list[dict[str, str]]:
        file_path = cohort_path / "phenotype" / "medication.tsv"
        if not file_path.exists():
            _append_unsupported_field(
                unsupported_fields,
                "treatment_exposures",
                "Missing staged phenotype file: medication.tsv",
            )
            return []

        fieldnames, source_rows = _read_tsv_table(file_path)
        headers_supported = _headers_support_measure(
            fieldnames,
            aliases=("CPZ_at_scan", "chlorpromazine_equivalent_mg_day"),
        )
        rows: list[dict[str, str]] = []
        for source_row in source_rows:
            source_subject_id = source_row.get("participant_id", "").strip()
            if source_subject_id not in subject_lookup:
                continue
            resolved_exposure = _resolve_score_from_aliases(
                source_row,
                ("CPZ_at_scan", "chlorpromazine_equivalent_mg_day"),
            )
            if resolved_exposure is None:
                continue
            exposure_value, source_treatment_label = resolved_exposure
            subject_row = subject_lookup[source_subject_id]
            rows.append(
                {
                    "cohort_id": self.source_identifier,
                    "subject_id": subject_row["subject_id"],
                    "visit_id": _canonical_visit_id(subject_row["subject_id"]),
                    "treatment_type": "antipsychotic_medication",
                    "treatment_name": "chlorpromazine_equivalent",
                    "exposure_value": exposure_value,
                    "exposure_unit": "mg/day",
                    "exposure_window": "same_visit",
                    "is_current_exposure": _bool_string(True),
                    "mapping_caveat": "Medication exposure is the public same-visit phenotype release only.",
                    "source_treatment_label": source_treatment_label,
                    "exposure_route": "",
                    "adherence_note": "",
                }
            )
        if not rows and not headers_supported:
            _append_unsupported_field(
                unsupported_fields,
                "treatment_exposures",
                "Staged phenotype file medication.tsv does not expose a supported chlorpromazine-equivalent "
                "column (expected one of: CPZ_at_scan, chlorpromazine_equivalent_mg_day).",
            )
        return rows


__all__ = ["FEPDS003944BenchmarkSourceAdapter"]

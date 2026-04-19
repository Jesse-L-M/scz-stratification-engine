"""Metadata and harmonization adapter for the UCLA CNP / ds000030 cohort."""

from __future__ import annotations

import csv
import io
from pathlib import Path

from ..dataset_registry import DatasetRegistryEntry
from .base import CohortHarmonizationBundle, OpenNeuroSnapshotBundle, OpenNeuroSourceAdapter

UCLA_DIAGNOSIS_CAVEAT = (
    "UCLA CNP is a transdiagnostic public cohort; schizophrenia, bipolar, ADHD, and control rows are preserved "
    "explicitly without upgrading the benchmark claim beyond cross-sectional representation coverage."
)
UCLA_SYMPTOM_CAVEAT = (
    "Public symptom rows stay cross-sectional only and do not imply a benchmarkable outcome family."
)
UCLA_COGNITION_CAVEAT = (
    "Public cognition rows are carried through as baseline cross-sectional measures only; no cross-cohort "
    "renorming or outcome implication is added."
)
UCLA_TREATMENT_CAVEAT = (
    "Medication rows reflect a cross-sectional public inventory only. Reported dose fields are retained as "
    "context and do not imply a harmonized dosing unit or treatment-response claim."
)
UCLA_MODALITY_CAVEAT = (
    "Only modality availability flags from participants.tsv are emitted here; no MRI, dMRI, or fMRI features "
    "are computed in this PR."
)
TSV_NULL_VALUES = frozenset({"", "n/a", "na", "nan", "null", "none", ".", "-9998", "-9999"})
_SYMPTOM_SCALE_FILES = {
    "bprs.tsv": "BPRS",
    "hamilton.tsv": "HAM-D",
    "saps.tsv": "SAPS",
    "sans.tsv": "SANS",
    "ymrs.tsv": "YMRS",
}
_COGNITION_SCALE_FILES = {
    "cvlt.tsv": "CVLT",
    "dkefs.tsv": "D-KEFS",
    "stroop.tsv": "Stroop",
    "taskswitch.tsv": "Task Switch",
    "vcap.tsv": "Visual Capacity",
    "wais.tsv": "WAIS",
    "wms.tsv": "WMS",
}


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


def _scales_from_snapshot(
    phenotype_files: tuple[str, ...],
    *,
    mapping: dict[str, str],
) -> tuple[str, ...]:
    file_names = {Path(filename).name for filename in phenotype_files}
    return tuple(label for filename, label in mapping.items() if filename in file_names)


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


def _append_unsupported_field(
    unsupported_fields: dict[str, tuple[str, ...]],
    table_name: str,
    message: str,
) -> None:
    unsupported_fields[table_name] = unsupported_fields.get(table_name, ()) + (message,)


def _diagnosis_group(enrollment_group: str) -> str:
    if enrollment_group == "SCHZ":
        return "schizophrenia"
    if enrollment_group == "BIPOLAR":
        return "bipolar_disorder"
    if enrollment_group == "ADHD":
        return "adhd"
    if enrollment_group == "CONTROL":
        return "control"
    return "unknown"


def _subject_mapping_note(enrollment_group: str) -> str:
    if enrollment_group == "SCHZ":
        return "Schizophrenia rows are preserved inside a transdiagnostic cohort for representation coverage only."
    if enrollment_group == "CONTROL":
        return "Control rows are retained as contextual comparison subjects only, not as a stronger benchmark claim."
    if enrollment_group in {"BIPOLAR", "ADHD"}:
        return "Non-schizophrenia rows are retained explicitly to support cross-sectional representation comparisons."
    return "Public participant label is missing or unmapped; diagnosis group remains unknown."


def _normalize_site_id(scanner_serial_number: str) -> str:
    serial = _clean_tsv_value(scanner_serial_number)
    if not serial:
        return "single_program_public_accession"
    if serial.endswith(".0"):
        serial = serial[:-2]
    return f"single_program_scanner_{serial}"


def _diagnosis_code(scid_label: str) -> str:
    cleaned = _clean_tsv_value(scid_label)
    if not cleaned:
        return ""
    return cleaned.partition(" ")[0]


class UCLACNPDS000030BenchmarkSourceAdapter(OpenNeuroSourceAdapter):
    """Audit and harmonize the UCLA CNP public snapshot conservatively."""

    source_identifier = "ucla-cnp-ds000030"
    supports_harmonization = True
    dataset_accession = "ds000030"
    dataset_tag = "1.0.0"
    github_repo = "OpenNeuroDatasets/ds000030"
    candidate_root_names = (
        "ucla-cnp-ds000030",
        "ucla_cnp_ds000030",
        "ds000030",
    )

    def normalize_bundle(self, bundle: OpenNeuroSnapshotBundle) -> DatasetRegistryEntry:
        dataset_name = str(bundle.dataset["latestSnapshot"]["description"]["Name"])
        participant_groups = _count_tabulated_values(bundle.participants_tsv, "diagnosis")
        total_rows = sum(participant_groups.values())
        symptom_scales = _scales_from_snapshot(
            bundle.phenotype_files,
            mapping=_SYMPTOM_SCALE_FILES,
        )
        cognition_scales = _scales_from_snapshot(
            bundle.phenotype_files,
            mapping=_COGNITION_SCALE_FILES,
        )
        has_medication_inventory = "medication.tsv" in {
            Path(filename).name for filename in bundle.phenotype_files
        }
        return DatasetRegistryEntry(
            dataset_id=self.source_identifier,
            dataset_label=dataset_name,
            access_tier="strict_open",
            population_scope=(
                "transdiagnostic community cohort with schizophrenia, bipolar, ADHD, and control groups"
            ),
            diagnosis_coverage=(
                "participants.tsv exposes explicit CONTROL, SCHZ, BIPOLAR, and ADHD groups "
                f"({participant_groups.get('CONTROL', 0)} CONTROL, "
                f"{participant_groups.get('SCHZ', 0)} SCHZ, "
                f"{participant_groups.get('BIPOLAR', 0)} BIPOLAR, "
                f"{participant_groups.get('ADHD', 0)} ADHD)."
            ),
            symptom_scales=symptom_scales,
            cognition_scales=cognition_scales,
            functioning_scales=(),
            treatment_variables=(
                ("cross-sectional medication inventory",)
                if has_medication_inventory
                else ()
            ),
            longitudinal_coverage=(
                "No repeated follow-up visit schedule or prospective public endpoint is "
                "described in the pinned snapshot metadata or README."
            ),
            outcome_availability=(
                "The public snapshot supports cross-sectional diagnosis, symptom, cognition, "
                "and medication context only. It does not confirm a public functioning scale "
                "or any benchmarkable longitudinal outcome family."
            ),
            modality_availability=("MRI", "dMRI", "fMRI"),
            site_structure=(
                "single recruitment program in the Los Angeles area; public metadata shows "
                "two scanner serial numbers but not a multi-site external-validation cohort"
            ),
            sample_size_note=(
                f"participants.tsv lists {total_rows} rows "
                f"({participant_groups.get('CONTROL', 0)} CONTROL, "
                f"{participant_groups.get('SCHZ', 0)} SCHZ, "
                f"{participant_groups.get('BIPOLAR', 0)} BIPOLAR, "
                f"{participant_groups.get('ADHD', 0)} ADHD)."
            ),
            known_limitations=(
                "Rich cross-sectional phenotyping is available, but the public snapshot does "
                "not document a benchmarkable functioning or prospective outcome window. The "
                "cohort is also transdiagnostic rather than a narrow schizophrenia-only "
                "outcome benchmark source."
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
                "Strict-open UCLA CNP snapshot with explicit schizophrenia labels and rich "
                "cross-sectional symptom/cognition coverage. It materially helps "
                "cross-sectional representation benchmarking, but it does not add a public "
                "outcome benchmark family."
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

        scid_by_subject = {
            row.get("participant_id", "").strip(): _clean_tsv_value(row.get("scid_dx1"))
            for row in _read_tsv_rows(cohort_path / "phenotype" / "scid.tsv")
            if row.get("participant_id", "").strip()
        }

        for participant in participants:
            source_subject_id = participant.get("participant_id", "").strip()
            if not source_subject_id:
                continue
            subject_id = _canonical_subject_id(self.source_identifier, source_subject_id)
            enrollment_group = participant.get("diagnosis", "").strip() or "unknown"
            baseline_age = participant.get("age", "").strip()
            visit_id = _canonical_visit_id(subject_id)
            subject_row = {
                "cohort_id": self.source_identifier,
                "subject_id": subject_id,
                "source_subject_id": source_subject_id,
                "population_scope": audit_entry.population_scope,
                "site_id": _normalize_site_id(participant.get("ScannerSerialNumber", "")),
                "sex": participant.get("gender", "").strip(),
                "baseline_age": baseline_age,
                "enrollment_group": enrollment_group,
                "has_longitudinal_followup": _bool_string(False),
                "representation_comparison_support": audit_entry.representation_comparison_support,
                "ancestry_group": "",
                "race_ethnicity": "",
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
                    "visit_note": "Public snapshot exposes baseline cross-sectional rows only.",
                }
            )

            scid_label = scid_by_subject.get(source_subject_id, "")
            diagnosis_rows.append(
                {
                    "cohort_id": self.source_identifier,
                    "subject_id": subject_id,
                    "visit_id": visit_id,
                    "diagnosis_system": "public_participants_label_with_scid_context",
                    "diagnosis_label": enrollment_group,
                    "diagnosis_group": _diagnosis_group(enrollment_group),
                    "diagnosis_granularity": "transdiagnostic_public_group_with_scid_context",
                    "is_primary_diagnosis": _bool_string(True),
                    "mapping_caveat": UCLA_DIAGNOSIS_CAVEAT,
                    "diagnosis_code": _diagnosis_code(scid_label),
                    "source_diagnosis_label": scid_label or enrollment_group,
                    "diagnosis_note": (
                        "participants.tsv group retained as the canonical diagnosis label; SCID text is carried as context."
                    ),
                }
            )

        unsupported_fields: dict[str, tuple[str, ...]] = {
            "functioning_scores": (
                "No public functioning scale is staged in the current UCLA CNP root.",
            ),
            "outcomes": (
                "ucla-cnp-ds000030 remains a cross-sectional representation cohort only; no benchmarkable outcome rows are emitted.",
            ),
        }
        symptom_rows = self._harmonize_measure_table(
            cohort_path=cohort_path,
            source_filename="saps.tsv",
            table_name="symptom_scores",
            subject_lookup=subject_lookup,
            instrument="SAPS",
            domain="positive_symptoms",
            measure="factor_delusions",
            score_direction="higher_worse",
            mapping_caveat=UCLA_SYMPTOM_CAVEAT,
            source_score_label="factor_delusions",
            unsupported_column_message=(
                "Staged phenotype file saps.tsv does not expose the supported factor_delusions summary column."
            ),
            unsupported_fields=unsupported_fields,
        )
        symptom_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="sans.tsv",
                table_name="symptom_scores",
                subject_lookup=subject_lookup,
                instrument="SANS",
                domain="negative_symptoms",
                measure="factor_avolition",
                score_direction="higher_worse",
                mapping_caveat=UCLA_SYMPTOM_CAVEAT,
                source_score_label="factor_avolition",
                unsupported_column_message=(
                    "Staged phenotype file sans.tsv does not expose the supported factor_avolition summary column."
                ),
                unsupported_fields=unsupported_fields,
            )
        )
        symptom_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="ymrs.tsv",
                table_name="symptom_scores",
                subject_lookup=subject_lookup,
                instrument="YMRS",
                domain="mania_symptoms",
                measure="total_score",
                score_direction="higher_worse",
                mapping_caveat=UCLA_SYMPTOM_CAVEAT,
                source_score_label="ymrs_score",
                unsupported_column_message=(
                    "Staged phenotype file ymrs.tsv does not expose the supported ymrs_score summary column."
                ),
                unsupported_fields=unsupported_fields,
            )
        )

        cognition_rows = self._harmonize_measure_table(
            cohort_path=cohort_path,
            source_filename="wais.tsv",
            table_name="cognition_scores",
            subject_lookup=subject_lookup,
            instrument="WAIS",
            domain="working_memory",
            measure="letter_number_sequencing_total_raw",
            score_direction="higher_better",
            mapping_caveat=UCLA_COGNITION_CAVEAT,
            source_score_label="lns_totalraw",
            unsupported_column_message=(
                "Staged phenotype file wais.tsv does not expose the supported lns_totalraw column."
            ),
            unsupported_fields=unsupported_fields,
            task_name="Letter-Number Sequencing",
        )
        cognition_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="wais.tsv",
                table_name="cognition_scores",
                subject_lookup=subject_lookup,
                instrument="WAIS",
                domain="verbal_ability",
                measure="vocabulary_total_raw",
                score_direction="higher_better",
                mapping_caveat=UCLA_COGNITION_CAVEAT,
                source_score_label="voc_totalraw",
                unsupported_column_message=(
                    "Staged phenotype file wais.tsv does not expose the supported voc_totalraw column."
                ),
                unsupported_fields=unsupported_fields,
                task_name="Vocabulary",
            )
        )
        cognition_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="cvlt.tsv",
                table_name="cognition_scores",
                subject_lookup=subject_lookup,
                instrument="CVLT",
                domain="verbal_memory",
                measure="long_delay_cued_recall_total",
                score_direction="higher_better",
                mapping_caveat=UCLA_COGNITION_CAVEAT,
                source_score_label="cvlt_ldc",
                unsupported_column_message=(
                    "Staged phenotype file cvlt.tsv does not expose the supported cvlt_ldc column."
                ),
                unsupported_fields=unsupported_fields,
            )
        )
        cognition_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="wms.tsv",
                table_name="cognition_scores",
                subject_lookup=subject_lookup,
                instrument="WMS",
                domain="visual_memory",
                measure="visual_reproduction_i_immediate_recall_total_raw",
                score_direction="higher_better",
                mapping_caveat=UCLA_COGNITION_CAVEAT,
                source_score_label="vr1ir_totalraw",
                unsupported_column_message=(
                    "Staged phenotype file wms.tsv does not expose the supported vr1ir_totalraw column."
                ),
                unsupported_fields=unsupported_fields,
                task_name="Visual Reproduction I",
            )
        )
        cognition_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="wms.tsv",
                table_name="cognition_scores",
                subject_lookup=subject_lookup,
                instrument="WMS",
                domain="visual_memory",
                measure="visual_reproduction_ii_recall_total_raw",
                score_direction="higher_better",
                mapping_caveat=UCLA_COGNITION_CAVEAT,
                source_score_label="vr2r_totalraw",
                unsupported_column_message=(
                    "Staged phenotype file wms.tsv does not expose the supported vr2r_totalraw column."
                ),
                unsupported_fields=unsupported_fields,
                task_name="Visual Reproduction II",
            )
        )
        cognition_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="wms.tsv",
                table_name="cognition_scores",
                subject_lookup=subject_lookup,
                instrument="WMS",
                domain="working_memory",
                measure="digit_span_backward_total_raw",
                score_direction="higher_better",
                mapping_caveat=UCLA_COGNITION_CAVEAT,
                source_score_label="ds_btrs",
                unsupported_column_message=(
                    "Staged phenotype file wms.tsv does not expose the supported ds_btrs column."
                ),
                unsupported_fields=unsupported_fields,
                task_name="Digit Span Backward",
            )
        )
        cognition_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="taskswitch.tsv",
                table_name="cognition_scores",
                subject_lookup=subject_lookup,
                instrument="Task Switch",
                domain="executive_function",
                measure="accuracy",
                score_direction="higher_better",
                mapping_caveat=UCLA_COGNITION_CAVEAT,
                source_score_label="ts_accuracy",
                unsupported_column_message=(
                    "Staged phenotype file taskswitch.tsv does not expose the supported ts_accuracy column."
                ),
                unsupported_fields=unsupported_fields,
            )
        )

        treatment_rows = self._harmonize_treatment_rows(
            cohort_path=cohort_path,
            subject_lookup=subject_lookup,
            unsupported_fields=unsupported_fields,
        )
        modality_rows = self._harmonize_modality_rows(
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
                "treatment_exposures": tuple(treatment_rows),
                "outcomes": (),
                "modality_features": tuple(modality_rows),
            },
            caveats=(
                "ucla-cnp-ds000030 stays explicitly cross-sectional in public form; no benchmarkable outcome rows are emitted.",
                "The cohort remains transdiagnostic, so added rows improve representation coverage without strengthening the narrow outcome claim.",
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
        source_score_label: str,
        unsupported_column_message: str,
        unsupported_fields: dict[str, tuple[str, ...]],
        task_name: str = "",
    ) -> list[dict[str, str]]:
        file_path = cohort_path / "phenotype" / source_filename
        if not file_path.exists():
            _append_unsupported_field(unsupported_fields, table_name, f"Missing staged phenotype file: {source_filename}")
            return []

        fieldnames, source_rows = _read_tsv_table(file_path)
        headers_supported = source_score_label in set(fieldnames)
        rows: list[dict[str, str]] = []
        for source_row in source_rows:
            source_subject_id = source_row.get("participant_id", "").strip()
            if source_subject_id not in subject_lookup:
                continue
            score_value = _clean_tsv_value(source_row.get(source_score_label))
            if not score_value:
                continue
            subject_row = subject_lookup[source_subject_id]
            canonical_row = {
                "cohort_id": self.source_identifier,
                "subject_id": subject_row["subject_id"],
                "visit_id": _canonical_visit_id(subject_row["subject_id"]),
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
                canonical_row["task_name"] = task_name
            rows.append(canonical_row)
        if not rows and not headers_supported:
            _append_unsupported_field(unsupported_fields, table_name, unsupported_column_message)
        return rows

    def _harmonize_treatment_rows(
        self,
        *,
        cohort_path: Path,
        subject_lookup: dict[str, dict[str, str]],
        unsupported_fields: dict[str, tuple[str, ...]],
    ) -> list[dict[str, str]]:
        file_path = cohort_path / "phenotype" / "medication.tsv"
        if not file_path.exists():
            _append_unsupported_field(unsupported_fields, "treatment_exposures", "Missing staged phenotype file: medication.tsv")
            return []

        rows: list[dict[str, str]] = []
        _, source_rows = _read_tsv_table(file_path)
        for source_row in source_rows:
            source_subject_id = source_row.get("participant_id", "").strip()
            if source_subject_id not in subject_lookup:
                continue
            subject_row = subject_lookup[source_subject_id]
            visit_id = _canonical_visit_id(subject_row["subject_id"])
            for index in range(1, 21):
                medication_name = _clean_tsv_value(source_row.get(f"med_name{index}"))
                medication_use = _clean_tsv_value(source_row.get(f"med_use{index}"))
                if medication_use.lower() not in {"1", "true", "yes"}:
                    continue
                if not medication_name:
                    continue
                reported_dose = _clean_tsv_value(source_row.get(f"med_dos{index}"))
                rows.append(
                    {
                        "cohort_id": self.source_identifier,
                        "subject_id": subject_row["subject_id"],
                        "visit_id": visit_id,
                        "treatment_type": "cross_sectional_medication_inventory",
                        "treatment_name": medication_name or f"medication_slot_{index}",
                        "exposure_value": reported_dose or "1",
                        "exposure_unit": (
                            "reported_public_dose_unit_unspecified"
                            if reported_dose
                            else "current_use_flag"
                        ),
                        "exposure_window": "same_visit",
                        "is_current_exposure": _bool_string(True),
                        "mapping_caveat": UCLA_TREATMENT_CAVEAT,
                        "source_treatment_label": f"med_name{index}",
                        "exposure_route": "",
                        "adherence_note": "",
                    }
                )
        if not rows:
            _append_unsupported_field(
                unsupported_fields,
                "treatment_exposures",
                "No current medication inventory rows were present in the staged UCLA CNP root.",
            )
        return rows

    def _harmonize_modality_rows(
        self,
        *,
        participants: list[dict[str, str]],
        subject_lookup: dict[str, dict[str, str]],
        unsupported_fields: dict[str, tuple[str, ...]],
    ) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        feature_specs = (
            ("T1w", "MRI", "t1w_available"),
            ("dwi", "dMRI", "dwi_available"),
            ("rest", "fMRI", "rest_available"),
            ("taskswitch", "fMRI", "task_switch_available"),
        )
        for participant in participants:
            source_subject_id = participant.get("participant_id", "").strip()
            if source_subject_id not in subject_lookup:
                continue
            subject_row = subject_lookup[source_subject_id]
            visit_id = _canonical_visit_id(subject_row["subject_id"])
            for source_field, modality_type, feature_name in feature_specs:
                value = _clean_tsv_value(participant.get(source_field))
                if value != "1":
                    continue
                rows.append(
                    {
                        "cohort_id": self.source_identifier,
                        "subject_id": subject_row["subject_id"],
                        "visit_id": visit_id,
                        "modality_type": modality_type,
                        "feature_name": feature_name,
                        "feature_value": "1",
                        "feature_unit": "binary",
                        "feature_source": "participants.tsv",
                        "mapping_caveat": UCLA_MODALITY_CAVEAT,
                        "feature_group": "availability",
                        "preprocessing_version": "",
                        "feature_quality_flag": "not_computed",
                    }
                )
        if not rows:
            _append_unsupported_field(
                unsupported_fields,
                "modality_features",
                "No participant-level modality availability flags were present in the staged UCLA CNP root.",
            )
        return rows


__all__ = ["UCLACNPDS000030BenchmarkSourceAdapter"]

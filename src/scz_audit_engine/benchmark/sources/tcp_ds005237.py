"""Metadata and harmonization adapter for the TCP / ds005237 cohort."""

from __future__ import annotations

import csv
import io
from pathlib import Path

from ..dataset_registry import DatasetRegistryEntry
from .base import CohortHarmonizationBundle, OpenNeuroSnapshotBundle, OpenNeuroSourceAdapter

TCP_DIAGNOSIS_CAVEAT = (
    "Public release exposes Patient versus GenPop labels only; psychosis-specific equivalence is not claimed."
)
TCP_OUTCOME_CAVEAT = (
    "Functioning measures remain concurrent same-visit public endpoints only; this does not support a "
    "prospective benchmark claim."
)
TCP_MODALITY_CAVEAT = (
    "Only modality availability is emitted here; no MRI or fMRI representation features are built in this PR."
)
BIDS_IMAGE_FILE_ENDINGS = (".nii", ".nii.gz", ".json")
MRI_BIDS_SUFFIXES = ("_t1w", "_t2w", "_flair")
FMRI_BIDS_SUFFIXES = ("_bold",)


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


def _diagnosis_group(enrollment_group: str) -> str:
    if enrollment_group == "Patient":
        return "broad_psychiatric_patient"
    if enrollment_group == "GenPop":
        return "general_population_control"
    return "unknown"


def _clean_tsv_value(value: str | None) -> str:
    return str(value or "").strip()


def _append_unsupported_field(
    unsupported_fields: dict[str, tuple[str, ...]],
    table_name: str,
    message: str,
) -> None:
    unsupported_fields[table_name] = unsupported_fields.get(table_name, ()) + (message,)


def _has_bids_like_files(subject_root: Path, *, bids_suffixes: tuple[str, ...]) -> bool:
    for path in subject_root.rglob("*"):
        if not path.is_file():
            continue
        lower_name = path.name.lower()
        if any(
            lower_name.endswith(f"{bids_suffix}{file_ending}")
            for bids_suffix in bids_suffixes
            for file_ending in BIDS_IMAGE_FILE_ENDINGS
        ):
            return True
    return False


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
        anat_root = subject_root / "anat"
        func_root = subject_root / "func"
        has_mri_files = anat_root.exists() and _has_bids_like_files(anat_root, bids_suffixes=MRI_BIDS_SUFFIXES)
        has_fmri_files = func_root.exists() and _has_bids_like_files(func_root, bids_suffixes=FMRI_BIDS_SUFFIXES)
        if has_mri_files:
            rows.append(
                {
                    "cohort_id": cohort_id,
                    "subject_id": subject_row["subject_id"],
                    "visit_id": _canonical_visit_id(subject_row["subject_id"]),
                    "modality_type": "MRI",
                    "feature_name": "available",
                    "feature_value": "1",
                    "feature_unit": "binary",
                    "feature_source": "staged_subject_tree",
                    "mapping_caveat": TCP_MODALITY_CAVEAT,
                    "feature_group": "availability",
                    "preprocessing_version": "",
                    "feature_quality_flag": "not_computed",
                }
            )
        if has_fmri_files:
            rows.append(
                {
                    "cohort_id": cohort_id,
                    "subject_id": subject_row["subject_id"],
                    "visit_id": _canonical_visit_id(subject_row["subject_id"]),
                    "modality_type": "fMRI",
                    "feature_name": "available",
                    "feature_value": "1",
                    "feature_unit": "binary",
                    "feature_source": "staged_subject_tree",
                    "mapping_caveat": TCP_MODALITY_CAVEAT,
                    "feature_group": "availability",
                    "preprocessing_version": "",
                    "feature_quality_flag": "not_computed",
                }
            )
    if not rows:
        unsupported_fields["modality_features"] = unsupported_fields.get("modality_features", ()) + (
            "No staged subject-level MRI or fMRI files were present in the current root.",
        )
    return rows


class TCPDS005237BenchmarkSourceAdapter(OpenNeuroSourceAdapter):
    """Audit and harmonize the public TCP / ds005237 cohort conservatively."""

    source_identifier = "tcp-ds005237"
    dataset_accession = "ds005237"
    dataset_tag = "1.1.3"
    github_repo = "OpenNeuroDatasets/ds005237"
    candidate_root_names = ("tcp-ds005237", "tcp_ds005237", "ds005237")

    def normalize_bundle(self, bundle: OpenNeuroSnapshotBundle) -> DatasetRegistryEntry:
        dataset_name = str(bundle.dataset["latestSnapshot"]["description"]["Name"])
        participant_groups = _count_tabulated_values(bundle.participants_tsv, "Group")
        participant_sites = _count_tabulated_values(bundle.participants_tsv, "Site")
        sample_note = (
            "README describes 241 participants (148 psychiatric, 93 healthy), while "
            f"participants.tsv currently lists {sum(participant_groups.values()) or 'unknown'} rows "
            f"({participant_groups.get('Patient', 0)} Patient, {participant_groups.get('GenPop', 0)} GenPop)."
        )
        site_note = (
            "two public sites (Yale Brain Imaging Center and McLean Hospital) per README"
            if participant_sites
            else "two public sites per README"
        )
        return DatasetRegistryEntry(
            dataset_id=self.source_identifier,
            dataset_label=dataset_name,
            access_level="public",
            population_scope="transdiagnostic psychiatry cohort with affective or psychotic illness history",
            diagnosis_coverage=(
                "Broad psychiatric illness vs healthy comparison; README cites affective or psychotic illness "
                "history, but public participants.tsv only exposes Patient vs GenPop groups."
            ),
            symptom_scales=("PANSS", "CGI", "MADRS", "PDSS", "YMRS", "CSSRS"),
            cognition_scales=("Shipley", "CFQ", "Test My Brain", "Stroop", "Hammer"),
            functioning_scales=("LIFE-RIFT", "MCAS"),
            treatment_variables=("psychotropic medication use",),
            longitudinal_coverage="No repeated public follow-up visits are described in the accession metadata or README.",
            outcome_availability=(
                "Poor functional outcome is potentially benchmarkable via LIFE-RIFT and MCAS; public metadata "
                "does not confirm one-year nonremission, persistent negative symptoms, or relapse follow-up."
            ),
            modality_availability=("MRI", "fMRI"),
            site_structure=site_note,
            sample_size_note=sample_note,
            known_limitations=(
                "Diagnosis granularity is weak in public participant metadata, and the public release does not "
                "document longitudinal follow-up needed for remission, persistence, or relapse endpoints."
            ),
            local_status="audited",
            benchmark_v0_eligibility="limited",
            representation_comparison_support="limited",
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
                "Public transdiagnostic MRI cohort with same-visit functioning endpoints and multi-site "
                "structure, but no prospectively usable public outcome window. The public label space is still "
                "too broad to count as narrow benchmark support for psychosis heterogeneity."
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
            enrollment_group = participant.get("Group", "").strip() or "unknown"
            baseline_age = participant.get("age", "").strip()
            visit_id = _canonical_visit_id(subject_id)
            subject_row = {
                "cohort_id": self.source_identifier,
                "subject_id": subject_id,
                "source_subject_id": source_subject_id,
                "population_scope": audit_entry.population_scope,
                "site_id": participant.get("Site", "").strip() or "unknown",
                "sex": participant.get("sex", "").strip(),
                "baseline_age": baseline_age,
                "enrollment_group": enrollment_group,
                "has_longitudinal_followup": _bool_string(False),
                "representation_comparison_support": audit_entry.representation_comparison_support,
                "ancestry_group": "",
                "race_ethnicity": "",
                "education_years": "",
                "mapping_note": (
                    "Public labels remain Patient versus GenPop only; cross-diagnostic equivalence is not claimed."
                ),
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
                    "visit_note": "Public staged root exposes baseline scan-time rows only.",
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
                    "diagnosis_granularity": "public_patient_vs_genpop_only",
                    "is_primary_diagnosis": _bool_string(True),
                    "mapping_caveat": TCP_DIAGNOSIS_CAVEAT,
                    "diagnosis_code": "",
                    "source_diagnosis_label": enrollment_group,
                    "diagnosis_note": "Labels come directly from participants.tsv.",
                }
            )

        unsupported_fields: dict[str, tuple[str, ...]] = {
            "cognition_scores": (
                "No local cognition tables are staged in the current public TCP root; cognition support remains unstated in harmonized rows.",
            ),
            "treatment_exposures": (
                "No staged medication exposure table is present in the current public TCP root.",
            ),
        }
        modality_rows = _harmonize_modality_rows(
            cohort_path,
            cohort_id=self.source_identifier,
            subject_lookup=subject_lookup,
            unsupported_fields=unsupported_fields,
        )

        symptom_rows = self._harmonize_measure_table(
            cohort_path=cohort_path,
            source_filename="panss01.tsv",
            table_name="symptom_scores",
            subject_lookup=subject_lookup,
            instrument="PANSS",
            domain="psychosis_symptoms",
            measure="total_score",
            score_direction="higher_worse",
            mapping_caveat="PANSS total is carried through from the public staged phenotype table only.",
            source_score_label="panss_total",
            unsupported_column_message=(
                "Staged phenotype file panss01.tsv does not expose the supported PANSS total column "
                "(expected: panss_total)."
            ),
            unsupported_fields=unsupported_fields,
        )
        symptom_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="qids01.tsv",
                table_name="symptom_scores",
                subject_lookup=subject_lookup,
                instrument="QIDS",
                domain="depressive_symptoms",
                measure="total_score",
                score_direction="higher_worse",
                mapping_caveat="QIDS total is retained as a same-visit symptom score only.",
                source_score_label="qids_total",
                unsupported_column_message=(
                    "Staged phenotype file qids01.tsv does not expose the supported QIDS total column "
                    "(expected: qids_total)."
                ),
                unsupported_fields=unsupported_fields,
            )
        )

        cognition_rows: list[dict[str, str]] = []
        functioning_rows = self._harmonize_measure_table(
            cohort_path=cohort_path,
            source_filename="lrift01.tsv",
            table_name="functioning_scores",
            subject_lookup=subject_lookup,
            instrument="LIFE-RIFT",
            domain="global_functioning",
            measure="total_score",
            score_direction="higher_worse",
            mapping_caveat=TCP_OUTCOME_CAVEAT,
            source_score_label="lrift_total",
            unsupported_column_message=(
                "Staged phenotype file lrift01.tsv does not expose the supported LIFE-RIFT total column "
                "(expected: lrift_total)."
            ),
            unsupported_fields=unsupported_fields,
        )
        functioning_rows.extend(
            self._harmonize_measure_table(
                cohort_path=cohort_path,
                source_filename="mcas01.tsv",
                table_name="functioning_scores",
                subject_lookup=subject_lookup,
                instrument="MCAS",
                domain="community_functioning",
                measure="total_score",
                score_direction="higher_better",
                mapping_caveat=TCP_OUTCOME_CAVEAT,
                source_score_label="mcas_total",
                unsupported_column_message=(
                    "Staged phenotype file mcas01.tsv does not expose the supported MCAS total column "
                    "(expected: mcas_total)."
                ),
                unsupported_fields=unsupported_fields,
            )
        )

        outcome_rows: list[dict[str, str]] = []
        patient_subject_ids = {
            row["subject_id"]
            for row in diagnosis_rows
            if row["diagnosis_group"] == "broad_psychiatric_patient"
        }
        for functioning_row in functioning_rows:
            if functioning_row["subject_id"] not in patient_subject_ids:
                continue
            outcome_direction = "lower_worse" if functioning_row["score_direction"] == "higher_better" else "higher_worse"
            outcome_rows.append(
                {
                    "cohort_id": self.source_identifier,
                    "subject_id": functioning_row["subject_id"],
                    "visit_id": functioning_row["visit_id"],
                    "outcome_family": "poor_functional_outcome",
                    "outcome_name": f"{functioning_row['instrument'].lower().replace('-', '_').replace('/', '_').replace(' ', '_')}_same_visit",
                    "outcome_value": functioning_row["score"],
                    "outcome_type": "continuous",
                    "predictor_timepoint": audit_entry.predictor_timepoint,
                    "outcome_timepoint": audit_entry.outcome_timepoint,
                    "outcome_window": audit_entry.outcome_window,
                    "outcome_is_prospective": _bool_string(False),
                    "concurrent_endpoint_only": _bool_string(True),
                    "outcome_definition_version": "benchmark_v0_concurrent_functioning",
                    "mapping_caveat": TCP_OUTCOME_CAVEAT,
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
                "treatment_exposures": (),
                "outcomes": tuple(outcome_rows),
                "modality_features": tuple(modality_rows),
            },
            caveats=(
                "tcp-ds005237 remains concurrent-only in public form; no prospective outcome window is claimed.",
                "Public diagnosis granularity remains Patient versus GenPop only, so the cohort stays explicitly limited.",
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
            elif table_name == "functioning_scores":
                canonical_row["recovery_domain"] = ""
            rows.append(canonical_row)
        if not rows and not headers_supported:
            _append_unsupported_field(unsupported_fields, table_name, unsupported_column_message)
        return rows


__all__ = ["TCPDS005237BenchmarkSourceAdapter"]

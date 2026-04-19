"""Metadata-only adapter for the UCLA CNP / ds000030 cohort."""

from __future__ import annotations

import csv
import io
from pathlib import Path

from ..dataset_registry import DatasetRegistryEntry
from .base import CohortHarmonizationBundle, OpenNeuroSnapshotBundle, OpenNeuroSourceAdapter

_NULL_VALUES = frozenset({"", "n/a", "na", "null", "none", "."})
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


class UCLACNPDS000030BenchmarkSourceAdapter(OpenNeuroSourceAdapter):
    """Audit the UCLA CNP public snapshot conservatively."""

    source_identifier = "ucla-cnp-ds000030"
    supports_harmonization = False
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
        raise NotImplementedError(
            "ucla-cnp-ds000030 is metadata-only in this PR and has no benchmark harmonization implementation."
        )


__all__ = ["UCLACNPDS000030BenchmarkSourceAdapter"]

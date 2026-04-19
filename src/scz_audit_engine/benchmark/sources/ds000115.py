"""Metadata-only adapter for the ds000115 schizophrenia working-memory cohort."""

from __future__ import annotations

import csv
import io
from pathlib import Path

from ..dataset_registry import DatasetRegistryEntry
from .base import CohortHarmonizationBundle, OpenNeuroSnapshotBundle, OpenNeuroSourceAdapter

_NULL_VALUES = frozenset({"", "n/a", "na", "null", "none", "."})


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
            if str(value or "").strip().lower() not in _NULL_VALUES:
                return True
    return False


class DS000115BenchmarkSourceAdapter(OpenNeuroSourceAdapter):
    """Audit the ds000115 public snapshot conservatively."""

    source_identifier = "ds000115"
    supports_harmonization = False
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
        raise NotImplementedError(
            "ds000115 is metadata-only in this PR and has no benchmark harmonization implementation."
        )


__all__ = ["DS000115BenchmarkSourceAdapter"]

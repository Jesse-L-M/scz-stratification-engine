"""Metadata adapter for the public first-episode psychosis EEG cohort."""

from __future__ import annotations

import csv
import io

from ..dataset_registry import DatasetRegistryEntry
from .base import OpenNeuroSnapshotBundle, OpenNeuroSourceAdapter


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


class FEPDS003944BenchmarkSourceAdapter(OpenNeuroSourceAdapter):
    """Audit the public ds003944 first-episode psychosis EEG cohort."""

    source_identifier = "fep-ds003944"
    dataset_accession = "ds003944"
    dataset_tag = "1.0.1"
    github_repo = "OpenNeuroDatasets/ds003944"

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
            sample_size_note=f"participants.tsv lists {total_rows} rows ({participant_groups.get('Psychosis', 0)} Psychosis, {participant_groups.get('Control', 0)} Control).",
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


__all__ = ["FEPDS003944BenchmarkSourceAdapter"]

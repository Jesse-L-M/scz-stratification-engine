"""Metadata adapter for the TCP / ds005237 cohort."""

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


class TCPDS005237BenchmarkSourceAdapter(OpenNeuroSourceAdapter):
    """Audit the public TCP / ds005237 cohort as benchmark metadata only."""

    source_identifier = "tcp-ds005237"
    dataset_accession = "ds005237"
    dataset_tag = "1.1.3"
    github_repo = "OpenNeuroDatasets/ds005237"

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
            benchmarkable_outcome_families=("poor_functional_outcome",),
            provenance_urls=(
                self.dataset_page_url,
                self.readme_url,
                self.participants_url,
            ),
            audit_summary=(
                "Public transdiagnostic MRI cohort with functioning measures and multi-site structure, but no "
                "confirmed public longitudinal endpoint coverage. The public label space is still too broad to "
                "count as benchmark-v0 support for psychosis heterogeneity."
            ),
        )


__all__ = ["TCPDS005237BenchmarkSourceAdapter"]

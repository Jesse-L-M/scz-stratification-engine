"""Dataset-registry contract and benchmark decision logic."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar

OUTCOME_FAMILIES = (
    "one_year_nonremission",
    "persistent_negative_symptoms",
    "poor_functional_outcome",
    "relapse_hospitalization_proxy",
)
ACCESS_LEVELS = ("public", "controlled", "gated")
LOCAL_STATUSES = ("candidate", "audited", "harmonized", "deferred")
BENCHMARK_ELIGIBLE_ACCESS_LEVELS = ("public",)
BENCHMARK_ELIGIBLE_LOCAL_STATUSES = ("audited", "harmonized")
REQUIRED_REGISTRY_COLUMNS = (
    "dataset_id",
    "dataset_label",
    "access_level",
    "population_scope",
    "diagnosis_coverage",
    "symptom_scales",
    "cognition_scales",
    "functioning_scales",
    "treatment_variables",
    "longitudinal_coverage",
    "outcome_availability",
    "modality_availability",
    "site_structure",
    "sample_size_note",
    "known_limitations",
    "local_status",
)
OPTIONAL_REGISTRY_COLUMNS = (
    "benchmarkable_outcome_families",
    "provenance_urls",
    "audit_summary",
)
REGISTRY_COLUMNS = REQUIRED_REGISTRY_COLUMNS + OPTIONAL_REGISTRY_COLUMNS


def _split_multi_value_field(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(";") if item.strip())


def _join_multi_value_field(values: tuple[str, ...]) -> str:
    return "; ".join(item for item in values if item)


@dataclass(frozen=True, slots=True)
class DatasetRegistryEntry:
    """Normalized registry row used for dataset audits."""

    dataset_id: str
    dataset_label: str
    access_level: str
    population_scope: str
    diagnosis_coverage: str
    symptom_scales: tuple[str, ...]
    cognition_scales: tuple[str, ...]
    functioning_scales: tuple[str, ...]
    treatment_variables: tuple[str, ...]
    longitudinal_coverage: str
    outcome_availability: str
    modality_availability: tuple[str, ...]
    site_structure: str
    sample_size_note: str
    known_limitations: str
    local_status: str = "audited"
    benchmarkable_outcome_families: tuple[str, ...] = ()
    provenance_urls: tuple[str, ...] = ()
    audit_summary: str = ""

    _required_text_fields: ClassVar[tuple[str, ...]] = (
        "dataset_id",
        "dataset_label",
        "population_scope",
        "diagnosis_coverage",
        "longitudinal_coverage",
        "outcome_availability",
        "site_structure",
        "sample_size_note",
        "known_limitations",
    )

    def __post_init__(self) -> None:
        for field_name in self._required_text_fields:
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.access_level not in ACCESS_LEVELS:
            raise ValueError(f"access_level must be one of {ACCESS_LEVELS}")
        if self.local_status not in LOCAL_STATUSES:
            raise ValueError(f"local_status must be one of {LOCAL_STATUSES}")
        invalid_outcomes = tuple(
            outcome
            for outcome in self.benchmarkable_outcome_families
            if outcome not in OUTCOME_FAMILIES
        )
        if invalid_outcomes:
            raise ValueError(f"unsupported outcome families: {invalid_outcomes}")

    def to_csv_row(self) -> dict[str, str]:
        return {
            "dataset_id": self.dataset_id,
            "dataset_label": self.dataset_label,
            "access_level": self.access_level,
            "population_scope": self.population_scope,
            "diagnosis_coverage": self.diagnosis_coverage,
            "symptom_scales": _join_multi_value_field(self.symptom_scales),
            "cognition_scales": _join_multi_value_field(self.cognition_scales),
            "functioning_scales": _join_multi_value_field(self.functioning_scales),
            "treatment_variables": _join_multi_value_field(self.treatment_variables),
            "longitudinal_coverage": self.longitudinal_coverage,
            "outcome_availability": self.outcome_availability,
            "modality_availability": _join_multi_value_field(self.modality_availability),
            "site_structure": self.site_structure,
            "sample_size_note": self.sample_size_note,
            "known_limitations": self.known_limitations,
            "local_status": self.local_status,
            "benchmarkable_outcome_families": _join_multi_value_field(self.benchmarkable_outcome_families),
            "provenance_urls": _join_multi_value_field(self.provenance_urls),
            "audit_summary": self.audit_summary,
        }

    @classmethod
    def from_csv_row(cls, row: dict[str, str]) -> "DatasetRegistryEntry":
        missing_columns = [column for column in REQUIRED_REGISTRY_COLUMNS if column not in row]
        if missing_columns:
            raise ValueError(f"missing required registry columns: {missing_columns}")
        return cls(
            dataset_id=row["dataset_id"].strip(),
            dataset_label=row["dataset_label"].strip(),
            access_level=row["access_level"].strip(),
            population_scope=row["population_scope"].strip(),
            diagnosis_coverage=row["diagnosis_coverage"].strip(),
            symptom_scales=_split_multi_value_field(row["symptom_scales"]),
            cognition_scales=_split_multi_value_field(row["cognition_scales"]),
            functioning_scales=_split_multi_value_field(row["functioning_scales"]),
            treatment_variables=_split_multi_value_field(row["treatment_variables"]),
            longitudinal_coverage=row["longitudinal_coverage"].strip(),
            outcome_availability=row["outcome_availability"].strip(),
            modality_availability=_split_multi_value_field(row["modality_availability"]),
            site_structure=row["site_structure"].strip(),
            sample_size_note=row["sample_size_note"].strip(),
            known_limitations=row["known_limitations"].strip(),
            local_status=row.get("local_status", "audited").strip() or "audited",
            benchmarkable_outcome_families=_split_multi_value_field(
                row.get("benchmarkable_outcome_families", "")
            ),
            provenance_urls=_split_multi_value_field(row.get("provenance_urls", "")),
            audit_summary=row.get("audit_summary", "").strip(),
        )

    def to_dict(self) -> dict[str, object]:
        payload = self.to_csv_row()
        return {
            **payload,
            "symptom_scales": list(self.symptom_scales),
            "cognition_scales": list(self.cognition_scales),
            "functioning_scales": list(self.functioning_scales),
            "treatment_variables": list(self.treatment_variables),
            "modality_availability": list(self.modality_availability),
            "benchmarkable_outcome_families": list(self.benchmarkable_outcome_families),
            "provenance_urls": list(self.provenance_urls),
        }


@dataclass(frozen=True, slots=True)
class BenchmarkDecision:
    """Decision state derived from audited registry rows."""

    state: str
    recommended_outcome_families: tuple[str, ...]
    support_by_outcome_family: dict[str, tuple[str, ...]]
    explanation: str

    def to_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "recommended_outcome_families": list(self.recommended_outcome_families),
            "support_by_outcome_family": {
                family: list(self.support_by_outcome_family[family]) for family in OUTCOME_FAMILIES
            },
            "explanation": self.explanation,
        }


def build_outcome_support(
    entries: tuple[DatasetRegistryEntry, ...],
) -> dict[str, tuple[str, ...]]:
    support: dict[str, list[str]] = {family: [] for family in OUTCOME_FAMILIES}
    for entry in entries:
        if entry.local_status not in BENCHMARK_ELIGIBLE_LOCAL_STATUSES:
            continue
        if entry.access_level not in BENCHMARK_ELIGIBLE_ACCESS_LEVELS:
            continue
        for family in entry.benchmarkable_outcome_families:
            support[family].append(entry.dataset_id)
    return {family: tuple(support[family]) for family in OUTCOME_FAMILIES}


def derive_benchmark_decision(entries: tuple[DatasetRegistryEntry, ...]) -> BenchmarkDecision:
    support_by_outcome = build_outcome_support(entries)
    support_sizes = {family: len(dataset_ids) for family, dataset_ids in support_by_outcome.items()}
    max_support = max(support_sizes.values(), default=0)

    if max_support >= 2:
        recommended = tuple(
            family for family in OUTCOME_FAMILIES if support_sizes[family] >= 2
        )
        state = "go"
        explanation = (
            "At least two public benchmark-eligible cohorts support a real benchmark outcome family. "
            f"Current cross-cohort support exists for {', '.join(recommended)}."
        )
    elif max_support == 1:
        recommended = tuple(
            family for family in OUTCOME_FAMILIES if support_sizes[family] == 1
        )
        state = "narrow-go"
        explanation = (
            "Only one public benchmark-eligible cohort supports each currently benchmarkable outcome family, "
            "so benchmark v0 must narrow scope and reduce any external-validation claim."
        )
    else:
        recommended = ()
        state = "no-go"
        explanation = (
            "The audited registry does not currently expose a benchmarkable real outcome family "
            "across public benchmark-eligible cohorts."
        )

    return BenchmarkDecision(
        state=state,
        recommended_outcome_families=recommended,
        support_by_outcome_family=support_by_outcome,
        explanation=explanation,
    )


def load_dataset_registry(path: str | Path) -> tuple[DatasetRegistryEntry, ...]:
    registry_path = Path(path)
    with registry_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return tuple(DatasetRegistryEntry.from_csv_row(row) for row in reader)


def write_dataset_registry(
    entries: tuple[DatasetRegistryEntry, ...],
    destination: str | Path,
) -> Path:
    output_path = Path(destination)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(REGISTRY_COLUMNS))
        writer.writeheader()
        for entry in entries:
            writer.writerow(entry.to_csv_row())
    return output_path


__all__ = [
    "ACCESS_LEVELS",
    "BenchmarkDecision",
    "DatasetRegistryEntry",
    "LOCAL_STATUSES",
    "OPTIONAL_REGISTRY_COLUMNS",
    "OUTCOME_FAMILIES",
    "REGISTRY_COLUMNS",
    "REQUIRED_REGISTRY_COLUMNS",
    "build_outcome_support",
    "derive_benchmark_decision",
    "load_dataset_registry",
    "write_dataset_registry",
]

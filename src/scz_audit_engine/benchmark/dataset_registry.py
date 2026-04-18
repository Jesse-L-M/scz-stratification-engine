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
BENCHMARK_V0_ELIGIBILITY_STATES = ("eligible", "limited", "ineligible")
BENCHMARK_ELIGIBLE_ACCESS_LEVELS = ("public",)
BENCHMARK_ELIGIBLE_LOCAL_STATUSES = ("audited", "harmonized")
REPRESENTATION_COMPARISON_SUPPORT_STATES = ("strong", "limited", "insufficient")
OUTCOME_TEMPORAL_VALIDITY_STATES = ("none", "concurrent_only", "prospective")
CLAIM_LEVELS = (
    "none",
    "cross_sectional_representation",
    "narrow_outcome_benchmark",
    "full_external_validation",
    "prospective_outcome_benchmark",
)
TRUE_VALUES = frozenset({"1", "true", "yes"})
FALSE_VALUES = frozenset({"", "0", "false", "no"})
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
    "benchmark_v0_eligibility",
    "representation_comparison_support",
    "predictor_timepoint",
    "outcome_timepoint",
    "outcome_window",
    "outcome_is_prospective",
    "concurrent_endpoint_only",
    "outcome_temporal_validity",
)
OPTIONAL_REGISTRY_COLUMNS = (
    "benchmarkable_outcome_families",
    "claim_level_ceiling",
    "claim_level_contributions",
    "provenance_urls",
    "audit_summary",
)
REGISTRY_COLUMNS = REQUIRED_REGISTRY_COLUMNS + OPTIONAL_REGISTRY_COLUMNS


def _split_multi_value_field(value: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in value.split(";") if item.strip())


def _join_multi_value_field(values: tuple[str, ...]) -> str:
    return "; ".join(item for item in values if item)


def _parse_bool(value: str | bool | None, *, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise ValueError(f"{field_name} must be a boolean-like value")


def _serialize_bool(value: bool) -> str:
    return "true" if value else "false"


def _flatten_support_map(
    support_by_outcome_family: dict[str, tuple[str, ...]],
) -> tuple[str, ...]:
    cohort_ids: list[str] = []
    seen: set[str] = set()
    for family in OUTCOME_FAMILIES:
        for dataset_id in support_by_outcome_family[family]:
            if dataset_id in seen:
                continue
            seen.add(dataset_id)
            cohort_ids.append(dataset_id)
    return tuple(cohort_ids)


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
    benchmark_v0_eligibility: str = "ineligible"
    representation_comparison_support: str = "insufficient"
    predictor_timepoint: str = "unmapped"
    outcome_timepoint: str = "unmapped"
    outcome_window: str = "unmapped"
    outcome_is_prospective: bool = False
    concurrent_endpoint_only: bool = False
    outcome_temporal_validity: str = "none"
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
        "representation_comparison_support",
        "predictor_timepoint",
        "outcome_timepoint",
        "outcome_window",
        "outcome_temporal_validity",
    )

    def __post_init__(self) -> None:
        for field_name in self._required_text_fields:
            if not getattr(self, field_name).strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.access_level not in ACCESS_LEVELS:
            raise ValueError(f"access_level must be one of {ACCESS_LEVELS}")
        if self.local_status not in LOCAL_STATUSES:
            raise ValueError(f"local_status must be one of {LOCAL_STATUSES}")
        if self.benchmark_v0_eligibility not in BENCHMARK_V0_ELIGIBILITY_STATES:
            raise ValueError(
                "benchmark_v0_eligibility must be one of "
                f"{BENCHMARK_V0_ELIGIBILITY_STATES}"
            )
        if self.representation_comparison_support not in REPRESENTATION_COMPARISON_SUPPORT_STATES:
            raise ValueError(
                "representation_comparison_support must be one of "
                f"{REPRESENTATION_COMPARISON_SUPPORT_STATES}"
            )
        if self.outcome_temporal_validity not in OUTCOME_TEMPORAL_VALIDITY_STATES:
            raise ValueError(
                "outcome_temporal_validity must be one of "
                f"{OUTCOME_TEMPORAL_VALIDITY_STATES}"
            )
        invalid_outcomes = tuple(
            outcome
            for outcome in self.benchmarkable_outcome_families
            if outcome not in OUTCOME_FAMILIES
        )
        if invalid_outcomes:
            raise ValueError(f"unsupported outcome families: {invalid_outcomes}")
        if self.benchmark_v0_eligibility in {"eligible", "limited"}:
            if self.access_level not in BENCHMARK_ELIGIBLE_ACCESS_LEVELS:
                raise ValueError(
                    "benchmark_v0_eligibility can only be eligible or limited for public cohorts"
                )
            if self.local_status not in BENCHMARK_ELIGIBLE_LOCAL_STATUSES:
                raise ValueError(
                    "benchmark_v0_eligibility can only be eligible or limited for audited or harmonized cohorts"
                )
            if not self.benchmarkable_outcome_families:
                raise ValueError(
                    "benchmark_v0_eligibility can only be eligible or limited when benchmarkable outcomes exist"
                )
        if self.benchmark_v0_eligibility == "eligible" and self.representation_comparison_support != "strong":
            raise ValueError(
                "benchmark_v0_eligibility=eligible requires strong representation comparison support"
            )
        if self.outcome_is_prospective and self.concurrent_endpoint_only:
            raise ValueError(
                "outcome_is_prospective and concurrent_endpoint_only cannot both be true"
            )
        if self.outcome_temporal_validity == "none":
            if self.benchmarkable_outcome_families:
                raise ValueError(
                    "outcome_temporal_validity=none is incompatible with benchmarkable outcome families"
                )
            if self.outcome_is_prospective or self.concurrent_endpoint_only:
                raise ValueError(
                    "outcome_temporal_validity=none cannot mark prospective or concurrent-only outcomes"
                )
        else:
            if not self.benchmarkable_outcome_families:
                raise ValueError(
                    "outcome_temporal_validity requires benchmarkable outcome families"
                )
        if self.outcome_is_prospective and self.outcome_temporal_validity != "prospective":
            raise ValueError(
                "outcome_is_prospective requires outcome_temporal_validity=prospective"
            )
        if self.outcome_temporal_validity == "prospective" and not self.outcome_is_prospective:
            raise ValueError(
                "outcome_temporal_validity=prospective requires outcome_is_prospective=true"
            )
        if self.concurrent_endpoint_only and self.outcome_temporal_validity != "concurrent_only":
            raise ValueError(
                "concurrent_endpoint_only requires outcome_temporal_validity=concurrent_only"
            )

    @property
    def has_benchmarkable_outcomes(self) -> bool:
        return bool(self.benchmarkable_outcome_families)

    @property
    def supports_cross_sectional_representation_claim(self) -> bool:
        return (
            self.access_level in BENCHMARK_ELIGIBLE_ACCESS_LEVELS
            and self.local_status in BENCHMARK_ELIGIBLE_LOCAL_STATUSES
            and self.representation_comparison_support == "strong"
        )

    @property
    def counts_toward_narrow_benchmark_support(self) -> bool:
        return self.benchmark_v0_eligibility == "eligible"

    @property
    def counts_toward_cross_cohort_go(self) -> bool:
        return self.counts_toward_narrow_benchmark_support

    @property
    def counts_toward_prospective_benchmark(self) -> bool:
        return self.counts_toward_narrow_benchmark_support and self.outcome_is_prospective

    @property
    def claim_level_contributions(self) -> tuple[str, ...]:
        contributions: list[str] = []
        if self.supports_cross_sectional_representation_claim:
            contributions.append("cross_sectional_representation")
        if self.counts_toward_narrow_benchmark_support:
            contributions.append("narrow_outcome_benchmark")
        if self.counts_toward_prospective_benchmark:
            contributions.append("prospective_outcome_benchmark")
        return tuple(contributions)

    @property
    def claim_level_ceiling(self) -> str:
        if not self.claim_level_contributions:
            return "none"
        return self.claim_level_contributions[-1]

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
            "benchmark_v0_eligibility": self.benchmark_v0_eligibility,
            "representation_comparison_support": self.representation_comparison_support,
            "predictor_timepoint": self.predictor_timepoint,
            "outcome_timepoint": self.outcome_timepoint,
            "outcome_window": self.outcome_window,
            "outcome_is_prospective": _serialize_bool(self.outcome_is_prospective),
            "concurrent_endpoint_only": _serialize_bool(self.concurrent_endpoint_only),
            "outcome_temporal_validity": self.outcome_temporal_validity,
            "benchmarkable_outcome_families": _join_multi_value_field(
                self.benchmarkable_outcome_families
            ),
            "claim_level_ceiling": self.claim_level_ceiling,
            "claim_level_contributions": _join_multi_value_field(
                self.claim_level_contributions
            ),
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
            benchmark_v0_eligibility=row.get("benchmark_v0_eligibility", "ineligible").strip()
            or "ineligible",
            representation_comparison_support=(
                row.get("representation_comparison_support", "insufficient").strip()
                or "insufficient"
            ),
            predictor_timepoint=row.get("predictor_timepoint", "unmapped").strip() or "unmapped",
            outcome_timepoint=row.get("outcome_timepoint", "unmapped").strip() or "unmapped",
            outcome_window=row.get("outcome_window", "unmapped").strip() or "unmapped",
            outcome_is_prospective=_parse_bool(
                row.get("outcome_is_prospective", "false"),
                field_name="outcome_is_prospective",
            ),
            concurrent_endpoint_only=_parse_bool(
                row.get("concurrent_endpoint_only", "false"),
                field_name="concurrent_endpoint_only",
            ),
            outcome_temporal_validity=(
                row.get("outcome_temporal_validity", "none").strip() or "none"
            ),
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
            "outcome_is_prospective": self.outcome_is_prospective,
            "concurrent_endpoint_only": self.concurrent_endpoint_only,
            "benchmark_v0_eligibility": self.benchmark_v0_eligibility,
            "counts_toward_cross_cohort_go": self.counts_toward_cross_cohort_go,
            "counts_toward_narrow_benchmark_support": self.counts_toward_narrow_benchmark_support,
            "counts_toward_prospective_benchmark": self.counts_toward_prospective_benchmark,
            "supports_cross_sectional_representation_claim": (
                self.supports_cross_sectional_representation_claim
            ),
            "benchmarkable_outcome_families": list(self.benchmarkable_outcome_families),
            "claim_level_contributions": list(self.claim_level_contributions),
            "provenance_urls": list(self.provenance_urls),
        }


@dataclass(frozen=True, slots=True)
class BenchmarkDecision:
    """Decision state derived from audited registry rows."""

    state: str
    claim_level: str
    recommended_outcome_families: tuple[str, ...]
    support_by_outcome_family: dict[str, tuple[str, ...]]
    full_external_validation_support_by_outcome_family: dict[str, tuple[str, ...]]
    prospective_support_by_outcome_family: dict[str, tuple[str, ...]]
    narrow_supporting_cohorts: tuple[str, ...]
    full_external_validation_cohorts: tuple[str, ...]
    concurrent_only_cohorts: tuple[str, ...]
    prospectively_usable_cohorts: tuple[str, ...]
    limiting_factors: tuple[str, ...]
    explanation: str
    claim_level_explanation: str

    def to_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "claim_level": self.claim_level,
            "recommended_outcome_families": list(self.recommended_outcome_families),
            "support_by_outcome_family": {
                family: list(self.support_by_outcome_family[family]) for family in OUTCOME_FAMILIES
            },
            "full_external_validation_support_by_outcome_family": {
                family: list(self.full_external_validation_support_by_outcome_family[family])
                for family in OUTCOME_FAMILIES
            },
            "prospective_support_by_outcome_family": {
                family: list(self.prospective_support_by_outcome_family[family])
                for family in OUTCOME_FAMILIES
            },
            "narrow_supporting_cohorts": list(self.narrow_supporting_cohorts),
            "full_external_validation_cohorts": list(self.full_external_validation_cohorts),
            "concurrent_only_cohorts": list(self.concurrent_only_cohorts),
            "prospectively_usable_cohorts": list(self.prospectively_usable_cohorts),
            "limiting_factors": list(self.limiting_factors),
            "explanation": self.explanation,
            "claim_level_explanation": self.claim_level_explanation,
        }


def build_outcome_support(
    entries: tuple[DatasetRegistryEntry, ...],
    *,
    prospective_only: bool = False,
) -> dict[str, tuple[str, ...]]:
    support: dict[str, list[str]] = {family: [] for family in OUTCOME_FAMILIES}
    for entry in entries:
        if not entry.counts_toward_narrow_benchmark_support:
            continue
        if prospective_only and not entry.counts_toward_prospective_benchmark:
            continue
        for family in entry.benchmarkable_outcome_families:
            support[family].append(entry.dataset_id)
    return {family: tuple(support[family]) for family in OUTCOME_FAMILIES}


def build_full_external_validation_support(
    support_by_outcome_family: dict[str, tuple[str, ...]],
) -> dict[str, tuple[str, ...]]:
    return {
        family: dataset_ids if len(dataset_ids) >= 2 else ()
        for family, dataset_ids in support_by_outcome_family.items()
    }


def derive_benchmark_decision(entries: tuple[DatasetRegistryEntry, ...]) -> BenchmarkDecision:
    support_by_outcome = build_outcome_support(entries)
    full_external_validation_support = build_full_external_validation_support(
        support_by_outcome
    )
    prospective_support = build_outcome_support(entries, prospective_only=True)
    support_sizes = {family: len(dataset_ids) for family, dataset_ids in support_by_outcome.items()}
    max_support = max(support_sizes.values(), default=0)
    narrow_supporting_cohorts = tuple(
        entry.dataset_id for entry in entries if entry.counts_toward_narrow_benchmark_support
    )
    concurrent_only_cohorts = tuple(
        entry.dataset_id
        for entry in entries
        if entry.has_benchmarkable_outcomes
        and entry.outcome_temporal_validity == "concurrent_only"
    )
    prospectively_usable_cohorts = tuple(
        entry.dataset_id
        for entry in entries
        if entry.counts_toward_prospective_benchmark
    )
    limited_representation_cohorts = tuple(
        entry.dataset_id
        for entry in entries
        if entry.representation_comparison_support == "limited"
    )
    full_external_validation_cohorts = _flatten_support_map(full_external_validation_support)
    if max_support >= 2:
        recommended = tuple(
            family for family in OUTCOME_FAMILIES if support_sizes[family] >= 2
        )
        state = "go"
        explanation = (
            "At least two public benchmark-eligible cohorts support the same benchmark outcome "
            f"family ({', '.join(recommended)}), so the feasibility gate clears beyond narrow-go."
        )
    elif max_support == 1:
        recommended = tuple(
            family for family in OUTCOME_FAMILIES if support_sizes[family] == 1
        )
        explanation_parts = [
            "Only one public benchmark-eligible cohort currently counts toward narrow benchmark "
            f"support for {', '.join(recommended)}."
        ]
        if limited_representation_cohorts:
            explanation_parts.append(
                "Cohorts with weaker public label granularity remain outside the claim count: "
                f"{', '.join(limited_representation_cohorts)}."
            )
        if not prospectively_usable_cohorts:
            explanation_parts.append(
                "Current public endpoint support is concurrent-only, so the repo remains "
                "narrow-go without a prospective claim."
            )
        state = "narrow-go"
        explanation = " ".join(explanation_parts)
    else:
        recommended = ()
        state = "no-go"
        explanation = (
            "The audited registry does not currently expose a benchmarkable real outcome family "
            "across public benchmark-eligible cohorts."
        )

    if any(len(dataset_ids) >= 2 for dataset_ids in prospective_support.values()):
        claim_level = "prospective_outcome_benchmark"
        claim_level_explanation = (
            "At least two benchmark-eligible cohorts support a prospective outcome window, so the "
            "repo can claim a prospective outcome benchmark."
        )
    elif any(len(dataset_ids) >= 2 for dataset_ids in full_external_validation_support.values()):
        claim_level = "full_external_validation"
        claim_level_explanation = (
            "At least two benchmark-eligible cohorts support the same outcome family, so the repo "
            "can claim full external validation for that outcome family."
        )
    elif max_support == 1:
        claim_level = "narrow_outcome_benchmark"
        claim_level_explanation = (
            "One benchmark-eligible cohort supports a real outcome family, so the repo can make a "
            "narrow outcome benchmark claim but not a full external-validation or prospective claim."
        )
    elif any(entry.supports_cross_sectional_representation_claim for entry in entries):
        claim_level = "cross_sectional_representation"
        claim_level_explanation = (
            "The public cohorts support cross-sectional representation comparison, but they do not "
            "yet support an honest outcome benchmark."
        )
    else:
        claim_level = "none"
        claim_level_explanation = (
            "The audited public cohorts do not yet support a defensible benchmark claim."
        )

    limiting_factors: list[str] = []
    if max_support == 1:
        limiting_factors.append(
            "Only one cohort currently counts toward narrow benchmark support."
        )
    if not full_external_validation_cohorts:
        limiting_factors.append(
            "No outcome family is currently supported by two benchmark-eligible public cohorts."
        )
    if not prospectively_usable_cohorts:
        limiting_factors.append(
            "No audited cohort currently exposes a prospectively usable public outcome window."
        )
    if limited_representation_cohorts:
        limiting_factors.append(
            "Public label granularity remains limited for "
            f"{', '.join(limited_representation_cohorts)}."
        )

    return BenchmarkDecision(
        state=state,
        claim_level=claim_level,
        recommended_outcome_families=recommended,
        support_by_outcome_family=support_by_outcome,
        full_external_validation_support_by_outcome_family=full_external_validation_support,
        prospective_support_by_outcome_family=prospective_support,
        narrow_supporting_cohorts=narrow_supporting_cohorts,
        full_external_validation_cohorts=full_external_validation_cohorts,
        concurrent_only_cohorts=concurrent_only_cohorts,
        prospectively_usable_cohorts=prospectively_usable_cohorts,
        limiting_factors=tuple(limiting_factors),
        explanation=explanation,
        claim_level_explanation=claim_level_explanation,
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
        writer = csv.DictWriter(
            handle,
            fieldnames=list(REGISTRY_COLUMNS),
            lineterminator="\n",
        )
        writer.writeheader()
        for entry in entries:
            writer.writerow(entry.to_csv_row())
    return output_path


__all__ = [
    "ACCESS_LEVELS",
    "BENCHMARK_V0_ELIGIBILITY_STATES",
    "CLAIM_LEVELS",
    "BenchmarkDecision",
    "DatasetRegistryEntry",
    "LOCAL_STATUSES",
    "OPTIONAL_REGISTRY_COLUMNS",
    "OUTCOME_FAMILIES",
    "OUTCOME_TEMPORAL_VALIDITY_STATES",
    "REGISTRY_COLUMNS",
    "REPRESENTATION_COMPARISON_SUPPORT_STATES",
    "REQUIRED_REGISTRY_COLUMNS",
    "build_full_external_validation_support",
    "build_outcome_support",
    "derive_benchmark_decision",
    "load_dataset_registry",
    "write_dataset_registry",
]

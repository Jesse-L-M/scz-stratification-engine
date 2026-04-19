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
ACCESS_TIERS = ("strict_open", "public_credentialed", "controlled")
ACCESS_TIER_DECISION_ORDER = ACCESS_TIERS
ALLOWED_ACCESS_TIERS_BY_DECISION = {
    "strict_open": ("strict_open",),
    "public_credentialed": ("strict_open", "public_credentialed"),
    "controlled": ACCESS_TIERS,
}
LEGACY_ACCESS_LEVEL_TO_TIER = {
    "public": "strict_open",
    "gated": "public_credentialed",
    "controlled": "controlled",
}
LOCAL_STATUSES = ("candidate", "audited", "harmonized", "deferred")
BENCHMARK_V0_ELIGIBILITY_STATES = ("eligible", "limited", "ineligible")
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
NEXT_STEP_RECOMMENDATIONS = (
    "remain_paused_at_no_go",
    "remain_paused_at_narrow_go",
    "continue_cross_sectional_representation_only",
    "defer_until_stronger_credentialed_or_controlled_data",
    "proceed_with_outcome_benchmark",
)
TRUE_VALUES = frozenset({"1", "true", "yes"})
FALSE_VALUES = frozenset({"", "0", "false", "no"})
REQUIRED_REGISTRY_COLUMNS = (
    "dataset_id",
    "dataset_label",
    "access_tier",
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


def _format_access_tier_scope(allowed_access_tiers: tuple[str, ...]) -> str:
    return " + ".join(allowed_access_tiers)


def _filter_entries_by_access_tier(
    entries: tuple["DatasetRegistryEntry", ...],
    *,
    allowed_access_tiers: tuple[str, ...],
) -> tuple["DatasetRegistryEntry", ...]:
    return tuple(
        entry for entry in entries if entry.access_tier in allowed_access_tiers
    )


def _normalize_access_tier(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    return LEGACY_ACCESS_LEVEL_TO_TIER.get(normalized, normalized)


@dataclass(frozen=True, slots=True)
class DatasetRegistryEntry:
    """Normalized registry row used for dataset audits."""

    dataset_id: str
    dataset_label: str
    access_tier: str
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
        if self.access_tier not in ACCESS_TIERS:
            raise ValueError(f"access_tier must be one of {ACCESS_TIERS}")
        if self.local_status not in LOCAL_STATUSES:
            raise ValueError(f"local_status must be one of {LOCAL_STATUSES}")
        if self.benchmark_v0_eligibility not in BENCHMARK_V0_ELIGIBILITY_STATES:
            raise ValueError(
                "benchmark_v0_eligibility must be one of "
                f"{BENCHMARK_V0_ELIGIBILITY_STATES}"
            )
        if (
            self.representation_comparison_support
            not in REPRESENTATION_COMPARISON_SUPPORT_STATES
        ):
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
            if self.local_status not in BENCHMARK_ELIGIBLE_LOCAL_STATUSES:
                raise ValueError(
                    "benchmark_v0_eligibility can only be eligible or limited for audited or harmonized cohorts"
                )
            if not self.benchmarkable_outcome_families:
                raise ValueError(
                    "benchmark_v0_eligibility can only be eligible or limited when benchmarkable outcomes exist"
                )
            if self.representation_comparison_support == "insufficient":
                raise ValueError(
                    "benchmark_v0_eligibility can only be eligible or limited when representation comparison support is strong or limited"
                )
        if (
            self.benchmark_v0_eligibility == "eligible"
            and self.representation_comparison_support != "strong"
        ):
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
        if (
            self.outcome_is_prospective
            and self.outcome_temporal_validity != "prospective"
        ):
            raise ValueError(
                "outcome_is_prospective requires outcome_temporal_validity=prospective"
            )
        if (
            self.outcome_temporal_validity == "prospective"
            and not self.outcome_is_prospective
        ):
            raise ValueError(
                "outcome_temporal_validity=prospective requires outcome_is_prospective=true"
            )
        if (
            self.concurrent_endpoint_only
            and self.outcome_temporal_validity != "concurrent_only"
        ):
            raise ValueError(
                "concurrent_endpoint_only requires outcome_temporal_validity=concurrent_only"
            )

    @property
    def has_benchmarkable_outcomes(self) -> bool:
        return bool(self.benchmarkable_outcome_families)

    @property
    def supports_cross_sectional_representation_if_access_allowed(self) -> bool:
        return (
            self.local_status in BENCHMARK_ELIGIBLE_LOCAL_STATUSES
            and self.representation_comparison_support == "strong"
        )

    @property
    def counts_toward_narrow_benchmark_support_if_access_allowed(self) -> bool:
        return self.benchmark_v0_eligibility == "eligible"

    @property
    def counts_toward_prospective_benchmark_if_access_allowed(self) -> bool:
        return (
            self.counts_toward_narrow_benchmark_support_if_access_allowed
            and self.outcome_is_prospective
        )

    @property
    def claim_level_contributions(self) -> tuple[str, ...]:
        contributions: list[str] = []
        if self.supports_cross_sectional_representation_if_access_allowed:
            contributions.append("cross_sectional_representation")
        if self.counts_toward_narrow_benchmark_support_if_access_allowed:
            contributions.append("narrow_outcome_benchmark")
        if self.counts_toward_prospective_benchmark_if_access_allowed:
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
            "access_tier": self.access_tier,
            "population_scope": self.population_scope,
            "diagnosis_coverage": self.diagnosis_coverage,
            "symptom_scales": _join_multi_value_field(self.symptom_scales),
            "cognition_scales": _join_multi_value_field(self.cognition_scales),
            "functioning_scales": _join_multi_value_field(self.functioning_scales),
            "treatment_variables": _join_multi_value_field(self.treatment_variables),
            "longitudinal_coverage": self.longitudinal_coverage,
            "outcome_availability": self.outcome_availability,
            "modality_availability": _join_multi_value_field(
                self.modality_availability
            ),
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
        missing_columns = [
            column
            for column in REQUIRED_REGISTRY_COLUMNS
            if column != "access_tier" and column not in row
        ]
        if "access_tier" not in row and "access_level" not in row:
            missing_columns.append("access_tier")
        if missing_columns:
            raise ValueError(f"missing required registry columns: {missing_columns}")
        raw_access_tier = row.get("access_tier")
        if raw_access_tier and raw_access_tier.strip():
            access_tier = raw_access_tier
        else:
            access_tier = row.get("access_level", "")
        return cls(
            dataset_id=row["dataset_id"].strip(),
            dataset_label=row["dataset_label"].strip(),
            access_tier=_normalize_access_tier(access_tier),
            population_scope=row["population_scope"].strip(),
            diagnosis_coverage=row["diagnosis_coverage"].strip(),
            symptom_scales=_split_multi_value_field(row["symptom_scales"]),
            cognition_scales=_split_multi_value_field(row["cognition_scales"]),
            functioning_scales=_split_multi_value_field(row["functioning_scales"]),
            treatment_variables=_split_multi_value_field(row["treatment_variables"]),
            longitudinal_coverage=row["longitudinal_coverage"].strip(),
            outcome_availability=row["outcome_availability"].strip(),
            modality_availability=_split_multi_value_field(
                row["modality_availability"]
            ),
            site_structure=row["site_structure"].strip(),
            sample_size_note=row["sample_size_note"].strip(),
            known_limitations=row["known_limitations"].strip(),
            local_status=row.get("local_status", "audited").strip() or "audited",
            benchmark_v0_eligibility=(
                row.get("benchmark_v0_eligibility", "ineligible").strip()
                or "ineligible"
            ),
            representation_comparison_support=(
                row.get("representation_comparison_support", "insufficient").strip()
                or "insufficient"
            ),
            predictor_timepoint=(
                row.get("predictor_timepoint", "unmapped").strip() or "unmapped"
            ),
            outcome_timepoint=(
                row.get("outcome_timepoint", "unmapped").strip() or "unmapped"
            ),
            outcome_window=row.get("outcome_window", "unmapped").strip()
            or "unmapped",
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
            "counts_toward_narrow_benchmark_support_if_access_allowed": (
                self.counts_toward_narrow_benchmark_support_if_access_allowed
            ),
            "counts_toward_prospective_benchmark_if_access_allowed": (
                self.counts_toward_prospective_benchmark_if_access_allowed
            ),
            "supports_cross_sectional_representation_if_access_allowed": (
                self.supports_cross_sectional_representation_if_access_allowed
            ),
            "benchmarkable_outcome_families": list(
                self.benchmarkable_outcome_families
            ),
            "claim_level_contributions": list(self.claim_level_contributions),
            "provenance_urls": list(self.provenance_urls),
        }


@dataclass(frozen=True, slots=True)
class BenchmarkDecisionLayer:
    """Benchmark decision state for one access-tier scope."""

    access_tier: str
    allowed_access_tiers: tuple[str, ...]
    state: str
    claim_level: str
    recommended_outcome_families: tuple[str, ...]
    support_by_outcome_family: dict[str, tuple[str, ...]]
    full_external_validation_support_by_outcome_family: dict[str, tuple[str, ...]]
    prospective_support_by_outcome_family: dict[str, tuple[str, ...]]
    cross_sectional_representation_cohorts: tuple[str, ...]
    narrow_supporting_cohorts: tuple[str, ...]
    full_external_validation_cohorts: tuple[str, ...]
    concurrent_only_cohorts: tuple[str, ...]
    prospectively_usable_cohorts: tuple[str, ...]
    limited_representation_cohorts: tuple[str, ...]
    limiting_factors: tuple[str, ...]
    explanation: str
    claim_level_explanation: str

    def to_dict(self) -> dict[str, object]:
        return {
            "access_tier": self.access_tier,
            "allowed_access_tiers": list(self.allowed_access_tiers),
            "state": self.state,
            "claim_level": self.claim_level,
            "recommended_outcome_families": list(self.recommended_outcome_families),
            "support_by_outcome_family": {
                family: list(self.support_by_outcome_family[family])
                for family in OUTCOME_FAMILIES
            },
            "full_external_validation_support_by_outcome_family": {
                family: list(
                    self.full_external_validation_support_by_outcome_family[family]
                )
                for family in OUTCOME_FAMILIES
            },
            "prospective_support_by_outcome_family": {
                family: list(self.prospective_support_by_outcome_family[family])
                for family in OUTCOME_FAMILIES
            },
            "cross_sectional_representation_cohorts": list(
                self.cross_sectional_representation_cohorts
            ),
            "narrow_supporting_cohorts": list(self.narrow_supporting_cohorts),
            "full_external_validation_cohorts": list(
                self.full_external_validation_cohorts
            ),
            "concurrent_only_cohorts": list(self.concurrent_only_cohorts),
            "prospectively_usable_cohorts": list(self.prospectively_usable_cohorts),
            "limited_representation_cohorts": list(
                self.limited_representation_cohorts
            ),
            "limiting_factors": list(self.limiting_factors),
            "explanation": self.explanation,
            "claim_level_explanation": self.claim_level_explanation,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkDecision:
    """Layered decision state derived from audited registry rows."""

    current_access_tier: str
    strict_open: BenchmarkDecisionLayer
    public_credentialed: BenchmarkDecisionLayer
    controlled: BenchmarkDecisionLayer
    recommended_next_step: str
    recommended_next_step_explanation: str

    def layer_for(self, access_tier: str) -> BenchmarkDecisionLayer:
        if access_tier not in ACCESS_TIER_DECISION_ORDER:
            raise ValueError(f"unsupported access_tier: {access_tier}")
        return getattr(self, access_tier)

    @property
    def _current_layer(self) -> BenchmarkDecisionLayer:
        return self.layer_for(self.current_access_tier)

    @property
    def state(self) -> str:
        return self._current_layer.state

    @property
    def claim_level(self) -> str:
        return self._current_layer.claim_level

    @property
    def recommended_outcome_families(self) -> tuple[str, ...]:
        return self._current_layer.recommended_outcome_families

    @property
    def support_by_outcome_family(self) -> dict[str, tuple[str, ...]]:
        return self._current_layer.support_by_outcome_family

    @property
    def full_external_validation_support_by_outcome_family(
        self,
    ) -> dict[str, tuple[str, ...]]:
        return self._current_layer.full_external_validation_support_by_outcome_family

    @property
    def prospective_support_by_outcome_family(self) -> dict[str, tuple[str, ...]]:
        return self._current_layer.prospective_support_by_outcome_family

    @property
    def cross_sectional_representation_cohorts(self) -> tuple[str, ...]:
        return self._current_layer.cross_sectional_representation_cohorts

    @property
    def narrow_supporting_cohorts(self) -> tuple[str, ...]:
        return self._current_layer.narrow_supporting_cohorts

    @property
    def full_external_validation_cohorts(self) -> tuple[str, ...]:
        return self._current_layer.full_external_validation_cohorts

    @property
    def concurrent_only_cohorts(self) -> tuple[str, ...]:
        return self._current_layer.concurrent_only_cohorts

    @property
    def prospectively_usable_cohorts(self) -> tuple[str, ...]:
        return self._current_layer.prospectively_usable_cohorts

    @property
    def limited_representation_cohorts(self) -> tuple[str, ...]:
        return self._current_layer.limited_representation_cohorts

    @property
    def limiting_factors(self) -> tuple[str, ...]:
        return self._current_layer.limiting_factors

    @property
    def explanation(self) -> str:
        return self._current_layer.explanation

    @property
    def claim_level_explanation(self) -> str:
        return self._current_layer.claim_level_explanation

    def to_dict(self) -> dict[str, object]:
        return {
            "current_access_tier": self.current_access_tier,
            "state": self.state,
            "claim_level": self.claim_level,
            "recommended_outcome_families": list(
                self.recommended_outcome_families
            ),
            "recommended_next_step": self.recommended_next_step,
            "recommended_next_step_explanation": (
                self.recommended_next_step_explanation
            ),
            "access_tier_decisions": {
                access_tier: self.layer_for(access_tier).to_dict()
                for access_tier in ACCESS_TIER_DECISION_ORDER
            },
        }


def build_outcome_support(
    entries: tuple[DatasetRegistryEntry, ...],
    *,
    allowed_access_tiers: tuple[str, ...] = ACCESS_TIERS,
    prospective_only: bool = False,
) -> dict[str, tuple[str, ...]]:
    support: dict[str, list[str]] = {family: [] for family in OUTCOME_FAMILIES}
    for entry in entries:
        if entry.access_tier not in allowed_access_tiers:
            continue
        if not entry.counts_toward_narrow_benchmark_support_if_access_allowed:
            continue
        if (
            prospective_only
            and not entry.counts_toward_prospective_benchmark_if_access_allowed
        ):
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


def _derive_benchmark_decision_layer(
    entries: tuple[DatasetRegistryEntry, ...],
    *,
    access_tier: str,
) -> BenchmarkDecisionLayer:
    allowed_access_tiers = ALLOWED_ACCESS_TIERS_BY_DECISION[access_tier]
    accessible_entries = _filter_entries_by_access_tier(
        entries,
        allowed_access_tiers=allowed_access_tiers,
    )
    support_by_outcome = build_outcome_support(
        accessible_entries,
        allowed_access_tiers=allowed_access_tiers,
    )
    full_external_validation_support = build_full_external_validation_support(
        support_by_outcome
    )
    prospective_support = build_outcome_support(
        accessible_entries,
        allowed_access_tiers=allowed_access_tiers,
        prospective_only=True,
    )
    support_sizes = {
        family: len(dataset_ids)
        for family, dataset_ids in support_by_outcome.items()
    }
    max_support = max(support_sizes.values(), default=0)
    narrow_supporting_cohorts = tuple(
        entry.dataset_id
        for entry in accessible_entries
        if entry.counts_toward_narrow_benchmark_support_if_access_allowed
    )
    cross_sectional_representation_cohorts = tuple(
        entry.dataset_id
        for entry in accessible_entries
        if entry.supports_cross_sectional_representation_if_access_allowed
    )
    concurrent_only_cohorts = tuple(
        entry.dataset_id
        for entry in accessible_entries
        if entry.has_benchmarkable_outcomes
        and entry.outcome_temporal_validity == "concurrent_only"
    )
    prospectively_usable_cohorts = tuple(
        entry.dataset_id
        for entry in accessible_entries
        if entry.counts_toward_prospective_benchmark_if_access_allowed
    )
    limited_representation_cohorts = tuple(
        entry.dataset_id
        for entry in accessible_entries
        if entry.representation_comparison_support == "limited"
    )
    full_external_validation_cohorts = _flatten_support_map(
        full_external_validation_support
    )
    access_scope = _format_access_tier_scope(allowed_access_tiers)

    if max_support >= 2:
        recommended = tuple(
            family for family in OUTCOME_FAMILIES if support_sizes[family] >= 2
        )
        state = "go"
        explanation = (
            f"Within {access_scope}, at least two audited eligible cohorts support the "
            f"same benchmark outcome family ({', '.join(recommended)}), so the "
            "feasibility gate clears beyond narrow-go."
        )
    elif max_support == 1:
        recommended = tuple(
            family for family in OUTCOME_FAMILIES if support_sizes[family] == 1
        )
        explanation_parts = [
            f"Within {access_scope}, only one audited eligible cohort currently supports "
            f"{', '.join(recommended)}."
        ]
        if limited_representation_cohorts:
            explanation_parts.append(
                "Cohorts with weaker diagnosis granularity remain outside the eligible "
                f"claim count: {', '.join(limited_representation_cohorts)}."
            )
        if not prospectively_usable_cohorts:
            explanation_parts.append(
                "Current endpoint support remains concurrent-only, so this access tier "
                "scope stays narrow-go without a prospective claim."
            )
        state = "narrow-go"
        explanation = " ".join(explanation_parts)
    else:
        recommended = ()
        state = "no-go"
        if cross_sectional_representation_cohorts:
            explanation = (
                f"Within {access_scope}, audited cohorts support cross-sectional "
                "representation comparison, but no eligible outcome family is currently "
                "supported."
            )
        else:
            explanation = (
                f"Within {access_scope}, the audited registry does not currently expose an "
                "eligible benchmark outcome family or strong cross-sectional "
                "representation support."
            )

    if any(len(dataset_ids) >= 2 for dataset_ids in prospective_support.values()):
        claim_level = "prospective_outcome_benchmark"
        claim_level_explanation = (
            f"Within {access_scope}, at least two eligible cohorts support a prospective "
            "outcome window, so the repo can claim a prospective outcome benchmark."
        )
    elif any(
        len(dataset_ids) >= 2
        for dataset_ids in full_external_validation_support.values()
    ):
        claim_level = "full_external_validation"
        claim_level_explanation = (
            f"Within {access_scope}, at least two eligible cohorts support the same "
            "outcome family, so the repo can claim full external validation."
        )
    elif max_support == 1:
        claim_level = "narrow_outcome_benchmark"
        claim_level_explanation = (
            f"Within {access_scope}, one eligible cohort supports a real outcome family, "
            "so the repo can make a narrow outcome benchmark claim but not a full "
            "external-validation or prospective claim."
        )
    elif cross_sectional_representation_cohorts:
        claim_level = "cross_sectional_representation"
        claim_level_explanation = (
            f"Within {access_scope}, the audited cohorts support cross-sectional "
            "representation comparison, but they do not yet support an honest outcome "
            "benchmark."
        )
    else:
        claim_level = "none"
        claim_level_explanation = (
            f"Within {access_scope}, the audited cohorts do not yet support a defensible "
            "benchmark claim."
        )

    limiting_factors: list[str] = []
    if not accessible_entries:
        limiting_factors.append(
            "No audited cohorts are currently available in this access tier scope."
        )
    if max_support == 1:
        limiting_factors.append(
            "Only one cohort currently counts toward narrow benchmark support."
        )
    if not full_external_validation_cohorts:
        limiting_factors.append(
            "No outcome family is currently supported by two eligible cohorts."
        )
    if not prospectively_usable_cohorts:
        limiting_factors.append(
            "No audited cohort currently exposes a prospectively usable outcome window."
        )
    if limited_representation_cohorts:
        limiting_factors.append(
            "Diagnosis granularity remains limited for "
            f"{', '.join(limited_representation_cohorts)}."
        )

    return BenchmarkDecisionLayer(
        access_tier=access_tier,
        allowed_access_tiers=allowed_access_tiers,
        state=state,
        claim_level=claim_level,
        recommended_outcome_families=recommended,
        support_by_outcome_family=support_by_outcome,
        full_external_validation_support_by_outcome_family=(
            full_external_validation_support
        ),
        prospective_support_by_outcome_family=prospective_support,
        cross_sectional_representation_cohorts=(
            cross_sectional_representation_cohorts
        ),
        narrow_supporting_cohorts=narrow_supporting_cohorts,
        full_external_validation_cohorts=full_external_validation_cohorts,
        concurrent_only_cohorts=concurrent_only_cohorts,
        prospectively_usable_cohorts=prospectively_usable_cohorts,
        limited_representation_cohorts=limited_representation_cohorts,
        limiting_factors=tuple(limiting_factors),
        explanation=explanation,
        claim_level_explanation=claim_level_explanation,
    )


def _layers_change_benchmarkability(
    previous_layer: BenchmarkDecisionLayer,
    current_layer: BenchmarkDecisionLayer,
) -> bool:
    return any(
        (
            current_layer.state != previous_layer.state,
            current_layer.claim_level != previous_layer.claim_level,
            current_layer.recommended_outcome_families
            != previous_layer.recommended_outcome_families,
            current_layer.narrow_supporting_cohorts
            != previous_layer.narrow_supporting_cohorts,
        )
    )


def _derive_next_step_recommendation(
    *,
    strict_open: BenchmarkDecisionLayer,
    public_credentialed: BenchmarkDecisionLayer,
    controlled: BenchmarkDecisionLayer,
) -> tuple[str, str]:
    if strict_open.state == "go":
        return (
            "proceed_with_outcome_benchmark",
            "Strict-open cohorts already clear the outcome benchmark gate, so the "
            "repo can move beyond the feasibility pause without broadening access "
            "requirements.",
        )
    if _layers_change_benchmarkability(
        strict_open, public_credentialed
    ) or _layers_change_benchmarkability(public_credentialed, controlled):
        return (
            "defer_until_stronger_credentialed_or_controlled_data",
            "Outcome benchmarkability improves only when broader-access cohorts are "
            "allowed, so any stronger outcome benchmark line should wait for "
            "credentialed or controlled data rather than over-claiming strict-open "
            "support.",
        )
    if len(strict_open.cross_sectional_representation_cohorts) >= 2:
        return (
            "continue_cross_sectional_representation_only",
            "Multiple strict-open cohorts now support cross-sectional representation "
            "comparison, but only one strict-open cohort still supports an outcome "
            "benchmark. The honest next phase is cross-sectional representation work "
            "only, not a stronger outcome benchmark line.",
        )
    if strict_open.state == "narrow-go":
        return (
            "remain_paused_at_narrow_go",
            "The audited cohorts do not materially improve benchmarkability beyond the "
            "current narrow-go lane, so the repo should remain paused at that boundary.",
        )
    return (
        "remain_paused_at_no_go",
        "The audited strict-open cohorts still do not clear the narrow-go gate, so "
        "the repo should remain paused at no-go until stronger benchmarkable support "
        "exists.",
    )


def derive_benchmark_decision(
    entries: tuple[DatasetRegistryEntry, ...],
) -> BenchmarkDecision:
    strict_open = _derive_benchmark_decision_layer(
        entries,
        access_tier="strict_open",
    )
    public_credentialed = _derive_benchmark_decision_layer(
        entries,
        access_tier="public_credentialed",
    )
    controlled = _derive_benchmark_decision_layer(
        entries,
        access_tier="controlled",
    )
    (
        recommended_next_step,
        recommended_next_step_explanation,
    ) = _derive_next_step_recommendation(
        strict_open=strict_open,
        public_credentialed=public_credentialed,
        controlled=controlled,
    )
    return BenchmarkDecision(
        current_access_tier="strict_open",
        strict_open=strict_open,
        public_credentialed=public_credentialed,
        controlled=controlled,
        recommended_next_step=recommended_next_step,
        recommended_next_step_explanation=recommended_next_step_explanation,
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
    "ACCESS_TIERS",
    "ACCESS_TIER_DECISION_ORDER",
    "ALLOWED_ACCESS_TIERS_BY_DECISION",
    "BENCHMARK_V0_ELIGIBILITY_STATES",
    "CLAIM_LEVELS",
    "NEXT_STEP_RECOMMENDATIONS",
    "BenchmarkDecision",
    "BenchmarkDecisionLayer",
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

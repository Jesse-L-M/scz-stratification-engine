import pytest

from scz_audit_engine.benchmark.dataset_registry import (
    REGISTRY_COLUMNS,
    DatasetRegistryEntry,
    derive_benchmark_decision,
    load_dataset_registry,
    write_dataset_registry,
)


def _entry(
    dataset_id: str,
    outcomes: tuple[str, ...],
    *,
    access_tier: str = "strict_open",
    local_status: str = "audited",
    benchmark_v0_eligibility: str | None = None,
    representation_comparison_support: str | None = None,
    outcome_is_prospective: bool = False,
    concurrent_endpoint_only: bool | None = None,
    outcome_temporal_validity: str | None = None,
) -> DatasetRegistryEntry:
    if benchmark_v0_eligibility is None:
        benchmark_v0_eligibility = (
            "eligible" if local_status in {"audited", "harmonized"} and outcomes else "ineligible"
        )
    if representation_comparison_support is None:
        if benchmark_v0_eligibility == "eligible":
            representation_comparison_support = "strong"
        elif benchmark_v0_eligibility == "limited":
            representation_comparison_support = "limited"
        else:
            representation_comparison_support = "strong"
    if concurrent_endpoint_only is None:
        concurrent_endpoint_only = bool(outcomes) and not outcome_is_prospective
    if outcome_temporal_validity is None:
        if outcome_is_prospective:
            outcome_temporal_validity = "prospective"
        elif outcomes:
            outcome_temporal_validity = "concurrent_only"
        else:
            outcome_temporal_validity = "none"
    predictor_timepoint = "baseline" if outcomes else "unmapped"
    outcome_timepoint = (
        "12_month_follow_up"
        if outcome_is_prospective
        else ("same_visit" if outcomes else "unmapped")
    )
    outcome_window = "12_month" if outcome_is_prospective else ("same_visit" if outcomes else "unmapped")
    return DatasetRegistryEntry(
        dataset_id=dataset_id,
        dataset_label=f"{dataset_id} label",
        access_tier=access_tier,
        population_scope="psychosis cohort",
        diagnosis_coverage="psychosis vs control",
        symptom_scales=("PANSS",),
        cognition_scales=("MATRICS",),
        functioning_scales=("GAF/GAS",) if outcomes else (),
        treatment_variables=("medication",),
        longitudinal_coverage="No repeated follow-up" if not outcome_is_prospective else "Twelve-month follow-up",
        outcome_availability="Poor functional outcome only" if outcomes else "No benchmarkable outcome",
        modality_availability=("MRI",),
        site_structure="single site",
        sample_size_note="n=10",
        known_limitations="fixture only",
        local_status=local_status,
        benchmark_v0_eligibility=benchmark_v0_eligibility,
        representation_comparison_support=representation_comparison_support,
        predictor_timepoint=predictor_timepoint,
        outcome_timepoint=outcome_timepoint,
        outcome_window=outcome_window,
        outcome_is_prospective=outcome_is_prospective,
        concurrent_endpoint_only=concurrent_endpoint_only,
        outcome_temporal_validity=outcome_temporal_validity,
        benchmarkable_outcome_families=outcomes,
        provenance_urls=("https://example.org",),
        audit_summary="fixture row",
    )


def test_registry_rows_round_trip_with_access_tier_column(tmp_path) -> None:
    destination = tmp_path / "dataset_registry.csv"
    entries = (
        _entry("cohort-a", ("poor_functional_outcome",)),
        _entry("cohort-b", (), access_tier="public_credentialed"),
    )

    write_dataset_registry(entries, destination)
    loaded_entries = load_dataset_registry(destination)

    assert tuple(loaded.dataset_id for loaded in loaded_entries) == ("cohort-a", "cohort-b")
    assert destination.read_text(encoding="utf-8").splitlines()[0].split(",") == list(REGISTRY_COLUMNS)


def test_registry_rows_distinguish_cross_sectional_narrow_and_prospective_support() -> None:
    representation_only = _entry("representation-only", ())
    concurrent = _entry("concurrent-cohort", ("poor_functional_outcome",))
    prospective = _entry(
        "prospective-cohort",
        ("poor_functional_outcome",),
        outcome_is_prospective=True,
    )
    limited = _entry(
        "limited-cohort",
        ("poor_functional_outcome",),
        benchmark_v0_eligibility="limited",
        representation_comparison_support="limited",
    )

    assert representation_only.claim_level_ceiling == "cross_sectional_representation"
    assert concurrent.outcome_temporal_validity == "concurrent_only"
    assert concurrent.claim_level_contributions == (
        "cross_sectional_representation",
        "narrow_outcome_benchmark",
    )
    assert concurrent.claim_level_ceiling == "narrow_outcome_benchmark"
    assert prospective.outcome_temporal_validity == "prospective"
    assert prospective.claim_level_ceiling == "prospective_outcome_benchmark"
    assert limited.representation_comparison_support == "limited"
    assert limited.claim_level_ceiling == "none"


def test_decision_logic_returns_go_narrow_go_and_no_go_for_strict_open() -> None:
    go = derive_benchmark_decision(
        (
            _entry("cohort-a", ("poor_functional_outcome",)),
            _entry("cohort-b", ("poor_functional_outcome",)),
        )
    )
    narrow_go = derive_benchmark_decision(
        (
            _entry("cohort-a", ("poor_functional_outcome",)),
            _entry("cohort-b", ()),
        )
    )
    no_go = derive_benchmark_decision(
        (
            _entry("cohort-a", (), representation_comparison_support="insufficient"),
            _entry("cohort-b", (), representation_comparison_support="insufficient"),
        )
    )

    assert go.state == "go"
    assert go.claim_level == "full_external_validation"
    assert go.recommended_outcome_families == ("poor_functional_outcome",)
    assert narrow_go.state == "narrow-go"
    assert narrow_go.claim_level == "narrow_outcome_benchmark"
    assert narrow_go.recommended_outcome_families == ("poor_functional_outcome",)
    assert no_go.state == "no-go"
    assert no_go.claim_level == "none"
    assert no_go.recommended_outcome_families == ()
    assert no_go.recommended_next_step == "remain_paused_at_no_go"
    assert "paused at no-go" in no_go.recommended_next_step_explanation


def test_prospective_support_upgrades_claim_level_to_prospective_outcome_benchmark() -> None:
    decision = derive_benchmark_decision(
        (
            _entry(
                "cohort-a",
                ("poor_functional_outcome",),
                outcome_is_prospective=True,
            ),
            _entry(
                "cohort-b",
                ("poor_functional_outcome",),
                outcome_is_prospective=True,
            ),
        )
    )

    assert decision.state == "go"
    assert decision.claim_level == "prospective_outcome_benchmark"
    assert decision.prospective_support_by_outcome_family["poor_functional_outcome"] == (
        "cohort-a",
        "cohort-b",
    )
    assert decision.prospectively_usable_cohorts == ("cohort-a", "cohort-b")


def test_public_credentialed_support_does_not_upgrade_strict_open_layer() -> None:
    decision = derive_benchmark_decision(
        (
            _entry("strict-open-cohort", ("poor_functional_outcome",)),
            _entry(
                "credentialed-cohort",
                ("poor_functional_outcome",),
                access_tier="public_credentialed",
            ),
        )
    )

    assert decision.state == "narrow-go"
    assert decision.public_credentialed.state == "go"
    assert decision.public_credentialed.claim_level == "full_external_validation"
    assert decision.public_credentialed.support_by_outcome_family["poor_functional_outcome"] == (
        "strict-open-cohort",
        "credentialed-cohort",
    )


def test_controlled_support_only_changes_controlled_layer() -> None:
    decision = derive_benchmark_decision(
        (
            _entry("strict-open-cohort", ("poor_functional_outcome",)),
            _entry(
                "controlled-cohort",
                ("poor_functional_outcome",),
                access_tier="controlled",
            ),
        )
    )

    assert decision.state == "narrow-go"
    assert decision.public_credentialed.state == "narrow-go"
    assert decision.controlled.state == "go"
    assert decision.controlled.full_external_validation_cohorts == (
        "strict-open-cohort",
        "controlled-cohort",
    )


def test_cross_sectional_only_expansion_recommends_representation_only_next_step() -> None:
    decision = derive_benchmark_decision(
        (
            _entry("narrow-outcome-cohort", ("poor_functional_outcome",)),
            _entry("representation-expansion", ()),
        )
    )

    assert decision.state == "narrow-go"
    assert decision.recommended_next_step == "continue_cross_sectional_representation_only"
    assert "cross-sectional representation work only" in decision.recommended_next_step_explanation


def test_harmonized_cohort_still_counts_as_eligible_support() -> None:
    decision = derive_benchmark_decision(
        (
            _entry("harmonized-cohort", ("poor_functional_outcome",), local_status="harmonized"),
            _entry("audited-cohort", ("poor_functional_outcome",), local_status="audited"),
        )
    )

    assert decision.state == "go"
    assert decision.full_external_validation_cohorts == (
        "harmonized-cohort",
        "audited-cohort",
    )


def test_cross_sectional_representation_claim_exists_without_outcome_benchmark() -> None:
    decision = derive_benchmark_decision(
        (
            _entry("representation-only", ()),
            _entry("metadata-only", (), representation_comparison_support="insufficient"),
        )
    )

    assert decision.state == "no-go"
    assert decision.claim_level == "cross_sectional_representation"


def test_benchmark_v0_eligibility_requires_audited_outcome_bearing_rows() -> None:
    with pytest.raises(
        ValueError,
        match="benchmark_v0_eligibility can only be eligible or limited for audited or harmonized cohorts",
    ):
        _entry(
            "candidate-cohort",
            ("poor_functional_outcome",),
            local_status="candidate",
            benchmark_v0_eligibility="eligible",
        )


def test_benchmark_v0_eligibility_requires_outcomes_and_non_insufficient_rep_support() -> None:
    with pytest.raises(
        ValueError,
        match="benchmark_v0_eligibility can only be eligible or limited when benchmarkable outcomes exist",
    ):
        _entry(
            "metadata-only",
            (),
            benchmark_v0_eligibility="limited",
            representation_comparison_support="limited",
            concurrent_endpoint_only=False,
        )

    with pytest.raises(
        ValueError,
        match="benchmark_v0_eligibility can only be eligible or limited when representation comparison support is strong or limited",
    ):
        _entry(
            "insufficient-support",
            ("poor_functional_outcome",),
            benchmark_v0_eligibility="limited",
            representation_comparison_support="insufficient",
        )


def test_eligible_rows_require_strong_representation_support() -> None:
    with pytest.raises(
        ValueError,
        match="benchmark_v0_eligibility=eligible requires strong representation comparison support",
    ):
        _entry(
            "weak-label-cohort",
            ("poor_functional_outcome",),
            benchmark_v0_eligibility="eligible",
            representation_comparison_support="limited",
        )


def test_temporal_flags_must_match_temporal_validity() -> None:
    with pytest.raises(
        ValueError,
        match="outcome_is_prospective and concurrent_endpoint_only cannot both be true",
    ):
        _entry(
            "broken-cohort",
            ("poor_functional_outcome",),
            outcome_is_prospective=True,
            concurrent_endpoint_only=True,
            outcome_temporal_validity="prospective",
        )


def test_prospective_temporal_validity_requires_prospective_boolean() -> None:
    with pytest.raises(
        ValueError,
        match="outcome_temporal_validity=prospective requires outcome_is_prospective=true",
    ):
        _entry(
            "misflagged-prospective-row",
            ("poor_functional_outcome",),
            outcome_is_prospective=False,
            concurrent_endpoint_only=False,
            outcome_temporal_validity="prospective",
        )


def test_legacy_public_access_level_column_maps_to_strict_open() -> None:
    row = {
        "dataset_id": "legacy-row",
        "dataset_label": "legacy-row label",
        "access_level": "public",
        "population_scope": "psychosis cohort",
        "diagnosis_coverage": "psychosis vs control",
        "symptom_scales": "PANSS",
        "cognition_scales": "MATRICS",
        "functioning_scales": "GAF/GAS",
        "treatment_variables": "medication",
        "longitudinal_coverage": "No repeated follow-up",
        "outcome_availability": "Poor functional outcome only",
        "modality_availability": "MRI",
        "site_structure": "single site",
        "sample_size_note": "n=10",
        "known_limitations": "fixture only",
        "local_status": "audited",
        "benchmark_v0_eligibility": "eligible",
        "representation_comparison_support": "strong",
        "predictor_timepoint": "baseline",
        "outcome_timepoint": "same_visit",
        "outcome_window": "same_visit",
        "outcome_is_prospective": "false",
        "concurrent_endpoint_only": "true",
        "outcome_temporal_validity": "concurrent_only",
        "benchmarkable_outcome_families": "poor_functional_outcome",
    }

    entry = DatasetRegistryEntry.from_csv_row(row)

    assert entry.access_tier == "strict_open"
    assert entry.claim_level_ceiling == "narrow_outcome_benchmark"


@pytest.mark.parametrize(
    ("legacy_access_level", "expected_access_tier"),
    (
        ("public", "strict_open"),
        ("gated", "public_credentialed"),
        ("controlled", "controlled"),
    ),
)
def test_legacy_access_levels_map_to_new_access_tiers(
    legacy_access_level: str,
    expected_access_tier: str,
) -> None:
    row = {
        "dataset_id": "legacy-row",
        "dataset_label": "legacy-row label",
        "access_level": legacy_access_level,
        "population_scope": "psychosis cohort",
        "diagnosis_coverage": "psychosis vs control",
        "symptom_scales": "PANSS",
        "cognition_scales": "MATRICS",
        "functioning_scales": "",
        "treatment_variables": "medication",
        "longitudinal_coverage": "No repeated follow-up",
        "outcome_availability": "No benchmarkable outcome",
        "modality_availability": "MRI",
        "site_structure": "single site",
        "sample_size_note": "n=10",
        "known_limitations": "fixture only",
        "local_status": "audited",
        "benchmark_v0_eligibility": "ineligible",
        "representation_comparison_support": "strong",
        "predictor_timepoint": "unmapped",
        "outcome_timepoint": "unmapped",
        "outcome_window": "unmapped",
        "outcome_is_prospective": "false",
        "concurrent_endpoint_only": "false",
        "outcome_temporal_validity": "none",
        "benchmarkable_outcome_families": "",
    }

    entry = DatasetRegistryEntry.from_csv_row(row)

    assert entry.access_tier == expected_access_tier

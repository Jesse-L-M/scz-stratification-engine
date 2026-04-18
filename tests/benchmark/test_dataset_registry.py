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
    access_level: str = "public",
    local_status: str = "audited",
    benchmark_v0_eligibility: str | None = None,
    representation_comparison_support: str | None = None,
    outcome_is_prospective: bool = False,
    concurrent_endpoint_only: bool | None = None,
    outcome_temporal_validity: str | None = None,
) -> DatasetRegistryEntry:
    if benchmark_v0_eligibility is None:
        benchmark_v0_eligibility = (
            "eligible"
            if access_level == "public" and local_status in {"audited", "harmonized"} and outcomes
            else "ineligible"
        )
    if representation_comparison_support is None:
        if benchmark_v0_eligibility == "eligible":
            representation_comparison_support = "strong"
        elif benchmark_v0_eligibility == "limited":
            representation_comparison_support = "limited"
        elif outcomes:
            representation_comparison_support = "strong"
        else:
            representation_comparison_support = "insufficient"
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
        access_level=access_level,
        population_scope="psychosis cohort",
        diagnosis_coverage="psychosis vs control",
        symptom_scales=("PANSS",),
        cognition_scales=("MATRICS",),
        functioning_scales=("GAF/GAS",),
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


def test_registry_rows_round_trip_with_required_columns(tmp_path) -> None:
    destination = tmp_path / "dataset_registry.csv"
    entries = (_entry("cohort-a", ("poor_functional_outcome",)), _entry("cohort-b", ()))

    write_dataset_registry(entries, destination)
    loaded_entries = load_dataset_registry(destination)

    assert tuple(loaded.dataset_id for loaded in loaded_entries) == ("cohort-a", "cohort-b")
    assert destination.read_text(encoding="utf-8").splitlines()[0].split(",") == list(REGISTRY_COLUMNS)


def test_registry_rows_distinguish_concurrent_prospective_and_limited_support() -> None:
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


def test_decision_logic_returns_go_narrow_go_and_no_go_claim_levels() -> None:
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
    no_go = derive_benchmark_decision((_entry("cohort-a", ()), _entry("cohort-b", ())))

    assert go.state == "go"
    assert go.claim_level == "full_external_validation"
    assert go.recommended_outcome_families == ("poor_functional_outcome",)
    assert narrow_go.state == "narrow-go"
    assert narrow_go.claim_level == "narrow_outcome_benchmark"
    assert narrow_go.recommended_outcome_families == ("poor_functional_outcome",)
    assert no_go.state == "no-go"
    assert no_go.claim_level == "none"
    assert no_go.recommended_outcome_families == ()


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


def test_controlled_access_cohort_does_not_upgrade_public_narrow_go_to_go() -> None:
    decision = derive_benchmark_decision(
        (
            _entry("public-cohort", ("poor_functional_outcome",), access_level="public"),
            _entry(
                "controlled-cohort",
                ("poor_functional_outcome",),
                access_level="controlled",
                benchmark_v0_eligibility="ineligible",
            ),
        )
    )

    assert decision.state == "narrow-go"
    assert decision.support_by_outcome_family["poor_functional_outcome"] == ("public-cohort",)


def test_harmonized_cohort_still_counts_as_benchmark_eligible_support() -> None:
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


def test_no_go_explanation_matches_public_benchmark_eligible_filter() -> None:
    decision = derive_benchmark_decision(
        (
            _entry(
                "controlled-cohort",
                ("poor_functional_outcome",),
                access_level="controlled",
                benchmark_v0_eligibility="ineligible",
            ),
            _entry(
                "candidate-public-cohort",
                ("poor_functional_outcome",),
                local_status="candidate",
                benchmark_v0_eligibility="ineligible",
            ),
        )
    )

    assert decision.state == "no-go"
    assert decision.claim_level == "none"
    assert "benchmarkable real outcome family" in decision.explanation


def test_limited_public_cohort_does_not_upgrade_narrow_go_to_go() -> None:
    decision = derive_benchmark_decision(
        (
            _entry("strong-public-cohort", ("poor_functional_outcome",)),
            _entry(
                "weak-label-cohort",
                ("poor_functional_outcome",),
                benchmark_v0_eligibility="limited",
                representation_comparison_support="limited",
            ),
        )
    )

    assert decision.state == "narrow-go"
    assert decision.support_by_outcome_family["poor_functional_outcome"] == (
        "strong-public-cohort",
    )


def test_cross_sectional_representation_claim_exists_without_outcome_benchmark() -> None:
    decision = derive_benchmark_decision(
        (
            _entry(
                "representation-only",
                (),
                representation_comparison_support="strong",
            ),
            _entry("metadata-only", ()),
        )
    )

    assert decision.state == "no-go"
    assert decision.claim_level == "cross_sectional_representation"


def test_benchmark_v0_eligibility_requires_public_audited_outcome_bearing_rows() -> None:
    with pytest.raises(
        ValueError,
        match="benchmark_v0_eligibility can only be eligible or limited for public cohorts",
    ):
        _entry(
            "controlled-cohort",
            ("poor_functional_outcome",),
            access_level="controlled",
            benchmark_v0_eligibility="eligible",
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


def test_ineligible_prospective_cohort_does_not_count_as_public_prospective_support() -> None:
    decision = derive_benchmark_decision(
        (
            _entry("public-concurrent", ("poor_functional_outcome",)),
            _entry(
                "controlled-prospective",
                ("poor_functional_outcome",),
                access_level="controlled",
                benchmark_v0_eligibility="ineligible",
                outcome_is_prospective=True,
                concurrent_endpoint_only=False,
                outcome_temporal_validity="prospective",
            ),
        )
    )

    assert decision.prospectively_usable_cohorts == ()
    assert (
        "No audited cohort currently exposes a prospectively usable public outcome window."
        in decision.limiting_factors
    )
    assert "Current public endpoint support is concurrent-only" in decision.explanation

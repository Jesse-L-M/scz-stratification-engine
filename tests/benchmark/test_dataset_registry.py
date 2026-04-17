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
) -> DatasetRegistryEntry:
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
        longitudinal_coverage="No repeated follow-up",
        outcome_availability="Poor functional outcome only",
        modality_availability=("MRI",),
        site_structure="single site",
        sample_size_note="n=10",
        known_limitations="fixture only",
        local_status=local_status,
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


def test_decision_logic_returns_go_narrow_go_and_no_go() -> None:
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
    assert go.recommended_outcome_families == ("poor_functional_outcome",)
    assert narrow_go.state == "narrow-go"
    assert narrow_go.recommended_outcome_families == ("poor_functional_outcome",)
    assert no_go.state == "no-go"
    assert no_go.recommended_outcome_families == ()


def test_controlled_access_cohort_does_not_upgrade_public_narrow_go_to_go() -> None:
    decision = derive_benchmark_decision(
        (
            _entry("public-cohort", ("poor_functional_outcome",), access_level="public"),
            _entry("controlled-cohort", ("poor_functional_outcome",), access_level="controlled"),
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
    assert decision.support_by_outcome_family["poor_functional_outcome"] == (
        "harmonized-cohort",
        "audited-cohort",
    )


def test_no_go_explanation_matches_public_benchmark_eligible_filter() -> None:
    decision = derive_benchmark_decision(
        (
            _entry("controlled-cohort", ("poor_functional_outcome",), access_level="controlled"),
            _entry("candidate-public-cohort", ("poor_functional_outcome",), local_status="candidate"),
        )
    )

    assert decision.state == "no-go"
    assert "public benchmark-eligible cohorts" in decision.explanation
    assert "controlled-access cohorts" not in decision.explanation

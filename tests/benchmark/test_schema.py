from scz_audit_engine.benchmark import CANONICAL_TABLE_NAMES, benchmark_schema


def test_benchmark_schema_defines_all_canonical_tables() -> None:
    schema = benchmark_schema()

    assert schema.version == "benchmark_v0"
    assert schema.table_names == CANONICAL_TABLE_NAMES


def test_benchmark_schema_includes_required_columns_for_each_table() -> None:
    schema = benchmark_schema()

    expected_required_columns = {
        "subjects": {
            "cohort_id",
            "subject_id",
            "source_subject_id",
            "population_scope",
            "site_id",
            "sex",
            "baseline_age",
            "enrollment_group",
            "has_longitudinal_followup",
            "representation_comparison_support",
        },
        "visits": {
            "cohort_id",
            "subject_id",
            "visit_id",
            "source_visit_id",
            "visit_order",
            "visit_timepoint_label",
            "visit_age",
            "days_from_baseline",
            "is_baseline",
        },
        "diagnoses": {
            "cohort_id",
            "subject_id",
            "visit_id",
            "diagnosis_system",
            "diagnosis_label",
            "diagnosis_group",
            "diagnosis_granularity",
            "is_primary_diagnosis",
            "mapping_caveat",
        },
        "symptom_scores": {
            "cohort_id",
            "subject_id",
            "visit_id",
            "instrument",
            "domain",
            "measure",
            "score",
            "score_direction",
            "is_harmonized_domain_score",
            "mapping_caveat",
        },
        "cognition_scores": {
            "cohort_id",
            "subject_id",
            "visit_id",
            "instrument",
            "domain",
            "measure",
            "score",
            "score_direction",
            "mapping_caveat",
        },
        "functioning_scores": {
            "cohort_id",
            "subject_id",
            "visit_id",
            "instrument",
            "domain",
            "measure",
            "score",
            "score_direction",
            "mapping_caveat",
        },
        "treatment_exposures": {
            "cohort_id",
            "subject_id",
            "visit_id",
            "treatment_type",
            "treatment_name",
            "exposure_value",
            "exposure_unit",
            "exposure_window",
            "is_current_exposure",
            "mapping_caveat",
        },
        "outcomes": {
            "cohort_id",
            "subject_id",
            "visit_id",
            "outcome_family",
            "outcome_name",
            "outcome_value",
            "outcome_type",
            "predictor_timepoint",
            "outcome_timepoint",
            "outcome_window",
            "outcome_is_prospective",
            "concurrent_endpoint_only",
            "outcome_definition_version",
            "mapping_caveat",
        },
        "modality_features": {
            "cohort_id",
            "subject_id",
            "visit_id",
            "modality_type",
            "feature_name",
            "feature_value",
            "feature_unit",
            "feature_source",
            "mapping_caveat",
        },
        "split_assignments": {
            "cohort_id",
            "subject_id",
            "split_name",
            "split_level",
            "split_protocol_version",
            "leakage_group_id",
        },
    }

    for table_name, expected_columns in expected_required_columns.items():
        table = schema.table(table_name)
        assert expected_columns.issubset(set(table.required_columns))


def test_outcomes_table_keeps_temporal_validity_fields_explicit() -> None:
    outcomes = benchmark_schema().table("outcomes")

    assert {
        "predictor_timepoint",
        "outcome_timepoint",
        "outcome_window",
        "outcome_is_prospective",
        "concurrent_endpoint_only",
    }.issubset(set(outcomes.required_columns))


def test_schema_keeps_diagnosis_granularity_and_mapping_caveats_first_class() -> None:
    schema = benchmark_schema()
    diagnoses = schema.table("diagnoses")

    assert "diagnosis_granularity" in diagnoses.required_columns
    assert "mapping_caveat" in diagnoses.required_columns

    tables_with_mapping_caveats = {
        table.name for table in schema.tables if "mapping_caveat" in table.required_columns
    }
    assert tables_with_mapping_caveats == {
        "diagnoses",
        "symptom_scores",
        "cognition_scores",
        "functioning_scores",
        "treatment_exposures",
        "outcomes",
        "modality_features",
    }


def test_schema_contract_does_not_reintroduce_strict_open_proxy_terms() -> None:
    serialized_contract = str(benchmark_schema().to_artifact_dict()).lower()

    for forbidden_term in (
        "strict-open",
        "stable_cognitive_burden_proxy",
        "state_noise_proxy_input",
        "visit_ambiguity_proxy_input",
    ):
        assert forbidden_term not in serialized_contract

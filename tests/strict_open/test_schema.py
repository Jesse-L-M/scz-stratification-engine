from scz_audit_engine.strict_open.schema import STRICT_OPEN_TABLE_SCHEMAS


EXPECTED_COLUMNS = {
    "subjects": {
        "subject_id",
        "source_dataset",
        "source_subject_id",
        "diagnosis",
        "site_id",
        "sex",
        "age_years",
    },
    "visits": {
        "visit_id",
        "subject_id",
        "visit_label",
        "visit_index",
        "days_from_baseline",
    },
    "cognition_scores": {
        "subject_id",
        "visit_id",
        "instrument",
        "measure",
        "score",
    },
    "symptom_behavior_scores": {
        "subject_id",
        "visit_id",
        "instrument",
        "measure",
        "score",
    },
    "mri_features": {
        "subject_id",
        "visit_id",
        "modality",
        "feature_name",
        "feature_value",
        "qc_status",
    },
    "derived_targets": {
        "subject_id",
        "visit_id",
        "target_name",
        "target_label",
        "target_value",
    },
    "biology_priors": {
        "feature_name",
        "target_name",
        "evidence_source",
        "evidence_score",
    },
}


def test_canonical_table_names_exist() -> None:
    assert set(STRICT_OPEN_TABLE_SCHEMAS) == set(EXPECTED_COLUMNS)


def test_expected_columns_exist_for_each_table_contract() -> None:
    for table_name, expected_columns in EXPECTED_COLUMNS.items():
        schema = STRICT_OPEN_TABLE_SCHEMAS[table_name]
        assert schema.name == table_name
        assert expected_columns.issubset(set(schema.columns))

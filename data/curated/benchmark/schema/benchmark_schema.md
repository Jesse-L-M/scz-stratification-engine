# Benchmark Schema

- Schema version: `benchmark_v0`
- Design posture: preserve the current `narrow-go` state without pretending every cohort is fully benchmark-ready.
- Claim-level honesty: concurrent-only endpoints, limited cohorts, diagnosis granularity, and mapping caveats stay first-class in the contract.

## Canonical Tables

| Table | Row grain | Purpose |
| --- | --- | --- |
| `subjects` | One row per subject enrolled in one cohort. | Subject-level cohort membership and baseline metadata needed to state who is in scope for the benchmark. |
| `visits` | One row per subject visit within one cohort. | Visit-level timing metadata for baseline and follow-up rows. |
| `diagnoses` | One row per diagnosis assertion for one subject at one visit. | Diagnosis labels and grouping fields, including granularity and mapping caveats. |
| `symptom_scores` | One row per symptom measure for one subject at one visit. | Symptom severity measurements and any harmonized symptom-domain scores. |
| `cognition_scores` | One row per cognition measure for one subject at one visit. | Cognitive task or scale measurements aligned to explicit domains. |
| `functioning_scores` | One row per functioning measure for one subject at one visit. | Functioning and recovery-relevant measurements used for outcome definition. |
| `treatment_exposures` | One row per treatment exposure record for one subject at one visit. | Treatment exposure rows that keep medication or intervention context explicit. |
| `outcomes` | One row per benchmark outcome definition for one subject at one visit. | Outcome rows that keep predictor timing, outcome timing, and prospective versus concurrent validity explicit. |
| `modality_features` | One row per modality feature for one subject at one visit. | Feature rows emitted from one modality without assuming cross-cohort harmonization yet. |
| `split_assignments` | One row per subject assignment to one benchmark split protocol. | Future split-assignment rows that can carry leakage controls without generating splits yet. |

## `subjects`

- Purpose: Subject-level cohort membership and baseline metadata needed to state who is in scope for the benchmark.
- Row grain: One row per subject enrolled in one cohort.
- Required columns: `cohort_id`, `subject_id`, `source_subject_id`, `population_scope`, `site_id`, `sex`, `baseline_age`, `enrollment_group`, `has_longitudinal_followup`, `representation_comparison_support`
- Optional columns: `ancestry_group`, `race_ethnicity`, `education_years`, `mapping_note`

## `visits`

- Purpose: Visit-level timing metadata for baseline and follow-up rows.
- Row grain: One row per subject visit within one cohort.
- Required columns: `cohort_id`, `subject_id`, `visit_id`, `source_visit_id`, `visit_order`, `visit_timepoint_label`, `visit_age`, `days_from_baseline`, `is_baseline`
- Optional columns: `visit_window_label`, `visit_status`, `visit_note`

## `diagnoses`

- Purpose: Diagnosis labels and grouping fields, including granularity and mapping caveats.
- Row grain: One row per diagnosis assertion for one subject at one visit.
- Required columns: `cohort_id`, `subject_id`, `visit_id`, `diagnosis_system`, `diagnosis_label`, `diagnosis_group`, `diagnosis_granularity`, `is_primary_diagnosis`, `mapping_caveat`
- Optional columns: `diagnosis_code`, `source_diagnosis_label`, `diagnosis_note`

## `symptom_scores`

- Purpose: Symptom severity measurements and any harmonized symptom-domain scores.
- Row grain: One row per symptom measure for one subject at one visit.
- Required columns: `cohort_id`, `subject_id`, `visit_id`, `instrument`, `domain`, `measure`, `score`, `score_direction`, `is_harmonized_domain_score`, `mapping_caveat`
- Optional columns: `score_unit`, `instrument_version`, `source_score_label`

## `cognition_scores`

- Purpose: Cognitive task or scale measurements aligned to explicit domains.
- Row grain: One row per cognition measure for one subject at one visit.
- Required columns: `cohort_id`, `subject_id`, `visit_id`, `instrument`, `domain`, `measure`, `score`, `score_direction`, `mapping_caveat`
- Optional columns: `score_unit`, `task_name`, `source_score_label`

## `functioning_scores`

- Purpose: Functioning and recovery-relevant measurements used for outcome definition.
- Row grain: One row per functioning measure for one subject at one visit.
- Required columns: `cohort_id`, `subject_id`, `visit_id`, `instrument`, `domain`, `measure`, `score`, `score_direction`, `mapping_caveat`
- Optional columns: `score_unit`, `source_score_label`, `recovery_domain`

## `treatment_exposures`

- Purpose: Treatment exposure rows that keep medication or intervention context explicit.
- Row grain: One row per treatment exposure record for one subject at one visit.
- Required columns: `cohort_id`, `subject_id`, `visit_id`, `treatment_type`, `treatment_name`, `exposure_value`, `exposure_unit`, `exposure_window`, `is_current_exposure`, `mapping_caveat`
- Optional columns: `source_treatment_label`, `exposure_route`, `adherence_note`

## `outcomes`

- Purpose: Outcome rows that keep predictor timing, outcome timing, and prospective versus concurrent validity explicit.
- Row grain: One row per benchmark outcome definition for one subject at one visit.
- Required columns: `cohort_id`, `subject_id`, `visit_id`, `outcome_family`, `outcome_name`, `outcome_value`, `outcome_type`, `predictor_timepoint`, `outcome_timepoint`, `outcome_window`, `outcome_is_prospective`, `concurrent_endpoint_only`, `outcome_definition_version`, `mapping_caveat`
- Optional columns: `outcome_unit`, `outcome_direction`, `outcome_threshold_label`

## `modality_features`

- Purpose: Feature rows emitted from one modality without assuming cross-cohort harmonization yet.
- Row grain: One row per modality feature for one subject at one visit.
- Required columns: `cohort_id`, `subject_id`, `visit_id`, `modality_type`, `feature_name`, `feature_value`, `feature_unit`, `feature_source`, `mapping_caveat`
- Optional columns: `feature_group`, `preprocessing_version`, `feature_quality_flag`

## `split_assignments`

- Purpose: Future split-assignment rows that can carry leakage controls without generating splits yet.
- Row grain: One row per subject assignment to one benchmark split protocol.
- Required columns: `cohort_id`, `subject_id`, `split_name`, `split_level`, `split_protocol_version`, `leakage_group_id`
- Optional columns: `fold_index`, `split_label`, `assignment_note`

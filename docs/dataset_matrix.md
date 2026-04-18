# Dataset Matrix

This document defines the dataset registry contract for the current benchmark
feasibility gate.

The goal in this phase is not to model. The goal is to say, in a machine-readable
way, what kind of benchmark the available cohorts can honestly support.

## Required Registry Columns

Maintain a checked-in dataset registry with at least these fields:

| Field | Meaning |
| --- | --- |
| `dataset_id` | Stable local identifier |
| `dataset_label` | Human-readable cohort name |
| `access_level` | `public`, `controlled`, or `gated` |
| `population_scope` | e.g. first-episode psychosis, schizophrenia, transdiagnostic psychiatry |
| `diagnosis_coverage` | Which diagnoses or groups are present publicly |
| `symptom_scales` | PANSS, BPRS, SAPS/SANS, BNSS, etc. |
| `cognition_scales` | MCCB or other cognition measures |
| `functioning_scales` | GAF, SOFAS, PSP, LIFE-RIFT, SFS, etc. |
| `treatment_variables` | medication, exposure, adherence, etc. |
| `longitudinal_coverage` | whether repeated visits are documented publicly |
| `outcome_availability` | which benchmark outcomes can be defined honestly |
| `modality_availability` | MRI, genetics, EEG, speech, etc. |
| `site_structure` | single-site vs multi-site |
| `sample_size_note` | rough usable size |
| `known_limitations` | what weakens claim strength |
| `local_status` | `candidate`, `audited`, `harmonized`, or `deferred` |
| `benchmark_v0_eligibility` | `eligible`, `limited`, or `ineligible` for the current narrow public benchmark |
| `representation_comparison_support` | `strong`, `limited`, or `insufficient` support for psychosis-relevant representation comparison |
| `predictor_timepoint` | predictor timing, e.g. `scan/baseline` |
| `outcome_timepoint` | endpoint timing, e.g. `same_visit` or `12_month_follow_up` |
| `outcome_window` | explicit outcome window, e.g. `same_visit` or `12_month` |
| `outcome_is_prospective` | whether the endpoint is future-looking |
| `concurrent_endpoint_only` | whether only same-visit endpoint support exists |
| `outcome_temporal_validity` | `none`, `concurrent_only`, or `prospective` |

The checked-in CSV should also emit derived row-level claim fields:

- `claim_level_ceiling`: the highest claim level that cohort could contribute
  toward if the surrounding support exists
- `claim_level_contributions`

## Representation-Support Rule

Use `representation_comparison_support` to distinguish:

- `strong`: public data is psychosis-relevant enough to support a real
  representation comparison
- `limited`: public data is psychosis-relevant but label granularity is too weak
  to count as main claim support
- `insufficient`: useful metadata or context only

`benchmark_v0_eligibility=eligible` should map to `representation_comparison_support=strong`.

## Temporal Outcome Rule

Every audited cohort with a benchmarkable outcome must state:

1. predictor timepoint
2. outcome timepoint
3. outcome window
4. whether the outcome is prospective
5. whether the cohort is concurrent-only
6. the resulting `outcome_temporal_validity`

Same-visit functioning endpoints are allowed in the registry, but they should
stay labeled as `concurrent_only`. They must not be presented as prospective
outcomes later.

## Claim-Level Rule

The registry should support this ordered claim ladder:

1. `none`
2. `cross_sectional_representation`
3. `narrow_outcome_benchmark`
4. `full_external_validation`
5. `prospective_outcome_benchmark`

Row-level metadata should make it possible to derive which levels a cohort can
contribute to without prose-only inference.

## Benchmark Decision Rule

Before schema or modeling work starts, the audited public registry should support
one of these benchmark decisions:

- `go`: at least two `benchmark_v0_eligibility=eligible` cohorts support the same
  outcome family
- `narrow-go`: exactly one cohort currently supports the narrowed benchmark claim
- `no-go`: no honest public benchmark outcome family exists yet

The decision state and the claim level are related but not identical. The repo
can remain `narrow-go` while the supported claim level is only
`narrow_outcome_benchmark`.

## Current Public Status

The current public audit should be read conservatively:

- `fep-ds003944` provides narrow concurrent support for
  `poor_functional_outcome`
- `tcp-ds005237` remains `limited` because public labels are too broad for the
  current psychosis representation claim
- current repo status remains `narrow-go`
- current supported claim level remains `narrow_outcome_benchmark`

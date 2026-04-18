# Benchmark Claim Levels

This repo uses an explicit ordered claim ladder.

## Ladder

| Level | Meaning | Minimum support |
| --- | --- | --- |
| `none` | no honest benchmark claim yet | no audited public support |
| `cross_sectional_representation` | public data can support a cross-sectional representation comparison, but not a real outcome benchmark | strong public representation support without an eligible outcome benchmark |
| `narrow_outcome_benchmark` | one cohort supports a real but still narrow outcome benchmark | one `benchmark_v0_eligibility=eligible` cohort with a benchmarkable outcome family |
| `full_external_validation` | the same outcome family is supported across at least two eligible cohorts | two eligible cohorts for the same family |
| `prospective_outcome_benchmark` | the benchmark is supported by future-looking outcomes rather than same-visit endpoints | two eligible cohorts with prospective outcome support for the same family |

## Row-Level Contribution Rules

The registry should expose enough metadata to derive cohort contributions:

- `representation_comparison_support=strong` can contribute to
  `cross_sectional_representation`
- `benchmark_v0_eligibility=eligible` can contribute to
  `narrow_outcome_benchmark`
- the same eligible cohort can contribute toward `full_external_validation` when
  paired with a second eligible cohort for the same family
- `outcome_is_prospective=true` marks whether that cohort can contribute to a
  later `prospective_outcome_benchmark`

## Current Repo Reading

Current public support remains:

- benchmark decision: `narrow-go`
- claim level: `narrow_outcome_benchmark`
- prospective claim support: none
- full external-validation support: none

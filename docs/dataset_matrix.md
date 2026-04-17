# Dataset Matrix

This document defines the minimum inventory the repo should maintain before
building benchmark models.

The project should not proceed by assumption. It should proceed from a concrete
cohort matrix that says what is actually available.

## Required Registry Columns

Maintain a checked-in dataset registry with at least these fields:

| Field | Meaning |
| --- | --- |
| `dataset_id` | Stable local identifier |
| `dataset_label` | Human-readable cohort name |
| `access_level` | `public`, `controlled`, or `gated` |
| `population_scope` | e.g. schizophrenia, first-episode psychosis, transdiagnostic psychosis |
| `diagnosis_coverage` | Which diagnoses or groups are present |
| `symptom_scales` | PANSS, BPRS, SAPS/SANS, BNSS, etc. |
| `cognition_scales` | MCCB or other cognition measures |
| `functioning_scales` | GAF, SOFAS, PSP, occupational / education outcomes |
| `treatment_variables` | medication, exposure, clozapine, adherence, etc. |
| `longitudinal_coverage` | whether repeated visits exist |
| `outcome_availability` | which benchmark outcomes can be defined |
| `modality_availability` | MRI, genetics, EEG, speech, etc. |
| `site_structure` | single-site vs multi-site |
| `sample_size_note` | rough usable size |
| `known_limitations` | what would weaken external validation |
| `local_status` | `candidate`, `audited`, `harmonized`, or `deferred` |

## Minimum Criteria For Inclusion In Benchmark V0

A cohort is eligible for the first benchmark only if it has:

1. subject-level identifiers
2. diagnosis or group labels
3. symptom measurements that can be mapped into a common contract
4. at least one real outcome of interest

Prefer cohorts that also have:

- repeated visits
- functioning measures
- cognition measures
- enough site or cohort heterogeneity to support transfer testing

## Initial Candidate Search Priorities

Prioritize:

1. public first-episode psychosis or early psychosis cohorts
2. public transdiagnostic psychosis cohorts
3. public schizophrenia cohorts only if they still support real outcomes

Do not prioritize a cohort just because it has imaging.

## Decision Rule

Before major modeling work starts, the registry should support one of these
statements:

- `go`: at least two cohorts support a real cross-cohort benchmark
- `narrow-go`: only one strong cohort exists, so the scope must narrow and the
  external-validation claim must be reduced
- `no-go`: available public data cannot support the benchmark honestly

## Immediate Deliverable

Create and maintain:

- `data/curated/benchmark/dataset_registry.csv`

That file should be the operational version of this document.

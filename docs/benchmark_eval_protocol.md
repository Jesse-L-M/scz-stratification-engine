# Benchmark Evaluation Protocol

This document defines the minimum evaluation rules for the new mainline
benchmark.

The repo should not claim to have answered the representation question unless it
meets these rules.

## Main Question

Which representation of psychosis heterogeneity reproduces across independent
datasets and improves prediction of intervention-relevant outcomes:
diagnosis, dimensions, trajectories, or clusters?

## Evaluation Priorities

The benchmark should evaluate:

1. reproducibility of representation structure
2. outcome-prediction value
3. transport across cohorts
4. calibration and coverage
5. failure modes

## Required Representation Families

The first benchmark should include:

- diagnosis-only
- symptom-dimension
- simple baseline clinical
- trajectory-aware where repeated measures exist

Clusters are optional in V0 and should be included only if they can be
implemented honestly and compared fairly.

## Required Baselines

At least one intentionally simple baseline must be present:

- diagnosis-only baseline
- low-tech clinical baseline from symptoms and available non-imaging features

More complex representations must beat these, not just each other.

## Validation Rules

### Split Rules

- split at the subject level
- prevent leakage across repeated visits
- freeze the split contract before model comparison

### Cross-Cohort Rules

- prefer train-on-one / test-on-another evaluation whenever feasible
- if only one cohort is usable, reduce the claim accordingly
- do not present within-cohort validation as if it were external validation

### Site Rules

- report site composition where applicable
- state when site holdout is impossible or statistically weak
- treat site confounding as a first-class failure mode

## Outcome Rules

- every result table must name the exact primary outcome
- outcome mapping differences across cohorts must be reported
- no synthetic target may serve as the primary benchmark endpoint

## Reporting Rules

Every benchmark report should answer:

1. which representation families were evaluated
2. which outcomes were used
3. which cohorts were used for train, validation, and test
4. what generalized and what failed
5. whether any added modality improved on low-tech clinical baselines

## Explicit Non-Claims

The benchmark does not by itself justify:

- discovery of true disease subtypes
- biomarker clinical utility
- target prioritization
- mechanism certainty

It only justifies claims about comparative benchmark performance under the
available data and evaluation rules.

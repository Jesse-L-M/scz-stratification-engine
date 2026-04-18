# Benchmark Evaluation Protocol

This document defines the current feasibility protocol for the benchmark line.

The active question in this PR is not which model wins. The active question is
what claim the available public cohorts can honestly support.

## Command In Scope

Use:

- `scz-audit benchmark audit-datasets`

This protocol remains the feasibility input to the separate schema phase. The
schema PR adds `scz-audit benchmark define-schema`, but no other benchmark
commands should move forward here.

## Required Audit Outputs

The dataset audit should emit:

1. current benchmark decision
2. current supported claim level
3. cohort-level temporal outcome metadata
4. outcome-family support grouped by:
   - narrow benchmark support
   - full external-validation support
   - prospective support
5. concurrent-only vs prospectively usable cohort lists
6. limiting factors that keep the repo conservative

## Claim-Boundary Rules

- same-visit endpoints must be labeled `concurrent_only`
- concurrent support must not be described as a prospective outcome benchmark
- one eligible cohort can justify only `narrow_outcome_benchmark`
- two eligible cohorts supporting the same family are required for
  `full_external_validation`
- prospective outcome claims require prospective public endpoints, not just
  repeated visits

## Current Conservative Reading

The current public outcome picture should remain:

- benchmark decision: `narrow-go`
- claim level: `narrow_outcome_benchmark`
- current outcome family support: `poor_functional_outcome`
- temporal validity: concurrent-only

The repo should continue to say no full external-validation claim yet.

## Deferred Until Later PRs

This protocol intentionally does not pull forward:

- benchmark schema work
- harmonization
- split generation
- representation builders
- model comparison
- biomarker-heavy benchmarking

Those belong only after this feasibility gate is settled.

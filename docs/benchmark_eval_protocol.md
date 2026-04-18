# Benchmark Evaluation Protocol

This document defines the current feasibility and harmonization protocol for the
benchmark line.

The active question is still not which model wins. The active question is what
claim the available public cohorts can honestly support, and how those cohorts
must be harmonized and split without over-claiming progress.

## Command In Scope

Use:

- `scz-audit benchmark audit-datasets`
- `scz-audit benchmark define-schema`
- `scz-audit benchmark harmonize`

`audit-datasets` remains the feasibility gate. `define-schema` freezes the
canonical table contract. `harmonize` now operationalizes those contracts by
writing canonical CSVs plus a deterministic split manifest.

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

The harmonization phase should emit:

1. canonical benchmark CSV tables in schema order
2. honest zero-row tables when a cohort has no supported rows
3. concurrent-only outcome rows where public support is concurrent-only
4. deterministic within-cohort subject-level split assignments
5. harmonization and split manifests with cohort caveats and coverage counts

## Claim-Boundary Rules

- same-visit endpoints must be labeled `concurrent_only`
- concurrent support must not be described as a prospective outcome benchmark
- one eligible cohort can justify only `narrow_outcome_benchmark`
- two eligible cohorts supporting the same family are required for
  `full_external_validation`
- prospective outcome claims require prospective public endpoints, not just
  repeated visits
- frozen within-cohort splits do not themselves create a full external-
  validation claim
- harmonization must preserve weak diagnosis granularity rather than smoothing
  it away

## Current Conservative Reading

The current public outcome picture should remain:

- benchmark decision: `narrow-go`
- claim level: `narrow_outcome_benchmark`
- current outcome family support: `poor_functional_outcome`
- temporal validity: concurrent-only

The repo should continue to say no full external-validation claim yet. The new
harmonization and split artifacts stay inside that narrowed lane.

## Current Harmonization And Split Contract

- `fep-ds003944` is the primary benchmark-eligible public cohort
- `tcp-ds005237` can be harmonized only conservatively and remains explicitly
  limited in public form
- outcome rows remain same-visit and concurrent-only where the public source is
  concurrent-only
- split assignments are deterministic, subject-level, and within-cohort
- split manifests must say explicitly that the current contract is not a full
  external-validation claim

## Deferred Until Later PRs

This protocol intentionally does not pull forward:

- representation builders
- model comparison
- biomarker-heavy benchmarking
- prospective benchmark claims

Those belong only after this feasibility gate is settled.

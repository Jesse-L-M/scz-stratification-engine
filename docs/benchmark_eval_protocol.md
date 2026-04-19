# Benchmark Evaluation Protocol

This document defines the current feasibility and harmonization protocol for the
benchmark line.

The active question is still not which model wins. The active question is what
claim the audited cohorts can honestly support, under which access tier, and
whether that result justifies any later benchmark implementation work.

## Command In Scope

Use:

- `scz-audit benchmark audit-datasets`
- `scz-audit benchmark define-schema`
- `scz-audit benchmark harmonize`

`audit-datasets` remains the feasibility gate. `define-schema` freezes the
canonical table contract. `harmonize` operationalizes only the cohorts already
approved for benchmark-table emission.

## Required Audit Outputs

The dataset audit should emit:

1. current `strict_open` benchmark decision
2. current supported claim level under `strict_open`
3. an access-tier decision table covering:
   - `strict_open`
   - `strict_open + public_credentialed`
   - `strict_open + public_credentialed + controlled`
4. cohort-level temporal outcome metadata
5. outcome-family support grouped by access-tier scope
6. an explicit next-step recommendation:
   - remain paused at `narrow-go`
   - continue only as a cross-sectional representation benchmark
   - wait for stronger credentialed/controlled data
7. limiting factors that keep the repo conservative

The harmonization phase should emit:

1. canonical benchmark CSV tables in schema order
2. honest zero-row tables when a cohort has no supported rows
3. concurrent-only outcome rows where public support is concurrent-only
4. deterministic within-cohort subject-level split assignments
5. harmonization and split manifests with cohort caveats and coverage counts

## Claim-Boundary Rules

- `strict_open` must remain a strict category
- `public_credentialed` must not silently count as `strict_open`
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

The current audited outcome picture should remain:

- current access tier in scope: `strict_open`
- benchmark decision: `narrow-go`
- claim level: `narrow_outcome_benchmark`
- current outcome family support: `poor_functional_outcome`
- temporal validity: concurrent-only
- added `strict_open` dataset-expansion cohorts improve representation support,
  not outcome benchmarkability
- current honest next step: cross-sectional representation benchmarking only, if
  the project continues

The repo should continue to say no full external-validation claim yet. This PR
line should also continue to say no stronger public-outcome claim unless a
second eligible cohort actually appears.

## Current Harmonization And Split Contract

- `fep-ds003944` is still the primary benchmark-eligible `strict_open` cohort
- `tcp-ds005237` can be harmonized only conservatively and remains explicitly
  limited in public form
- new dataset-expansion cohorts stay metadata-only in this PR line
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
- harmonization of the new expansion cohorts

Those belong only after this feasibility gate is settled again.

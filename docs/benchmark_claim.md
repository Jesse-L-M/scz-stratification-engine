# Benchmark Claim

## Current Mainline Claim

This repo's active job is still to preserve the benchmark feasibility boundary
while making the dataset registry explicit enough to say what is possible under
each access tier.

Current repo status:

- current access tier in scope: `strict_open`
- benchmark decision: `narrow-go`
- supported claim level: `narrow_outcome_benchmark`
- recommended next step: `continue_cross_sectional_representation_only`
- current `strict_open` outcome support: one eligible cohort with a concurrent
  `poor_functional_outcome` endpoint

## What The Repo Can Claim Right Now

- the registry and audit report can now separate `strict_open`,
  `public_credentialed`, and `controlled` support explicitly
- the public audit can distinguish cross-sectional-only cohorts from real
  outcome-supporting cohorts
- `ucla-cnp-ds000030` and `ds000115` materially improve the honest
  cross-sectional representation-benchmark picture
- one `strict_open` cohort (`fep-ds003944`) still supports a narrow concurrent
  outcome benchmark
- `tcp-ds005237` remains useful context and comparison metadata, but not outcome
  claim evidence

## What The Repo Cannot Claim Right Now

- full external validation under `strict_open`
- a prospective outcome benchmark
- a stronger outcome benchmark justified by audited `public_credentialed` data
- harmonized cross-cohort tables for the new expansion candidates
- representation benchmarking results
- biomarker value
- mechanism or subtype discovery

## Mainline Commitments

- keep access tiers machine-readable and separate
- keep `strict_open` strict
- treat temporal outcome validity as structured metadata, not prose only
- keep diagnosis granularity and mapping caveats explicit in schema contracts
- keep cohort notes operational
- keep old `strict_open` scientific docs visibly archived

## Handoff Rules For Future Agents

- treat this file and [`docs/benchmark_pivot_roadmap.md`](benchmark_pivot_roadmap.md)
  as the current project framing
- treat old `strict_open` scientific docs as superseded unless explicitly reused
  as infrastructure reference
- do not pull harmonization, representation, or modeling work forward in this
  PR line
- do not treat `public_credentialed` or `controlled` improvements as evidence
  for the `strict_open` line

# Benchmark Claim

## Current Mainline Claim

This repo's active job is to preserve the benchmark feasibility boundary while
defining the canonical benchmark schema ahead of harmonization or modeling.

Current repo status:

- benchmark decision: `narrow-go`
- supported claim level: `narrow_outcome_benchmark`
- current public support: one benchmark-eligible cohort with a concurrent
  `poor_functional_outcome` endpoint
- schema status: canonical benchmark tables are now defined in code and emitted
  as benchmark artifacts

## What The Repo Can Claim Right Now

- the public audit surface can distinguish concurrent endpoints from prospective
  outcomes
- the registry and reports can state the current claim level explicitly
- one public cohort currently supports a narrow, concurrent outcome benchmark
- the benchmark namespace can define canonical schema artifacts without implying
  harmonization or stronger claims
- `tcp-ds005237` remains useful metadata and comparison context, but not full
  narrow-support claim evidence

## What The Repo Cannot Claim Right Now

- full external validation
- a prospective outcome benchmark
- harmonized cross-cohort tables
- representation benchmarking results
- biomarker value
- mechanism or subtype discovery

## Mainline Commitments

- keep the benchmark decision conservative and machine-readable
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

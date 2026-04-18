# Benchmark Claim

## Current Mainline Claim

This repo's active job is to harden benchmark feasibility and claim boundaries
before schema or modeling work begins.

Current repo status:

- benchmark decision: `narrow-go`
- supported claim level: `narrow_outcome_benchmark`
- current public support: one benchmark-eligible cohort with a concurrent
  `poor_functional_outcome` endpoint

## What The Repo Can Claim Right Now

- the public audit surface can distinguish concurrent endpoints from prospective
  outcomes
- the registry and reports can state the current claim level explicitly
- one public cohort currently supports a narrow, concurrent outcome benchmark
- `tcp-ds005237` remains useful metadata and comparison context, but not full
  narrow-support claim evidence

## What The Repo Cannot Claim Right Now

- full external validation
- a prospective outcome benchmark
- schema readiness
- harmonized cross-cohort tables
- representation benchmarking results
- biomarker value
- mechanism or subtype discovery

## Mainline Commitments

- keep the benchmark decision conservative and machine-readable
- treat temporal outcome validity as structured metadata, not prose only
- keep cohort notes operational
- keep old `strict_open` scientific docs visibly archived

## Handoff Rules For Future Agents

- treat this file and [`docs/benchmark_pivot_roadmap.md`](benchmark_pivot_roadmap.md)
  as the current project framing
- treat old `strict_open` scientific docs as superseded unless explicitly reused
  as infrastructure reference
- do not pull schema, harmonization, representation, or modeling work forward in
  this PR line

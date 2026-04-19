# Benchmark Pivot Roadmap

This is the canonical roadmap for the benchmark line.

The old `strict_open` roadmap is archived. New work should follow this file.

## Current Phase

Current phase: cross-sectional representation builders.

Current stop line:

- do not pull modeling forward
- do not pull benchmark comparison metrics forward
- do not pull biomarker-heavy benchmarking forward

## Sequencing

1. Feasibility hardening
   Deliver a machine-readable claim ladder, temporal outcome fields, operational
   cohort notes, conservative benchmark reporting, and archived `strict_open`
   scientific docs.
2. Benchmark schema
   Define the canonical tables only after the feasible claim boundary is settled.
3. Harmonization and split contracts
   Build harmonized tables and frozen split rules only after the schema is
   justified by the feasibility gate.
4. Representation and model benchmarking
   First build cross-sectional representation artifacts, then compare
   diagnosis, dimensions, trajectories, or clusters only after the repo can
   honestly state what kind of outcome benchmark exists.

## Current Status

- current benchmark decision: `narrow-go`
- current claim level: `narrow_outcome_benchmark`
- current public support is concurrent-only
- canonical benchmark tables, harmonized outputs, split contracts, and
  cross-sectional representation artifacts now exist
- full external validation remains intentionally out of scope until another
  eligible cohort exists

## What To Preserve From `strict_open`

Keep and reuse only infrastructure that is still helpful:

- source adapters
- provenance and run manifests
- path contracts
- harmonization patterns when they become relevant
- split-discipline and leakage rules when later phases begin

Do not reuse old `strict_open` scientific claims as active guidance.

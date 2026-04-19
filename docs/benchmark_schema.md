# Benchmark Schema

This PR defines the canonical benchmark tables for the current benchmark
question without pretending the public cohorts are already fully harmonized or
prospectively benchmark-ready.

## Why These Tables Exist

The benchmark line needs explicit contracts for subject membership, visit
timing, diagnosis labeling, symptom and cognition measures, functioning,
treatment exposure, outcomes, modality features, and future split assignments.
Those contracts make the benchmark surface operational in code before any
harmonization or modeling work begins.

## Why Outcomes Are First-Class

The real benchmark question is outcome-oriented, not proxy-target oriented. The
schema therefore keeps `outcomes` as a canonical table with explicit predictor
timepoints, outcome timepoints, outcome windows, and concurrent-versus-
prospective fields.

## How The Schema Handles Heterogeneity Honestly

- `diagnosis_granularity` is explicit in `diagnoses`
- `mapping_caveat` is explicit in the cohort-facing measurement tables
- `outcome_is_prospective` and `concurrent_endpoint_only` are explicit in
  `outcomes`
- `representation_comparison_support` and `has_longitudinal_followup` stay
  explicit in `subjects`

This preserves the current `narrow-go` state: limited cohorts, concurrent-only
endpoint cohorts, and stronger future cohorts can all be represented without
over-claiming equivalence.

## CLI And Artifacts

Run:

- `scz-audit benchmark define-schema`

That command writes:

- `data/curated/benchmark/schema/benchmark_schema.json`
- `data/curated/benchmark/schema/benchmark_schema.md`
- `data/processed/benchmark/manifests/benchmark_define_schema_run_manifest.json`

## Still Deferred

- subject-level outcome construction
- model training and evaluation
- biomarker logic

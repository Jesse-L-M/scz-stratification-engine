# Benchmark Harmonization

This document describes the current operational harmonization contract for the
benchmark mainline.

## Cohorts In Scope

- `fep-ds003944`: primary benchmark-eligible public cohort
- `tcp-ds005237`: harmonized conservatively, but still limited in public form

## What The Harmonizer Writes

Run:

- `scz-audit benchmark harmonize`

That command writes canonical CSV tables under
`data/processed/benchmark/harmonized/`, plus:

- `harmonization_manifest.json`
- `data/processed/benchmark/manifests/benchmark_split_manifest.json`

Runtime `benchmark_harmonize_run_manifest.json` files may still be emitted
during command execution, but they are treated as untracked run provenance
rather than canonical checked-in artifacts.

The staged OpenNeuro snapshot JSON can be stored either in a flattened local
shape or in the raw GraphQL wrapper shape. For `fep-ds003944`, the harmonizer
accepts both the small local derived score columns used in fixtures and the
source-aligned ds003944 columns such as `BPRST18`, `OVERALLTSCR`, `FULL2IQ`,
`GAS`, and `CPZ_at_scan`. SAPS and SANS remain stricter: the current benchmark
contract only accepts staged total columns (`saps_total`, `sans_total`) and
does not derive totals from raw item grids. When a staged root includes
`root_files.json`, its snapshot tag and description remain authoritative over
any stale `latestSnapshot` fields saved inside `dataset_metadata.json`.

The checked-in harmonization summary is also deterministic by design: it omits
timestamps, git SHA, and absolute machine-local paths, and records stable
artifact references rather than invocation-specific output locations.

## How This Preserves `narrow-go`

- outcome rows remain explicitly concurrent-only where the public source is
  concurrent-only
- `tcp-ds005237` keeps broad public diagnosis labels (`Patient` vs `GenPop`)
  instead of pretending psychosis-specific equivalence
- sparse tables stay sparse; unsupported tables are emitted with headers and
  zero rows instead of fabricated values
- split assignments are within-cohort subject-level contracts only, not an
  external-validation claim

## Current Sparse Areas

- `tcp-ds005237` currently has no staged cognition rows in the public fixture
  root
- `tcp-ds005237` currently has no staged treatment-exposure rows in the public
  fixture root
- both cohorts remain same-visit outcome cohorts in public form

## Deferred On Purpose

- representation builders
- benchmark model training
- outcome performance reporting
- biomarker-heavy benchmarking
- any upgrade from `narrow-go` to `go`

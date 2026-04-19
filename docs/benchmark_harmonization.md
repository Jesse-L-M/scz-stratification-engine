# Benchmark Harmonization

This document describes the current operational harmonization contract for the
benchmark mainline.

## Cohorts In Scope

- `fep-ds003944`: primary benchmark-eligible public cohort
- `tcp-ds005237`: harmonized conservatively, but still limited in public form
- `ucla-cnp-ds000030`: harmonized as a cross-sectional-only representation cohort
- `ds000115`: harmonized as a small cross-sectional-only representation cohort

## What The Harmonizer Writes

Run:

- `scz-audit benchmark harmonize`
- `scz-audit benchmark build-representations`

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
does not derive totals from raw item grids. For the new expansion cohorts, the
harmonizer carries through only explicitly staged cross-sectional summary
columns that are already supported by the public snapshot subset. When a staged
root includes `root_files.json`, its snapshot tag and description remain
authoritative over any stale `latestSnapshot` fields saved inside
`dataset_metadata.json`.

The checked-in harmonization summary is also deterministic by design: it omits
timestamps, git SHA, and absolute machine-local paths, and records stable
artifact references rather than invocation-specific output locations.

The representation builder writes checked-in cross-sectional benchmark artifacts
under `data/processed/benchmark/representations/`. Those artifacts consume the
harmonized tables directly and stay deterministic across reruns.

## How This Preserves `narrow-go`

- outcome rows remain explicitly concurrent-only where the public source is
  concurrent-only
- `tcp-ds005237` keeps broad public diagnosis labels (`Patient` vs `GenPop`)
  instead of pretending psychosis-specific equivalence
- `ucla-cnp-ds000030` and `ds000115` add only cross-sectional representation
  rows and still emit zero outcome rows
- sparse tables stay sparse; unsupported tables are emitted with headers and
  zero rows instead of fabricated values
- split assignments are within-cohort subject-level contracts only, not an
  external-validation claim, even for newly added unlabeled cohorts

## Current Sparse Areas

- `ucla-cnp-ds000030` currently contributes diagnosis, symptom, cognition,
  medication-inventory, and modality-availability rows, but no functioning or
  outcome rows
- `ds000115` currently contributes diagnosis plus conservative symptom and
  cognition rows, but no treatment, functioning, modality, or outcome rows
- `tcp-ds005237` currently has no staged cognition rows in the public fixture
  root
- `tcp-ds005237` currently has no staged treatment-exposure rows in the public
  fixture root
- only `fep-ds003944` and `tcp-ds005237` remain outcome-bearing public cohorts,
  and both stay same-visit only

## Deferred On Purpose

- benchmark model training
- outcome performance reporting
- biomarker-heavy benchmarking
- any upgrade from `narrow-go` to `go`

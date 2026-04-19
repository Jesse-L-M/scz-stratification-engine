# Benchmark Representations

This document describes the current representation-artifact contract for the
benchmark mainline.

## Command In Scope

Run:

- `scz-audit benchmark build-representations`

The command consumes the harmonized benchmark tables and deterministic split
assignments already written by `scz-audit benchmark harmonize`.

## Families Emitted Today

- `diagnosis_anchor.csv`
  - label-space baseline with conservative case/control, psychosis,
    schizophrenia, bipolar, ADHD, sibling-context, and broad-patient flags
- `symptom_profile.csv`
  - cohort-normalized symptom-domain summaries where public symptom rows exist
- `cognition_profile.csv`
  - cohort-normalized cognition-domain summaries where public cognition rows
    exist
- `clinical_snapshot.csv`
  - sparse multimodal summary over symptom, cognition, functioning, treatment,
    modality availability, and outcome-row presence
- `representation_manifest.json`
  - stable coverage and comparability summary for the representation families

## Current Claim Boundary

- these artifacts improve cross-sectional comparison readiness only
- they do not upgrade the repo beyond `narrow-go`
- they do not create a new public outcome family
- they do not constitute benchmark model results
- `ucla-cnp-ds000030` and `ds000115` remain cross-sectional-only cohorts with
  zero outcome rows in this layer

## Sparse Areas That Stay Explicit

- `tcp-ds005237` still has zero cognition rows in the current public staged root
- `ucla-cnp-ds000030` contributes no functioning or outcome rows
- `ds000115` contributes no treatment, modality, functioning, or outcome rows
- control or sibling comparison rows may have blank symptom or outcome-derived
  summaries even when they remain valuable for cross-sectional representation
  comparison

## Deferred On Purpose

- benchmark model fitting
- representation winner claims
- outcome-performance reporting
- biomarker-heavy representation branches
- any upgrade from `narrow-go` to `go`

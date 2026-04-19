# Cross-Sectional Benchmark Report

This document summarizes the first actual comparison of the checked-in
benchmark representation families.

## Scope

- command: `scz-audit benchmark run-benchmark`
- lane: cross-sectional representation benchmarking only
- posture: `narrow-go`
- claim level: `narrow_outcome_benchmark`

## Tasks In Scope

Headline tasks:

- `fep-ds003944` / `psychosis_vs_control`
- `ucla-cnp-ds000030` / `schizophrenia_vs_non_schizophrenia_context`

Context-only tasks:

- `ds000115` / `schizophrenia_vs_non_schizophrenia_family_context`
- `tcp-ds005237` / `patient_vs_genpop_context_only`

## Current Reading

- `fep-ds003944` is not train-evaluable under the frozen split contract because
  the train row is psychosis only.
- `ucla-cnp-ds000030` is the only headline task with train-time class coverage,
  but both evaluation splits are single-class non-schizophrenia holdouts.
- `ds000115` is not train-evaluable under the frozen split contract because the
  train rows are non-schizophrenia family-context rows only.
- `tcp-ds005237` remains context only and label-limited. It is not evidence for
  psychosis-specific representation quality.

## Family Comparison

- `diagnosis_anchor` is evaluable on the UCLA and TCP tasks and sets the current
  trivial label-anchor baseline.
- `symptom_profile` is only partially evaluable and drops the UCLA test row plus
  the TCP control row because those rows have no substantive family support.
- `cognition_profile` is evaluable only on UCLA and performs worse than
  `diagnosis_anchor`.
- `clinical_snapshot` is evaluable on UCLA and TCP but does not provide a
  stronger headline result than `diagnosis_anchor`.

No non-baseline family meaningfully beats `diagnosis_anchor` on the headline
tasks. Any ties occur only on tiny single-class holdouts.

## Recommendation

Continue only as a descriptive artifact repo. The current benchmark result is
useful because it shows the representation lane can be compared deterministically,
but it does not justify a broader comparison/model PR, a stronger public claim,
or any prospective framing.

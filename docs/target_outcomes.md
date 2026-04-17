# Target Outcomes

This repo should optimize for real, intervention-relevant outcomes.

Synthetic proxy targets are out of scope for the mainline benchmark unless they
are explicitly labeled as auxiliary QC variables rather than scientific
endpoints.

## Outcome Priority Order

Use the highest-ranked outcome that can be defined honestly across the selected
cohorts:

1. one-year nonremission
2. persistent negative symptoms
3. poor functional outcome
4. relapse or hospitalization proxy

## Outcome Definitions

### One-Year Nonremission

Use when:

- symptom and follow-up information support a one-year remission or
  nonremission label

Do not use when:

- remission status would have to be heavily imputed
- follow-up windows are too inconsistent to defend the label

### Persistent Negative Symptoms

Use when:

- negative-symptom measures exist at repeated visits
- persistence can be defined from the available timeline

Do not use when:

- negative symptoms are only weakly captured
- the available signal is dominated by one acute cross-sectional measure

### Poor Functional Outcome

Use when:

- functioning, social, educational, occupational, or recovery-relevant measures
  exist
- the threshold for poor outcome can be written explicitly

Do not use when:

- functioning is only described narratively
- the outcome is just a loose proxy for symptom severity

### Relapse Or Hospitalization Proxy

Use only if the better outcomes are unavailable.

This is an acceptable fallback, not the preferred headline outcome.

## Mainline Rules

- each benchmark run must declare exactly which outcome is primary
- outcomes must be cohort-mapped explicitly, not assumed equivalent by name
- the repo must record known mapping caveats for each cohort
- if an outcome is only supportable in one cohort, that limitation must be
  stated in the final report

## What Not To Use As Mainline Targets

- `stable_cognitive_burden_proxy`
- `global_cognition_dev`
- `state_noise_score`
- unsupervised cluster membership as its own endpoint

Those may appear later only as:

- QC variables
- coverage diagnostics
- auxiliary covariates

## Deliverable Requirement

Before benchmark modeling begins, each selected cohort should have a short
cohort-specific note that states:

1. which primary outcome is available
2. how it is defined locally
3. what assumptions or caveats remain

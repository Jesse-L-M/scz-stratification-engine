# Target Outcomes

This repo should optimize for real, intervention-relevant outcomes, but the
current phase is still about proving which outcome families are publicly
supportable.

## Outcome Priority Order

Use the highest-ranked outcome that can be defined honestly across the selected
cohorts:

1. one-year nonremission
2. persistent negative symptoms
3. poor functional outcome
4. relapse or hospitalization proxy

## Temporal Mapping Requirement

Every cohort note and every registry row should state:

1. predictor timepoint
2. outcome timepoint
3. outcome window
4. whether the outcome is prospective
5. whether the cohort is concurrent-only

If the outcome is measured at the same visit as the predictors, treat it as a
concurrent endpoint, not a prospective outcome.

## Outcome Family Rules

### One-Year Nonremission

Use when:

- remission status can be defined at an explicit future window
- follow-up timing is documented tightly enough to defend the label

Do not use when:

- remission would have to be inferred from one acute cross-sectional assessment
- follow-up timing is missing or too inconsistent

### Persistent Negative Symptoms

Use when:

- negative-symptom measures exist across repeated visits
- persistence is defined from an explicit public timeline

Do not use when:

- negative symptoms are only available at one visit
- persistence would be guessed from a same-visit severity score

### Poor Functional Outcome

Use when:

- functioning, social, educational, occupational, or recovery-relevant measures
  exist
- the registry can say whether the endpoint is same-visit or future-looking

Do not use when:

- functioning is only described narratively
- a same-visit score is later described as a prospective endpoint

### Relapse Or Hospitalization Proxy

Use only if the better outcomes are unavailable and the relapse window is still
written explicitly.

## Current Public Posture

The current public cohorts only support the third-ranked family, and they only
support it as a concurrent endpoint:

- current supportable family: `poor_functional_outcome`
- current temporal status: `concurrent_only`
- current prospective support: none

That is enough for a narrow feasibility claim, not a prospective benchmark
claim.

## Cohort Note Template

Before benchmark modeling begins, each selected cohort note should state:

1. exact outcome family currently supportable
2. whether the endpoint is concurrent or prospective
3. what the local outcome window is
4. what assumptions or caveats remain

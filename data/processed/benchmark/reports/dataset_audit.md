# Benchmark Dataset Audit

- Generated at: `2026-04-18T15:22:36.988901Z`
- Current benchmark decision: `narrow-go`
- Current claim level supported: `narrow_outcome_benchmark`
- Decision explanation: Only one public benchmark-eligible cohort currently counts toward narrow benchmark support for poor_functional_outcome. Cohorts with weaker public label granularity remain outside the claim count: tcp-ds005237. Current public endpoint support is concurrent-only, so the repo remains narrow-go without a prospective claim.
- Claim-level explanation: One benchmark-eligible cohort supports a real outcome family, so the repo can make a narrow outcome benchmark claim but not a full external-validation or prospective claim.
- Narrow benchmark supporting cohorts: `fep-ds003944`
- Full external-validation cohorts: none
- Concurrent-only cohorts: `tcp-ds005237`, `fep-ds003944`
- Prospectively usable cohorts: none
- Limiting factors: Only one cohort currently counts toward narrow benchmark support.; No outcome family is currently supported by two benchmark-eligible public cohorts.; No audited cohort currently exposes a prospectively usable public outcome window.; Public label granularity remains limited for tcp-ds005237.

## Outcome Family Support

| Outcome family | Narrow benchmark support | Full external-validation support | Prospective support |
| --- | --- | --- | --- |
| `one_year_nonremission` | none | none | none |
| `persistent_negative_symptoms` | none | none | none |
| `poor_functional_outcome` | fep-ds003944 | none | none |
| `relapse_hospitalization_proxy` | none | none | none |

## Audited Cohorts

| Dataset | Access | Local status | Benchmark v0 eligibility | Representation support | Temporal validity | Claim ceiling | Narrow support | Outcome families |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `tcp-ds005237` | `public` | `audited` | `limited` | `limited` | `concurrent_only` | `none` | `no` | poor_functional_outcome |
| `fep-ds003944` | `public` | `audited` | `eligible` | `strong` | `concurrent_only` | `narrow_outcome_benchmark` | `yes` | poor_functional_outcome |

## Cohort Notes

### `tcp-ds005237`
- Label: Transdiagnostic Connectome Project
- Local status: audited
- Benchmark v0 eligibility: limited
- Representation comparison support: limited
- Predictor timepoint: scan/baseline
- Outcome timepoint: same_visit_functioning_assessment
- Outcome window: same_visit
- Outcome temporal validity: concurrent_only
- Concurrent endpoint only: yes
- Prospectively usable: no
- Claim level contributions: none
- Benchmarkable outcome families: poor_functional_outcome
- Diagnosis coverage: Broad psychiatric illness vs healthy comparison; README cites affective or psychotic illness history, but public participants.tsv only exposes Patient vs GenPop groups.
- Functioning scales: LIFE-RIFT, MCAS
- Longitudinal coverage: No repeated public follow-up visits are described in the accession metadata or README.
- Outcome availability: Poor functional outcome is potentially benchmarkable via LIFE-RIFT and MCAS; public metadata does not confirm one-year nonremission, persistent negative symptoms, or relapse follow-up.
- Major limitations: Diagnosis granularity is weak in public participant metadata, and the public release does not document longitudinal follow-up needed for remission, persistence, or relapse endpoints.
- Audit summary: Public transdiagnostic MRI cohort with same-visit functioning endpoints and multi-site structure, but no prospectively usable public outcome window. The public label space is still too broad to count as narrow benchmark support for psychosis heterogeneity.
- Primary sources: https://openneuro.org/datasets/ds005237, https://raw.githubusercontent.com/OpenNeuroDatasets/ds005237/1.1.3/README, https://raw.githubusercontent.com/OpenNeuroDatasets/ds005237/1.1.3/participants.tsv

### `fep-ds003944`
- Label: EEG: First Episode Psychosis vs. Control Resting Task 1
- Local status: audited
- Benchmark v0 eligibility: eligible
- Representation comparison support: strong
- Predictor timepoint: scan/baseline
- Outcome timepoint: same_visit_functioning_assessment
- Outcome window: same_visit
- Outcome temporal validity: concurrent_only
- Concurrent endpoint only: yes
- Prospectively usable: no
- Claim level contributions: cross_sectional_representation, narrow_outcome_benchmark
- Benchmarkable outcome families: poor_functional_outcome
- Diagnosis coverage: participants.tsv lists 50 Psychosis and 32 Control participants.
- Functioning scales: GAF/GAS, SFS
- Longitudinal coverage: No repeated follow-up visits are described in the accession metadata or README.
- Outcome availability: Poor functional outcome is potentially benchmarkable via GAF/GAS and SFS; public metadata does not confirm one-year nonremission, persistent negative symptoms, or relapse follow-up.
- Major limitations: Public metadata describes cross-sectional assessments at scan time, so remission, persistence, and relapse endpoints are not currently supported.
- Audit summary: Public first-episode psychosis EEG cohort with symptom, cognition, and same-visit functioning measures. It supports a narrow concurrent outcome benchmark, but not a prospective outcome claim.
- Primary sources: https://openneuro.org/datasets/ds003944, https://raw.githubusercontent.com/OpenNeuroDatasets/ds003944/1.0.1/README, https://raw.githubusercontent.com/OpenNeuroDatasets/ds003944/1.0.1/participants.tsv

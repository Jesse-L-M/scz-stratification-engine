# Benchmark Dataset Audit

- Current access tier in scope: `strict_open`
- Current benchmark decision under `strict_open`: `narrow-go`
- Current claim level under `strict_open`: `narrow_outcome_benchmark`
- Recommended next step: `continue_cross_sectional_representation_only`
- Recommendation explanation: Multiple strict-open cohorts now support cross-sectional representation comparison, but only one strict-open cohort still supports an outcome benchmark. The honest next phase is cross-sectional representation work only, not a stronger outcome benchmark line.

## Access-Tier Decisions

| Access tier in scope | Included cohort tiers | Decision | Claim level | Cross-sectional representation cohorts | Narrow support cohorts |
| --- | --- | --- | --- | --- | --- |
| `strict_open` | `strict_open` | `narrow-go` | `narrow_outcome_benchmark` | fep-ds003944, ucla-cnp-ds000030, ds000115 | fep-ds003944 |
| `public_credentialed` | `strict_open`, `public_credentialed` | `narrow-go` | `narrow_outcome_benchmark` | fep-ds003944, ucla-cnp-ds000030, ds000115 | fep-ds003944 |
| `controlled` | `strict_open`, `public_credentialed`, `controlled` | `narrow-go` | `narrow_outcome_benchmark` | fep-ds003944, ucla-cnp-ds000030, ds000115 | fep-ds003944 |

## Access-Tier Notes

### `strict_open`
- Included cohort tiers: `strict_open`
- Decision explanation: Within strict_open, only one audited eligible cohort currently supports poor_functional_outcome. Cohorts with weaker diagnosis granularity remain outside the eligible claim count: tcp-ds005237. Current endpoint support remains concurrent-only, so this access tier scope stays narrow-go without a prospective claim.
- Claim-level explanation: Within strict_open, one eligible cohort supports a real outcome family, so the repo can make a narrow outcome benchmark claim but not a full external-validation or prospective claim.
- Cross-sectional representation cohorts: `fep-ds003944`, `ucla-cnp-ds000030`, `ds000115`
- Narrow benchmark supporting cohorts: `fep-ds003944`
- Full external-validation cohorts: none
- Concurrent-only cohorts: `tcp-ds005237`, `fep-ds003944`
- Prospectively usable cohorts: none
- Limiting factors: Only one cohort currently counts toward narrow benchmark support.; No outcome family is currently supported by two eligible cohorts.; No audited cohort currently exposes a prospectively usable outcome window.; Diagnosis granularity remains limited for tcp-ds005237.

### `public_credentialed`
- Included cohort tiers: `strict_open`, `public_credentialed`
- Decision explanation: Within strict_open + public_credentialed, only one audited eligible cohort currently supports poor_functional_outcome. Cohorts with weaker diagnosis granularity remain outside the eligible claim count: tcp-ds005237. Current endpoint support remains concurrent-only, so this access tier scope stays narrow-go without a prospective claim.
- Claim-level explanation: Within strict_open + public_credentialed, one eligible cohort supports a real outcome family, so the repo can make a narrow outcome benchmark claim but not a full external-validation or prospective claim.
- Cross-sectional representation cohorts: `fep-ds003944`, `ucla-cnp-ds000030`, `ds000115`
- Narrow benchmark supporting cohorts: `fep-ds003944`
- Full external-validation cohorts: none
- Concurrent-only cohorts: `tcp-ds005237`, `fep-ds003944`
- Prospectively usable cohorts: none
- Limiting factors: Only one cohort currently counts toward narrow benchmark support.; No outcome family is currently supported by two eligible cohorts.; No audited cohort currently exposes a prospectively usable outcome window.; Diagnosis granularity remains limited for tcp-ds005237.

### `controlled`
- Included cohort tiers: `strict_open`, `public_credentialed`, `controlled`
- Decision explanation: Within strict_open + public_credentialed + controlled, only one audited eligible cohort currently supports poor_functional_outcome. Cohorts with weaker diagnosis granularity remain outside the eligible claim count: tcp-ds005237. Current endpoint support remains concurrent-only, so this access tier scope stays narrow-go without a prospective claim.
- Claim-level explanation: Within strict_open + public_credentialed + controlled, one eligible cohort supports a real outcome family, so the repo can make a narrow outcome benchmark claim but not a full external-validation or prospective claim.
- Cross-sectional representation cohorts: `fep-ds003944`, `ucla-cnp-ds000030`, `ds000115`
- Narrow benchmark supporting cohorts: `fep-ds003944`
- Full external-validation cohorts: none
- Concurrent-only cohorts: `tcp-ds005237`, `fep-ds003944`
- Prospectively usable cohorts: none
- Limiting factors: Only one cohort currently counts toward narrow benchmark support.; No outcome family is currently supported by two eligible cohorts.; No audited cohort currently exposes a prospectively usable outcome window.; Diagnosis granularity remains limited for tcp-ds005237.

## Outcome Family Support By Access Tier

| Access tier in scope | Outcome family | Narrow benchmark support | Full external-validation support | Prospective support |
| --- | --- | --- | --- | --- |
| `strict_open` | `one_year_nonremission` | none | none | none |
| `strict_open` | `persistent_negative_symptoms` | none | none | none |
| `strict_open` | `poor_functional_outcome` | fep-ds003944 | none | none |
| `strict_open` | `relapse_hospitalization_proxy` | none | none | none |
| `public_credentialed` | `one_year_nonremission` | none | none | none |
| `public_credentialed` | `persistent_negative_symptoms` | none | none | none |
| `public_credentialed` | `poor_functional_outcome` | fep-ds003944 | none | none |
| `public_credentialed` | `relapse_hospitalization_proxy` | none | none | none |
| `controlled` | `one_year_nonremission` | none | none | none |
| `controlled` | `persistent_negative_symptoms` | none | none | none |
| `controlled` | `poor_functional_outcome` | fep-ds003944 | none | none |
| `controlled` | `relapse_hospitalization_proxy` | none | none | none |

## Audited Cohorts

| Dataset | Access tier | Local status | Benchmark v0 eligibility | Representation support | Temporal validity | Claim ceiling | Narrow support if tier allowed | Outcome families |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `tcp-ds005237` | `strict_open` | `audited` | `limited` | `limited` | `concurrent_only` | `none` | `no` | poor_functional_outcome |
| `fep-ds003944` | `strict_open` | `audited` | `eligible` | `strong` | `concurrent_only` | `narrow_outcome_benchmark` | `yes` | poor_functional_outcome |
| `ucla-cnp-ds000030` | `strict_open` | `audited` | `ineligible` | `strong` | `none` | `cross_sectional_representation` | `no` | none |
| `ds000115` | `strict_open` | `audited` | `ineligible` | `strong` | `none` | `cross_sectional_representation` | `no` | none |

## Cohort Notes

### `tcp-ds005237`
- Label: Transdiagnostic Connectome Project
- Access tier: strict_open
- Local status: audited
- Benchmark v0 eligibility: limited
- Representation comparison support: limited
- Predictor timepoint: scan/baseline
- Outcome timepoint: same_visit_functioning_assessment
- Outcome window: same_visit
- Outcome temporal validity: concurrent_only
- Concurrent endpoint only: yes
- Prospectively usable: no
- Cross-sectional representation support if tier allowed: no
- Narrow benchmark support if tier allowed: no
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
- Access tier: strict_open
- Local status: audited
- Benchmark v0 eligibility: eligible
- Representation comparison support: strong
- Predictor timepoint: scan/baseline
- Outcome timepoint: same_visit_functioning_assessment
- Outcome window: same_visit
- Outcome temporal validity: concurrent_only
- Concurrent endpoint only: yes
- Prospectively usable: no
- Cross-sectional representation support if tier allowed: yes
- Narrow benchmark support if tier allowed: yes
- Claim level contributions: cross_sectional_representation, narrow_outcome_benchmark
- Benchmarkable outcome families: poor_functional_outcome
- Diagnosis coverage: participants.tsv lists 50 Psychosis and 32 Control participants.
- Functioning scales: GAF/GAS, SFS
- Longitudinal coverage: No repeated follow-up visits are described in the accession metadata or README.
- Outcome availability: Poor functional outcome is potentially benchmarkable via GAF/GAS and SFS; public metadata does not confirm one-year nonremission, persistent negative symptoms, or relapse follow-up.
- Major limitations: Public metadata describes cross-sectional assessments at scan time, so remission, persistence, and relapse endpoints are not currently supported.
- Audit summary: Public first-episode psychosis EEG cohort with symptom, cognition, and same-visit functioning measures. It supports a narrow concurrent outcome benchmark, but not a prospective outcome claim.
- Primary sources: https://openneuro.org/datasets/ds003944, https://raw.githubusercontent.com/OpenNeuroDatasets/ds003944/1.0.1/README, https://raw.githubusercontent.com/OpenNeuroDatasets/ds003944/1.0.1/participants.tsv

### `ucla-cnp-ds000030`
- Label: UCLA Consortium for Neuropsychiatric Phenomics LA5c Study
- Access tier: strict_open
- Local status: audited
- Benchmark v0 eligibility: ineligible
- Representation comparison support: strong
- Predictor timepoint: unmapped
- Outcome timepoint: unmapped
- Outcome window: unmapped
- Outcome temporal validity: none
- Concurrent endpoint only: no
- Prospectively usable: no
- Cross-sectional representation support if tier allowed: yes
- Narrow benchmark support if tier allowed: no
- Claim level contributions: cross_sectional_representation
- Benchmarkable outcome families: none
- Diagnosis coverage: participants.tsv exposes explicit CONTROL, SCHZ, BIPOLAR, and ADHD groups (130 CONTROL, 50 SCHZ, 49 BIPOLAR, 43 ADHD).
- Functioning scales: none confirmed
- Longitudinal coverage: No repeated follow-up visit schedule or prospective public endpoint is described in the pinned snapshot metadata or README.
- Outcome availability: The public snapshot supports cross-sectional diagnosis, symptom, cognition, and medication context only. It does not confirm a public functioning scale or any benchmarkable longitudinal outcome family.
- Major limitations: Rich cross-sectional phenotyping is available, but the public snapshot does not document a benchmarkable functioning or prospective outcome window. The cohort is also transdiagnostic rather than a narrow schizophrenia-only outcome benchmark source.
- Audit summary: Strict-open UCLA CNP snapshot with explicit schizophrenia labels and rich cross-sectional symptom/cognition coverage. It materially helps cross-sectional representation benchmarking, but it does not add a public outcome benchmark family.
- Primary sources: https://openneuro.org/datasets/ds000030, https://raw.githubusercontent.com/OpenNeuroDatasets/ds000030/1.0.0/README, https://raw.githubusercontent.com/OpenNeuroDatasets/ds000030/1.0.0/participants.tsv

### `ds000115`
- Label: Working memory in healthy and schizophrenic individuals
- Access tier: strict_open
- Local status: audited
- Benchmark v0 eligibility: ineligible
- Representation comparison support: strong
- Predictor timepoint: unmapped
- Outcome timepoint: unmapped
- Outcome window: unmapped
- Outcome temporal validity: none
- Concurrent endpoint only: no
- Prospectively usable: no
- Cross-sectional representation support if tier allowed: yes
- Narrow benchmark support if tier allowed: no
- Claim level contributions: cross_sectional_representation
- Benchmarkable outcome families: none
- Diagnosis coverage: participants.tsv exposes SCZ, SCZ-SIB, CON, and CON-SIB groups (23 SCZ, 35 SCZ-SIB, 20 CON, 21 CON-SIB).
- Functioning scales: none confirmed
- Longitudinal coverage: No repeated follow-up visit schedule or prospective public endpoint is described in the pinned snapshot metadata or README.
- Outcome availability: The public snapshot exposes cross-sectional symptom severity and working-memory performance only. It does not confirm a public functioning scale or any benchmarkable longitudinal outcome family.
- Major limitations: The strict-open snapshot is small, family-structured, and entirely cross-sectional. It lacks public functioning outcomes, relapse/remission windows, or other benchmarkable longitudinal endpoints.
- Audit summary: Strict-open schizophrenia working-memory cohort with explicit case, sibling, and control labels plus cross-sectional cognition. It is a low-weight representation sanity-check cohort only and does not add a public outcome benchmark family.
- Primary sources: https://openneuro.org/datasets/ds000115, https://raw.githubusercontent.com/OpenNeuroDatasets/ds000115/00001/README, https://raw.githubusercontent.com/OpenNeuroDatasets/ds000115/00001/participants.tsv

# Benchmark Dataset Audit

- Generated at: `2026-04-18T13:29:06.330645Z`
- Recommended decision: `narrow-go`
- Explanation: Only one public benchmark-eligible cohort supports each currently benchmarkable outcome family, so benchmark v0 must narrow scope and reduce any external-validation claim.

## Outcome Family Support

| Outcome family | Benchmark-v0 eligible cohorts counting toward `go` |
| --- | --- |
| `one_year_nonremission` | none |
| `persistent_negative_symptoms` | none |
| `poor_functional_outcome` | fep-ds003944 |
| `relapse_hospitalization_proxy` | none |

## Audited Cohorts

| Dataset | Access | Local status | Benchmark v0 eligibility | Counts toward `go` | Population | Benchmarkable outcome families |
| --- | --- | --- | --- | --- | --- | --- |
| `tcp-ds005237` | `public` | `audited` | `limited` | `no` | transdiagnostic psychiatry cohort with affective or psychotic illness history | poor_functional_outcome |
| `fep-ds003944` | `public` | `audited` | `eligible` | `yes` | first-episode psychosis case-control cohort | poor_functional_outcome |

## Cohort Notes

### `tcp-ds005237`
- Label: Transdiagnostic Connectome Project
- Local status: audited
- Benchmark v0 eligibility: limited
- Counts toward cross-cohort `go`: no
- Diagnosis coverage: Broad psychiatric illness vs healthy comparison; README cites affective or psychotic illness history, but public participants.tsv only exposes Patient vs GenPop groups.
- Functioning scales: LIFE-RIFT, MCAS
- Longitudinal coverage: No repeated public follow-up visits are described in the accession metadata or README.
- Outcome availability: Poor functional outcome is potentially benchmarkable via LIFE-RIFT and MCAS; public metadata does not confirm one-year nonremission, persistent negative symptoms, or relapse follow-up.
- Major limitations: Diagnosis granularity is weak in public participant metadata, and the public release does not document longitudinal follow-up needed for remission, persistence, or relapse endpoints.
- Primary sources: https://openneuro.org/datasets/ds005237, https://raw.githubusercontent.com/OpenNeuroDatasets/ds005237/1.1.3/README, https://raw.githubusercontent.com/OpenNeuroDatasets/ds005237/1.1.3/participants.tsv

### `fep-ds003944`
- Label: EEG: First Episode Psychosis vs. Control Resting Task 1
- Local status: audited
- Benchmark v0 eligibility: eligible
- Counts toward cross-cohort `go`: yes
- Diagnosis coverage: participants.tsv lists 50 Psychosis and 32 Control participants.
- Functioning scales: GAF/GAS, SFS
- Longitudinal coverage: No repeated follow-up visits are described in the accession metadata or README.
- Outcome availability: Poor functional outcome is potentially benchmarkable via GAF/GAS and SFS; public metadata does not confirm one-year nonremission, persistent negative symptoms, or relapse follow-up.
- Major limitations: Public metadata describes cross-sectional assessments at scan time, so remission, persistence, and relapse endpoints are not currently supported.
- Primary sources: https://openneuro.org/datasets/ds003944, https://raw.githubusercontent.com/OpenNeuroDatasets/ds003944/1.0.1/README, https://raw.githubusercontent.com/OpenNeuroDatasets/ds003944/1.0.1/participants.tsv

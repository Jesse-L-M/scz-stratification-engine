# Psychosis Benchmark Pivot Roadmap

This document replaces the prior `strict-open v0` roadmap as the recommended
mainline plan for this repo.

`strict_open/` should be treated as an exploratory spike and reusable
infrastructure source, not the scientific center of the project.

## Bottom Line

The repo should no longer optimize for a single-cohort schizophrenia
`stable_cognitive_burden_proxy` or a trait/state model trained against synthetic
targets.

The mainline project should instead answer this question:

> Which representation of psychosis heterogeneity reproduces across independent
> datasets and improves prediction of intervention-relevant outcomes:
> diagnosis, dimensions, trajectories, or clusters?

## New Project Frame

### Primary Goal

Build a reproducible multi-cohort benchmark for psychosis heterogeneity
representations using real outcomes and explicit external-validation rules.

### What This Repo Should Now Be

- a patient- and cohort-level benchmarking engine
- a harmonization and evaluation framework across psychosis datasets
- a place to compare diagnosis, dimensions, trajectories, and simple clusters
- a place to test whether biomarkers add value only after low-tech clinical
  representations are established

### What This Repo Should Not Be

- a subtype-discovery repo
- a biomarker-first repo
- a target-prioritization repo
- a single-cohort schizophrenia audit engine used as the main scientific claim
- a model-first project looking for a question to justify it

## What To Preserve From `strict_open`

Keep and adapt:

- source adapter pattern
- provenance and run manifests
- harmonization layer separation from source-specific ingest
- frozen split contracts
- explicit leakage rules
- baseline-first evaluation discipline
- go / no-go gates

Do not preserve as mainline scientific assumptions:

- `stable_cognitive_burden_proxy`
- `global_cognition_dev` as a core endpoint
- `state_noise_score` as the main target
- a trait/state neural model as the default centerpiece
- MRI-first framing
- TCP-only framing

## New Namespace Direction

The recommended mainline namespace is a new one, such as:

- `benchmark/`
- `psychosis_benchmark/`

`strict_open/` can remain in the repo as archived exploratory work until useful
infrastructure is extracted or copied forward.

## Canonical Scientific Scope

### Main Comparison Families

The first benchmark should compare:

1. diagnosis-only representations
2. symptom-dimension representations
3. simple longitudinal or early-course representations
4. simple cluster representations where supportable

### Outcome Priorities

Use real outcomes only. Prefer, in order:

1. one-year nonremission
2. persistent negative symptoms
3. poor functional outcome
4. relapse or hospitalization proxies if the better outcomes are unavailable

### Modality Rule

Imaging, genetics, and other biomarkers should enter only as later ablations.

The project should first establish whether diagnosis, dimensions, and
trajectory-aware representations already explain most of the useful signal.

## Replacement PR Sequence

### PR0: Roadmap Reset

**Purpose**

Reset the thesis before landing more code tied to the old targets.

**Files / Directories To Add Or Modify**

- `README.md`
- `docs/strict_open_pr_roadmap.md`
- `docs/benchmark_claim.md`
- `docs/dataset_matrix.md`
- `docs/target_outcomes.md`

**Expected Outputs / Artifacts**

- explicit statement that `strict_open` is exploratory
- explicit mainline benchmark question
- dataset inventory template
- real-outcome contract

**Acceptance Criteria**

- repo no longer claims that the mainline question is cognitive stability or
  trial-noise estimation
- docs explicitly prioritize multi-cohort benchmarking over single-cohort proxy
  targets
- real outcomes are defined before new modeling PRs land

**Non-Goals**

- no new modeling code
- no new target definitions
- no new biomarkers

### PR1: Extract Reusable Infrastructure Into The New Namespace

**Purpose**

Preserve the useful engineering substrate without preserving the old scientific
objective.

**Files / Directories To Add Or Modify**

- `src/scz_audit_engine/benchmark/__init__.py`
- `src/scz_audit_engine/benchmark/paths.py`
- `src/scz_audit_engine/benchmark/provenance.py`
- `src/scz_audit_engine/benchmark/run_manifest.py`
- `src/scz_audit_engine/cli.py`
- `config/benchmark_v0.toml`

**Expected Outputs / Artifacts**

- new benchmark namespace
- benchmark config
- benchmark path contract
- benchmark CLI group

**Acceptance Criteria**

- the new namespace exists without carrying old target semantics
- manifests, paths, and provenance are reusable from day one
- `strict_open` remains isolated rather than mixed into the new path

**Non-Goals**

- no cohort ingest yet
- no benchmark schema yet
- no scores or targets yet

### PR2: Dataset Registry And Access Reality Check

**Purpose**

Decide what question the available data can actually answer.

**Files / Directories To Add Or Modify**

- `docs/dataset_matrix.md`
- `data/curated/benchmark/dataset_registry.csv`
- source adapters for at least two candidate public cohorts

**Expected Outputs / Artifacts**

- a concrete cohort registry with:
  - access level
  - diagnosis coverage
  - symptom scales
  - cognition coverage
  - functioning coverage
  - longitudinal coverage
  - treatment variables
  - outcome availability
  - modality availability

**Acceptance Criteria**

- at least two candidate cohorts are audited
- the repo states clearly which real outcomes are currently benchmarkable
- the project can make a go / no-go call on multi-cohort benchmarking

**Non-Goals**

- no modeling
- no clusters
- no biomarker claims

### PR3: Canonical Benchmark Schema

**Purpose**

Define tables around the actual question rather than around proxy targets.

**Canonical Tables Should Include**

- `subjects`
- `visits`
- `diagnoses`
- `symptom_scores`
- `cognition_scores`
- `functioning_scores`
- `treatment_exposures`
- `outcomes`
- `modality_features`
- `split_assignments`

**Acceptance Criteria**

- schema supports real outcomes explicitly
- schema supports cross-cohort harmonization
- proxy targets are not required by the core contract

### PR4: Harmonization And Evaluation Protocol

**Purpose**

Freeze the rules before comparisons begin.

**Files / Directories To Add Or Modify**

- cohort harmonizers
- `docs/benchmark_eval_protocol.md`
- split-definition logic

**Evaluation Rules Must Cover**

- subject-level leakage prevention
- external-validation policy
- cross-site caveats
- repeat-visit handling
- outcome-window definitions
- cohort-specific mapping caveats

**Acceptance Criteria**

- at least two cohorts can be harmonized into the same schema
- splits are frozen before benchmarking
- outcome definitions are explicit and reproducible

### PR5: Representation Builders

**Purpose**

Turn harmonized data into comparable representation families.

**Initial Families**

- diagnosis-only
- symptom dimensions
- simple baseline clinical representation
- simple trajectory-aware summaries where repeated data exist
- simple clusters only as a comparator

**Acceptance Criteria**

- each family is implemented with clear provenance
- each family can be evaluated on the same outcomes
- clusters are not privileged over dimensions or trajectories

### PR6: Baseline Benchmark V0

**Purpose**

Answer the main question with the simplest honest benchmark.

**Compare**

- diagnosis
- dimensions
- early-course or trajectory-aware summaries where possible
- simple clusters

**Evaluate**

- prediction of real outcomes
- calibration
- cohort transfer
- subgroup coverage
- failure cases

**Acceptance Criteria**

- the repo can state which representation families reproduce across datasets
- the repo can state which ones improve prediction over simple baselines
- the report ends in a real go / no-go conclusion

### PR7: Trajectory Extension

**Purpose**

Go deeper only if longitudinal data quality justifies it.

**Focus**

- persistent negative symptoms
- functional non-recovery
- early poor-outcome trajectories

**Acceptance Criteria**

- trajectory work is only pursued if repeated-measures coverage is sufficient
- longitudinal models are compared against simpler non-longitudinal baselines

### PR8: Biomarker / Modality Ablations

**Purpose**

Make biomarkers earn their place.

**Scope**

- imaging
- genetics
- other modalities available in the harmonized cohorts

**Acceptance Criteria**

- biomarkers are evaluated only as incremental value-add
- reporting explicitly states whether they improve on clinical
  representations

### PR9: Decision Report

**Purpose**

Emit the decision the project exists to make.

**Final Report Must Answer**

1. what reproduces
2. what predicts useful outcomes
3. what fails to generalize
4. whether biomarkers add anything
5. what the next research move should be

## Guidance For Existing Work

### `strict_open` Work That Is Safe To Keep

- ingest
- audit
- provenance
- harmonization scaffolding
- split scaffolding

### `strict_open` Work That Should Not Guide The Mainline

- target construction around `stable_cognitive_burden_proxy`
- baseline families designed around the old soft targets
- the trait/state model plan
- biology-context outputs as a near-term focus

## Guidance For The Pending Baseline PR

Do not merge the pending baseline PR unchanged if it is tied to:

- `global_cognition_dev`
- `state_noise_score`
- `stable_cognitive_burden_proxy`

That branch can still be mined later for:

- split-contract validation logic
- report-generation patterns
- support-gap accounting
- manifest discipline

## Immediate Next Step

Open a roadmap-reset PR first.

That PR should:

1. update `README.md`
2. land this roadmap
3. add a benchmark claim doc
4. add a dataset matrix doc
5. stop the repo from implicitly optimizing for the old thesis

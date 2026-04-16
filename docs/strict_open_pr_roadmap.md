# strict-open v0 PR Roadmap

This roadmap is for `strict-open v0` only.

It does not assume gated data, HCP-YA, sponsor-grade enrichment, treatment-response modeling, or any commercial claim. It uses a parallel `strict_open/` namespace and keeps `ds005237 / TCP` as the default core dataset.

## Assumptions

- Python `src/` layout
- `pytest` for tests
- one CLI entrypoint such as `scz-audit strict-open ...`
- `PyTorch` for core modeling
- `MONAI` for MRI ingestion, transforms, and imaging-model utilities
- `CUDA`-friendly training and inference path
- containerized GPU runtime as part of the execution path
- Allen Human Brain Atlas and Open Targets are biology-context / evidence-card layers, not core predictive machinery
- product and scientific claims stay vendor-neutral, while the execution layer is explicitly NVIDIA-friendly
- default installs stay light until the model PR, with heavyweight runtime dependencies added as optional groups or training-only dependencies
- `TensorRT` / `Dynamo` and `BioNeMo` are future-evaluation items, not `strict-open v0` dependencies
- strict-open commands should converge on a shared config file and run-manifest format early, before baselines or model training

## PR Sequence

### PR1: Add strict-open v0 charter and claim-boundary docs

**Purpose**

Lock the product framing before code exists.

**Why This PR Comes Now**

If the claim boundary is fuzzy at the start, later PRs will quietly drift into gated-data assumptions or sponsor language.

**Files / Directories To Add Or Modify**

- `README.md`
- `docs/strict_open_claim.md`
- `docs/strict_open_scope.md`
- `docs/strict_open_sources.md`
- `docs/strict_open_execution_stack.md`
- `pyproject.toml`
- test and CI config files as needed for a minimal repo bootstrap

**Tests To Add**

- `tests/test_docs_presence.py`

**CLI Commands Or Workflows Affected**

- none yet

**Expected Outputs / Artifacts**

- written claim boundary for `strict-open v0`
- written non-goals
- default source list anchored on `ds005237 / TCP`
- written execution-stack note covering `PyTorch`, `MONAI`, `CUDA`, containerized GPU runtime, and future-only evaluation of `TensorRT` / `Dynamo` / `BioNeMo`

**Acceptance Criteria**

- docs explicitly frame this as a public cohort stability and noise audit engine with a strict-open public-feasibility boundary
- docs explicitly exclude gated-data claims and sponsor-grade positioning
- docs state that Allen and Open Targets are interpretation layers, not core model drivers
- docs state that product/spec choices are vendor-neutral while implementation is GPU-native and NVIDIA-friendly
- `pyproject.toml` stays minimal and does not force heavyweight training dependencies into the default install
- repo has enough bootstrap structure for later PRs to add tests cleanly

**Non-Goals**

- no `strict_open/` package yet
- no CLI surface
- no ingest or model code
- no HCP-YA or gated-data references in the core path
- no forced use of irrelevant NVIDIA tools

### PR2: Add strict_open package skeleton, config, schema, path contract, and CLI stubs

**Purpose**

Create the parallel namespace, canonical table definitions, shared config and run-manifest plumbing, and output directory contract.

**Why This PR Comes Now**

Every later implementation slice depends on a stable package root and a stable place for artifacts to land.

**Files / Directories To Add Or Modify**

- `src/scz_audit_engine/__init__.py`
- `src/scz_audit_engine/cli.py`
- `src/scz_audit_engine/strict_open/__init__.py`
- `src/scz_audit_engine/strict_open/schema.py`
- `src/scz_audit_engine/strict_open/paths.py`
- `src/scz_audit_engine/strict_open/run_manifest.py`
- `config/strict_open_v0.toml`
- `docker/Dockerfile.gpu`
- `docker/README.md`
- `data/raw/strict_open/README.md`
- `data/processed/strict_open/README.md`
- `data/curated/strict_open/README.md`
- `data/processed/strict_open/manifests/README.md`
- `examples/strict_open_v0/README.md`

**Tests To Add**

- `tests/strict_open/test_schema.py`
- `tests/strict_open/test_paths.py`
- `tests/strict_open/test_cli.py`
- `tests/strict_open/test_run_manifest.py`

**CLI Commands Or Workflows Affected**

- `scz-audit strict-open ingest`
- `scz-audit strict-open audit`
- `scz-audit strict-open harmonize`
- `scz-audit strict-open define-splits`
- `scz-audit strict-open build-features`
- `scz-audit strict-open build-targets`
- `scz-audit strict-open train-baselines`
- `scz-audit strict-open train`
- `scz-audit strict-open eval`
- `scz-audit strict-open report`

These can be stubs in this PR.

**Expected Outputs / Artifacts**

- package skeleton
- canonical schema definitions
- directory contract for raw, processed, curated, example, and report outputs
- base config file for `strict-open v0`
- shared run-manifest contract covering dataset source/version, command used, git SHA, seed, output paths, and timestamps
- documented GPU container contract for local and CI execution

**Acceptance Criteria**

- `strict_open/` exists as a parallel namespace
- canonical tables are defined for `subjects`, `visits`, `cognition_scores`, `symptom_behavior_scores`, `mri_features`, `derived_targets`, and `biology_priors`
- CLI help works for the `strict-open` command group
- a default `config/strict_open_v0.toml` exists and is usable across later commands
- all declared outputs resolve under `strict_open` paths
- a shared run-manifest writer exists for later PRs to use
- GPU container files exist without forcing model or ingest implementation into this PR
- heavyweight runtime dependencies remain optional or absent from the default install

**Non-Goals**

- no ingest logic
- no harmonization logic
- no features or targets
- no model training
- no `BioNeMo`, `Holoscan`, `Isaac`, `Parabricks`, or inference-optimization dependencies
- no environment yak-shaving beyond a minimal GPU container contract

### PR3: Add TCP raw ingest, provenance, and audit profile outputs

**Purpose**

Load `ds005237 / TCP` into raw storage and produce audit outputs that show what is actually available.

**Why This PR Comes Now**

The data audit should shape the canonical implementation, not be backfilled after the model exists.

**Files / Directories To Add Or Modify**

- `src/scz_audit_engine/strict_open/sources/__init__.py`
- `src/scz_audit_engine/strict_open/sources/base.py`
- `src/scz_audit_engine/strict_open/sources/tcp_ds005237.py`
- `src/scz_audit_engine/strict_open/audit.py`
- `src/scz_audit_engine/strict_open/provenance.py`
- `tests/fixtures/tcp_raw/` fixtures as needed

**Tests To Add**

- `tests/strict_open/test_sources.py`
- `tests/strict_open/test_audit.py`
- `tests/strict_open/test_provenance.py`

**CLI Commands Or Workflows Affected**

- `scz-audit strict-open ingest --source tcp`
- `scz-audit strict-open audit`

**Expected Outputs / Artifacts**

- raw TCP extracts under `data/raw/strict_open/tcp/`
- audit outputs under `data/processed/strict_open/profiles/`
- provenance artifacts under `data/processed/strict_open/manifests/`
- profile tables or summaries for subject counts, diagnosis breakdown, repeat-visit availability, cognition inventory, symptom inventory, MRI/QC inventory, and missingness
- source manifest with dataset accession or version, file hashes where possible, ingest timestamp, and command metadata
- raw-to-processed provenance map that later PRs can extend rather than reinvent

**Acceptance Criteria**

- TCP ingest runs without touching any gated data path
- audit output is machine-readable and reproducible
- ingest and audit runs emit a shared run manifest and source manifest
- missingness and repeat-visit availability are visible enough to guide the next PRs

**Non-Goals**

- no harmonization
- no Allen or Open Targets integration
- no additional public cohorts yet
- no modeling
- no NVIDIA-specific modeling dependencies yet beyond the documented runtime direction

### PR4: Add harmonization layer, canonical tables, and split protocol

**Purpose**

Convert TCP raw extracts into the canonical subject/visit-centered schema and freeze the evaluation split rules before any baseline training.

**Why This PR Comes Now**

Features, targets, and evaluation will all rot if harmonization is ad hoc or if train, validation, and test handling is not frozen before baselines.

**Files / Directories To Add Or Modify**

- `src/scz_audit_engine/strict_open/harmonize.py`
- `src/scz_audit_engine/strict_open/splits.py`
- `docs/strict_open_eval_protocol.md`
- optional mapping helpers such as `src/scz_audit_engine/strict_open/mappings/tcp.py`

**Tests To Add**

- `tests/strict_open/test_harmonize.py`
- `tests/strict_open/test_splits.py`

**CLI Commands Or Workflows Affected**

- `scz-audit strict-open harmonize`
- `scz-audit strict-open define-splits`

**Expected Outputs / Artifacts**

- canonical tables under `data/processed/strict_open/harmonized/`
- deterministic IDs and provenance-preserving mappings
- split assignments and split manifests under `data/processed/strict_open/splits/`
- written evaluation protocol for subject-level splits, site-aware evaluation, train/validation/test definitions, and repeat-visit handling

**Acceptance Criteria**

- canonical `subjects`, `visits`, `cognition_scores`, `symptom_behavior_scores`, and `mri_features` tables are emitted
- missing values and unmapped fields are handled explicitly
- source adapters stay dumb and harmonization stays separate
- subject-level splits are frozen before baselines
- site-aware evaluation rules are explicit
- repeat visits are grouped in a way that prevents leakage across train, validation, and test

**Non-Goals**

- no derived features
- no soft target
- no baseline training

### PR5: Add derived features and public-only soft target construction

**Purpose**

Turn canonical tables into model-ready features and define the `stable_cognitive_burden_proxy`.

**Why This PR Comes Now**

Baselines and trait/state modeling should train on the same feature and target contract that later evaluation will inspect.

**Files / Directories To Add Or Modify**

- `src/scz_audit_engine/strict_open/features.py`
- `src/scz_audit_engine/strict_open/targets.py`

**Tests To Add**

- `tests/strict_open/test_features.py`
- `tests/strict_open/test_targets.py`

**CLI Commands Or Workflows Affected**

- `scz-audit strict-open build-features`
- `scz-audit strict-open build-targets`

**Expected Outputs / Artifacts**

- feature tables under `data/processed/strict_open/features/`
- derived target outputs with `global_cognition_dev`, `state_noise_score`, and `stable_cognitive_burden_proxy`, where the proxy is used to contrast against state noise and estimate the patient's stable baseline-like signal

**Acceptance Criteria**

- features include cognition domain summaries, symptom composites, state-noise proxies, missingness indicators, and compact MRI summaries
- the soft target is probabilistic, not categorical
- the target uses symptom/function proxies only where available
- `stable_cognitive_burden_proxy` is framed as a stable baseline-like estimate rather than a claim about a true baseline
- feature and target outputs are keyed to the frozen split contract rather than redefining train, validation, and test behavior ad hoc
- no treatment history or gated longitudinal assumptions leak into the target

**Non-Goals**

- no hard clinical label
- no sponsor-style enrichment logic
- no model training yet
- no split-rule changes in this PR

### PR6: Add baselines and baseline evaluation

**Purpose**

Set the performance floor and make it obvious whether the core idea is dead early.

**Why This PR Comes Now**

If simple baselines are confounded or useless, there is no reason to hide that behind a more complex trait/state model.

**Files / Directories To Add Or Modify**

- `src/scz_audit_engine/strict_open/baselines.py`
- `src/scz_audit_engine/strict_open/baseline_eval.py`

**Tests To Add**

- `tests/strict_open/test_baselines.py`

**CLI Commands Or Workflows Affected**

- `scz-audit strict-open train-baselines`

**Expected Outputs / Artifacts**

- baseline models under `data/processed/strict_open/models/baselines/`
- baseline summary report under `data/processed/strict_open/reports/`

**Acceptance Criteria**

- includes cognition-only, symptom-only, MRI-only, simple multimodal, and snapshot latent baselines
- baseline report is reproducible and easy to compare later against the trait/state model
- baseline outputs are tied to the frozen split manifest and run manifest
- obvious confounds are surfaced instead of hand-waved

**Non-Goals**

- no neural trait/state model yet
- no biology-context layer

### PR7: Add trait/state model and training loop

**Purpose**

Implement the first model that explicitly separates stable signal from visit-level noise and emits confidence.

**Why This PR Comes Now**

Only after baseline behavior is known does it make sense to pay the complexity cost of a trait/state model.

**Files / Directories To Add Or Modify**

- `pyproject.toml`
- `src/scz_audit_engine/strict_open/model.py`
- `src/scz_audit_engine/strict_open/train.py`
- `src/scz_audit_engine/strict_open/losses.py`
- `src/scz_audit_engine/strict_open/imaging.py`

**Tests To Add**

- `tests/strict_open/test_model.py`

**CLI Commands Or Workflows Affected**

- `scz-audit strict-open train`

**Expected Outputs / Artifacts**

- checkpoints and training outputs under `data/processed/strict_open/models/trait_state/`
- persisted predictions, latent outputs, and confidence outputs
- PyTorch training path and MONAI-backed MRI preprocessing or utilities

**Acceptance Criteria**

- training runs end to end on the strict-open feature contract
- model emits `z_trait`, `z_state`, a prediction, and a confidence output
- MRI branch is implemented with `MONAI` where it helps, without contorting the broader architecture
- training path is `CUDA`-capable when GPU is available and remains runnable on CPU for tests
- heavyweight runtime dependencies land here as optional extras or training-only dependencies, not as an unconditional default install
- architecture stays small and readable

**Non-Goals**

- no claim of success yet
- no biology-context integration into the training loop
- no extra public datasets merged into the core path
- no `BioNeMo`, `TensorRT`, or `Dynamo` dependency in `strict-open v0`

### PR8: Add evaluation suite and explicit go / no-go gate

**Purpose**

Measure whether the model is actually doing the thing the project claims.

**Why This PR Comes Now**

This is the first real decision point. Training that merely runs is not evidence.

**Files / Directories To Add Or Modify**

- `src/scz_audit_engine/strict_open/eval.py`
- `src/scz_audit_engine/strict_open/metrics.py`

**Tests To Add**

- `tests/strict_open/test_eval.py`

**CLI Commands Or Workflows Affected**

- `scz-audit strict-open eval`

**Expected Outputs / Artifacts**

- evaluation reports under `data/processed/strict_open/reports/`
- stability, calibration, site-leakage, and missing-modality summaries

**Acceptance Criteria**

- evaluation explicitly reports whether trait stability beats snapshot baselines
- calibration and abstention behavior are measured directly
- site leakage is measured directly
- missing-modality robustness is measured directly
- evaluation checks whether the model can confidently flag noisy or ambiguous visits that would otherwise contaminate a trial endpoint
- evaluation is run against the frozen subject-level split and site-aware protocol
- report ends in a clear go / no-go decision

**Non-Goals**

- no new model features hidden inside evaluation
- no outreach or packaging docs yet

### PR9: Add biology context and evidence-card layer

**Purpose**

Attach biology-context outputs to the model result without turning them into predictive claims.

**Why This PR Comes Now**

Interpretation belongs after the predictive core is proven to be at least directionally sane.

**Files / Directories To Add Or Modify**

- `src/scz_audit_engine/strict_open/sources/allen.py`
- `src/scz_audit_engine/strict_open/sources/opentargets_public.py`
- `src/scz_audit_engine/strict_open/biology.py`

**Tests To Add**

- `tests/strict_open/test_biology.py`

**CLI Commands Or Workflows Affected**

- `scz-audit strict-open build-biology`

**Expected Outputs / Artifacts**

- biology-context artifacts under `data/raw/strict_open/biology/`
- evidence-card outputs under `data/processed/strict_open/reports/biology_context/`

**Acceptance Criteria**

- evidence card is traceable back to public sources
- Allen and Open Targets stay outside the core predictive machinery
- output language is clearly interpretive, not causal

**Non-Goals**

- no retraining the model around biology priors
- no mechanism claims

### PR10: Add reporting artifact and access / outreach packet docs

**Purpose**

Package the technical work into a `Cohort Integrity Report` and the docs that explain why PIs and sponsors need this audit to de-risk trial readouts and what closed data unlocks next.

**Why This PR Comes Now**

The report should reflect a finished `strict-open v0` evaluation state, not a moving target.

**Files / Directories To Add Or Modify**

- `src/scz_audit_engine/strict_open/reporting.py`
- `docs/strict_open_results.md`
- `docs/strict_open_closed_data_unlocks.md`
- `docs/strict_open_access_packet.md`
- `examples/strict_open_v0/input_sample.json`

**Tests To Add**

- `tests/strict_open/test_reporting.py`

**CLI Commands Or Workflows Affected**

- `scz-audit strict-open report`

**Expected Outputs / Artifacts**

- one clean `Cohort Integrity Report` generated by `scz-audit strict-open report`, with cohort-level state-noise contamination, site-leakage warnings, individual patient stability tiering, abstain flags, and short biology-context evidence cards
- docs that explain why public data is insufficient, why PIs and sponsors need this audit to de-risk trial readouts, and what exact closed data would unlock next

**Acceptance Criteria**

- the example report is generated from the reporting command rather than hand-authored markdown that can drift
- the generated report is understandable in about one minute
- docs make a narrow, credible case for why PIs and sponsors need this audit to de-risk trial readouts and what exact closed data would unlock next
- all language stays inside `strict-open v0` boundaries

**Non-Goals**

- no sponsor-grade or commercial claims
- no gated-data integration
- no private-partner workflows

## Recommended First PR To Open Immediately

Open `PR1: Add strict-open v0 charter and claim-boundary docs`.

Reason: this repo is effectively blank. The highest-leverage first move is to freeze the wording, the non-goals, the default data assumptions, and the execution-stack stance before any package structure or CLI surface is added.

## Main Failure Modes And Pause Gates

### After PR3

Pause if `ds005237 / TCP` is too thin to support the canonical tables without heroic assumptions.

Do not rescue the plan by immediately pulling in HCP-YA or gated data.

### After PR5

Pause if the soft target is mostly site, scanner, missingness, or other state noise.

Also pause if the available symptom or function proxies are too thin to justify the proxy at all.

### After PR6

Pause if the baselines only learn confounds or there is no signal worth separating into `trait` and `state`.

### After PR8

Pause or kill `strict-open v0` if:

- trait stability does not beat snapshot baselines
- calibration and abstention are not sane
- site leakage remains high
- missing-modality robustness falls apart

If PR8 looks promising, the next optional step after this roadmap is a second truly public cohort for robustness testing.

If PR8 is weak, do not expand cohort scope just to rescue the story.

### After PR9 Or PR10

Trim or rewrite anything that starts sounding causal, sponsor-ready, or commercial.

## Parallelization Notes

Most of this should stay serial.

Useful exceptions:

- PR9 can be developed after canonical identifiers are stable, but should merge only after PR8
- PR10 docs can be drafted early, but should merge only after PR8 and ideally after PR9
- GPU container work in PR2 can be prepared in parallel with early doc drafting, but should stay minimal and not balloon into environment yak-shaving
- optional second-cohort work should only begin after a strong PR8 result and should not run in parallel with the core 10-PR path

Everything else should merge in order. The goal is not speed through parallel scaffolding. The goal is a sequence of small PRs that each prove something real.

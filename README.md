# scz-audit-engine

`scz-audit-engine` now treats `strict_open` as exploratory infrastructure, not
the main scientific line.

The recommended mainline direction for this repo is a multi-cohort psychosis
benchmark that compares competing representations of heterogeneity against real,
intervention-relevant outcomes.

The main question is:

> Which representation of psychosis heterogeneity reproduces across independent
> datasets and improves prediction of intervention-relevant outcomes:
> diagnosis, dimensions, trajectories, or clusters?

Current source-of-truth docs:

- [`docs/strict_open_pr_roadmap.md`](docs/strict_open_pr_roadmap.md):
  replacement roadmap and PR sequence
- [`docs/benchmark_claim.md`](docs/benchmark_claim.md): explicit project claim
  and boundaries
- [`docs/dataset_matrix.md`](docs/dataset_matrix.md): cohort inventory contract
- [`docs/target_outcomes.md`](docs/target_outcomes.md): real-outcome contract
- [`docs/benchmark_eval_protocol.md`](docs/benchmark_eval_protocol.md):
  evaluation rules for the new mainline

Current code scaffold:

- `src/scz_audit_engine/benchmark/`: benchmark namespace for repo-relative
  paths, provenance helpers, dataset registry contracts, and dataset-audit
  source adapters
- `config/benchmark_v0.toml`: benchmark config for shared path contracts and
  defaults
- `scz-audit benchmark audit-datasets`: writes the benchmark dataset registry,
  audit reports, and run manifest from audited cohort metadata
- `data/curated/benchmark/dataset_registry.csv`: checked-in benchmark cohort
  registry for the current v0 scope

Practical note:

- `strict_open/` can still be mined for reusable ingest, provenance,
  harmonization, and split logic.
- pending `strict_open` baseline and proxy-target work should remain parked
  unless and until specific plumbing is intentionally extracted into the new
  `benchmark/` namespace.

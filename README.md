# scz-audit-engine

`scz-audit-engine` is currently in the benchmark dataset and outcome feasibility
phase.

Current repo status:

- benchmark decision: `narrow-go`
- supported claim level: `narrow_outcome_benchmark`
- public support today: one benchmark-eligible cohort with a concurrent
  `poor_functional_outcome` endpoint
- benchmark schema status: canonical tables and schema artifacts are defined, but
  harmonization and modeling remain deferred
- not yet supported: full external validation, prospective outcome claims,
  harmonization, split generation, representation builders, or modeling

`strict_open` remains in the repo as archived exploratory infrastructure. It is
not the active scientific line.

Current source-of-truth docs:

- [`docs/benchmark_claim.md`](docs/benchmark_claim.md): current project claim and
  explicit non-claims
- [`docs/benchmark_claim_levels.md`](docs/benchmark_claim_levels.md): ordered
  claim ladder for feasibility gating
- [`docs/dataset_matrix.md`](docs/dataset_matrix.md): dataset registry contract
  and benchmark decision rules
- [`docs/target_outcomes.md`](docs/target_outcomes.md): outcome-family and
  temporal mapping rules
- [`docs/benchmark_eval_protocol.md`](docs/benchmark_eval_protocol.md):
  feasibility audit protocol for `scz-audit benchmark audit-datasets`
- [`docs/benchmark_schema.md`](docs/benchmark_schema.md): canonical benchmark
  table contracts and artifact outputs
- [`docs/benchmark_pivot_roadmap.md`](docs/benchmark_pivot_roadmap.md):
  canonical roadmap from feasibility hardening to later schema work

Current code scaffold:

- `src/scz_audit_engine/benchmark/`: benchmark namespace for path contracts,
  provenance, dataset registry logic, and source adapters
- `config/benchmark_v0.toml`: benchmark config for shared path contracts and
  defaults
- `scz-audit benchmark audit-datasets`: writes the checked-in dataset registry,
  audit reports, and run manifest from audited cohort metadata
- `scz-audit benchmark define-schema`: writes the canonical schema JSON,
  Markdown artifact, and run manifest
- `data/curated/benchmark/dataset_registry.csv`: machine-readable cohort
  registry with temporal outcome and claim-level fields
- `data/curated/benchmark/schema/benchmark_schema.json`: machine-readable table
  contract emitted by the schema command

Practical note:

- `strict_open/` can still be mined for reusable ingest, provenance,
  harmonization, and split logic
- old `strict_open` scientific docs are superseded and retained only as archive
- harmonization and modeling work stay intentionally deferred until a later PR

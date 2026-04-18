# strict-open v0 Evaluation Protocol

> Superseded.
> This is archived exploratory documentation.
> Active benchmark guidance lives in [`docs/benchmark_eval_protocol.md`](benchmark_eval_protocol.md)
> and [`docs/benchmark_pivot_roadmap.md`](benchmark_pivot_roadmap.md).

This protocol freezes the default split and evaluation contract for `strict-open v0` before any baseline models exist.

## Scope

- Dataset in scope: `ds005237 / TCP`
- Data access boundary: public strict-open inputs only
- Evaluation unit: subject
- Artifact roots:
  - `data/processed/strict_open/harmonized/`
  - `data/processed/strict_open/splits/`

## Canonical data contract

`scz-audit strict-open harmonize` emits the canonical tables defined in `schema.py`:

- `subjects`
- `visits`
- `cognition_scores`
- `symptom_behavior_scores`
- `mri_features`

The harmonization layer is separate from the source adapter layer. Source adapters stay source-aligned and only stage or inventory public TCP inputs. Canonical subject, visit, score, and MRI availability logic lives in `strict_open/harmonize.py`.

## Public-data rule

If a raw TCP file is only available as a git-annex pointer in the strict-open ingest boundary, it is treated as inaccessible for harmonization. The pipeline does not fabricate tabular rows from annex-backed content and records those limitations in `harmonization_manifest.json`.

## Visit contract

- Canonical visits are deterministic and subject-centered.
- Available phenotype visit/date fields are reused when present.
- If a subject has no accessible visit-stamped phenotype rows, the pipeline emits a single baseline visit.
- MRI availability and motion-QC rows are attached to the first canonical visit for each subject because the current public TCP imaging paths do not expose a reliable multi-visit session map.
- MRI modality availability is claimed only from locally staged accessible image payloads. Manifest-only listings are reported as inaccessible, not available.

## Default split protocol

`scz-audit strict-open define-splits` freezes one default split assignment for all downstream PRs.

- Split unit: subject
- Default split names: `train`, `validation`, `test`
- Default ratios: `0.6 / 0.2 / 0.2`
- Seed source: `seed` in `config/strict_open_v0.toml`
- Determinism: subject ordering is derived from a seed-stable hash and the assignment sequence is fixed from the configured quotas
- Diagnosis handling: subjects are ordered within diagnosis strata first, then interleaved before assignment so diagnosis composition is preserved as much as the current cohort allows

## Leakage policy

- All rows for a subject land in exactly one split.
- All visits for a subject inherit that subject split.
- No visit-level reassignment is allowed.
- Any future baseline, feature, or target command must reuse `split_assignments.csv` exactly.

## Site-aware evaluation rule

- Site composition is reported in `split_manifest.json` for every split.
- Site counts are part of the frozen audit surface and must remain visible in downstream evaluation reports.
- Strict site holdout is not required for `strict-open v0` because the current public TCP site coverage is too limited to support a reliable site-isolated test set without overstating the guarantee.

## Required artifacts

`harmonize` writes:

- `subjects.csv`
- `visits.csv`
- `cognition_scores.csv`
- `symptom_behavior_scores.csv`
- `mri_features.csv`
- `harmonization_manifest.json`

`define-splits` writes:

- `split_assignments.csv`
- `split_manifest.json`

The split manifest must continue to report:

- command
- git SHA
- seed
- split method
- counts by split
- counts by split x diagnosis
- counts by split x site
- repeat-visit policy
- caveats caused by sparse site structure or limited follow-up

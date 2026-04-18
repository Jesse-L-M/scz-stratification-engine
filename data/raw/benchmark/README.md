# raw/benchmark

Place source-aligned benchmark inputs here.

Use source- or cohort-specific subdirectories under this root as the multi-
cohort benchmark expands.

The current harmonizer expects one directory per cohort, for example:

- `fep-ds003944/`
- `tcp-ds005237/`

Each staged cohort root should remain source-aligned and minimal: metadata
snapshot files such as `dataset_metadata.json`, `participants.tsv`,
`phenotype_files.json`, and any staged `phenotype/*.tsv` tables that are
actually available locally. The JSON snapshot files may be stored either as a
flattened object (`{"dataset": ...}`, `{"files": ...}`) or in the raw GraphQL
shape (`{"data": {"dataset": ...}}`, `{"data": {"snapshot": {"files": ...}}}`).
`README.txt` and `root_files.json` are optional for harmonization. When
`root_files.json` is present, its saved snapshot metadata is treated as the
authoritative pinned snapshot contract.

`modality_features.csv` is more conservative: it is only populated when staged
subject-level modality files are actually present under the cohort root. For
`fep-ds003944`, that means real staged EEG files or an `eeg/` subtree, not just
any file under `sub-*/`. For `tcp-ds005237`, that means BIDS-like MRI/fMRI
files such as `*_T1w.json`, `*_T2w.nii.gz`, or `*_bold.json`, not arbitrary
notes under `anat/` or `func/`.

For `fep-ds003944` symptom scores, staged SAPS/SANS total columns remain
required. The harmonizer does not reconstruct benchmark totals from raw
`SAPS_Q*` or `SANS_Q*` item grids.

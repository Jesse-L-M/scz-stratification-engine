# Cross-Sectional Representation Benchmark

## Scope

- This report stays in the current cross-sectional representation lane only.
- The repo posture remains `narrow-go` at claim level `narrow_outcome_benchmark`.
- Rows with no substantive family support were skipped. Remaining blank feature values were zero-filled within each family.
- Balanced accuracy on a single-class validation or test split collapses to observed-class recall and is low-confidence context only.

## Headline Tasks

- `fep-ds003944` / `psychosis_vs_control`: evaluable families = none.
  Caveat: This remains the strongest current strict-open psychosis/control task, but the frozen split contract is still concurrent-only and extremely small.
  validation: evaluable = none; single-class eval = none; skipped = diagnosis_anchor (Train split has one class only (negative=0,positive=1).), symptom_profile (Train split has one class only (negative=0,positive=1).), cognition_profile (Train split has one class only (negative=0,positive=1).), clinical_snapshot (Train split has one class only (negative=0,positive=1).).
  test: evaluable = none; single-class eval = none; skipped = diagnosis_anchor (Train split has one class only (negative=0,positive=1).), symptom_profile (Train split has one class only (negative=0,positive=1).), cognition_profile (Train split has one class only (negative=0,positive=1).), clinical_snapshot (Train split has one class only (negative=0,positive=1).).
- `ucla-cnp-ds000030` / `schizophrenia_vs_non_schizophrenia_context`: evaluable families = clinical_snapshot, cognition_profile, diagnosis_anchor, symptom_profile.
  Caveat: This task is useful for representation comparison, but it remains transdiagnostic context rather than a stronger psychosis-specific or outcome claim.
  validation: evaluable = diagnosis_anchor, symptom_profile, cognition_profile, clinical_snapshot; single-class eval = diagnosis_anchor, symptom_profile, cognition_profile, clinical_snapshot; skipped = none.
  test: evaluable = diagnosis_anchor, cognition_profile, clinical_snapshot; single-class eval = diagnosis_anchor, cognition_profile, clinical_snapshot; skipped = symptom_profile (No labeled test rows with usable feature support.).

## Context-Only Tasks

- `ds000115` / `schizophrenia_vs_non_schizophrenia_family_context`: evaluable families = none.
  Caveat: This cohort is tiny and family-structured. It is context only and should not be treated as headline evidence for schizophrenia separability.
  validation: evaluable = none; single-class eval = none; skipped = diagnosis_anchor (Train split has one class only (negative=2,positive=0).), symptom_profile (Train split has one class only (negative=2,positive=0).), cognition_profile (Train split has one class only (negative=2,positive=0).), clinical_snapshot (Train split has one class only (negative=2,positive=0).).
  test: evaluable = none; single-class eval = none; skipped = diagnosis_anchor (Train split has one class only (negative=2,positive=0).), symptom_profile (Train split has one class only (negative=2,positive=0).), cognition_profile (Train split has one class only (negative=2,positive=0).), clinical_snapshot (Train split has one class only (negative=2,positive=0).).
- `tcp-ds005237` / `patient_vs_genpop_context_only`: evaluable families = clinical_snapshot, diagnosis_anchor.
  Caveat: Public TCP labels remain broad patient versus GenPop only, so this task is explicitly context only and not psychosis-specific evidence.
  validation: evaluable = diagnosis_anchor, clinical_snapshot; single-class eval = diagnosis_anchor, clinical_snapshot; skipped = symptom_profile (Train split has one class only (negative=0,positive=1).), cognition_profile (No labeled train rows with usable feature support.).
  test: evaluable = diagnosis_anchor, clinical_snapshot; single-class eval = diagnosis_anchor, clinical_snapshot; skipped = symptom_profile (Train split has one class only (negative=0,positive=1).), cognition_profile (No labeled train rows with usable feature support.).

## Recommendation

- Recommendation: `continue_only_as_descriptive_artifact_repo`
- Reason: Headline comparisons do not clear the diagnosis_anchor baseline on fully comparable non-single-class holdouts. Keep the repo at the descriptive artifact layer rather than pulling model comparison forward.
- Baseline check: No headline task produced a fully comparable non-single-class diagnosis_anchor result, so there is no meaningful baseline race to call.

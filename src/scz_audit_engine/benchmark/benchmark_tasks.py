"""Explicit cross-sectional task registry for the benchmark mainline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BenchmarkTask:
    """Task metadata and label mapping for a single benchmark cohort."""

    cohort_id: str
    task_name: str
    task_scope: str
    headline_status: str
    label_definition: str
    label_caveat: str
    is_context_only: bool
    positive_diagnosis_groups: tuple[str, ...]
    negative_diagnosis_groups: tuple[str, ...]

    def label_for_diagnosis_group(self, diagnosis_group: str) -> int | None:
        if diagnosis_group in self.positive_diagnosis_groups:
            return 1
        if diagnosis_group in self.negative_diagnosis_groups:
            return 0
        return None

    def to_dict(self) -> dict[str, object]:
        return {
            "cohort_id": self.cohort_id,
            "task_name": self.task_name,
            "task_scope": self.task_scope,
            "headline_status": self.headline_status,
            "label_definition": self.label_definition,
            "label_caveat": self.label_caveat,
            "is_context_only": self.is_context_only,
            "positive_diagnosis_groups": list(self.positive_diagnosis_groups),
            "negative_diagnosis_groups": list(self.negative_diagnosis_groups),
        }


BENCHMARK_TASKS = (
    BenchmarkTask(
        cohort_id="fep-ds003944",
        task_name="psychosis_vs_control",
        task_scope="headline_cross_sectional",
        headline_status="headline",
        label_definition="psychosis versus control within the public FEP cross-sectional cohort",
        label_caveat=(
            "This remains the strongest current strict-open psychosis/control task, but the frozen split contract "
            "is still concurrent-only and extremely small."
        ),
        is_context_only=False,
        positive_diagnosis_groups=("psychosis",),
        negative_diagnosis_groups=("control",),
    ),
    BenchmarkTask(
        cohort_id="ucla-cnp-ds000030",
        task_name="schizophrenia_vs_non_schizophrenia_context",
        task_scope="headline_cross_sectional_transdiagnostic_context",
        headline_status="headline",
        label_definition="schizophrenia versus pooled non-schizophrenia context labels within UCLA CNP",
        label_caveat=(
            "This task is useful for representation comparison, but it remains transdiagnostic context rather than a "
            "stronger psychosis-specific or outcome claim."
        ),
        is_context_only=False,
        positive_diagnosis_groups=("schizophrenia",),
        negative_diagnosis_groups=("control", "bipolar_disorder", "adhd"),
    ),
    BenchmarkTask(
        cohort_id="ds000115",
        task_name="schizophrenia_vs_non_schizophrenia_family_context",
        task_scope="context_only_family_structure",
        headline_status="context_only",
        label_definition="schizophrenia versus pooled sibling/control family-context labels within ds000115",
        label_caveat=(
            "This cohort is tiny and family-structured. It is context only and should not be treated as headline "
            "evidence for schizophrenia separability."
        ),
        is_context_only=True,
        positive_diagnosis_groups=("schizophrenia",),
        negative_diagnosis_groups=("schizophrenia_sibling", "control_sibling", "control"),
    ),
    BenchmarkTask(
        cohort_id="tcp-ds005237",
        task_name="patient_vs_genpop_context_only",
        task_scope="context_only_broad_patient_vs_genpop",
        headline_status="context_only",
        label_definition="broad psychiatric patient versus general population control within TCP",
        label_caveat=(
            "Public TCP labels remain broad patient versus GenPop only, so this task is explicitly context only and "
            "not psychosis-specific evidence."
        ),
        is_context_only=True,
        positive_diagnosis_groups=("broad_psychiatric_patient",),
        negative_diagnosis_groups=("general_population_control",),
    ),
)


def benchmark_task_registry() -> tuple[BenchmarkTask, ...]:
    """Return the explicit benchmark task registry in deterministic order."""

    return BENCHMARK_TASKS


__all__ = ["BENCHMARK_TASKS", "BenchmarkTask", "benchmark_task_registry"]

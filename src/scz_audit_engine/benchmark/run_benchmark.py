"""Deterministic cross-sectional representation benchmark execution and reporting."""

from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

from .benchmark_tasks import BenchmarkTask, benchmark_task_registry
from .provenance import write_json_artifact, write_text_artifact
from .representations import (
    CLINICAL_SNAPSHOT_COLUMNS,
    COGNITION_PROFILE_COLUMNS,
    DIAGNOSIS_ANCHOR_COLUMNS,
    REPRESENTATION_FAMILY_FILES,
    SYMPTOM_PROFILE_COLUMNS,
)
from .run_manifest import build_run_manifest, utc_now_iso, write_run_manifest

TASK_RESULTS_NAME = "cross_sectional_task_results.csv"
SUMMARY_JSON_NAME = "cross_sectional_summary.json"
SUMMARY_MARKDOWN_NAME = "cross_sectional_summary.md"
TASK_REGISTRY_NAME = "benchmark_task_registry.json"
README_NAME = "README.md"
RUN_MANIFEST_NAME = "benchmark_run_benchmark_run_manifest.json"
BASELINE_FAMILY = "diagnosis_anchor"
EVAL_SPLITS = ("validation", "test")

REPRESENTATION_FEATURE_COLUMNS = {
    "diagnosis_anchor": (
        "diagnosis_is_case",
        "diagnosis_is_control",
        "diagnosis_psychosis_flag",
        "diagnosis_schizophrenia_flag",
        "diagnosis_bipolar_flag",
        "diagnosis_adhd_flag",
        "diagnosis_broad_psychiatric_flag",
        "diagnosis_general_population_flag",
        "diagnosis_family_context_flag",
    ),
    "symptom_profile": tuple(
        column
        for column in SYMPTOM_PROFILE_COLUMNS
        if column.startswith("symptom_")
    ),
    "cognition_profile": tuple(
        column
        for column in COGNITION_PROFILE_COLUMNS
        if column.startswith("cognition_")
    ),
    "clinical_snapshot": tuple(
        column
        for column in CLINICAL_SNAPSHOT_COLUMNS
        if column
        in {
            "symptom_burden_mean_z",
            "cognition_performance_mean_z",
            "functioning_status_mean_z",
            "treatment_exposure_count",
            "current_treatment_count",
            "modality_availability_count",
            "modality_type_count",
            "outcome_row_count",
        }
    ),
}


@dataclass(frozen=True, slots=True)
class RepresentationExample:
    """Representation row aligned to benchmark task labels and split assignments."""

    cohort_id: str
    subject_id: str
    visit_id: str
    split_name: str
    diagnosis_group: str
    feature_values: tuple[float, ...]
    has_usable_features: bool


@dataclass(frozen=True, slots=True)
class TaskResult:
    """Single task x family x split benchmark outcome."""

    task_name: str
    cohort_id: str
    task_scope: str
    headline_status: str
    label_definition: str
    is_context_only: bool
    representation_family: str
    split: str
    train_count: int
    eval_count: int
    class_balance: str
    accuracy: float | None
    balanced_accuracy: float | None
    evaluation_caveat: str
    skip_reason: str

    def is_evaluable(self) -> bool:
        return not self.skip_reason

    def to_row(self) -> dict[str, str]:
        return {
            "task_name": self.task_name,
            "cohort_id": self.cohort_id,
            "task_scope": self.task_scope,
            "headline_status": self.headline_status,
            "label_definition": self.label_definition,
            "is_context_only": _format_bool(self.is_context_only),
            "representation_family": self.representation_family,
            "split": self.split,
            "train_count": str(self.train_count),
            "eval_count": str(self.eval_count),
            "class_balance": self.class_balance,
            "accuracy": _format_metric(self.accuracy),
            "balanced_accuracy": _format_metric(self.balanced_accuracy),
            "evaluation_caveat": self.evaluation_caveat,
            "skip_reason": self.skip_reason,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkRunArtifacts:
    """Paths and counts emitted by the cross-sectional benchmark runner."""

    harmonized_root: Path
    representations_root: Path
    benchmarks_root: Path
    manifests_root: Path
    task_results_path: Path
    summary_json_path: Path
    summary_markdown_path: Path
    task_registry_path: Path
    readme_path: Path
    run_manifest_path: Path
    evaluable_result_count: int
    skipped_result_count: int
    recommendation: str

    def to_summary(self) -> dict[str, object]:
        return {
            "harmonized_dir": str(self.harmonized_root),
            "representations_dir": str(self.representations_root),
            "benchmarks_dir": str(self.benchmarks_root),
            "manifests_dir": str(self.manifests_root),
            "task_results": str(self.task_results_path),
            "summary_json": str(self.summary_json_path),
            "summary_markdown": str(self.summary_markdown_path),
            "task_registry": str(self.task_registry_path),
            "run_manifest": str(self.run_manifest_path),
            "evaluable_results": self.evaluable_result_count,
            "skipped_results": self.skipped_result_count,
            "recommendation": self.recommendation,
        }


def run_cross_sectional_benchmark(
    *,
    harmonized_root: str | Path,
    representations_root: str | Path,
    benchmarks_root: str | Path,
    manifests_root: str | Path,
    repo_root: str | Path | None,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
) -> BenchmarkRunArtifacts:
    """Run the current cross-sectional representation benchmark and write stable artifacts."""

    harmonized_path = Path(harmonized_root).resolve()
    representations_path = Path(representations_root).resolve()
    benchmarks_path = Path(benchmarks_root).resolve()
    manifests_path = Path(manifests_root).resolve()
    generated_at = utc_now_iso()

    required_inputs = {
        "diagnoses": harmonized_path / "diagnoses.csv",
        "split_assignments": harmonized_path / "split_assignments.csv",
        "representation_manifest": representations_path / "representation_manifest.json",
        **{
            family_name: representations_path / filename
            for family_name, filename in REPRESENTATION_FAMILY_FILES.items()
        },
    }
    for input_path in required_inputs.values():
        if not input_path.exists():
            raise FileNotFoundError(f"Missing benchmark input at {input_path}")

    tasks = benchmark_task_registry()
    diagnoses = _read_csv_rows(required_inputs["diagnoses"])
    split_assignments = _read_csv_rows(required_inputs["split_assignments"])
    representation_manifest = json.loads(required_inputs["representation_manifest"].read_text(encoding="utf-8"))

    diagnosis_by_visit = _build_diagnosis_lookup(diagnoses)
    split_by_subject = _build_split_lookup(split_assignments)
    family_examples = {
        family_name: _load_representation_examples(
            family_name=family_name,
            rows=_read_csv_rows(required_inputs[family_name]),
            diagnosis_by_visit=diagnosis_by_visit,
            split_by_subject=split_by_subject,
        )
        for family_name in REPRESENTATION_FAMILY_FILES
    }

    results: list[TaskResult] = []
    for task in tasks:
        for family_name in REPRESENTATION_FAMILY_FILES:
            results.extend(
                _evaluate_task_family(
                    task=task,
                    family_name=family_name,
                    examples=family_examples[family_name],
                )
            )

    task_results_path = _write_task_results(results, benchmarks_path / TASK_RESULTS_NAME)
    task_registry_path = write_json_artifact(
        {"tasks": [task.to_dict() for task in tasks]},
        benchmarks_path / TASK_REGISTRY_NAME,
    )
    summary_payload = _build_summary_payload(
        tasks=tasks,
        results=results,
        representation_manifest=representation_manifest,
        seed=seed,
    )
    summary_json_path = write_json_artifact(summary_payload, benchmarks_path / SUMMARY_JSON_NAME)
    summary_markdown_path = write_text_artifact(
        _build_summary_markdown(summary_payload),
        benchmarks_path / SUMMARY_MARKDOWN_NAME,
    )
    readme_path = write_text_artifact(_build_benchmarks_readme(), benchmarks_path / README_NAME)

    run_manifest_path = write_run_manifest(
        build_run_manifest(
            dataset_source="benchmark",
            command=command,
            git_sha=git_sha,
            seed=seed,
            repo_root=repo_root,
            output_paths={
                "task_results": task_results_path,
                "summary_json": summary_json_path,
                "summary_markdown": summary_markdown_path,
                "task_registry": task_registry_path,
                "benchmarks_readme": readme_path,
            },
            timestamp=generated_at,
        ),
        manifests_path / RUN_MANIFEST_NAME,
    )

    evaluable_result_count = sum(1 for result in results if result.is_evaluable())
    skipped_result_count = len(results) - evaluable_result_count
    return BenchmarkRunArtifacts(
        harmonized_root=harmonized_path,
        representations_root=representations_path,
        benchmarks_root=benchmarks_path,
        manifests_root=manifests_path,
        task_results_path=task_results_path,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        task_registry_path=task_registry_path,
        readme_path=readme_path,
        run_manifest_path=run_manifest_path,
        evaluable_result_count=evaluable_result_count,
        skipped_result_count=skipped_result_count,
        recommendation=summary_payload["recommendation"]["decision"],
    )


def _evaluate_task_family(
    *,
    task: BenchmarkTask,
    family_name: str,
    examples: tuple[RepresentationExample, ...],
) -> list[TaskResult]:
    labeled_examples = [
        (example, task.label_for_diagnosis_group(example.diagnosis_group))
        for example in examples
        if example.cohort_id == task.cohort_id
    ]
    usable_examples = [
        (example, label)
        for example, label in labeled_examples
        if label is not None and example.has_usable_features
    ]
    train_examples = [
        (example, int(label))
        for example, label in usable_examples
        if example.split_name == "train"
    ]
    train_count = len(train_examples)
    train_balance = Counter(label for _, label in train_examples)
    train_skip_reason = ""
    if train_count == 0:
        train_skip_reason = "No labeled train rows with usable feature support."
    elif len(train_balance) < 2:
        train_skip_reason = (
            "Train split has one class only "
            f"({_format_class_balance(train_balance)})."
        )

    centroid_by_label = _fit_nearest_centroid(train_examples) if not train_skip_reason else {}
    results: list[TaskResult] = []
    for split_name in EVAL_SPLITS:
        eval_examples = [
            (example, int(label))
            for example, label in usable_examples
            if example.split_name == split_name
        ]
        eval_count = len(eval_examples)
        eval_balance = Counter(label for _, label in eval_examples)
        skip_reason = train_skip_reason
        if not skip_reason and eval_count == 0:
            skip_reason = f"No labeled {split_name} rows with usable feature support."

        accuracy = None
        balanced_accuracy = None
        evaluation_caveat = ""
        if not skip_reason:
            labels = [label for _, label in eval_examples]
            predictions = [
                _predict_label(example.feature_values, centroid_by_label)
                for example, _ in eval_examples
            ]
            accuracy = _compute_accuracy(labels, predictions)
            balanced_accuracy = _compute_balanced_accuracy(labels, predictions)
            if len(eval_balance) == 1:
                evaluation_caveat = "single_class_eval"

        results.append(
            TaskResult(
                task_name=task.task_name,
                cohort_id=task.cohort_id,
                task_scope=task.task_scope,
                headline_status=task.headline_status,
                label_definition=task.label_definition,
                is_context_only=task.is_context_only,
                representation_family=family_name,
                split=split_name,
                train_count=train_count,
                eval_count=eval_count,
                class_balance=_format_class_balance(eval_balance),
                accuracy=accuracy,
                balanced_accuracy=balanced_accuracy,
                evaluation_caveat=evaluation_caveat,
                skip_reason=skip_reason,
            )
        )
    return results


def _load_representation_examples(
    *,
    family_name: str,
    rows: list[dict[str, str]],
    diagnosis_by_visit: dict[tuple[str, str, str], str],
    split_by_subject: dict[tuple[str, str], str],
) -> tuple[RepresentationExample, ...]:
    feature_columns = REPRESENTATION_FEATURE_COLUMNS[family_name]
    examples: list[RepresentationExample] = []
    for row in rows:
        visit_key = (row["cohort_id"], row["subject_id"], row["visit_id"])
        subject_key = (row["cohort_id"], row["subject_id"])
        if visit_key not in diagnosis_by_visit:
            raise KeyError(f"Missing diagnosis row for representation visit {visit_key}")
        if subject_key not in split_by_subject:
            raise KeyError(f"Missing split assignment for representation subject {subject_key}")
        split_name = split_by_subject[subject_key]
        raw_values = tuple(row.get(column, "") for column in feature_columns)
        examples.append(
            RepresentationExample(
                cohort_id=row["cohort_id"],
                subject_id=row["subject_id"],
                visit_id=row["visit_id"],
                split_name=split_name,
                diagnosis_group=diagnosis_by_visit[visit_key],
                feature_values=tuple(0.0 if value == "" else float(value) for value in raw_values),
                has_usable_features=any(value != "" for value in raw_values),
            )
        )
    return tuple(examples)


def _build_diagnosis_lookup(rows: list[dict[str, str]]) -> dict[tuple[str, str, str], str]:
    lookup: dict[tuple[str, str, str], str] = {}
    for row in rows:
        key = (row["cohort_id"], row["subject_id"], row["visit_id"])
        if key not in lookup or row.get("is_primary_diagnosis", "").lower() == "true":
            lookup[key] = row["diagnosis_group"]
    return lookup


def _build_split_lookup(rows: list[dict[str, str]]) -> dict[tuple[str, str], str]:
    return {
        (row["cohort_id"], row["subject_id"]): row["split_name"]
        for row in rows
    }


def _fit_nearest_centroid(
    examples: list[tuple[RepresentationExample, int]],
) -> dict[int, tuple[float, ...]]:
    by_label: dict[int, list[tuple[float, ...]]] = defaultdict(list)
    for example, label in examples:
        by_label[label].append(example.feature_values)

    centroids: dict[int, tuple[float, ...]] = {}
    for label, vectors in by_label.items():
        centroids[label] = tuple(
            sum(values) / len(values)
            for values in zip(*vectors)
        )
    return centroids


def _predict_label(
    feature_values: tuple[float, ...],
    centroid_by_label: dict[int, tuple[float, ...]],
) -> int:
    return min(
        centroid_by_label,
        key=lambda label: (_squared_distance(feature_values, centroid_by_label[label]), label),
    )


def _squared_distance(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    return sum(math.pow(left_value - right_value, 2.0) for left_value, right_value in zip(left, right))


def _compute_accuracy(labels: list[int], predictions: list[int]) -> float:
    correct = sum(1 for label, prediction in zip(labels, predictions) if label == prediction)
    return correct / len(labels)


def _compute_balanced_accuracy(labels: list[int], predictions: list[int]) -> float:
    recalls: list[float] = []
    for label in sorted(set(labels)):
        true_positive = sum(
            1
            for observed, predicted in zip(labels, predictions)
            if observed == label and predicted == label
        )
        false_negative = sum(
            1
            for observed, predicted in zip(labels, predictions)
            if observed == label and predicted != label
        )
        recalls.append(true_positive / (true_positive + false_negative))
    return sum(recalls) / len(recalls)


def _write_task_results(results: list[TaskResult], destination: Path) -> Path:
    fieldnames = [
        "task_name",
        "cohort_id",
        "task_scope",
        "headline_status",
        "label_definition",
        "is_context_only",
        "representation_family",
        "split",
        "train_count",
        "eval_count",
        "class_balance",
        "accuracy",
        "balanced_accuracy",
        "evaluation_caveat",
        "skip_reason",
    ]
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for result in results:
            writer.writerow(result.to_row())
    return destination


def _build_summary_payload(
    *,
    tasks: tuple[BenchmarkTask, ...],
    results: list[TaskResult],
    representation_manifest: dict[str, object],
    seed: int,
) -> dict[str, object]:
    headline_tasks = [task for task in tasks if not task.is_context_only]
    context_tasks = [task for task in tasks if task.is_context_only]
    headline_summary = [_build_task_summary(task, results) for task in headline_tasks]
    context_summary = [_build_task_summary(task, results) for task in context_tasks]
    family_evaluability = _build_family_evaluability(results)
    baseline_comparison = _build_baseline_comparison(results, tasks=headline_tasks)
    recommendation = _build_recommendation(
        headline_tasks=headline_tasks,
        results=results,
        baseline_comparison=baseline_comparison,
    )

    return {
        "benchmark_scope": "cross_sectional_representation_only",
        "decision": "narrow-go",
        "claim_level": "narrow_outcome_benchmark",
        "recommended_next_step_before_benchmark": "continue_cross_sectional_representation_only",
        "seed": seed,
        "benchmark_method": {
            "classifier": "train_only_nearest_centroid",
            "family_feature_columns": {
                family_name: list(columns)
                for family_name, columns in REPRESENTATION_FEATURE_COLUMNS.items()
            },
            "missing_value_policy": (
                "Rows with no substantive family features are skipped. Remaining blank feature values are zero-filled "
                "within the selected family columns for deterministic distance calculations."
            ),
            "metric_caveat": (
                "Balanced accuracy is computed over the classes observed in each evaluation split. On single-class "
                "validation or test splits it collapses to observed-class recall and should not be treated as strong "
                "separability evidence."
            ),
        },
        "representation_families": list(REPRESENTATION_FAMILY_FILES),
        "tasks": [task.to_dict() for task in tasks],
        "headline_tasks": headline_summary,
        "context_only_tasks": context_summary,
        "family_evaluability": family_evaluability,
        "baseline_comparison": baseline_comparison,
        "results_overview": {
            "total_results": len(results),
            "evaluable_results": sum(1 for result in results if result.is_evaluable()),
            "skipped_results": sum(1 for result in results if not result.is_evaluable()),
            "single_class_eval_results": sum(1 for result in results if result.evaluation_caveat == "single_class_eval"),
        },
        "recommendation": recommendation,
        "claim_boundary_statement": representation_manifest.get(
            "claim_boundary_statement",
            "This benchmark remains cross-sectional representation comparison only.",
        ),
        "current_limitations": list(representation_manifest.get("current_limitations", [])),
    }


def _build_task_summary(task: BenchmarkTask, results: list[TaskResult]) -> dict[str, object]:
    task_results = [result for result in results if result.task_name == task.task_name]
    evaluable_results = [result for result in task_results if result.is_evaluable()]
    skipped_results = [result for result in task_results if not result.is_evaluable()]
    evaluable_families = sorted({result.representation_family for result in evaluable_results})
    best_family_by_split: dict[str, list[str]] = {}
    if not task.is_context_only:
        for split_name in EVAL_SPLITS:
            split_results = [
                result
                for result in evaluable_results
                if result.split == split_name
                and result.balanced_accuracy is not None
                and result.evaluation_caveat != "single_class_eval"
            ]
            if not split_results:
                continue
            best_score = max(result.balanced_accuracy for result in split_results if result.balanced_accuracy is not None)
            best_family_by_split[split_name] = [
                result.representation_family
                for result in split_results
                if result.balanced_accuracy == best_score
            ]

    split_rows = []
    for split_name in EVAL_SPLITS:
        split_results = [result for result in task_results if result.split == split_name]
        split_rows.append(
            {
                "split": split_name,
                "evaluable_families": [
                    result.representation_family
                    for result in split_results
                    if result.is_evaluable()
                ],
                "single_class_eval_families": [
                    result.representation_family
                    for result in split_results
                    if result.evaluation_caveat == "single_class_eval"
                ],
                "skipped_families": {
                    result.representation_family: result.skip_reason
                    for result in split_results
                    if result.skip_reason
                },
            }
        )

    return {
        "cohort_id": task.cohort_id,
        "task_name": task.task_name,
        "task_scope": task.task_scope,
        "headline_status": task.headline_status,
        "label_definition": task.label_definition,
        "label_caveat": task.label_caveat,
        "is_context_only": task.is_context_only,
        "evaluable_families": evaluable_families,
        "best_family_by_split": best_family_by_split,
        "evaluable_result_count": len(evaluable_results),
        "skipped_result_count": len(skipped_results),
        "split_rows": split_rows,
    }


def _build_family_evaluability(results: list[TaskResult]) -> dict[str, dict[str, object]]:
    summary: dict[str, dict[str, object]] = {}
    for family_name in REPRESENTATION_FAMILY_FILES:
        family_results = [result for result in results if result.representation_family == family_name]
        skip_reasons = sorted({result.skip_reason for result in family_results if result.skip_reason})
        summary[family_name] = {
            "evaluable_results": sum(1 for result in family_results if result.is_evaluable()),
            "skipped_results": sum(1 for result in family_results if not result.is_evaluable()),
            "tasks_with_any_evaluable_split": sorted(
                {
                    result.task_name
                    for result in family_results
                    if result.is_evaluable()
                }
            ),
            "skip_reasons": skip_reasons,
        }
    return summary


def _build_baseline_comparison(
    results: list[TaskResult],
    *,
    tasks: list[BenchmarkTask],
) -> dict[str, object]:
    task_names = {task.task_name for task in tasks}
    baseline_results = {
        (result.task_name, result.split): result
        for result in results
        if result.representation_family == BASELINE_FAMILY
        and result.task_name in task_names
        and result.is_evaluable()
        and result.evaluation_caveat != "single_class_eval"
    }
    comparable_headline_pairs = [
        {"task_name": task_name, "split": split_name}
        for task_name, split_name in sorted(baseline_results)
    ]
    families_with_full_meaningful_headline_coverage: list[str] = []
    beating_families: list[str] = []
    for family_name in REPRESENTATION_FAMILY_FILES:
        if family_name == BASELINE_FAMILY:
            continue
        family_results = {
            (result.task_name, result.split): result
            for result in results
            if result.representation_family == family_name
            and result.task_name in task_names
            and result.is_evaluable()
            and result.evaluation_caveat != "single_class_eval"
        }
        if not baseline_results or set(family_results) != set(baseline_results):
            continue
        families_with_full_meaningful_headline_coverage.append(family_name)
        if all(
            (family_results[key].balanced_accuracy or 0.0) >= (baseline_results[key].balanced_accuracy or 0.0)
            for key in baseline_results
        ) and any(
            (family_results[key].balanced_accuracy or 0.0) > (baseline_results[key].balanced_accuracy or 0.0)
            for key in baseline_results
        ):
            beating_families.append(family_name)

    if not comparable_headline_pairs:
        summary = (
            "No headline task produced a fully comparable non-single-class diagnosis_anchor result, so there is no "
            "meaningful baseline race to call."
        )
    elif not families_with_full_meaningful_headline_coverage:
        summary = (
            "No non-baseline family has fully comparable non-single-class headline coverage against diagnosis_anchor."
        )
    elif not beating_families:
        summary = (
            "No non-baseline family meaningfully beat diagnosis_anchor across fully comparable non-single-class "
            "headline task/split pairs."
        )
    else:
        summary = "Some non-baseline families beat diagnosis_anchor on headline tasks."

    return {
        "baseline_family": BASELINE_FAMILY,
        "comparable_headline_pairs": comparable_headline_pairs,
        "families_with_full_meaningful_headline_coverage": families_with_full_meaningful_headline_coverage,
        "families_beating_baseline_on_headline_tasks": beating_families,
        "summary": summary,
    }


def _build_recommendation(
    *,
    headline_tasks: list[BenchmarkTask],
    results: list[TaskResult],
    baseline_comparison: dict[str, object],
) -> dict[str, object]:
    headline_task_names = {task.task_name for task in headline_tasks}
    headline_results = [
        result
        for result in results
        if result.task_name in headline_task_names and result.is_evaluable()
    ]
    headline_non_single_class_results = [
        result
        for result in headline_results
        if result.evaluation_caveat != "single_class_eval"
    ]
    beating_families = list(baseline_comparison["families_beating_baseline_on_headline_tasks"])
    if beating_families:
        decision = "continue_to_narrow_model_comparison_pr"
        summary = (
            "At least one non-baseline family beats diagnosis_anchor across fully comparable non-single-class "
            "headline holdouts. The repo can justify one more narrow comparison/model PR."
        )
    elif headline_results:
        decision = "continue_only_as_descriptive_artifact_repo"
        summary = (
            "Headline comparisons do not clear the diagnosis_anchor baseline on fully comparable non-single-class "
            "holdouts. Keep the repo at the descriptive artifact layer rather than pulling model comparison forward."
        )
    else:
        decision = "pause"
        summary = (
            "No headline task produced an evaluable holdout comparison under the frozen splits. Pause benchmark "
            "expansion at the current artifact layer."
        )

    rationale: list[str] = []
    for task in headline_tasks:
        task_results = [result for result in results if result.task_name == task.task_name]
        task_evaluable_results = [result for result in task_results if result.is_evaluable()]
        task_meaningful_results = [
            result
            for result in task_evaluable_results
            if result.evaluation_caveat != "single_class_eval"
        ]
        if not task_evaluable_results:
            rationale.append(
                f"{task.cohort_id} / {task.task_name} has no evaluable holdout comparison under the frozen split contract."
            )
        elif not task_meaningful_results:
            rationale.append(
                f"{task.cohort_id} / {task.task_name} only yields single-class holdout rows, so it does not provide meaningful headline evidence."
            )

    if beating_families:
        rationale.append(
            "Families clearing the baseline on fully comparable non-single-class headline pairs: "
            + ", ".join(beating_families)
            + "."
        )
    elif baseline_comparison["comparable_headline_pairs"]:
        rationale.append(
            "No non-baseline family beats diagnosis_anchor across fully comparable non-single-class headline task/split pairs."
        )
    else:
        rationale.append(
            "There are no fully comparable non-single-class headline task/split pairs for a meaningful diagnosis_anchor baseline race."
        )
    return {
        "decision": decision,
        "summary": summary,
        "rationale": rationale,
    }


def _build_summary_markdown(summary_payload: dict[str, object]) -> str:
    headline_tasks = summary_payload["headline_tasks"]
    context_tasks = summary_payload["context_only_tasks"]
    baseline_comparison = summary_payload["baseline_comparison"]
    recommendation = summary_payload["recommendation"]

    lines = [
        "# Cross-Sectional Representation Benchmark",
        "",
        "## Scope",
        "",
        "- This report stays in the current cross-sectional representation lane only.",
        "- The repo posture remains `narrow-go` at claim level `narrow_outcome_benchmark`.",
        "- Rows with no substantive family support were skipped. Remaining blank feature values were zero-filled within each family.",
        "- Balanced accuracy on a single-class validation or test split collapses to observed-class recall and is low-confidence context only.",
        "",
        "## Headline Tasks",
        "",
    ]
    lines.extend(_render_task_markdown_rows(headline_tasks))
    lines.extend(
        [
            "",
            "## Context-Only Tasks",
            "",
        ]
    )
    lines.extend(_render_task_markdown_rows(context_tasks))
    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            f"- Recommendation: `{recommendation['decision']}`",
            f"- Reason: {recommendation['summary']}",
            f"- Baseline check: {baseline_comparison['summary']}",
        ]
    )
    return "\n".join(lines)


def _render_task_markdown_rows(task_rows: list[dict[str, object]]) -> list[str]:
    lines: list[str] = []
    for task_row in task_rows:
        lines.append(
            f"- `{task_row['cohort_id']}` / `{task_row['task_name']}`: "
            f"evaluable families = {', '.join(task_row['evaluable_families']) or 'none'}."
        )
        lines.append(f"  Caveat: {task_row['label_caveat']}")
        for split_row in task_row["split_rows"]:
            split_name = split_row["split"]
            evaluable_families = ", ".join(split_row["evaluable_families"]) or "none"
            single_class = ", ".join(split_row["single_class_eval_families"]) or "none"
            skipped_pairs = ", ".join(
                f"{family} ({reason})"
                for family, reason in split_row["skipped_families"].items()
            ) or "none"
            lines.append(
                f"  {split_name}: evaluable = {evaluable_families}; single-class eval = {single_class}; skipped = {skipped_pairs}."
            )
    return lines


def _build_benchmarks_readme() -> str:
    return """# processed/benchmark/benchmarks

Store deterministic cross-sectional benchmark outputs here.

Current expected outputs from `scz-audit benchmark run-benchmark`:

- `benchmark_task_registry.json`
- `cross_sectional_task_results.csv`
- `cross_sectional_summary.json`
- `cross_sectional_summary.md`

These checked-in artifacts should stay deterministic across reruns and should
not embed machine-local paths or volatile timestamps.
"""


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _format_metric(value: float | None) -> str:
    return "" if value is None else f"{value:.6f}"


def _format_class_balance(counter: Counter[int]) -> str:
    return f"negative={counter.get(0, 0)},positive={counter.get(1, 0)}"


def _format_bool(value: bool) -> str:
    return "true" if value else "false"


__all__ = [
    "BASELINE_FAMILY",
    "BenchmarkRunArtifacts",
    "README_NAME",
    "RUN_MANIFEST_NAME",
    "SUMMARY_JSON_NAME",
    "SUMMARY_MARKDOWN_NAME",
    "TASK_REGISTRY_NAME",
    "TASK_RESULTS_NAME",
    "run_cross_sectional_benchmark",
]

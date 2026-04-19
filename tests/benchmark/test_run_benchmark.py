import csv
import json
import shutil
from pathlib import Path

from scz_audit_engine.benchmark.benchmark_tasks import benchmark_task_registry
from scz_audit_engine.benchmark.harmonize import run_benchmark_harmonization
from scz_audit_engine.benchmark.representations import run_benchmark_representation_build
from scz_audit_engine.benchmark.run_benchmark import (
    TaskResult,
    _build_baseline_comparison,
    _build_recommendation,
    run_cross_sectional_benchmark,
)

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "benchmark_sources"


def test_benchmark_task_registry_captures_headline_and_context_boundaries() -> None:
    tasks = benchmark_task_registry()

    assert [(task.cohort_id, task.task_name) for task in tasks] == [
        ("fep-ds003944", "psychosis_vs_control"),
        ("ucla-cnp-ds000030", "schizophrenia_vs_non_schizophrenia_context"),
        ("ds000115", "schizophrenia_vs_non_schizophrenia_family_context"),
        ("tcp-ds005237", "patient_vs_genpop_context_only"),
    ]
    assert [task.headline_status for task in tasks] == [
        "headline",
        "headline",
        "context_only",
        "context_only",
    ]
    assert [task.is_context_only for task in tasks] == [False, False, True, True]
    assert tasks[1].negative_diagnosis_groups == ("control", "bipolar_disorder", "adhd")
    assert tasks[2].label_for_diagnosis_group("schizophrenia") == 1
    assert tasks[2].label_for_diagnosis_group("control_sibling") == 0
    assert tasks[3].label_for_diagnosis_group("psychosis") is None


def test_run_benchmark_emits_outputs_and_records_skip_boundaries(tmp_path) -> None:
    harmonized_root, representations_root, manifests_root = _prepare_representation_fixture(tmp_path)
    benchmarks_root = tmp_path / "benchmarks"

    results = run_cross_sectional_benchmark(
        harmonized_root=harmonized_root,
        representations_root=representations_root,
        benchmarks_root=benchmarks_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "run-benchmark"],
        git_sha="abc1234",
        seed=1729,
    )

    assert results.evaluable_result_count == 11
    assert results.skipped_result_count == 21
    assert results.recommendation == "continue_only_as_descriptive_artifact_repo"
    assert Path(results.task_results_path).exists()
    assert Path(results.summary_json_path).exists()
    assert Path(results.summary_markdown_path).exists()
    assert Path(results.task_registry_path).exists()
    assert Path(results.readme_path).exists()
    assert Path(results.run_manifest_path).exists()

    task_rows = _read_csv_rows(results.task_results_path)
    assert len(task_rows) == 32
    assert any(
        row["task_name"] == "psychosis_vs_control"
        and row["representation_family"] == "diagnosis_anchor"
        and row["split"] == "validation"
        and "Train split has one class only" in row["skip_reason"]
        for row in task_rows
    )
    assert any(
        row["task_name"] == "patient_vs_genpop_context_only"
        and row["representation_family"] == "diagnosis_anchor"
        and row["split"] == "validation"
        and row["accuracy"] == "1.000000"
        and row["evaluation_caveat"] == "single_class_eval"
        for row in task_rows
    )
    assert any(
        row["task_name"] == "schizophrenia_vs_non_schizophrenia_context"
        and row["representation_family"] == "cognition_profile"
        and row["split"] == "test"
        and row["accuracy"] == "0.000000"
        for row in task_rows
    )

    summary_payload = json.loads(results.summary_json_path.read_text(encoding="utf-8"))
    assert summary_payload["recommendation"]["decision"] == "continue_only_as_descriptive_artifact_repo"
    assert summary_payload["baseline_comparison"]["comparable_headline_pairs"] == []
    assert summary_payload["baseline_comparison"]["families_beating_baseline_on_headline_tasks"] == []
    assert [task["task_name"] for task in summary_payload["headline_tasks"]] == [
        "psychosis_vs_control",
        "schizophrenia_vs_non_schizophrenia_context",
    ]
    assert [task["task_name"] for task in summary_payload["context_only_tasks"]] == [
        "schizophrenia_vs_non_schizophrenia_family_context",
        "patient_vs_genpop_context_only",
    ]
    assert all(task["headline_status"] == "headline" for task in summary_payload["headline_tasks"])
    assert all(task["is_context_only"] is True for task in summary_payload["context_only_tasks"])
    assert all(task["best_family_by_split"] == {} for task in summary_payload["headline_tasks"])
    assert all(task["best_family_by_split"] == {} for task in summary_payload["context_only_tasks"])

    task_registry_payload = json.loads(results.task_registry_path.read_text(encoding="utf-8"))
    assert [task["headline_status"] for task in task_registry_payload["tasks"]] == [
        "headline",
        "headline",
        "context_only",
        "context_only",
    ]

    summary_json_text = results.summary_json_path.read_text(encoding="utf-8")
    summary_markdown_text = results.summary_markdown_path.read_text(encoding="utf-8")
    assert str(tmp_path) not in summary_json_text
    assert str(tmp_path) not in summary_markdown_text
    assert "generated_at" not in summary_json_text

    run_manifest = json.loads(results.run_manifest_path.read_text(encoding="utf-8"))
    assert run_manifest["command"] == ["scz-audit", "benchmark", "run-benchmark"]


def test_baseline_comparison_requires_full_meaningful_headline_coverage() -> None:
    first_task, second_task = benchmark_task_registry()[:2]
    results = [
        _task_result(first_task, family_name="diagnosis_anchor", split="test", balanced_accuracy=0.7),
        _task_result(second_task, family_name="diagnosis_anchor", split="validation", balanced_accuracy=0.6),
        _task_result(second_task, family_name="symptom_profile", split="validation", balanced_accuracy=0.9),
        _task_result(first_task, family_name="clinical_snapshot", split="test", balanced_accuracy=0.8),
        _task_result(second_task, family_name="clinical_snapshot", split="validation", balanced_accuracy=0.7),
    ]

    comparison = _build_baseline_comparison(results, tasks=[first_task, second_task])

    assert comparison["comparable_headline_pairs"] == [
        {"task_name": "psychosis_vs_control", "split": "test"},
        {"task_name": "schizophrenia_vs_non_schizophrenia_context", "split": "validation"},
    ]
    assert comparison["families_with_full_meaningful_headline_coverage"] == ["clinical_snapshot"]
    assert comparison["families_beating_baseline_on_headline_tasks"] == ["clinical_snapshot"]


def test_recommendation_requires_non_baseline_family_to_clear_baseline() -> None:
    first_task = benchmark_task_registry()[0]
    results = [
        _task_result(first_task, family_name="diagnosis_anchor", split="test", balanced_accuracy=0.8),
        _task_result(first_task, family_name="clinical_snapshot", split="test", balanced_accuracy=0.6),
    ]

    comparison = _build_baseline_comparison(results, tasks=[first_task])
    recommendation = _build_recommendation(
        headline_tasks=[first_task],
        results=results,
        baseline_comparison=comparison,
    )

    assert comparison["families_beating_baseline_on_headline_tasks"] == []
    assert recommendation["decision"] == "continue_only_as_descriptive_artifact_repo"
    assert "do not clear the diagnosis_anchor baseline" in recommendation["summary"]


def test_run_benchmark_is_deterministic_and_location_independent(tmp_path) -> None:
    first_raw = tmp_path / "first-raw"
    second_raw = tmp_path / "second-raw"
    shutil.copytree(FIXTURE_ROOT, first_raw)
    shutil.copytree(FIXTURE_ROOT, second_raw)

    first = _run_full_benchmark_stack(tmp_path / "first", first_raw)
    second = _run_full_benchmark_stack(tmp_path / "second", second_raw)

    assert first.task_results_path.read_text(encoding="utf-8") == second.task_results_path.read_text(encoding="utf-8")
    assert first.summary_json_path.read_text(encoding="utf-8") == second.summary_json_path.read_text(encoding="utf-8")
    assert first.summary_markdown_path.read_text(encoding="utf-8") == second.summary_markdown_path.read_text(
        encoding="utf-8"
    )
    assert first.task_registry_path.read_text(encoding="utf-8") == second.task_registry_path.read_text(
        encoding="utf-8"
    )
    assert b"\r\n" not in first.task_results_path.read_bytes()
    assert str(tmp_path) not in first.summary_json_path.read_text(encoding="utf-8")


def _run_full_benchmark_stack(base_dir: Path, raw_root: Path):
    harmonized_root = base_dir / "harmonized"
    manifests_root = base_dir / "manifests"
    representations_root = base_dir / "representations"
    benchmarks_root = base_dir / "benchmarks"

    run_benchmark_harmonization(
        raw_root=raw_root,
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize", "--raw-root", str(raw_root)],
        git_sha="abc1234",
        seed=1729,
    )
    run_benchmark_representation_build(
        harmonized_root=harmonized_root,
        representations_root=representations_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "build-representations"],
        git_sha="abc1234",
        seed=1729,
    )
    return run_cross_sectional_benchmark(
        harmonized_root=harmonized_root,
        representations_root=representations_root,
        benchmarks_root=benchmarks_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "run-benchmark"],
        git_sha="abc1234",
        seed=1729,
    )


def _prepare_representation_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    harmonized_root = tmp_path / "harmonized"
    manifests_root = tmp_path / "manifests"
    representations_root = tmp_path / "representations"
    run_benchmark_harmonization(
        raw_root=FIXTURE_ROOT,
        harmonized_root=harmonized_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "harmonize", "--raw-root", str(FIXTURE_ROOT)],
        git_sha="abc1234",
        seed=1729,
    )
    run_benchmark_representation_build(
        harmonized_root=harmonized_root,
        representations_root=representations_root,
        manifests_root=manifests_root,
        repo_root=Path(__file__).resolve().parents[2],
        command=["scz-audit", "benchmark", "build-representations"],
        git_sha="abc1234",
        seed=1729,
    )
    return harmonized_root, representations_root, manifests_root


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _task_result(
    task,
    *,
    family_name: str,
    split: str,
    balanced_accuracy: float,
    evaluation_caveat: str = "",
) -> TaskResult:
    return TaskResult(
        task_name=task.task_name,
        cohort_id=task.cohort_id,
        task_scope=task.task_scope,
        headline_status=task.headline_status,
        label_definition=task.label_definition,
        is_context_only=task.is_context_only,
        representation_family=family_name,
        split=split,
        train_count=10,
        eval_count=10,
        class_balance="negative=5,positive=5",
        accuracy=balanced_accuracy,
        balanced_accuracy=balanced_accuracy,
        evaluation_caveat=evaluation_caveat,
        skip_reason="",
    )

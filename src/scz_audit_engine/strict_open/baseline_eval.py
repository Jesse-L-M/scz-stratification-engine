"""Baseline training and evaluation for strict-open."""

from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .baselines import (
    BASELINE_FAMILY_NAMES,
    apply_linear_calibrator,
    baseline_family_spec,
    build_family_state,
    compute_signal,
    fit_linear_calibrator,
    list_baseline_family_specs,
    unsupported_target_reason,
)
from .provenance import write_json_artifact
from .run_manifest import build_run_manifest, write_run_manifest
from .splits import SPLIT_ORDER
from .targets import TARGET_LABELS

BASELINE_PREDICTION_COLUMNS = (
    "baseline_name",
    "baseline_label",
    "target_name",
    "split",
    "subject_id",
    "visit_id",
    "diagnosis",
    "site_id",
    "feature_family_available_count",
    "missingness_burden",
    "signal_value",
    "prediction",
    "target_value",
)
_MISSINGNESS_HEAVY_THRESHOLD = 0.5


def run_strict_open_baseline_training(
    *,
    features_root: str | Path,
    targets_root: str | Path,
    splits_root: str | Path,
    manifests_root: str | Path,
    models_root: str | Path,
    reports_root: str | Path,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
) -> dict[str, str]:
    """Train deterministic baseline calibrators and evaluate them by split."""

    features_path = Path(features_root)
    targets_path = Path(targets_root)
    splits_path = Path(splits_root)
    manifests_path = Path(manifests_root)
    models_path = Path(models_root)
    reports_path = Path(reports_root)

    visit_features_path = features_path / "visit_features.csv"
    feature_manifest_path = features_path / "feature_manifest.json"
    derived_targets_path = targets_path / "derived_targets.csv"
    target_manifest_path = targets_path / "target_manifest.json"
    split_assignments_path = splits_path / "split_assignments.csv"
    split_manifest_path = splits_path / "split_manifest.json"

    required_paths = (
        visit_features_path,
        feature_manifest_path,
        derived_targets_path,
        target_manifest_path,
        split_assignments_path,
        split_manifest_path,
    )
    for required_path in required_paths:
        if not required_path.exists():
            raise FileNotFoundError(f"Missing strict-open input at {required_path}")

    feature_rows = _read_csv_rows(visit_features_path)
    target_rows = _read_csv_rows(derived_targets_path)
    split_rows = _read_csv_rows(split_assignments_path)
    feature_manifest = _load_json_if_exists(feature_manifest_path)
    target_manifest = _load_json_if_exists(target_manifest_path)
    split_manifest = _load_json_if_exists(split_manifest_path)

    split_by_subject = {row["subject_id"]: row["split"] for row in split_rows}
    feature_by_key: dict[tuple[str, str], dict[str, str]] = {}
    split_validation_mismatches: list[dict[str, str | None]] = []
    for feature_row in feature_rows:
        visit_key = (feature_row["subject_id"], feature_row["visit_id"])
        if visit_key in feature_by_key:
            raise ValueError(f"Duplicate feature row for {visit_key}")
        feature_by_key[visit_key] = feature_row
        expected_split = split_by_subject.get(feature_row["subject_id"])
        if expected_split != feature_row.get("split"):
            split_validation_mismatches.append(
                {
                    "subject_id": feature_row["subject_id"],
                    "visit_id": feature_row["visit_id"],
                    "feature_split": feature_row.get("split"),
                    "expected_split": expected_split,
                }
            )
    if split_validation_mismatches:
        raise ValueError(
            "Baseline inputs do not match the frozen split contract: "
            f"{split_validation_mismatches[0]}"
        )

    target_records_by_name: dict[str, list[dict[str, Any]]] = defaultdict(list)
    orphan_target_rows: list[dict[str, str]] = []
    for target_row in target_rows:
        target_name = target_row.get("target_name", "")
        if target_name not in TARGET_LABELS:
            raise ValueError(f"Unknown strict-open target {target_name}")

        visit_key = (target_row["subject_id"], target_row["visit_id"])
        feature_row = feature_by_key.get(visit_key)
        if feature_row is None:
            orphan_target_rows.append(target_row)
            continue

        target_value = _parse_float(target_row.get("target_value"))
        if target_value is None:
            raise ValueError(f"Invalid target value for {visit_key} and {target_name}")

        target_records_by_name[target_name].append(
            {
                "baseline_feature_row": feature_row,
                "diagnosis": feature_row.get("diagnosis", "") or "unknown",
                "feature_family_available_count": _parse_int(
                    feature_row.get("feature_family_available_count")
                )
                or 0,
                "missingness_burden": _parse_float(feature_row.get("missingness_burden")) or 0.0,
                "site_id": feature_row.get("site_id", "") or "unknown",
                "split": feature_row["split"],
                "subject_id": target_row["subject_id"],
                "target_label": target_row.get("target_label", TARGET_LABELS[target_name]),
                "target_name": target_name,
                "target_value": target_value,
                "visit_id": target_row["visit_id"],
            }
        )

    if orphan_target_rows:
        raise ValueError(
            "Derived targets reference visits that are missing from visit features: "
            f"{orphan_target_rows[0]}"
        )

    train_feature_rows = [row for row in feature_rows if row.get("split") == "train"]
    available_target_counts = _available_target_counts(target_records_by_name)

    prediction_rows: list[dict[str, str]] = []
    metrics_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    skipped_pairs: list[dict[str, Any]] = []
    support_gap_rows: list[dict[str, Any]] = []
    diagnosis_rows: list[dict[str, Any]] = []
    site_rows: list[dict[str, Any]] = []
    missingness_rows: list[dict[str, Any]] = []
    registry_targets: dict[str, dict[str, Any]] = {}

    for baseline_name in BASELINE_FAMILY_NAMES:
        family_spec = baseline_family_spec(baseline_name)
        family_state = build_family_state(baseline_name, train_feature_rows)
        registry_targets[baseline_name] = {
            "baseline_name": baseline_name,
            "label": family_spec.label,
            "description": family_spec.description,
            "signal_recipe": family_spec.signal_recipe,
            "feature_columns": list(family_spec.feature_columns),
            "supported_targets": list(family_spec.supported_targets),
            "unsupported_targets": dict(sorted(family_spec.unsupported_targets.items())),
            "family_state": family_state,
            "targets": {},
        }

        for target_name in TARGET_LABELS:
            available_records = sorted(
                target_records_by_name.get(target_name, []),
                key=lambda row: (row["subject_id"], row["visit_id"]),
            )
            available_by_split = available_target_counts[target_name]
            pair_skip_reason = None
            if not available_records:
                pair_skip_reason = (
                    f"No evaluable {target_name} rows were emitted on the current strict-open path."
                )
            else:
                pair_skip_reason = unsupported_target_reason(baseline_name, target_name)

            if pair_skip_reason is not None:
                skipped_pairs.append(
                    {
                        "baseline_name": baseline_name,
                        "baseline_label": family_spec.label,
                        "target_name": target_name,
                        "reason": pair_skip_reason,
                    }
                )
                registry_targets[baseline_name]["targets"][target_name] = {
                    "status": "skipped",
                    "reason": pair_skip_reason,
                    "available_target_rows": len(available_records),
                }
                _append_empty_eval_rows(
                    metrics_rows=metrics_rows,
                    coverage_rows=coverage_rows,
                    baseline_name=baseline_name,
                    baseline_label=family_spec.label,
                    target_name=target_name,
                    available_by_split=available_by_split,
                    reason=pair_skip_reason,
                )
                continue

            supported_records: list[dict[str, Any]] = []
            support_gap_counts: dict[str, Counter[str]] = defaultdict(Counter)
            for record in available_records:
                signal_value, support_reason = compute_signal(
                    baseline_name,
                    target_name,
                    record["baseline_feature_row"],
                    family_state,
                )
                if signal_value is None:
                    support_gap_counts[record["split"]][support_reason or "Baseline support was unavailable."] += 1
                    continue
                supported_records.append({**record, "signal_value": signal_value})

            train_supported = [record for record in supported_records if record["split"] == "train"]
            if not train_supported:
                pair_skip_reason = (
                    "No train rows had both target coverage and baseline support under the frozen split contract."
                )
                skipped_pairs.append(
                    {
                        "baseline_name": baseline_name,
                        "baseline_label": family_spec.label,
                        "target_name": target_name,
                        "reason": pair_skip_reason,
                    }
                )
                registry_targets[baseline_name]["targets"][target_name] = {
                    "status": "skipped",
                    "reason": pair_skip_reason,
                    "available_target_rows": len(available_records),
                    "support_gap_reasons": _serialize_reason_counts(support_gap_counts),
                }
                _append_empty_eval_rows(
                    metrics_rows=metrics_rows,
                    coverage_rows=coverage_rows,
                    baseline_name=baseline_name,
                    baseline_label=family_spec.label,
                    target_name=target_name,
                    available_by_split=available_by_split,
                    reason=pair_skip_reason,
                )
                support_gap_rows.extend(
                    _support_gap_rows(
                        baseline_name=baseline_name,
                        baseline_label=family_spec.label,
                        target_name=target_name,
                        support_gap_counts=support_gap_counts,
                    )
                )
                continue

            fit = fit_linear_calibrator(
                [(record["signal_value"], record["target_value"]) for record in train_supported]
            )
            registry_targets[baseline_name]["targets"][target_name] = {
                "status": "trained",
                "available_target_rows": len(available_records),
                "train_row_count": len(train_supported),
                "fit": fit,
                "support_gap_reasons": _serialize_reason_counts(support_gap_counts),
            }

            by_split_supported = {
                split_name: [
                    record
                    for record in supported_records
                    if record["split"] == split_name
                ]
                for split_name in SPLIT_ORDER
            }
            by_split_available = {
                split_name: [
                    record
                    for record in available_records
                    if record["split"] == split_name
                ]
                for split_name in SPLIT_ORDER
            }

            support_gap_rows.extend(
                _support_gap_rows(
                    baseline_name=baseline_name,
                    baseline_label=family_spec.label,
                    target_name=target_name,
                    support_gap_counts=support_gap_counts,
                )
            )

            for split_name in SPLIT_ORDER:
                available_split_records = by_split_available[split_name]
                supported_split_records = by_split_supported[split_name]
                predictions: list[float] = []
                targets_for_metrics: list[float] = []
                for record in supported_split_records:
                    prediction = apply_linear_calibrator(record["signal_value"], fit)
                    predictions.append(prediction)
                    targets_for_metrics.append(record["target_value"])
                    prediction_rows.append(
                        {
                            "baseline_name": baseline_name,
                            "baseline_label": family_spec.label,
                            "target_name": target_name,
                            "split": split_name,
                            "subject_id": record["subject_id"],
                            "visit_id": record["visit_id"],
                            "diagnosis": record["diagnosis"],
                            "site_id": record["site_id"],
                            "feature_family_available_count": str(record["feature_family_available_count"]),
                            "missingness_burden": _format_float(record["missingness_burden"]),
                            "signal_value": _format_float(record["signal_value"]),
                            "prediction": _format_float(prediction),
                            "target_value": _format_float(record["target_value"]),
                        }
                    )

                metrics_rows.append(
                    _metrics_row(
                        baseline_name=baseline_name,
                        baseline_label=family_spec.label,
                        target_name=target_name,
                        split_name=split_name,
                        predictions=predictions,
                        targets=targets_for_metrics,
                    )
                )
                coverage_rows.append(
                    _coverage_row(
                        baseline_name=baseline_name,
                        baseline_label=family_spec.label,
                        target_name=target_name,
                        split_name=split_name,
                        available_target_rows=len(available_split_records),
                        predicted_rows=len(supported_split_records),
                        status="trained",
                        reason=None,
                    )
                )
                diagnosis_rows.extend(
                    _group_coverage_rows(
                        baseline_name=baseline_name,
                        baseline_label=family_spec.label,
                        target_name=target_name,
                        split_name=split_name,
                        group_name="diagnosis",
                        available_records=available_split_records,
                        supported_records=supported_split_records,
                        extractor=lambda record: record["diagnosis"],
                    )
                )
                site_rows.extend(
                    _group_coverage_rows(
                        baseline_name=baseline_name,
                        baseline_label=family_spec.label,
                        target_name=target_name,
                        split_name=split_name,
                        group_name="site_id",
                        available_records=available_split_records,
                        supported_records=supported_split_records,
                        extractor=lambda record: record["site_id"],
                    )
                )
                missingness_rows.extend(
                    _group_coverage_rows(
                        baseline_name=baseline_name,
                        baseline_label=family_spec.label,
                        target_name=target_name,
                        split_name=split_name,
                        group_name="missingness_bucket",
                        available_records=available_split_records,
                        supported_records=supported_split_records,
                        extractor=_missingness_bucket,
                    )
                )

    baseline_predictions_path = _write_csv_rows(
        prediction_rows,
        models_path / "baseline_predictions.csv",
        fieldnames=BASELINE_PREDICTION_COLUMNS,
    )
    baseline_registry_path = models_path / "baseline_registry.json"
    baseline_summary_json_path = reports_path / "baseline_summary.json"
    baseline_summary_md_path = reports_path / "baseline_summary.md"

    source = str(
        target_manifest.get(
            "source",
            feature_manifest.get("source", split_manifest.get("source", "tcp")),
        )
    )
    dataset_version = (
        target_manifest.get("dataset_version")
        or feature_manifest.get("dataset_version")
        or split_manifest.get("dataset_version")
    )
    output_paths = {
        "baseline_predictions": str(baseline_predictions_path),
        "baseline_registry": str(baseline_registry_path),
        "baseline_summary_json": str(baseline_summary_json_path),
        "baseline_summary_md": str(baseline_summary_md_path),
    }

    registry_payload = {
        "command": list(command),
        "source": source,
        "dataset_version": dataset_version,
        "git_sha": git_sha,
        "seed": seed,
        "features_dir": str(features_path),
        "targets_dir": str(targets_path),
        "splits_dir": str(splits_path),
        "models_dir": str(models_path),
        "input_paths": _existing_paths(
            visit_features_path,
            feature_manifest_path,
            derived_targets_path,
            target_manifest_path,
            split_assignments_path,
            split_manifest_path,
        ),
        "output_paths": output_paths,
        "baseline_families": [
            registry_targets[baseline_name]
            for baseline_name in BASELINE_FAMILY_NAMES
        ],
        "fit_policy": (
            "Any fitted normalization and target calibration is estimated on train rows only; validation and "
            "test rows are evaluated strictly out of sample."
        ),
    }
    write_json_artifact(registry_payload, baseline_registry_path)

    comparison_rows = _comparison_rows(metrics_rows, coverage_rows)
    summary_payload = {
        "command": list(command),
        "source": source,
        "dataset_version": dataset_version,
        "git_sha": git_sha,
        "seed": seed,
        "features_dir": str(features_path),
        "targets_dir": str(targets_path),
        "splits_dir": str(splits_path),
        "models_dir": str(models_path),
        "reports_dir": str(reports_path),
        "input_paths": registry_payload["input_paths"],
        "output_paths": output_paths,
        "baseline_families": list_baseline_family_specs(),
        "available_target_rows_by_split": {
            target_name: {
                split_name: available_target_counts[target_name][split_name]
                for split_name in SPLIT_ORDER
            }
            for target_name in TARGET_LABELS
        },
        "metrics_table": metrics_rows,
        "coverage_table": coverage_rows,
        "skipped_baseline_targets": skipped_pairs,
        "support_gap_reasons": support_gap_rows,
        "comparison_table": comparison_rows,
        "confounds": {
            "coverage_by_diagnosis": diagnosis_rows,
            "coverage_by_site": site_rows,
            "missingness_support": missingness_rows,
            "missingness_heavy_threshold": _MISSINGNESS_HEAVY_THRESHOLD,
        },
        "public_path_limit_note": target_manifest.get("public_path_limit_note"),
        "fit_policy": registry_payload["fit_policy"],
        "split_contract_validation": {
            "mismatched_feature_rows": len(split_validation_mismatches),
            "orphan_target_rows": len(orphan_target_rows),
            "policy": (
                "Baseline evaluation inherits split values from visit features and checks those rows against the "
                "frozen subject-level split assignments before fitting or scoring."
            ),
        },
    }
    write_json_artifact(summary_payload, baseline_summary_json_path)
    baseline_summary_md_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_summary_md_path.write_text(
        _render_summary_markdown(summary_payload),
        encoding="utf-8",
    )

    run_manifest_path = manifests_path / "tcp_train_baselines_run_manifest.json"
    run_manifest = build_run_manifest(
        dataset_source=source,
        dataset_version=dataset_version,
        command=command,
        git_sha=git_sha,
        seed=seed,
        output_paths=output_paths,
    )
    write_run_manifest(run_manifest, run_manifest_path)

    return {
        "baseline_predictions": str(baseline_predictions_path),
        "baseline_registry": str(baseline_registry_path),
        "baseline_summary_json": str(baseline_summary_json_path),
        "baseline_summary_md": str(baseline_summary_md_path),
        "run_manifest": str(run_manifest_path),
        "models_dir": str(models_path),
        "reports_dir": str(reports_path),
    }


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _write_csv_rows(rows: list[dict[str, str]], destination: Path, *, fieldnames: tuple[str, ...]) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({fieldname: row.get(fieldname, "") for fieldname in fieldnames})
    return destination


def _load_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _available_target_counts(
    target_records_by_name: dict[str, list[dict[str, Any]]],
) -> dict[str, Counter[str]]:
    counts = {target_name: Counter() for target_name in TARGET_LABELS}
    for target_name, records in target_records_by_name.items():
        counts[target_name].update(record["split"] for record in records)
    return counts


def _append_empty_eval_rows(
    *,
    metrics_rows: list[dict[str, Any]],
    coverage_rows: list[dict[str, Any]],
    baseline_name: str,
    baseline_label: str,
    target_name: str,
    available_by_split: Counter[str],
    reason: str,
) -> None:
    for split_name in SPLIT_ORDER:
        metrics_rows.append(
            {
                "baseline_name": baseline_name,
                "baseline_label": baseline_label,
                "target_name": target_name,
                "split": split_name,
                "prediction_count": 0,
                "mae": None,
                "rmse": None,
                "mean_prediction": None,
                "mean_target": None,
                "status": "skipped",
                "reason": reason,
            }
        )
        coverage_rows.append(
            _coverage_row(
                baseline_name=baseline_name,
                baseline_label=baseline_label,
                target_name=target_name,
                split_name=split_name,
                available_target_rows=available_by_split[split_name],
                predicted_rows=0,
                status="skipped",
                reason=reason,
            )
        )


def _coverage_row(
    *,
    baseline_name: str,
    baseline_label: str,
    target_name: str,
    split_name: str,
    available_target_rows: int,
    predicted_rows: int,
    status: str,
    reason: str | None,
) -> dict[str, Any]:
    coverage_rate = round((predicted_rows / available_target_rows), 6) if available_target_rows else 0.0
    return {
        "baseline_name": baseline_name,
        "baseline_label": baseline_label,
        "target_name": target_name,
        "split": split_name,
        "available_target_rows": available_target_rows,
        "predicted_rows": predicted_rows,
        "coverage_rate": coverage_rate,
        "status": status,
        "reason": reason,
    }


def _metrics_row(
    *,
    baseline_name: str,
    baseline_label: str,
    target_name: str,
    split_name: str,
    predictions: list[float],
    targets: list[float],
) -> dict[str, Any]:
    if not predictions:
        return {
            "baseline_name": baseline_name,
            "baseline_label": baseline_label,
            "target_name": target_name,
            "split": split_name,
            "prediction_count": 0,
            "mae": None,
            "rmse": None,
            "mean_prediction": None,
            "mean_target": None,
            "status": "trained",
            "reason": None,
        }

    absolute_errors = [abs(prediction - target) for prediction, target in zip(predictions, targets, strict=True)]
    squared_errors = [(prediction - target) ** 2 for prediction, target in zip(predictions, targets, strict=True)]
    return {
        "baseline_name": baseline_name,
        "baseline_label": baseline_label,
        "target_name": target_name,
        "split": split_name,
        "prediction_count": len(predictions),
        "mae": round(sum(absolute_errors) / len(absolute_errors), 6),
        "rmse": round(math.sqrt(sum(squared_errors) / len(squared_errors)), 6),
        "mean_prediction": round(sum(predictions) / len(predictions), 6),
        "mean_target": round(sum(targets) / len(targets), 6),
        "status": "trained",
        "reason": None,
    }


def _support_gap_rows(
    *,
    baseline_name: str,
    baseline_label: str,
    target_name: str,
    support_gap_counts: dict[str, Counter[str]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for split_name in SPLIT_ORDER:
        counter = support_gap_counts.get(split_name, Counter())
        for reason, count in sorted(counter.items()):
            rows.append(
                {
                    "baseline_name": baseline_name,
                    "baseline_label": baseline_label,
                    "target_name": target_name,
                    "split": split_name,
                    "reason": reason,
                    "count": count,
                }
            )
    return rows


def _serialize_reason_counts(support_gap_counts: dict[str, Counter[str]]) -> dict[str, dict[str, int]]:
    return {
        split_name: dict(sorted(counter.items()))
        for split_name, counter in sorted(support_gap_counts.items())
    }


def _group_coverage_rows(
    *,
    baseline_name: str,
    baseline_label: str,
    target_name: str,
    split_name: str,
    group_name: str,
    available_records: list[dict[str, Any]],
    supported_records: list[dict[str, Any]],
    extractor,
) -> list[dict[str, Any]]:
    available_counts: Counter[str] = Counter()
    supported_counts: Counter[str] = Counter()
    for record in available_records:
        available_counts[_normalize_group(extractor(record))] += 1
    for record in supported_records:
        supported_counts[_normalize_group(extractor(record))] += 1

    rows: list[dict[str, Any]] = []
    for group_value in sorted(available_counts):
        available_count = available_counts[group_value]
        predicted_count = supported_counts[group_value]
        rows.append(
            {
                "baseline_name": baseline_name,
                "baseline_label": baseline_label,
                "target_name": target_name,
                "split": split_name,
                group_name: group_value,
                "available_target_rows": available_count,
                "predicted_rows": predicted_count,
                "coverage_rate": round((predicted_count / available_count), 6) if available_count else 0.0,
            }
        )
    return rows


def _comparison_rows(
    metrics_rows: list[dict[str, Any]],
    coverage_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    coverage_lookup = {
        (
            row["baseline_name"],
            row["target_name"],
            row["split"],
        ): row
        for row in coverage_rows
    }
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for metrics_row in metrics_rows:
        if metrics_row["status"] != "trained" or metrics_row["prediction_count"] <= 0:
            continue
        grouped[(metrics_row["target_name"], metrics_row["split"])].append(metrics_row)

    comparison_rows: list[dict[str, Any]] = []
    for target_name, split_name in sorted(grouped):
        ranked_rows = sorted(
            grouped[(target_name, split_name)],
            key=lambda row: (
                row["mae"],
                row["rmse"],
                -coverage_lookup[(row["baseline_name"], target_name, split_name)]["coverage_rate"],
                row["baseline_name"],
            ),
        )
        for rank, metrics_row in enumerate(ranked_rows, start=1):
            coverage_row = coverage_lookup[(metrics_row["baseline_name"], target_name, split_name)]
            comparison_rows.append(
                {
                    "target_name": target_name,
                    "split": split_name,
                    "rank": rank,
                    "baseline_name": metrics_row["baseline_name"],
                    "baseline_label": metrics_row["baseline_label"],
                    "mae": metrics_row["mae"],
                    "rmse": metrics_row["rmse"],
                    "prediction_count": metrics_row["prediction_count"],
                    "coverage_rate": coverage_row["coverage_rate"],
                }
            )
    return comparison_rows


def _render_summary_markdown(summary_payload: dict[str, Any]) -> str:
    lines = [
        "# Strict-open baseline summary",
        "",
        "## Target availability",
        "| target | train | validation | test |",
        "| --- | ---: | ---: | ---: |",
    ]
    for target_name in TARGET_LABELS:
        availability = summary_payload["available_target_rows_by_split"][target_name]
        lines.append(
            f"| {target_name} | {availability['train']} | {availability['validation']} | {availability['test']} |"
        )

    lines.extend(
        [
            "",
            "## Comparison table",
            "| target | split | rank | baseline | MAE | RMSE | n | coverage |",
            "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    if summary_payload["comparison_table"]:
        for row in summary_payload["comparison_table"]:
            lines.append(
                "| "
                f"{row['target_name']} | {row['split']} | {row['rank']} | {row['baseline_name']} | "
                f"{_markdown_number(row['mae'])} | {_markdown_number(row['rmse'])} | "
                f"{row['prediction_count']} | {_markdown_number(row['coverage_rate'])} |"
            )
    else:
        lines.append("| none | none | 0 | none | n/a | n/a | 0 | 0 |")

    lines.extend(["", "## Skipped baseline-target pairs"])
    if summary_payload["skipped_baseline_targets"]:
        for row in summary_payload["skipped_baseline_targets"]:
            lines.append(f"- `{row['baseline_name']} x {row['target_name']}`: {row['reason']}")
    else:
        lines.append("- none")

    diagnosis_rows = _aggregate_confounds(summary_payload["confounds"]["coverage_by_diagnosis"], "diagnosis")
    lines.extend(
        [
            "",
            "## Coverage by diagnosis",
            "| baseline | target | diagnosis | available | predicted | coverage |",
            "| --- | --- | --- | ---: | ---: | ---: |",
        ]
    )
    if diagnosis_rows:
        for row in diagnosis_rows:
            lines.append(
                "| "
                f"{row['baseline_name']} | {row['target_name']} | {row['diagnosis']} | "
                f"{row['available_target_rows']} | {row['predicted_rows']} | {_markdown_number(row['coverage_rate'])} |"
            )
    else:
        lines.append("| none | none | none | 0 | 0 | 0 |")

    site_rows = _aggregate_confounds(summary_payload["confounds"]["coverage_by_site"], "site_id")
    lines.extend(
        [
            "",
            "## Coverage by site",
            "| baseline | target | site | available | predicted | coverage |",
            "| --- | --- | --- | ---: | ---: | ---: |",
        ]
    )
    if site_rows:
        for row in site_rows:
            lines.append(
                "| "
                f"{row['baseline_name']} | {row['target_name']} | {row['site_id']} | "
                f"{row['available_target_rows']} | {row['predicted_rows']} | {_markdown_number(row['coverage_rate'])} |"
            )
    else:
        lines.append("| none | none | none | 0 | 0 | 0 |")

    missingness_rows = _aggregate_confounds(
        summary_payload["confounds"]["missingness_support"],
        "missingness_bucket",
    )
    lines.extend(
        [
            "",
            "## Missingness-heavy support",
            (
                f"Threshold: rows with `missingness_burden >= {summary_payload['confounds']['missingness_heavy_threshold']}` "
                "are marked `heavy_missingness`."
            ),
            "",
            "| baseline | target | bucket | available | predicted | coverage |",
            "| --- | --- | --- | ---: | ---: | ---: |",
        ]
    )
    if missingness_rows:
        for row in missingness_rows:
            lines.append(
                "| "
                f"{row['baseline_name']} | {row['target_name']} | {row['missingness_bucket']} | "
                f"{row['available_target_rows']} | {row['predicted_rows']} | {_markdown_number(row['coverage_rate'])} |"
            )
    else:
        lines.append("| none | none | none | 0 | 0 | 0 |")

    public_note = summary_payload.get("public_path_limit_note")
    if public_note:
        lines.extend(["", "## Public-path note", public_note])

    lines.append("")
    return "\n".join(lines)


def _aggregate_confounds(rows: list[dict[str, Any]], group_name: str) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        group_key = (row["baseline_name"], row["target_name"], row[group_name])
        if group_key not in grouped:
            grouped[group_key] = {
                "baseline_name": row["baseline_name"],
                "target_name": row["target_name"],
                group_name: row[group_name],
                "available_target_rows": 0,
                "predicted_rows": 0,
            }
        grouped[group_key]["available_target_rows"] += row["available_target_rows"]
        grouped[group_key]["predicted_rows"] += row["predicted_rows"]

    aggregated = []
    for payload in grouped.values():
        available = payload["available_target_rows"]
        predicted = payload["predicted_rows"]
        aggregated.append(
            {
                **payload,
                "coverage_rate": round((predicted / available), 6) if available else 0.0,
            }
        )
    return sorted(aggregated, key=lambda row: (row["baseline_name"], row["target_name"], row[group_name]))


def _existing_paths(*paths: Path) -> list[str]:
    return [str(path) for path in paths if path.exists()]


def _parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _format_float(value: float) -> str:
    return f"{value:.6f}"


def _missingness_bucket(record: dict[str, Any]) -> str:
    return (
        "heavy_missingness"
        if record["missingness_burden"] >= _MISSINGNESS_HEAVY_THRESHOLD
        else "lower_missingness"
    )


def _normalize_group(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    return str(value)


def _markdown_number(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


__all__ = [
    "BASELINE_PREDICTION_COLUMNS",
    "run_strict_open_baseline_training",
]

"""Feature construction for strict-open harmonized artifacts."""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .provenance import write_json_artifact
from .run_manifest import build_run_manifest, write_run_manifest

FEATURE_COLUMNS = (
    "subject_id",
    "visit_id",
    "split",
    "diagnosis",
    "site_id",
    "visit_index",
    "days_from_baseline",
    "subject_visit_count",
    "days_since_previous_visit",
    "visit_nonbaseline_indicator",
    "visit_temporal_gap_missing_indicator",
    "visit_ambiguity_proxy_input",
    "cognition_score_count",
    "cognition_instrument_count",
    "cognition_score_mean",
    "cognition_score_max",
    "cognition_available",
    "cognition_missing_indicator",
    "symptom_score_count",
    "symptom_instrument_count",
    "symptom_score_mean",
    "symptom_score_max",
    "symptom_available",
    "symptom_missing_indicator",
    "mri_available_modality_count",
    "mri_missing_modality_count",
    "mri_present_fraction",
    "mri_qc_measure_count",
    "mri_mean_fd_mean",
    "mri_mean_fd_max",
    "mri_qc_missing_indicator",
    "feature_family_available_count",
    "stable_support_family_count",
    "missing_feature_family_count",
    "missing_feature_family_fraction",
    "missingness_burden",
    "state_noise_proxy_input",
)

_MOTION_QC_SCALE = 0.5


def run_strict_open_feature_build(
    *,
    harmonized_root: str | Path,
    splits_root: str | Path,
    manifests_root: str | Path,
    features_root: str | Path,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
) -> dict[str, str]:
    """Build deterministic visit-level features from harmonized strict-open tables."""

    harmonized_path = Path(harmonized_root)
    splits_path = Path(splits_root)
    manifests_path = Path(manifests_root)
    features_path = Path(features_root)

    subjects_path = harmonized_path / "subjects.csv"
    visits_path = harmonized_path / "visits.csv"
    cognition_path = harmonized_path / "cognition_scores.csv"
    symptom_path = harmonized_path / "symptom_behavior_scores.csv"
    mri_path = harmonized_path / "mri_features.csv"
    split_assignments_path = splits_path / "split_assignments.csv"
    harmonization_manifest_path = harmonized_path / "harmonization_manifest.json"
    split_manifest_path = splits_path / "split_manifest.json"

    required_paths = (
        subjects_path,
        visits_path,
        cognition_path,
        symptom_path,
        mri_path,
        split_assignments_path,
    )
    for required_path in required_paths:
        if not required_path.exists():
            raise FileNotFoundError(f"Missing strict-open input at {required_path}")

    subject_rows = _read_csv_rows(subjects_path)
    visit_rows = sorted(
        _read_csv_rows(visits_path),
        key=lambda row: (row["subject_id"], _sort_int(row.get("visit_index")), row["visit_id"]),
    )
    cognition_rows = _read_csv_rows(cognition_path)
    symptom_rows = _read_csv_rows(symptom_path)
    mri_rows = _read_csv_rows(mri_path)
    split_rows = _read_csv_rows(split_assignments_path)
    harmonization_manifest = _load_json_if_exists(harmonization_manifest_path)
    split_manifest = _load_json_if_exists(split_manifest_path)

    subjects_by_id = {row["subject_id"]: row for row in subject_rows}
    split_by_subject = {row["subject_id"]: row["split"] for row in split_rows}
    cognition_by_visit = _group_rows_by_visit(cognition_rows)
    symptom_by_visit = _group_rows_by_visit(symptom_rows)
    mri_by_visit = _group_rows_by_visit(mri_rows)

    visits_by_subject: dict[str, list[dict[str, str]]] = defaultdict(list)
    for visit_row in visit_rows:
        visits_by_subject[visit_row["subject_id"]].append(visit_row)

    feature_rows: list[dict[str, str]] = []
    visits_missing_split = 0

    for subject_id in sorted(visits_by_subject):
        previous_days: int | None = None
        subject_visits = visits_by_subject[subject_id]
        subject_row = subjects_by_id.get(subject_id)
        if subject_row is None:
            raise ValueError(f"Visit references missing subject row for {subject_id}")
        split_name = split_by_subject.get(subject_id)
        if split_name is None:
            visits_missing_split += len(subject_visits)
            raise ValueError(f"Missing frozen split assignment for {subject_id}")

        for visit_row in subject_visits:
            visit_id = visit_row["visit_id"]
            visit_index = _parse_int(visit_row.get("visit_index"))
            days_from_baseline = _parse_int(visit_row.get("days_from_baseline"))
            days_since_previous_visit = None
            if previous_days is not None and days_from_baseline is not None:
                days_since_previous_visit = days_from_baseline - previous_days

            visit_nonbaseline = 1 if (visit_index or 0) > 0 else 0
            temporal_gap_missing = 1 if visit_nonbaseline and days_since_previous_visit is None else 0
            visit_ambiguity_proxy = (
                0.0
                if visit_nonbaseline == 0
                else 0.5 + (0.5 * temporal_gap_missing)
            )

            cognition_stats = _score_family_stats(cognition_by_visit.get(visit_id, ()))
            symptom_stats = _score_family_stats(symptom_by_visit.get(visit_id, ()))
            mri_stats = _mri_family_stats(mri_by_visit.get(visit_id, ()))

            feature_family_available_count = (
                cognition_stats["available"]
                + symptom_stats["available"]
                + mri_stats["available"]
            )
            missing_feature_family_count = 3 - feature_family_available_count
            missing_feature_family_fraction = missing_feature_family_count / 3.0
            state_noise_proxy_input = _state_noise_proxy_input(
                missing_feature_family_fraction=missing_feature_family_fraction,
                mri_present_fraction=mri_stats["present_fraction"],
                mean_fd_mean=mri_stats["mean_fd_mean"],
                qc_missing_indicator=mri_stats["qc_missing_indicator"],
                visit_ambiguity_proxy=visit_ambiguity_proxy,
                temporal_gap_missing=temporal_gap_missing,
            )

            feature_rows.append(
                {
                    "subject_id": subject_id,
                    "visit_id": visit_id,
                    "split": split_name,
                    "diagnosis": subject_row.get("diagnosis", ""),
                    "site_id": subject_row.get("site_id", ""),
                    "visit_index": _format_optional_int(visit_index),
                    "days_from_baseline": _format_optional_int(days_from_baseline),
                    "subject_visit_count": str(len(subject_visits)),
                    "days_since_previous_visit": _format_optional_int(days_since_previous_visit),
                    "visit_nonbaseline_indicator": str(visit_nonbaseline),
                    "visit_temporal_gap_missing_indicator": str(temporal_gap_missing),
                    "visit_ambiguity_proxy_input": _format_float(visit_ambiguity_proxy),
                    "cognition_score_count": str(cognition_stats["score_count"]),
                    "cognition_instrument_count": str(cognition_stats["instrument_count"]),
                    "cognition_score_mean": _format_optional_float(cognition_stats["score_mean"]),
                    "cognition_score_max": _format_optional_float(cognition_stats["score_max"]),
                    "cognition_available": str(cognition_stats["available"]),
                    "cognition_missing_indicator": str(cognition_stats["missing_indicator"]),
                    "symptom_score_count": str(symptom_stats["score_count"]),
                    "symptom_instrument_count": str(symptom_stats["instrument_count"]),
                    "symptom_score_mean": _format_optional_float(symptom_stats["score_mean"]),
                    "symptom_score_max": _format_optional_float(symptom_stats["score_max"]),
                    "symptom_available": str(symptom_stats["available"]),
                    "symptom_missing_indicator": str(symptom_stats["missing_indicator"]),
                    "mri_available_modality_count": str(mri_stats["available_modality_count"]),
                    "mri_missing_modality_count": str(mri_stats["missing_modality_count"]),
                    "mri_present_fraction": _format_optional_float(mri_stats["present_fraction"]),
                    "mri_qc_measure_count": str(mri_stats["qc_measure_count"]),
                    "mri_mean_fd_mean": _format_optional_float(mri_stats["mean_fd_mean"]),
                    "mri_mean_fd_max": _format_optional_float(mri_stats["mean_fd_max"]),
                    "mri_qc_missing_indicator": str(mri_stats["qc_missing_indicator"]),
                    "feature_family_available_count": str(feature_family_available_count),
                    "stable_support_family_count": str(
                        symptom_stats["available"] + mri_stats["available"]
                    ),
                    "missing_feature_family_count": str(missing_feature_family_count),
                    "missing_feature_family_fraction": _format_float(missing_feature_family_fraction),
                    "missingness_burden": _format_float(missing_feature_family_fraction),
                    "state_noise_proxy_input": _format_float(state_noise_proxy_input),
                }
            )

            if days_from_baseline is not None:
                previous_days = days_from_baseline

    visit_features_path = _write_csv_rows(
        feature_rows,
        features_path / "visit_features.csv",
        fieldnames=FEATURE_COLUMNS,
    )
    feature_manifest_path = features_path / "feature_manifest.json"

    feature_coverage_summary = _feature_coverage_summary(feature_rows)
    missingness_summary = _missingness_summary(feature_rows)
    unavailable_feature_families = [
        family_name
        for family_name, available_count in (
            ("cognition", feature_coverage_summary["visits_with_cognition"]),
            ("symptom", feature_coverage_summary["visits_with_symptoms"]),
            ("mri", feature_coverage_summary["visits_with_any_mri"]),
            ("motion_qc", feature_coverage_summary["visits_with_motion_qc"]),
        )
        if available_count == 0
    ]
    feature_limitations = [
        (
            f"{family_name} evidence is only available for {available_count} of "
            f"{feature_coverage_summary['total_visits']} visits on the strict-open public path."
        )
        for family_name, available_count in (
            ("cognition", feature_coverage_summary["visits_with_cognition"]),
            ("symptom", feature_coverage_summary["visits_with_symptoms"]),
            ("mri", feature_coverage_summary["visits_with_any_mri"]),
            ("motion_qc", feature_coverage_summary["visits_with_motion_qc"]),
        )
        if 0 < available_count < feature_coverage_summary["total_visits"]
    ]

    source = str(harmonization_manifest.get("source", split_manifest.get("source", "tcp")))
    source_identifier = str(
        harmonization_manifest.get(
            "source_identifier",
            split_manifest.get("source_identifier", "tcp-ds005237"),
        )
    )
    dataset_version = (
        harmonization_manifest.get("dataset_version")
        or split_manifest.get("dataset_version")
    )
    output_paths = {
        "visit_features": str(visit_features_path),
        "feature_manifest": str(feature_manifest_path),
    }
    feature_manifest = {
        "command": list(command),
        "source": source,
        "source_identifier": source_identifier,
        "dataset_version": dataset_version,
        "git_sha": git_sha,
        "seed": seed,
        "harmonized_dir": str(harmonized_path),
        "splits_dir": str(splits_path),
        "input_paths": _existing_paths(
            subjects_path,
            visits_path,
            cognition_path,
            symptom_path,
            mri_path,
            split_assignments_path,
            harmonization_manifest_path,
            split_manifest_path,
        ),
        "output_paths": output_paths,
        "row_counts": {
            "visit_features": len(feature_rows),
        },
        "rows_by_split": dict(sorted(Counter(row["split"] for row in feature_rows).items())),
        "feature_columns": list(FEATURE_COLUMNS),
        "feature_coverage_summary": feature_coverage_summary,
        "missingness_summary": missingness_summary,
        "unavailable_feature_families": unavailable_feature_families,
        "feature_limitations": feature_limitations,
        "split_contract_validation": {
            "visits_missing_split": visits_missing_split,
            "policy": (
                "Feature rows inherit the frozen subject-level split assignment; "
                "feature construction does not redefine split membership."
            ),
        },
    }
    write_json_artifact(feature_manifest, feature_manifest_path)

    run_manifest_path = manifests_path / "tcp_build_features_run_manifest.json"
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
        "visit_features": str(visit_features_path),
        "feature_manifest": str(feature_manifest_path),
        "run_manifest": str(run_manifest_path),
        "features_dir": str(features_path),
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


def _group_rows_by_visit(rows: list[dict[str, str]]) -> dict[str, tuple[dict[str, str], ...]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["visit_id"]].append(row)
    return {visit_id: tuple(group_rows) for visit_id, group_rows in grouped.items()}


def _score_family_stats(rows: tuple[dict[str, str], ...]) -> dict[str, int | float | None]:
    scores: list[float] = []
    instruments: set[str] = set()
    for row in rows:
        score = _parse_float(row.get("score"))
        if score is None:
            continue
        scores.append(score)
        instrument = (row.get("instrument") or "").strip()
        if instrument:
            instruments.add(instrument)

    score_count = len(scores)
    available = 1 if score_count > 0 else 0
    return {
        "score_count": score_count,
        "instrument_count": len(instruments),
        "score_mean": _mean(scores),
        "score_max": max(scores) if scores else None,
        "available": available,
        "missing_indicator": 0 if available else 1,
    }


def _mri_family_stats(rows: tuple[dict[str, str], ...]) -> dict[str, int | float | None]:
    availability_values = [
        _parse_float(row.get("feature_value"))
        for row in rows
        if row.get("feature_name") == "available"
    ]
    qc_values = [
        _parse_float(row.get("feature_value"))
        for row in rows
        if row.get("feature_name") == "mean_fd"
    ]
    clean_availability_values = [value for value in availability_values if value is not None]
    clean_qc_values = [value for value in qc_values if value is not None]

    total_modalities = len(clean_availability_values)
    available_modality_count = sum(1 for value in clean_availability_values if value > 0)
    missing_modality_count = total_modalities - available_modality_count
    present_fraction = None
    if total_modalities > 0:
        present_fraction = available_modality_count / total_modalities

    return {
        "available_modality_count": available_modality_count,
        "missing_modality_count": missing_modality_count,
        "present_fraction": present_fraction,
        "qc_measure_count": len(clean_qc_values),
        "mean_fd_mean": _mean(clean_qc_values),
        "mean_fd_max": max(clean_qc_values) if clean_qc_values else None,
        "available": 1 if available_modality_count > 0 else 0,
        "qc_missing_indicator": 0 if clean_qc_values else 1,
    }


def _state_noise_proxy_input(
    *,
    missing_feature_family_fraction: float,
    mri_present_fraction: float | None,
    mean_fd_mean: float | None,
    qc_missing_indicator: int | float,
    visit_ambiguity_proxy: float,
    temporal_gap_missing: int | float,
) -> float:
    motion_component = 1.0 if mean_fd_mean is None else min(max(mean_fd_mean / _MOTION_QC_SCALE, 0.0), 1.0)
    mri_missing_component = 1.0 if mri_present_fraction is None else 1.0 - mri_present_fraction
    return _clamp(
        _mean(
            [
                missing_feature_family_fraction,
                mri_missing_component,
                motion_component,
                float(qc_missing_indicator),
                visit_ambiguity_proxy,
                float(temporal_gap_missing),
            ]
        )
        or 0.0
    )


def _feature_coverage_summary(rows: list[dict[str, str]]) -> dict[str, int]:
    return {
        "total_visits": len(rows),
        "visits_with_cognition": sum(int(row["cognition_available"]) for row in rows),
        "visits_with_symptoms": sum(int(row["symptom_available"]) for row in rows),
        "visits_with_any_mri": sum(1 for row in rows if int(row["mri_available_modality_count"]) > 0),
        "visits_with_motion_qc": sum(1 for row in rows if int(row["mri_qc_measure_count"]) > 0),
    }


def _missingness_summary(rows: list[dict[str, str]]) -> dict[str, float | int]:
    missingness_values = [_parse_float(row["missingness_burden"]) or 0.0 for row in rows]
    return {
        "mean_missing_feature_family_fraction": round(_mean(missingness_values) or 0.0, 6),
        "visits_missing_cognition": sum(int(row["cognition_missing_indicator"]) for row in rows),
        "visits_missing_symptoms": sum(int(row["symptom_missing_indicator"]) for row in rows),
        "visits_missing_any_mri": sum(1 for row in rows if int(row["mri_available_modality_count"]) == 0),
        "visits_missing_motion_qc": sum(int(row["mri_qc_missing_indicator"]) for row in rows),
    }


def _existing_paths(*paths: Path) -> list[str]:
    return sorted(str(path) for path in paths if path.exists())


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    try:
        return float(candidate)
    except ValueError:
        return None


def _parse_int(value: str | None) -> int | None:
    parsed = _parse_float(value)
    if parsed is None:
        return None
    return int(parsed)


def _sort_int(value: str | None) -> int:
    parsed = _parse_int(value)
    if parsed is None:
        return 0
    return parsed


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _format_float(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".") or "0"


def _format_optional_float(value: float | None) -> str:
    if value is None:
        return ""
    return _format_float(value)


def _format_optional_int(value: int | None) -> str:
    if value is None:
        return ""
    return str(value)


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(value, upper))


__all__ = ["FEATURE_COLUMNS", "run_strict_open_feature_build"]

"""Public-only derived target construction for strict-open features."""

from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .provenance import write_json_artifact
from .run_manifest import build_run_manifest, write_run_manifest
from .schema import DERIVED_TARGETS

TARGET_LABELS = {
    "global_cognition_dev": "Global cognition deviation",
    "state_noise_score": "Visit state noise score",
    "stable_cognitive_burden_proxy": "Stable cognitive burden proxy",
}
_COGNITION_SCORE_SCALE = 3.0
_SYMPTOM_SCORE_SCALE = 4.0


def run_strict_open_target_build(
    *,
    features_root: str | Path,
    harmonized_root: str | Path,
    splits_root: str | Path,
    manifests_root: str | Path,
    targets_root: str | Path,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
) -> dict[str, str]:
    """Build public-only soft targets from deterministic strict-open features."""

    features_path = Path(features_root)
    harmonized_path = Path(harmonized_root)
    splits_path = Path(splits_root)
    manifests_path = Path(manifests_root)
    targets_path = Path(targets_root)

    visit_features_path = features_path / "visit_features.csv"
    feature_manifest_path = features_path / "feature_manifest.json"
    split_assignments_path = splits_path / "split_assignments.csv"
    split_manifest_path = splits_path / "split_manifest.json"
    harmonization_manifest_path = harmonized_path / "harmonization_manifest.json"

    required_paths = (
        visit_features_path,
        split_assignments_path,
    )
    for required_path in required_paths:
        if not required_path.exists():
            raise FileNotFoundError(f"Missing strict-open input at {required_path}")

    feature_rows = _read_csv_rows(visit_features_path)
    split_rows = _read_csv_rows(split_assignments_path)
    feature_manifest = _load_json_if_exists(feature_manifest_path)
    split_manifest = _load_json_if_exists(split_manifest_path)
    harmonization_manifest = _load_json_if_exists(harmonization_manifest_path)

    split_by_subject = {row["subject_id"]: row["split"] for row in split_rows}
    split_validation_mismatches = []
    for row in feature_rows:
        expected_split = split_by_subject.get(row["subject_id"])
        if expected_split != row.get("split"):
            split_validation_mismatches.append(
                {
                    "subject_id": row["subject_id"],
                    "visit_id": row["visit_id"],
                    "feature_split": row.get("split"),
                    "expected_split": expected_split,
                }
            )
    if split_validation_mismatches:
        raise ValueError(
            "Feature rows do not match the frozen split contract: "
            f"{split_validation_mismatches[0]}"
        )

    total_visits = len(feature_rows)
    total_visits_by_split = Counter(row["split"] for row in feature_rows)
    emitted_by_target_and_split: dict[str, Counter[str]] = defaultdict(Counter)
    omission_reasons: dict[str, Counter[str]] = defaultdict(Counter)
    target_rows_with_split: list[tuple[dict[str, str], str]] = []

    for row in sorted(feature_rows, key=lambda item: (item["subject_id"], item["visit_id"])):
        split_name = row["split"]
        visit_id = row["visit_id"]
        state_noise_score = _clamp(_parse_float(row.get("state_noise_proxy_input")) or 0.0)
        target_rows_with_split.append(
            (
                _target_row(
                    subject_id=row["subject_id"],
                    visit_id=visit_id,
                    target_name="state_noise_score",
                    target_value=state_noise_score,
                ),
                split_name,
            )
        )
        emitted_by_target_and_split["state_noise_score"][split_name] += 1

        cognition_count = _parse_int(row.get("cognition_score_count")) or 0
        cognition_mean = _parse_float(row.get("cognition_score_mean"))
        global_cognition_dev: float | None = None
        if cognition_count > 0 and cognition_mean is not None:
            global_cognition_dev = _shrink_toward_midpoint(
                _bounded_signal(cognition_mean, scale=_COGNITION_SCORE_SCALE),
                evidence_count=cognition_count,
                full_evidence_count=2,
            )
            target_rows_with_split.append(
                (
                    _target_row(
                        subject_id=row["subject_id"],
                        visit_id=visit_id,
                        target_name="global_cognition_dev",
                        target_value=global_cognition_dev,
                    ),
                    split_name,
                )
            )
            emitted_by_target_and_split["global_cognition_dev"][split_name] += 1
        else:
            omission_reasons["global_cognition_dev"]["insufficient_cognition_evidence"] += 1
            omission_reasons["stable_cognitive_burden_proxy"]["insufficient_cognition_evidence"] += 1
            continue

        symptom_count = _parse_int(row.get("symptom_score_count")) or 0
        symptom_mean = _parse_float(row.get("symptom_score_mean"))
        if symptom_count <= 0 or symptom_mean is None:
            omission_reasons["stable_cognitive_burden_proxy"]["insufficient_supporting_symptom_proxy"] += 1
            continue

        support_family_count = _parse_int(row.get("stable_support_family_count")) or 0
        if support_family_count < 2:
            omission_reasons["stable_cognitive_burden_proxy"]["insufficient_supporting_evidence"] += 1
            continue

        symptom_component = _shrink_toward_midpoint(
            _bounded_signal(symptom_mean, scale=_SYMPTOM_SCORE_SCALE),
            evidence_count=symptom_count,
            full_evidence_count=2,
        )
        support_factor = 0.9 if support_family_count == 2 else 1.0
        stable_cognitive_burden_proxy = _clamp(
            (0.65 * global_cognition_dev + 0.35 * symptom_component)
            * (1.0 - (0.35 * state_noise_score))
            * support_factor
        )
        target_rows_with_split.append(
            (
                _target_row(
                    subject_id=row["subject_id"],
                    visit_id=visit_id,
                    target_name="stable_cognitive_burden_proxy",
                    target_value=stable_cognitive_burden_proxy,
                ),
                split_name,
            )
        )
        emitted_by_target_and_split["stable_cognitive_burden_proxy"][split_name] += 1

    derived_target_rows = [
        target_row
        for target_row, _split_name in sorted(
            target_rows_with_split,
            key=lambda item: (
                item[0]["subject_id"],
                item[0]["visit_id"],
                item[0]["target_name"],
            ),
        )
    ]
    derived_targets_path = _write_csv_rows(
        derived_target_rows,
        targets_path / "derived_targets.csv",
        fieldnames=DERIVED_TARGETS.columns,
    )
    target_manifest_path = targets_path / "target_manifest.json"

    counts_by_target_name = {target_name: 0 for target_name in TARGET_LABELS}
    counts_by_target_name.update(Counter(row["target_name"] for row in derived_target_rows))
    coverage_by_split = _coverage_by_split(
        total_visits_by_split=total_visits_by_split,
        emitted_by_target_and_split=emitted_by_target_and_split,
    )
    public_path_limit_note = None
    if (
        counts_by_target_name.get("global_cognition_dev", 0) < total_visits
        or counts_by_target_name.get("stable_cognitive_burden_proxy", 0) < total_visits
    ):
        public_path_limit_note = (
            "The live strict-open public path is too thin for full cognition-derived target coverage; "
            "targets are emitted only where public evidence is sufficient."
        )

    source = str(
        feature_manifest.get(
            "source",
            harmonization_manifest.get("source", split_manifest.get("source", "tcp")),
        )
    )
    dataset_version = (
        feature_manifest.get("dataset_version")
        or harmonization_manifest.get("dataset_version")
        or split_manifest.get("dataset_version")
    )
    output_paths = {
        "derived_targets": str(derived_targets_path),
        "target_manifest": str(target_manifest_path),
    }
    target_manifest = {
        "command": list(command),
        "source": source,
        "dataset_version": dataset_version,
        "git_sha": git_sha,
        "seed": seed,
        "features_dir": str(features_path),
        "harmonized_dir": str(harmonized_path),
        "splits_dir": str(splits_path),
        "input_paths": _existing_paths(
            visit_features_path,
            feature_manifest_path,
            split_assignments_path,
            split_manifest_path,
            harmonization_manifest_path,
        ),
        "output_paths": output_paths,
        "row_counts": {
            "derived_targets": len(derived_target_rows),
        },
        "counts_by_target_name": dict(sorted(counts_by_target_name.items())),
        "target_coverage_by_split": coverage_by_split,
        "reasons_targets_were_unavailable": {
            target_name: dict(sorted(omission_reasons[target_name].items()))
            for target_name in TARGET_LABELS
        },
        "public_path_limit_note": public_path_limit_note,
        "split_contract_validation": {
            "mismatched_rows": len(split_validation_mismatches),
            "policy": (
                "Targets inherit split values from visit features and are checked against the "
                "frozen subject-level split assignments; no target-specific split reassignment occurs."
            ),
        },
        "target_generation_policy": (
            "Targets are continuous public-only soft targets using fixed bounded score transforms rather than "
            "cohort-relative normalization. Missing evidence leaves target rows omitted instead of synthesized."
        ),
    }
    write_json_artifact(target_manifest, target_manifest_path)

    run_manifest_path = manifests_path / "tcp_build_targets_run_manifest.json"
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
        "derived_targets": str(derived_targets_path),
        "target_manifest": str(target_manifest_path),
        "run_manifest": str(run_manifest_path),
        "targets_dir": str(targets_path),
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


def _coverage_by_split(
    *,
    total_visits_by_split: Counter[str],
    emitted_by_target_and_split: dict[str, Counter[str]],
) -> dict[str, dict[str, dict[str, float | int]]]:
    payload: dict[str, dict[str, dict[str, float | int]]] = {}
    split_names = tuple(sorted(total_visits_by_split))
    for target_name in TARGET_LABELS:
        split_payload: dict[str, dict[str, float | int]] = {}
        for split_name in split_names:
            emitted = emitted_by_target_and_split[target_name][split_name]
            total = total_visits_by_split[split_name]
            rate = round((emitted / total), 6) if total else 0.0
            split_payload[split_name] = {
                "emitted": emitted,
                "total_visits": total,
                "rate": rate,
            }
        payload[target_name] = split_payload
    return payload


def _target_row(
    *,
    subject_id: str,
    visit_id: str,
    target_name: str,
    target_value: float,
) -> dict[str, str]:
    return {
        "subject_id": subject_id,
        "visit_id": visit_id,
        "target_name": target_name,
        "target_label": TARGET_LABELS[target_name],
        "target_value": _format_float(target_value),
    }


def _shrink_toward_midpoint(value: float, *, evidence_count: int, full_evidence_count: int) -> float:
    evidence_weight = min(max(evidence_count, 0) / full_evidence_count, 1.0)
    return _clamp(0.5 + ((value - 0.5) * evidence_weight))


def _bounded_signal(value: float, *, scale: float) -> float:
    if value <= 0:
        return 0.0
    return _clamp(1.0 - math.exp(-value / scale))


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


def _format_float(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".") or "0"


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(value, upper))


__all__ = ["TARGET_LABELS", "run_strict_open_target_build"]

"""Deterministic baseline family definitions for strict-open."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class BaselineFamilySpec:
    """Serializable metadata for a strict-open baseline family."""

    name: str
    label: str
    description: str
    signal_recipe: str
    feature_columns: tuple[str, ...]
    supported_targets: tuple[str, ...]
    unsupported_targets: dict[str, str]


_BASELINE_FAMILY_SPECS = (
    BaselineFamilySpec(
        name="cognition_only_snapshot",
        label="Cognition-only snapshot baseline",
        description=(
            "A single current-visit cognition composite calibrated against cognition-derived strict-open targets."
        ),
        signal_recipe=(
            "Weighted composite over cognition score mean, cognition score max, and cognition evidence count."
        ),
        feature_columns=(
            "cognition_available",
            "cognition_score_count",
            "cognition_score_mean",
            "cognition_score_max",
        ),
        supported_targets=("global_cognition_dev", "stable_cognitive_burden_proxy"),
        unsupported_targets={
            "state_noise_score": (
                "The cognition-only snapshot baseline does not use MRI, visit-gap, or missingness inputs needed "
                "to honestly proxy state noise."
            ),
        },
    ),
    BaselineFamilySpec(
        name="symptom_only_snapshot",
        label="Symptom-only snapshot baseline",
        description=(
            "A single current-visit symptom burden composite calibrated against the stable burden proxy target."
        ),
        signal_recipe=(
            "Weighted composite over symptom score mean, symptom score max, and symptom evidence count."
        ),
        feature_columns=(
            "symptom_available",
            "symptom_score_count",
            "symptom_score_mean",
            "symptom_score_max",
        ),
        supported_targets=("stable_cognitive_burden_proxy",),
        unsupported_targets={
            "global_cognition_dev": (
                "The symptom-only snapshot baseline omits cognition inputs required for the cognition deviation target."
            ),
            "state_noise_score": (
                "The symptom-only snapshot baseline omits MRI and visit-quality inputs required for the state noise target."
            ),
        },
    ),
    BaselineFamilySpec(
        name="mri_only_snapshot",
        label="MRI-only snapshot baseline",
        description=(
            "A current-visit MRI completeness and motion-quality composite calibrated against visit state noise."
        ),
        signal_recipe=(
            "Weighted composite over MRI present fraction, motion quality, and modality availability."
        ),
        feature_columns=(
            "mri_available_modality_count",
            "mri_present_fraction",
            "mri_mean_fd_mean",
            "mri_qc_missing_indicator",
        ),
        supported_targets=("state_noise_score",),
        unsupported_targets={
            "global_cognition_dev": (
                "The MRI-only snapshot baseline lacks cognition evidence required for cognition-derived targets."
            ),
            "stable_cognitive_burden_proxy": (
                "The MRI-only snapshot baseline lacks cognition and symptom support required for the stable burden proxy."
            ),
        },
    ),
    BaselineFamilySpec(
        name="simple_multimodal_snapshot",
        label="Simple multimodal snapshot baseline",
        description=(
            "A direct multimodal snapshot that requires cognition, symptom, and MRI support on the same visit."
        ),
        signal_recipe=(
            "Fixed weighted average of the cognition, symptom, and MRI snapshot composites from the same visit."
        ),
        feature_columns=(
            "cognition_available",
            "cognition_score_count",
            "cognition_score_mean",
            "cognition_score_max",
            "symptom_available",
            "symptom_score_count",
            "symptom_score_mean",
            "symptom_score_max",
            "mri_available_modality_count",
            "mri_present_fraction",
            "mri_mean_fd_mean",
            "mri_qc_missing_indicator",
        ),
        supported_targets=(
            "state_noise_score",
            "global_cognition_dev",
            "stable_cognitive_burden_proxy",
        ),
        unsupported_targets={},
    ),
    BaselineFamilySpec(
        name="snapshot_latent_baseline",
        label="Snapshot latent baseline",
        description=(
            "A deterministic one-dimensional latent composite built from train-only normalized current-visit features."
        ),
        signal_recipe=(
            "Mean oriented z-score over available cognition, symptom, MRI completeness, motion, and missingness features."
        ),
        feature_columns=(
            "feature_family_available_count",
            "cognition_available",
            "cognition_score_mean",
            "symptom_available",
            "symptom_score_mean",
            "mri_present_fraction",
            "mri_mean_fd_mean",
            "missingness_burden",
        ),
        supported_targets=(
            "state_noise_score",
            "global_cognition_dev",
            "stable_cognitive_burden_proxy",
        ),
        unsupported_targets={},
    ),
)

BASELINE_FAMILY_SPECS = {spec.name: spec for spec in _BASELINE_FAMILY_SPECS}
BASELINE_FAMILY_NAMES = tuple(spec.name for spec in _BASELINE_FAMILY_SPECS)
_LATENT_FEATURES = (
    ("cognition_score_mean", 1.0),
    ("symptom_score_mean", 1.0),
    ("mri_present_fraction", 1.0),
    ("mri_mean_fd_mean", -1.0),
    ("missingness_burden", -1.0),
)
_MOTION_SCALE = 0.5


def list_baseline_family_specs() -> list[dict[str, Any]]:
    """Return serializable metadata for all baseline families."""

    return [
        {
            "baseline_name": spec.name,
            "label": spec.label,
            "description": spec.description,
            "signal_recipe": spec.signal_recipe,
            "feature_columns": list(spec.feature_columns),
            "supported_targets": list(spec.supported_targets),
            "unsupported_targets": dict(sorted(spec.unsupported_targets.items())),
        }
        for spec in _BASELINE_FAMILY_SPECS
    ]


def baseline_family_spec(name: str) -> BaselineFamilySpec:
    """Return a baseline family spec by name."""

    return BASELINE_FAMILY_SPECS[name]


def unsupported_target_reason(baseline_name: str, target_name: str) -> str | None:
    """Return the design-level reason a target is unsupported by a baseline family."""

    return BASELINE_FAMILY_SPECS[baseline_name].unsupported_targets.get(target_name)


def build_family_state(
    baseline_name: str,
    train_feature_rows: list[dict[str, str]],
) -> dict[str, Any]:
    """Fit train-only baseline state."""

    if baseline_name != "snapshot_latent_baseline":
        return {}

    normalization: dict[str, dict[str, float]] = {}
    for feature_name, direction in _LATENT_FEATURES:
        values = [
            value
            for row in train_feature_rows
            if (value := _parse_float(row.get(feature_name))) is not None
        ]
        mean_value = sum(values) / len(values) if values else 0.0
        if len(values) > 1:
            variance = sum((value - mean_value) ** 2 for value in values) / len(values)
            scale = math.sqrt(variance)
        else:
            scale = 0.0
        normalization[feature_name] = {
            "mean": round(mean_value, 6),
            "scale": round(scale if scale > 1e-12 else 1.0, 6),
            "direction": direction,
        }
    return {"normalization": normalization}


def compute_signal(
    baseline_name: str,
    target_name: str,
    feature_row: dict[str, str],
    family_state: dict[str, Any],
) -> tuple[float | None, str | None]:
    """Compute a row-level baseline signal or return an explicit support failure reason."""

    if baseline_name == "cognition_only_snapshot":
        return _cognition_signal(feature_row)
    if baseline_name == "symptom_only_snapshot":
        return _symptom_signal(feature_row)
    if baseline_name == "mri_only_snapshot":
        return _mri_signal(feature_row)
    if baseline_name == "simple_multimodal_snapshot":
        return _simple_multimodal_signal(feature_row)
    if baseline_name == "snapshot_latent_baseline":
        return _snapshot_latent_signal(target_name, feature_row, family_state)
    raise ValueError(f"Unknown baseline family {baseline_name}")


def fit_linear_calibrator(signal_target_pairs: list[tuple[float, float]]) -> dict[str, Any]:
    """Fit a deterministic one-dimensional calibrator on train-only rows."""

    if not signal_target_pairs:
        raise ValueError("At least one train row is required to fit a baseline calibrator.")

    signals = [signal for signal, _target in signal_target_pairs]
    targets = [target for _signal, target in signal_target_pairs]
    mean_signal = sum(signals) / len(signals)
    mean_target = sum(targets) / len(targets)
    signal_variance = sum((signal - mean_signal) ** 2 for signal in signals)
    if len(signal_target_pairs) == 1 or signal_variance <= 1e-12:
        return {
            "kind": "mean_only",
            "train_row_count": len(signal_target_pairs),
            "mean_signal": round(mean_signal, 6),
            "mean_target": round(mean_target, 6),
            "signal_variance": round(signal_variance, 6),
            "slope": 0.0,
            "intercept": round(mean_target, 6),
        }

    covariance = sum(
        (signal - mean_signal) * (target - mean_target)
        for signal, target in signal_target_pairs
    )
    slope = covariance / signal_variance
    intercept = mean_target - (slope * mean_signal)
    return {
        "kind": "univariate_linear",
        "train_row_count": len(signal_target_pairs),
        "mean_signal": round(mean_signal, 6),
        "mean_target": round(mean_target, 6),
        "signal_variance": round(signal_variance, 6),
        "slope": round(slope, 6),
        "intercept": round(intercept, 6),
    }


def apply_linear_calibrator(signal_value: float, fit: dict[str, Any]) -> float:
    """Generate a bounded baseline prediction from a fitted calibrator."""

    prediction = float(fit["intercept"]) + (float(fit["slope"]) * signal_value)
    return _clamp(prediction)


def _cognition_signal(feature_row: dict[str, str]) -> tuple[float | None, str | None]:
    cognition_available = _parse_int(feature_row.get("cognition_available")) or 0
    cognition_mean = _parse_float(feature_row.get("cognition_score_mean"))
    if cognition_available <= 0 or cognition_mean is None:
        return None, "Current-visit cognition evidence is unavailable for this row."

    cognition_max = _parse_float(feature_row.get("cognition_score_max"))
    cognition_count = _parse_int(feature_row.get("cognition_score_count")) or 0
    signal_value = (
        (0.7 * _clamp(cognition_mean / 4.0))
        + (0.2 * _clamp((cognition_max if cognition_max is not None else cognition_mean) / 4.0))
        + (0.1 * _clamp(cognition_count / 4.0))
    )
    return round(signal_value, 6), None


def _symptom_signal(feature_row: dict[str, str]) -> tuple[float | None, str | None]:
    symptom_available = _parse_int(feature_row.get("symptom_available")) or 0
    symptom_mean = _parse_float(feature_row.get("symptom_score_mean"))
    if symptom_available <= 0 or symptom_mean is None:
        return None, "Current-visit symptom evidence is unavailable for this row."

    symptom_max = _parse_float(feature_row.get("symptom_score_max"))
    symptom_count = _parse_int(feature_row.get("symptom_score_count")) or 0
    signal_value = (
        (0.7 * _clamp(symptom_mean / 4.0))
        + (0.2 * _clamp((symptom_max if symptom_max is not None else symptom_mean) / 4.0))
        + (0.1 * _clamp(symptom_count / 4.0))
    )
    return round(signal_value, 6), None


def _mri_signal(feature_row: dict[str, str]) -> tuple[float | None, str | None]:
    modality_count = _parse_int(feature_row.get("mri_available_modality_count")) or 0
    present_fraction = _parse_float(feature_row.get("mri_present_fraction"))
    if modality_count <= 0 or present_fraction is None:
        return None, "Current-visit MRI completeness is unavailable for this row."

    mean_fd = _parse_float(feature_row.get("mri_mean_fd_mean"))
    qc_missing_indicator = _parse_int(feature_row.get("mri_qc_missing_indicator")) or 0
    if mean_fd is None or qc_missing_indicator > 0:
        return None, "Current-visit MRI motion QC is unavailable for this row."

    motion_quality = 1.0 - _clamp(mean_fd / _MOTION_SCALE)
    modality_fraction = _clamp(modality_count / 9.0)
    signal_value = (
        (0.55 * _clamp(present_fraction))
        + (0.30 * _clamp(motion_quality))
        + (0.15 * modality_fraction)
    )
    return round(signal_value, 6), None


def _simple_multimodal_signal(feature_row: dict[str, str]) -> tuple[float | None, str | None]:
    cognition_signal, cognition_reason = _cognition_signal(feature_row)
    symptom_signal, symptom_reason = _symptom_signal(feature_row)
    mri_signal, mri_reason = _mri_signal(feature_row)

    missing_support = [
        reason
        for reason in (cognition_reason, symptom_reason, mri_reason)
        if reason is not None
    ]
    if missing_support:
        return None, "The simple multimodal snapshot requires current-visit cognition, symptom, and MRI support."

    signal_value = (
        (0.4 * cognition_signal)
        + (0.35 * symptom_signal)
        + (0.25 * mri_signal)
    )
    return round(signal_value, 6), None


def _snapshot_latent_signal(
    target_name: str,
    feature_row: dict[str, str],
    family_state: dict[str, Any],
) -> tuple[float | None, str | None]:
    family_count = _parse_int(feature_row.get("feature_family_available_count")) or 0
    cognition_available = _parse_int(feature_row.get("cognition_available")) or 0
    symptom_available = _parse_int(feature_row.get("symptom_available")) or 0

    if target_name == "state_noise_score" and family_count < 2:
        return None, "The snapshot latent baseline requires at least two feature families for state noise."
    if target_name == "global_cognition_dev":
        if cognition_available <= 0:
            return None, "The snapshot latent baseline requires cognition evidence for cognition deviation."
        if family_count < 2:
            return None, "The snapshot latent baseline requires cognition plus one additional feature family."
    if target_name == "stable_cognitive_burden_proxy":
        if cognition_available <= 0 or symptom_available <= 0:
            return None, "The snapshot latent baseline requires cognition and symptom evidence for stable burden."

    normalization = family_state.get("normalization", {})
    values: list[float] = []
    for feature_name, _direction in _LATENT_FEATURES:
        raw_value = _parse_float(feature_row.get(feature_name))
        if raw_value is None:
            continue
        feature_normalization = normalization.get(feature_name, {})
        mean_value = float(feature_normalization.get("mean", 0.0))
        scale_value = float(feature_normalization.get("scale", 1.0)) or 1.0
        direction = float(feature_normalization.get("direction", 1.0))
        values.append(direction * ((raw_value - mean_value) / scale_value))

    if target_name == "state_noise_score" and len(values) < 2:
        return None, "The snapshot latent baseline needs at least two non-missing normalized components."
    if target_name == "global_cognition_dev" and len(values) < 2:
        return None, "The snapshot latent baseline needs cognition plus one additional normalized component."
    if target_name == "stable_cognitive_burden_proxy" and len(values) < 2:
        return None, "The snapshot latent baseline needs cognition and symptom support for stable burden."

    signal_value = sum(values) / len(values)
    return round(signal_value, 6), None


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


def _clamp(value: float, *, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


__all__ = [
    "BASELINE_FAMILY_NAMES",
    "BASELINE_FAMILY_SPECS",
    "BaselineFamilySpec",
    "apply_linear_calibrator",
    "baseline_family_spec",
    "build_family_state",
    "compute_signal",
    "fit_linear_calibrator",
    "list_baseline_family_specs",
    "unsupported_target_reason",
]

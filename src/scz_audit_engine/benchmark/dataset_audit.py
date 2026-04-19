"""Dataset-audit orchestration for the benchmark registry."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .dataset_registry import (
    ACCESS_TIER_DECISION_ORDER,
    OUTCOME_FAMILIES,
    BenchmarkDecision,
    BenchmarkDecisionLayer,
    DatasetRegistryEntry,
    derive_benchmark_decision,
    write_dataset_registry,
)
from .provenance import write_json_artifact, write_text_artifact
from .run_manifest import build_run_manifest, utc_now_iso, write_run_manifest
from .sources import SourceAdapter, build_default_source_adapters

JSON_REPORT_NAME = "dataset_audit.json"
MARKDOWN_REPORT_NAME = "dataset_audit.md"
RUN_MANIFEST_NAME = "benchmark_audit_datasets_run_manifest.json"


def _format_cohort_list(cohorts: tuple[str, ...]) -> str:
    return ", ".join(f"`{cohort}`" for cohort in cohorts) if cohorts else "none"


def _format_access_tier_scope(allowed_access_tiers: tuple[str, ...]) -> str:
    return ", ".join(f"`{access_tier}`" for access_tier in allowed_access_tiers)


@dataclass(frozen=True, slots=True)
class DatasetAuditArtifacts:
    """Paths and structured outputs emitted by the benchmark dataset audit."""

    registry_path: Path
    json_report_path: Path
    markdown_report_path: Path
    manifest_path: Path
    decision: BenchmarkDecision
    entries: tuple[DatasetRegistryEntry, ...]
    generated_at: str

    def to_summary(self) -> dict[str, object]:
        return {
            "audited_cohort_count": len(self.entries),
            "current_access_tier": self.decision.current_access_tier,
            "decision": self.decision.state,
            "claim_level": self.decision.claim_level,
            "recommended_outcome_families": list(
                self.decision.recommended_outcome_families
            ),
            "recommended_next_step": self.decision.recommended_next_step,
            "dataset_registry": str(self.registry_path),
            "json_report": str(self.json_report_path),
            "markdown_report": str(self.markdown_report_path),
            "run_manifest": str(self.manifest_path),
        }


def _render_markdown_report(
    entries: tuple[DatasetRegistryEntry, ...],
    decision: BenchmarkDecision,
) -> str:
    lines = [
        "# Benchmark Dataset Audit",
        "",
        f"- Current access tier in scope: `{decision.current_access_tier}`",
        f"- Current benchmark decision under `strict_open`: `{decision.state}`",
        f"- Current claim level under `strict_open`: `{decision.claim_level}`",
        f"- Recommended next step: `{decision.recommended_next_step}`",
        f"- Recommendation explanation: {decision.recommended_next_step_explanation}",
        "",
        "## Access-Tier Decisions",
        "",
        (
            "| Access tier in scope | Included cohort tiers | Decision | Claim level | "
            "Cross-sectional representation cohorts | Narrow support cohorts |"
        ),
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for access_tier in ACCESS_TIER_DECISION_ORDER:
        layer = decision.layer_for(access_tier)
        lines.append(
            "| "
            f"`{layer.access_tier}` | "
            f"{_format_access_tier_scope(layer.allowed_access_tiers)} | "
            f"`{layer.state}` | "
            f"`{layer.claim_level}` | "
            f"{', '.join(layer.cross_sectional_representation_cohorts) or 'none'} | "
            f"{', '.join(layer.narrow_supporting_cohorts) or 'none'} |"
        )

    lines.extend(["", "## Access-Tier Notes", ""])
    for access_tier in ACCESS_TIER_DECISION_ORDER:
        layer = decision.layer_for(access_tier)
        lines.extend(
            [
                f"### `{layer.access_tier}`",
                f"- Included cohort tiers: {_format_access_tier_scope(layer.allowed_access_tiers)}",
                f"- Decision explanation: {layer.explanation}",
                f"- Claim-level explanation: {layer.claim_level_explanation}",
                (
                    "- Cross-sectional representation cohorts: "
                    f"{_format_cohort_list(layer.cross_sectional_representation_cohorts)}"
                ),
                (
                    "- Narrow benchmark supporting cohorts: "
                    f"{_format_cohort_list(layer.narrow_supporting_cohorts)}"
                ),
                (
                    "- Full external-validation cohorts: "
                    f"{_format_cohort_list(layer.full_external_validation_cohorts)}"
                ),
                (
                    "- Concurrent-only cohorts: "
                    f"{_format_cohort_list(layer.concurrent_only_cohorts)}"
                ),
                (
                    "- Prospectively usable cohorts: "
                    f"{_format_cohort_list(layer.prospectively_usable_cohorts)}"
                ),
                (
                    "- Limiting factors: "
                    + (
                        "; ".join(layer.limiting_factors)
                        if layer.limiting_factors
                        else "none"
                    )
                ),
                "",
            ]
        )

    lines.extend(
        [
            "## Outcome Family Support By Access Tier",
            "",
            (
                "| Access tier in scope | Outcome family | Narrow benchmark support | "
                "Full external-validation support | Prospective support |"
            ),
            "| --- | --- | --- | --- | --- |",
        ]
    )
    for access_tier in ACCESS_TIER_DECISION_ORDER:
        layer = decision.layer_for(access_tier)
        for family in OUTCOME_FAMILIES:
            lines.append(
                "| "
                f"`{layer.access_tier}` | "
                f"`{family}` | "
                f"{', '.join(layer.support_by_outcome_family[family]) or 'none'} | "
                f"{', '.join(layer.full_external_validation_support_by_outcome_family[family]) or 'none'} | "
                f"{', '.join(layer.prospective_support_by_outcome_family[family]) or 'none'} |"
            )

    lines.extend(
        [
            "",
            "## Audited Cohorts",
            "",
            (
                "| Dataset | Access tier | Local status | Benchmark v0 eligibility | "
                "Representation support | Temporal validity | Claim ceiling | "
                "Narrow support if tier allowed | Outcome families |"
            ),
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for entry in entries:
        families = ", ".join(entry.benchmarkable_outcome_families) or "none"
        lines.append(
            "| "
            f"`{entry.dataset_id}` | `{entry.access_tier}` | `{entry.local_status}` | "
            f"`{entry.benchmark_v0_eligibility}` | `{entry.representation_comparison_support}` | "
            f"`{entry.outcome_temporal_validity}` | `{entry.claim_level_ceiling}` | "
            f"`{'yes' if entry.counts_toward_narrow_benchmark_support_if_access_allowed else 'no'}` | {families} |"
        )

    lines.extend(["", "## Cohort Notes", ""])
    for entry in entries:
        lines.append(f"### `{entry.dataset_id}`")
        lines.append(f"- Label: {entry.dataset_label}")
        lines.append(f"- Access tier: {entry.access_tier}")
        lines.append(f"- Local status: {entry.local_status}")
        lines.append(f"- Benchmark v0 eligibility: {entry.benchmark_v0_eligibility}")
        lines.append(
            f"- Representation comparison support: {entry.representation_comparison_support}"
        )
        lines.append(f"- Predictor timepoint: {entry.predictor_timepoint}")
        lines.append(f"- Outcome timepoint: {entry.outcome_timepoint}")
        lines.append(f"- Outcome window: {entry.outcome_window}")
        lines.append(f"- Outcome temporal validity: {entry.outcome_temporal_validity}")
        lines.append(
            f"- Concurrent endpoint only: {'yes' if entry.concurrent_endpoint_only else 'no'}"
        )
        lines.append(
            f"- Prospectively usable: {'yes' if entry.outcome_is_prospective else 'no'}"
        )
        lines.append(
            "- Cross-sectional representation support if tier allowed: "
            f"{'yes' if entry.supports_cross_sectional_representation_if_access_allowed else 'no'}"
        )
        lines.append(
            "- Narrow benchmark support if tier allowed: "
            f"{'yes' if entry.counts_toward_narrow_benchmark_support_if_access_allowed else 'no'}"
        )
        lines.append(
            f"- Claim level contributions: {', '.join(entry.claim_level_contributions) or 'none'}"
        )
        lines.append(
            "- Benchmarkable outcome families: "
            f"{', '.join(entry.benchmarkable_outcome_families) or 'none'}"
        )
        lines.append(f"- Diagnosis coverage: {entry.diagnosis_coverage}")
        lines.append(
            f"- Functioning scales: {', '.join(entry.functioning_scales) or 'none confirmed'}"
        )
        lines.append(f"- Longitudinal coverage: {entry.longitudinal_coverage}")
        lines.append(f"- Outcome availability: {entry.outcome_availability}")
        lines.append(f"- Major limitations: {entry.known_limitations}")
        if entry.audit_summary:
            lines.append(f"- Audit summary: {entry.audit_summary}")
        if entry.provenance_urls:
            lines.append(f"- Primary sources: {', '.join(entry.provenance_urls)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _build_layer_outcome_family_support(
    layer: BenchmarkDecisionLayer,
) -> dict[str, dict[str, object]]:
    return {
        family: {
            "narrow_benchmark_support": {
                "count": len(layer.support_by_outcome_family[family]),
                "cohorts": list(layer.support_by_outcome_family[family]),
            },
            "full_external_validation_support": {
                "count": len(
                    layer.full_external_validation_support_by_outcome_family[family]
                ),
                "cohorts": list(
                    layer.full_external_validation_support_by_outcome_family[family]
                ),
            },
            "prospective_support": {
                "count": len(layer.prospective_support_by_outcome_family[family]),
                "cohorts": list(layer.prospective_support_by_outcome_family[family]),
            },
        }
        for family in OUTCOME_FAMILIES
    }


def _build_json_report(
    entries: tuple[DatasetRegistryEntry, ...],
    decision: BenchmarkDecision,
) -> dict[str, Any]:
    return {
        "decision": decision.to_dict(),
        "outcome_family_support_by_access_tier": {
            access_tier: _build_layer_outcome_family_support(
                decision.layer_for(access_tier)
            )
            for access_tier in ACCESS_TIER_DECISION_ORDER
        },
        "audited_cohorts": [entry.to_dict() for entry in entries],
    }


def run_benchmark_dataset_audit(
    *,
    registry_path: str | Path,
    reports_root: str | Path,
    manifests_root: str | Path,
    repo_root: str | Path | None,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
    adapters: tuple[SourceAdapter, ...] | None = None,
) -> DatasetAuditArtifacts:
    audit_adapters = adapters or build_default_source_adapters()
    entries = tuple(adapter.audit() for adapter in audit_adapters)
    decision = derive_benchmark_decision(entries)
    generated_at = utc_now_iso()

    registry_output_path = write_dataset_registry(entries, registry_path)
    reports_dir = Path(reports_root)
    manifests_dir = Path(manifests_root)

    json_report_path = write_json_artifact(
        _build_json_report(entries, decision),
        reports_dir / JSON_REPORT_NAME,
    )
    markdown_report_path = write_text_artifact(
        _render_markdown_report(entries, decision),
        reports_dir / MARKDOWN_REPORT_NAME,
    )
    manifest_path = write_run_manifest(
        build_run_manifest(
            dataset_source="benchmark-registry",
            command=command,
            git_sha=git_sha,
            seed=seed,
            repo_root=repo_root,
            output_paths={
                "dataset_registry": registry_output_path,
                "json_report": json_report_path,
                "markdown_report": markdown_report_path,
            },
            timestamp=generated_at,
        ),
        manifests_dir / RUN_MANIFEST_NAME,
    )
    return DatasetAuditArtifacts(
        registry_path=registry_output_path,
        json_report_path=json_report_path,
        markdown_report_path=markdown_report_path,
        manifest_path=manifest_path,
        decision=decision,
        entries=entries,
        generated_at=generated_at,
    )


__all__ = [
    "DatasetAuditArtifacts",
    "JSON_REPORT_NAME",
    "MARKDOWN_REPORT_NAME",
    "RUN_MANIFEST_NAME",
    "run_benchmark_dataset_audit",
]

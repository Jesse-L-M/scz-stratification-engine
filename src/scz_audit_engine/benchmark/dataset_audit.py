"""Dataset-audit orchestration for the benchmark registry."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .dataset_registry import (
    OUTCOME_FAMILIES,
    BenchmarkDecision,
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
            "decision": self.decision.state,
            "recommended_outcome_families": list(self.decision.recommended_outcome_families),
            "dataset_registry": str(self.registry_path),
            "json_report": str(self.json_report_path),
            "markdown_report": str(self.markdown_report_path),
            "run_manifest": str(self.manifest_path),
        }


def _render_markdown_report(
    entries: tuple[DatasetRegistryEntry, ...],
    decision: BenchmarkDecision,
    *,
    generated_at: str,
) -> str:
    lines = [
        "# Benchmark Dataset Audit",
        "",
        f"- Generated at: `{generated_at}`",
        f"- Recommended decision: `{decision.state}`",
        f"- Explanation: {decision.explanation}",
        "",
        "## Outcome Family Support",
        "",
        "| Outcome family | Public benchmark-eligible cohorts |",
        "| --- | --- |",
    ]
    for family in OUTCOME_FAMILIES:
        cohorts = decision.support_by_outcome_family[family]
        display = ", ".join(cohorts) if cohorts else "none"
        lines.append(f"| `{family}` | {display} |")

    lines.extend(
        [
            "",
            "## Audited Cohorts",
            "",
            "| Dataset | Access | Population | Benchmarkable outcome families |",
            "| --- | --- | --- | --- |",
        ]
    )
    for entry in entries:
        families = ", ".join(entry.benchmarkable_outcome_families) or "none"
        lines.append(
            f"| `{entry.dataset_id}` | `{entry.access_level}` | {entry.population_scope} | {families} |"
        )

    lines.extend(["", "## Cohort Notes", ""])
    for entry in entries:
        lines.append(f"### `{entry.dataset_id}`")
        lines.append(f"- Label: {entry.dataset_label}")
        lines.append(f"- Diagnosis coverage: {entry.diagnosis_coverage}")
        lines.append(
            f"- Functioning scales: {', '.join(entry.functioning_scales) or 'none confirmed'}"
        )
        lines.append(f"- Longitudinal coverage: {entry.longitudinal_coverage}")
        lines.append(f"- Outcome availability: {entry.outcome_availability}")
        lines.append(f"- Major limitations: {entry.known_limitations}")
        if entry.provenance_urls:
            lines.append(f"- Primary sources: {', '.join(entry.provenance_urls)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _build_json_report(
    entries: tuple[DatasetRegistryEntry, ...],
    decision: BenchmarkDecision,
    *,
    generated_at: str,
) -> dict[str, Any]:
    return {
        "generated_at": generated_at,
        "decision": decision.to_dict(),
        "outcome_family_support": {
            family: {
                "count": len(decision.support_by_outcome_family[family]),
                "cohorts": list(decision.support_by_outcome_family[family]),
            }
            for family in OUTCOME_FAMILIES
        },
        "audited_cohorts": [entry.to_dict() for entry in entries],
    }


def run_benchmark_dataset_audit(
    *,
    registry_path: str | Path,
    reports_root: str | Path,
    manifests_root: str | Path,
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
        _build_json_report(entries, decision, generated_at=generated_at),
        reports_dir / JSON_REPORT_NAME,
    )
    markdown_report_path = write_text_artifact(
        _render_markdown_report(entries, decision, generated_at=generated_at),
        reports_dir / MARKDOWN_REPORT_NAME,
    )
    manifest_path = write_run_manifest(
        build_run_manifest(
            dataset_source="benchmark-registry",
            command=command,
            git_sha=git_sha,
            seed=seed,
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

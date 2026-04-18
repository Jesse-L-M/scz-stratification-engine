"""Artifact writers for the canonical benchmark schema."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .provenance import write_json_artifact, write_text_artifact
from .run_manifest import build_run_manifest, utc_now_iso, write_run_manifest
from .schema import BenchmarkSchema, benchmark_schema

JSON_SCHEMA_NAME = "benchmark_schema.json"
MARKDOWN_SCHEMA_NAME = "benchmark_schema.md"
RUN_MANIFEST_NAME = "benchmark_define_schema_run_manifest.json"


@dataclass(frozen=True, slots=True)
class BenchmarkSchemaArtifacts:
    """Paths and metadata emitted by schema definition."""

    schema: BenchmarkSchema
    json_schema_path: Path
    markdown_schema_path: Path
    manifest_path: Path
    generated_at: str

    def to_summary(self) -> dict[str, object]:
        return {
            "schema_version": self.schema.version,
            "table_count": len(self.schema.tables),
            "tables": list(self.schema.table_names),
            "json_schema": str(self.json_schema_path),
            "markdown_schema": str(self.markdown_schema_path),
            "run_manifest": str(self.manifest_path),
        }


def _build_schema_json_payload(schema: BenchmarkSchema) -> dict[str, object]:
    return schema.to_artifact_dict()


def _render_schema_markdown(schema: BenchmarkSchema) -> str:
    lines = [
        "# Benchmark Schema",
        "",
        f"- Schema version: `{schema.version}`",
        "- Design posture: preserve the current `narrow-go` state without pretending every cohort is fully benchmark-ready.",
        "- Claim-level honesty: concurrent-only endpoints, limited cohorts, diagnosis granularity, and mapping caveats stay first-class in the contract.",
        "",
        "## Canonical Tables",
        "",
        "| Table | Row grain | Purpose |",
        "| --- | --- | --- |",
    ]
    for table in schema.tables:
        lines.append(f"| `{table.name}` | {table.row_grain} | {table.purpose} |")

    for table in schema.tables:
        lines.extend(
            [
                "",
                f"## `{table.name}`",
                "",
                f"- Purpose: {table.purpose}",
                f"- Row grain: {table.row_grain}",
                f"- Required columns: {', '.join(f'`{column}`' for column in table.required_columns)}",
                f"- Optional columns: {', '.join(f'`{column}`' for column in table.optional_columns) or 'none'}",
            ]
        )

    return "\n".join(lines).rstrip() + "\n"


def run_benchmark_define_schema(
    *,
    schema_root: str | Path,
    manifests_root: str | Path,
    repo_root: str | Path | None,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
) -> BenchmarkSchemaArtifacts:
    """Write the canonical benchmark schema artifacts and provenance."""

    schema = benchmark_schema()
    generated_at = utc_now_iso()
    schema_dir = Path(schema_root)
    manifests_dir = Path(manifests_root)

    json_schema_path = write_json_artifact(
        _build_schema_json_payload(schema),
        schema_dir / JSON_SCHEMA_NAME,
    )
    markdown_schema_path = write_text_artifact(
        _render_schema_markdown(schema),
        schema_dir / MARKDOWN_SCHEMA_NAME,
    )
    manifest_path = write_run_manifest(
        build_run_manifest(
            dataset_source="benchmark-schema",
            command=command,
            git_sha=git_sha,
            seed=seed,
            repo_root=repo_root,
            output_paths={
                "json_schema": json_schema_path,
                "markdown_schema": markdown_schema_path,
            },
            timestamp=generated_at,
        ),
        manifests_dir / RUN_MANIFEST_NAME,
    )

    return BenchmarkSchemaArtifacts(
        schema=schema,
        json_schema_path=json_schema_path,
        markdown_schema_path=markdown_schema_path,
        manifest_path=manifest_path,
        generated_at=generated_at,
    )


__all__ = [
    "BenchmarkSchemaArtifacts",
    "JSON_SCHEMA_NAME",
    "MARKDOWN_SCHEMA_NAME",
    "RUN_MANIFEST_NAME",
    "run_benchmark_define_schema",
]

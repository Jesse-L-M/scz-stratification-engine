import json

from scz_audit_engine.benchmark.dataset_audit import run_benchmark_dataset_audit
from scz_audit_engine.benchmark.dataset_registry import DatasetRegistryEntry
from scz_audit_engine.benchmark.sources import SourceAdapter


def _entry(
    dataset_id: str,
    outcomes: tuple[str, ...],
    *,
    benchmark_v0_eligibility: str = "eligible",
    representation_comparison_support: str = "strong",
) -> DatasetRegistryEntry:
    return DatasetRegistryEntry(
        dataset_id=dataset_id,
        dataset_label=f"{dataset_id} label",
        access_level="public",
        population_scope="psychosis cohort",
        diagnosis_coverage="psychosis vs control",
        symptom_scales=("PANSS",),
        cognition_scales=("MATRICS",),
        functioning_scales=("GAF/GAS",),
        treatment_variables=("medication",),
        longitudinal_coverage="No repeated follow-up",
        outcome_availability="Poor functional outcome only",
        modality_availability=("MRI",),
        site_structure="single site",
        sample_size_note="n=10",
        known_limitations="fixture only",
        local_status="audited",
        benchmark_v0_eligibility=benchmark_v0_eligibility,
        representation_comparison_support=representation_comparison_support,
        predictor_timepoint="baseline",
        outcome_timepoint="same_visit",
        outcome_window="same_visit",
        outcome_is_prospective=False,
        concurrent_endpoint_only=True,
        outcome_temporal_validity="concurrent_only",
        benchmarkable_outcome_families=outcomes,
        provenance_urls=("https://example.org",),
        audit_summary="fixture row",
    )


class _FixtureAdapter(SourceAdapter):
    source_identifier = "fixture"

    def __init__(self, entry: DatasetRegistryEntry) -> None:
        self._entry = entry

    def audit(self) -> DatasetRegistryEntry:
        return self._entry


def test_markdown_report_surfaces_claim_level_temporal_and_support_lists(tmp_path) -> None:
    entries = (
        _entry("public-cohort", ("poor_functional_outcome",)),
        _entry(
            "weak-label-cohort",
            ("poor_functional_outcome",),
            benchmark_v0_eligibility="limited",
            representation_comparison_support="limited",
        ),
    )
    artifacts = run_benchmark_dataset_audit(
        registry_path=tmp_path / "dataset_registry.csv",
        reports_root=tmp_path / "reports",
        manifests_root=tmp_path / "manifests",
        repo_root=None,
        command=["scz-audit", "benchmark", "audit-datasets"],
        git_sha="deadbeef",
        seed=1729,
        adapters=tuple(_FixtureAdapter(entry) for entry in entries),
    )

    markdown = artifacts.markdown_report_path.read_text(encoding="utf-8")

    assert "- Current benchmark decision: `narrow-go`" in markdown
    assert "- Current claim level supported: `narrow_outcome_benchmark`" in markdown
    assert "- Concurrent-only cohorts: `public-cohort`, `weak-label-cohort`" in markdown
    assert "- Prospectively usable cohorts: none" in markdown
    assert (
        "| Outcome family | Narrow benchmark support | Full external-validation support | Prospective support |"
        in markdown
    )
    assert "| `poor_functional_outcome` | public-cohort | none | none |" in markdown
    assert (
        "`weak-label-cohort` | `public` | `audited` | `limited` | `limited` | `concurrent_only` | `none`"
        in markdown
    )


def test_json_report_exposes_claim_level_and_temporal_fields(tmp_path) -> None:
    entries = (
        _entry("public-cohort", ("poor_functional_outcome",)),
        _entry(
            "weak-label-cohort",
            ("poor_functional_outcome",),
            benchmark_v0_eligibility="limited",
            representation_comparison_support="limited",
        ),
    )
    artifacts = run_benchmark_dataset_audit(
        registry_path=tmp_path / "dataset_registry.csv",
        reports_root=tmp_path / "reports",
        manifests_root=tmp_path / "manifests",
        repo_root=None,
        command=["scz-audit", "benchmark", "audit-datasets"],
        git_sha="deadbeef",
        seed=1729,
        adapters=tuple(_FixtureAdapter(entry) for entry in entries),
    )

    payload = json.loads(artifacts.json_report_path.read_text(encoding="utf-8"))
    by_id = {entry["dataset_id"]: entry for entry in payload["audited_cohorts"]}

    assert payload["decision"]["claim_level"] == "narrow_outcome_benchmark"
    assert payload["decision"]["narrow_supporting_cohorts"] == ["public-cohort"]
    assert payload["decision"]["full_external_validation_cohorts"] == []
    assert payload["decision"]["concurrent_only_cohorts"] == [
        "public-cohort",
        "weak-label-cohort",
    ]
    assert payload["outcome_family_support"]["poor_functional_outcome"]["narrow_benchmark_support"] == {
        "count": 1,
        "cohorts": ["public-cohort"],
    }
    assert payload["outcome_family_support"]["poor_functional_outcome"]["prospective_support"] == {
        "count": 0,
        "cohorts": [],
    }
    assert by_id["public-cohort"]["benchmark_v0_eligibility"] == "eligible"
    assert by_id["public-cohort"]["counts_toward_narrow_benchmark_support"] is True
    assert by_id["public-cohort"]["outcome_temporal_validity"] == "concurrent_only"
    assert by_id["public-cohort"]["claim_level_ceiling"] == "full_external_validation"
    assert by_id["weak-label-cohort"]["benchmark_v0_eligibility"] == "limited"
    assert by_id["weak-label-cohort"]["counts_toward_narrow_benchmark_support"] is False
    assert by_id["weak-label-cohort"]["representation_comparison_support"] == "limited"

import json

from scz_audit_engine.benchmark.dataset_audit import run_benchmark_dataset_audit
from scz_audit_engine.benchmark.dataset_registry import DatasetRegistryEntry
from scz_audit_engine.benchmark.sources import SourceAdapter


def _entry(
    dataset_id: str,
    outcomes: tuple[str, ...],
    *,
    access_tier: str = "strict_open",
    benchmark_v0_eligibility: str = "eligible",
    representation_comparison_support: str = "strong",
) -> DatasetRegistryEntry:
    return DatasetRegistryEntry(
        dataset_id=dataset_id,
        dataset_label=f"{dataset_id} label",
        access_tier=access_tier,
        population_scope="psychosis cohort",
        diagnosis_coverage="psychosis vs control",
        symptom_scales=("PANSS",),
        cognition_scales=("MATRICS",),
        functioning_scales=("GAF/GAS",) if outcomes else (),
        treatment_variables=("medication",),
        longitudinal_coverage="No repeated follow-up",
        outcome_availability="Poor functional outcome only" if outcomes else "No benchmarkable outcome",
        modality_availability=("MRI",),
        site_structure="single site",
        sample_size_note="n=10",
        known_limitations="fixture only",
        local_status="audited",
        benchmark_v0_eligibility=benchmark_v0_eligibility,
        representation_comparison_support=representation_comparison_support,
        predictor_timepoint="baseline" if outcomes else "unmapped",
        outcome_timepoint="same_visit" if outcomes else "unmapped",
        outcome_window="same_visit" if outcomes else "unmapped",
        outcome_is_prospective=False,
        concurrent_endpoint_only=bool(outcomes),
        outcome_temporal_validity="concurrent_only" if outcomes else "none",
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


def test_markdown_report_surfaces_access_tier_layers_and_recommendation(tmp_path) -> None:
    entries = (
        _entry("strict-open-outcome", ("poor_functional_outcome",)),
        _entry("strict-open-representation", (), benchmark_v0_eligibility="ineligible"),
        _entry(
            "credentialed-outcome",
            ("poor_functional_outcome",),
            access_tier="public_credentialed",
        ),
        _entry(
            "weak-label-cohort",
            ("poor_functional_outcome",),
            access_tier="public_credentialed",
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

    assert "- Current benchmark decision under `strict_open`: `narrow-go`" in markdown
    assert "- Current claim level under `strict_open`: `narrow_outcome_benchmark`" in markdown
    assert "- Recommended next step: `defer_until_stronger_credentialed_or_controlled_data`" in markdown
    assert (
        "| `strict_open` | `strict_open` | `narrow-go` | `narrow_outcome_benchmark` | "
        "strict-open-outcome, strict-open-representation | strict-open-outcome |"
        in markdown
    )
    assert (
        "| `public_credentialed` | `strict_open`, `public_credentialed` | `go` | "
        "`full_external_validation` | strict-open-outcome, strict-open-representation, credentialed-outcome | "
        "strict-open-outcome, credentialed-outcome |"
        in markdown
    )
    assert "| `controlled` | `strict_open`, `public_credentialed`, `controlled` | `go` |" in markdown
    assert (
        "| `public_credentialed` | `poor_functional_outcome` | strict-open-outcome, credentialed-outcome | "
        "strict-open-outcome, credentialed-outcome | none |"
        in markdown
    )
    assert (
        "| `credentialed-outcome` | `public_credentialed` | `audited` | `eligible` | `strong` | "
        "`concurrent_only` | `narrow_outcome_benchmark` | `yes` | poor_functional_outcome |"
        in markdown
    )
    assert "- Access tier: public_credentialed" in markdown


def test_json_report_exposes_access_tier_decisions_and_row_level_support_flags(tmp_path) -> None:
    entries = (
        _entry("strict-open-outcome", ("poor_functional_outcome",)),
        _entry("strict-open-representation", (), benchmark_v0_eligibility="ineligible"),
        _entry(
            "credentialed-outcome",
            ("poor_functional_outcome",),
            access_tier="public_credentialed",
        ),
        _entry(
            "weak-label-cohort",
            ("poor_functional_outcome",),
            access_tier="public_credentialed",
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
    strict_open = payload["decision"]["access_tier_decisions"]["strict_open"]
    public_credentialed = payload["decision"]["access_tier_decisions"]["public_credentialed"]

    assert payload["decision"]["current_access_tier"] == "strict_open"
    assert payload["decision"]["claim_level"] == "narrow_outcome_benchmark"
    assert (
        payload["decision"]["recommended_next_step"]
        == "defer_until_stronger_credentialed_or_controlled_data"
    )
    assert strict_open["narrow_supporting_cohorts"] == ["strict-open-outcome"]
    assert strict_open["full_external_validation_cohorts"] == []
    assert public_credentialed["state"] == "go"
    assert public_credentialed["claim_level"] == "full_external_validation"
    assert public_credentialed["narrow_supporting_cohorts"] == [
        "strict-open-outcome",
        "credentialed-outcome",
    ]
    assert payload["outcome_family_support_by_access_tier"]["strict_open"]["poor_functional_outcome"] == {
        "narrow_benchmark_support": {
            "count": 1,
            "cohorts": ["strict-open-outcome"],
        },
        "full_external_validation_support": {
            "count": 0,
            "cohorts": [],
        },
        "prospective_support": {
            "count": 0,
            "cohorts": [],
        },
    }
    assert payload["outcome_family_support_by_access_tier"]["public_credentialed"]["poor_functional_outcome"] == {
        "narrow_benchmark_support": {
            "count": 2,
            "cohorts": ["strict-open-outcome", "credentialed-outcome"],
        },
        "full_external_validation_support": {
            "count": 2,
            "cohorts": ["strict-open-outcome", "credentialed-outcome"],
        },
        "prospective_support": {
            "count": 0,
            "cohorts": [],
        },
    }
    assert by_id["strict-open-outcome"]["access_tier"] == "strict_open"
    assert (
        by_id["strict-open-outcome"]["counts_toward_narrow_benchmark_support_if_access_allowed"]
        is True
    )
    assert by_id["strict-open-outcome"]["claim_level_ceiling"] == "narrow_outcome_benchmark"
    assert by_id["strict-open-representation"]["claim_level_contributions"] == [
        "cross_sectional_representation"
    ]
    assert by_id["credentialed-outcome"]["access_tier"] == "public_credentialed"
    assert by_id["weak-label-cohort"]["benchmark_v0_eligibility"] == "limited"
    assert (
        by_id["weak-label-cohort"]["counts_toward_narrow_benchmark_support_if_access_allowed"]
        is False
    )


def test_dataset_audit_reports_are_deterministic_and_omit_generated_timestamps(tmp_path) -> None:
    entries = (
        _entry("strict-open-outcome", ("poor_functional_outcome",)),
        _entry("strict-open-representation", (), benchmark_v0_eligibility="ineligible"),
        _entry(
            "credentialed-outcome",
            ("poor_functional_outcome",),
            access_tier="public_credentialed",
        ),
    )
    adapters = tuple(_FixtureAdapter(entry) for entry in entries)

    first = run_benchmark_dataset_audit(
        registry_path=tmp_path / "first" / "dataset_registry.csv",
        reports_root=tmp_path / "first" / "reports",
        manifests_root=tmp_path / "first" / "manifests",
        repo_root=None,
        command=["scz-audit", "benchmark", "audit-datasets"],
        git_sha="deadbeef",
        seed=1729,
        adapters=adapters,
    )
    second = run_benchmark_dataset_audit(
        registry_path=tmp_path / "second" / "dataset_registry.csv",
        reports_root=tmp_path / "second" / "reports",
        manifests_root=tmp_path / "second" / "manifests",
        repo_root=None,
        command=["scz-audit", "benchmark", "audit-datasets"],
        git_sha="cafebabe",
        seed=1729,
        adapters=adapters,
    )

    first_json = json.loads(first.json_report_path.read_text(encoding="utf-8"))
    second_json = json.loads(second.json_report_path.read_text(encoding="utf-8"))
    assert first_json == second_json
    assert "generated_at" not in first_json

    first_markdown = first.markdown_report_path.read_text(encoding="utf-8")
    second_markdown = second.markdown_report_path.read_text(encoding="utf-8")
    assert first_markdown == second_markdown
    assert "Generated at:" not in first_markdown

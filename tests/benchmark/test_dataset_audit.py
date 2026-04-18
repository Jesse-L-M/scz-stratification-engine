import json

from scz_audit_engine.benchmark.dataset_audit import run_benchmark_dataset_audit
from scz_audit_engine.benchmark.dataset_registry import DatasetRegistryEntry
from scz_audit_engine.benchmark.sources import SourceAdapter


def _entry(
    dataset_id: str,
    outcomes: tuple[str, ...],
    *,
    access_level: str = "public",
    local_status: str = "audited",
    benchmark_v0_eligibility: str | None = None,
) -> DatasetRegistryEntry:
    if benchmark_v0_eligibility is None:
        benchmark_v0_eligibility = (
            "eligible"
            if access_level == "public" and local_status in {"audited", "harmonized"} and outcomes
            else "ineligible"
        )
    return DatasetRegistryEntry(
        dataset_id=dataset_id,
        dataset_label=f"{dataset_id} label",
        access_level=access_level,
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
        local_status=local_status,
        benchmark_v0_eligibility=benchmark_v0_eligibility,
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


def test_markdown_report_labels_support_as_public_benchmark_eligible(tmp_path) -> None:
    entries = (
        _entry("public-cohort", ("poor_functional_outcome",), access_level="public"),
        _entry(
            "weak-label-cohort",
            ("poor_functional_outcome",),
            benchmark_v0_eligibility="limited",
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

    assert "| Outcome family | Benchmark-v0 eligible cohorts counting toward `go` |" in markdown
    assert "| `poor_functional_outcome` | public-cohort |" in markdown
    assert "`weak-label-cohort` | `public` | `audited` | `limited` | `no` | psychosis cohort" in markdown


def test_json_report_exposes_benchmark_v0_eligibility_and_counting_flag(tmp_path) -> None:
    entries = (
        _entry("public-cohort", ("poor_functional_outcome",)),
        _entry(
            "weak-label-cohort",
            ("poor_functional_outcome",),
            benchmark_v0_eligibility="limited",
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

    assert by_id["public-cohort"]["benchmark_v0_eligibility"] == "eligible"
    assert by_id["public-cohort"]["counts_toward_cross_cohort_go"] is True
    assert by_id["weak-label-cohort"]["benchmark_v0_eligibility"] == "limited"
    assert by_id["weak-label-cohort"]["counts_toward_cross_cohort_go"] is False

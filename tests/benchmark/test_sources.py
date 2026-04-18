import json
from pathlib import Path

import pytest

from scz_audit_engine.benchmark.sources import (
    FEPDS003944BenchmarkSourceAdapter,
    TCPDS005237BenchmarkSourceAdapter,
    build_default_source_adapters,
)

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "benchmark_sources"


def test_tcp_adapter_normalizes_fixture_snapshot_without_subject_level_leakage() -> None:
    adapter = TCPDS005237BenchmarkSourceAdapter(snapshot_root=FIXTURE_ROOT / "tcp_ds005237")

    entry = adapter.audit()

    assert entry.dataset_id == "tcp-ds005237"
    assert entry.access_level == "public"
    assert entry.benchmark_v0_eligibility == "limited"
    assert entry.representation_comparison_support == "limited"
    assert entry.outcome_temporal_validity == "concurrent_only"
    assert entry.outcome_is_prospective is False
    assert entry.benchmarkable_outcome_families == ("poor_functional_outcome",)
    assert "LIFE-RIFT" in entry.functioning_scales
    assert "sub-" not in json.dumps(entry.to_dict())


def test_fep_adapter_normalizes_fixture_snapshot_without_subject_level_leakage() -> None:
    adapter = FEPDS003944BenchmarkSourceAdapter(snapshot_root=FIXTURE_ROOT / "fep_ds003944")

    entry = adapter.audit()

    assert entry.dataset_id == "fep-ds003944"
    assert entry.population_scope == "first-episode psychosis case-control cohort"
    assert entry.benchmark_v0_eligibility == "eligible"
    assert entry.representation_comparison_support == "strong"
    assert entry.outcome_temporal_validity == "concurrent_only"
    assert entry.claim_level_contributions == (
        "cross_sectional_representation",
        "narrow_outcome_benchmark",
    )
    assert entry.claim_level_ceiling == "narrow_outcome_benchmark"
    assert "SAPS" in entry.symptom_scales
    assert entry.benchmarkable_outcome_families == ("poor_functional_outcome",)
    assert "sub-" not in json.dumps(entry.to_dict())


def test_default_source_adapter_builder_can_audit_two_candidates() -> None:
    adapters = build_default_source_adapters(
        {
            "tcp-ds005237": FIXTURE_ROOT / "tcp_ds005237",
            "fep-ds003944": FIXTURE_ROOT / "fep_ds003944",
        }
    )

    audited = tuple(adapter.audit() for adapter in adapters)

    assert len(audited) == 2
    assert tuple(entry.dataset_id for entry in audited) == ("tcp-ds005237", "fep-ds003944")


def test_live_openneuro_loader_uses_pinned_snapshot_metadata_not_latest_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scz_audit_engine.benchmark.sources.base as base_module

    def fake_graphql_query(query: str, variables: dict[str, object]) -> dict[str, object]:
        if "query DatasetMetadata" in query:
            return {
                "dataset": {
                    "id": "ds003944",
                    "name": "dataset shell",
                    "metadata": {
                        "species": "Human",
                        "studyDesign": "",
                        "studyDomain": "",
                        "modalities": ["eeg"],
                        "ages": [20],
                    },
                }
            }
        if "query SnapshotRootFiles" in query:
            return {
                "snapshot": {
                    "tag": "1.0.1",
                    "description": {
                        "Name": "Pinned snapshot name",
                        "DatasetDOI": "doi:10.18112/openneuro.ds003944.v1.0.1",
                        "License": "CC0",
                        "Authors": ["Example Author"],
                        "ReferencesAndLinks": ["https://openneuro.org/datasets/ds003944"],
                    },
                    "files": [
                        {"id": "phenotype-tree", "filename": "phenotype", "size": 0, "directory": True, "annexed": None},
                        {"id": "participants", "filename": "participants.tsv", "size": 10, "directory": False, "annexed": None},
                    ],
                }
            }
        if "query PhenotypeFiles" in query:
            return {"snapshot": {"files": [{"filename": "gafgas.tsv", "directory": False, "annexed": None}]}}
        raise AssertionError(f"unexpected query: {query}")

    def fake_fetch_text(url: str, *, required: bool = True) -> str | None:
        if url.endswith("/README"):
            return "fixture readme"
        if url.endswith("/participants.tsv"):
            return "participant_id\ttype\nsub-1\tPsychosis\n"
        if required:
            raise AssertionError(f"unexpected required url: {url}")
        return None

    monkeypatch.setattr(base_module, "_graphql_query", fake_graphql_query)
    monkeypatch.setattr(base_module, "_fetch_text", fake_fetch_text)

    bundle = FEPDS003944BenchmarkSourceAdapter().load_snapshot_bundle()

    assert bundle.dataset["latestSnapshot"]["tag"] == "1.0.1"
    assert bundle.dataset["latestSnapshot"]["description"]["Name"] == "Pinned snapshot name"

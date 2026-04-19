import json
import shutil
from pathlib import Path

import pytest

from scz_audit_engine.benchmark.sources import (
    DS000115BenchmarkSourceAdapter,
    FEPDS003944BenchmarkSourceAdapter,
    TCPDS005237BenchmarkSourceAdapter,
    UCLACNPDS000030BenchmarkSourceAdapter,
    build_default_source_adapters,
)

FIXTURE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "benchmark_sources"


def test_tcp_adapter_normalizes_fixture_snapshot_without_subject_level_leakage() -> None:
    adapter = TCPDS005237BenchmarkSourceAdapter(snapshot_root=FIXTURE_ROOT / "tcp_ds005237")

    entry = adapter.audit()

    assert entry.dataset_id == "tcp-ds005237"
    assert entry.access_tier == "strict_open"
    assert entry.benchmark_v0_eligibility == "limited"
    assert entry.representation_comparison_support == "limited"
    assert entry.outcome_temporal_validity == "concurrent_only"
    assert entry.outcome_is_prospective is False
    assert entry.benchmarkable_outcome_families == ("poor_functional_outcome",)
    assert "LIFE-RIFT" in entry.functioning_scales
    assert "sub-" not in json.dumps(entry.to_dict())


def test_ucla_cnp_adapter_normalizes_fixture_snapshot_as_cross_sectional_only() -> None:
    adapter = UCLACNPDS000030BenchmarkSourceAdapter(
        snapshot_root=FIXTURE_ROOT / "ucla_cnp_ds000030"
    )

    entry = adapter.audit()

    assert entry.dataset_id == "ucla-cnp-ds000030"
    assert entry.access_tier == "strict_open"
    assert entry.benchmark_v0_eligibility == "ineligible"
    assert entry.representation_comparison_support == "strong"
    assert entry.outcome_temporal_validity == "none"
    assert entry.claim_level_contributions == ("cross_sectional_representation",)
    assert entry.claim_level_ceiling == "cross_sectional_representation"
    assert "SAPS" in entry.symptom_scales
    assert "WAIS" in entry.cognition_scales
    assert entry.benchmarkable_outcome_families == ()
    assert "sub-" not in json.dumps(entry.to_dict())


def test_ds000115_adapter_normalizes_fixture_snapshot_as_low_weight_representation_check() -> None:
    adapter = DS000115BenchmarkSourceAdapter(snapshot_root=FIXTURE_ROOT / "ds000115")

    entry = adapter.audit()

    assert entry.dataset_id == "ds000115"
    assert entry.access_tier == "strict_open"
    assert entry.benchmark_v0_eligibility == "ineligible"
    assert entry.representation_comparison_support == "strong"
    assert entry.outcome_temporal_validity == "none"
    assert entry.claim_level_contributions == ("cross_sectional_representation",)
    assert entry.claim_level_ceiling == "cross_sectional_representation"
    assert "SAPS" in entry.symptom_scales
    assert "n-back accuracy/RT" in entry.cognition_scales
    assert entry.benchmarkable_outcome_families == ()
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
            "ucla-cnp-ds000030": FIXTURE_ROOT / "ucla_cnp_ds000030",
            "ds000115": FIXTURE_ROOT / "ds000115",
        }
    )

    audited = tuple(adapter.audit() for adapter in adapters)

    assert len(audited) == 4
    assert tuple(entry.dataset_id for entry in audited) == (
        "tcp-ds005237",
        "fep-ds003944",
        "ucla-cnp-ds000030",
        "ds000115",
    )


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


def test_snapshot_loader_tolerates_minimal_staged_root_without_readme_or_root_files(tmp_path) -> None:
    source_root = FIXTURE_ROOT / "fep_ds003944"
    minimal_root = tmp_path / "fep-ds003944"
    minimal_root.mkdir()
    (minimal_root / "phenotype").mkdir()
    shutil.copy2(source_root / "dataset_metadata.json", minimal_root / "dataset_metadata.json")
    shutil.copy2(source_root / "participants.tsv", minimal_root / "participants.tsv")
    shutil.copy2(source_root / "phenotype_files.json", minimal_root / "phenotype_files.json")
    shutil.copy2(source_root / "phenotype" / "bprs.tsv", minimal_root / "phenotype" / "bprs.tsv")

    entry = FEPDS003944BenchmarkSourceAdapter(snapshot_root=minimal_root).audit()

    assert entry.dataset_id == "fep-ds003944"
    assert entry.dataset_label == "EEG: First Episode Psychosis vs. Control Resting Task 1"


def test_snapshot_loader_tolerates_wrapped_graphql_payloads_and_synthesizes_latest_snapshot(tmp_path) -> None:
    source_root = FIXTURE_ROOT / "fep_ds003944"
    working_root = tmp_path / "fep-ds003944"
    shutil.copytree(source_root, working_root)

    dataset_payload = json.loads((working_root / "dataset_metadata.json").read_text(encoding="utf-8"))
    dataset_without_snapshot = dict(dataset_payload["dataset"])
    dataset_without_snapshot.pop("latestSnapshot", None)
    (working_root / "dataset_metadata.json").write_text(
        json.dumps({"data": {"dataset": dataset_without_snapshot}}, indent=2),
        encoding="utf-8",
    )
    root_files_payload = json.loads((working_root / "root_files.json").read_text(encoding="utf-8"))
    (working_root / "root_files.json").write_text(
        json.dumps(
            {
                "data": {
                    "snapshot": {
                        "tag": "1.0.1",
                        "description": dataset_payload["dataset"]["latestSnapshot"]["description"],
                        "files": root_files_payload["files"],
                    }
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    phenotype_files_payload = json.loads((working_root / "phenotype_files.json").read_text(encoding="utf-8"))
    (working_root / "phenotype_files.json").write_text(
        json.dumps({"data": {"snapshot": {"files": phenotype_files_payload["files"]}}}, indent=2),
        encoding="utf-8",
    )

    entry = FEPDS003944BenchmarkSourceAdapter(snapshot_root=working_root).audit()

    assert entry.dataset_id == "fep-ds003944"
    assert entry.dataset_label == "EEG: First Episode Psychosis vs. Control Resting Task 1"


def test_snapshot_loader_prefers_saved_snapshot_metadata_over_stale_latest_snapshot_fields(tmp_path) -> None:
    source_root = FIXTURE_ROOT / "fep_ds003944"
    working_root = tmp_path / "fep-ds003944"
    shutil.copytree(source_root, working_root)

    dataset_payload = json.loads((working_root / "dataset_metadata.json").read_text(encoding="utf-8"))
    stale_dataset = dict(dataset_payload["dataset"])
    stale_dataset["latestSnapshot"] = {
        "tag": "9.9.9",
        "description": {
            "Name": "WRONG NAME",
            "DatasetDOI": "doi:stale",
        },
    }
    (working_root / "dataset_metadata.json").write_text(
        json.dumps({"data": {"dataset": stale_dataset}}, indent=2),
        encoding="utf-8",
    )
    root_files_payload = json.loads((working_root / "root_files.json").read_text(encoding="utf-8"))
    (working_root / "root_files.json").write_text(
        json.dumps(
            {
                "data": {
                    "snapshot": {
                        "tag": dataset_payload["dataset"]["latestSnapshot"]["tag"],
                        "description": dataset_payload["dataset"]["latestSnapshot"]["description"],
                        "files": root_files_payload["files"],
                    }
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    bundle = FEPDS003944BenchmarkSourceAdapter(snapshot_root=working_root).load_snapshot_bundle()

    assert bundle.dataset["latestSnapshot"]["tag"] == "1.0.1"
    assert bundle.dataset["latestSnapshot"]["description"]["Name"] == "EEG: First Episode Psychosis vs. Control Resting Task 1"


def test_fep_harmonize_keeps_unknown_participant_labels_unknown(tmp_path) -> None:
    source_root = FIXTURE_ROOT / "fep_ds003944"
    working_root = tmp_path / "fep-ds003944"
    shutil.copytree(source_root, working_root)
    (working_root / "participants.tsv").write_text(
        "participant_id\ttype\tage\tgender\trace\tethnicity\n"
        "sub-1448\t\t15.7\tM\tWhite\tNot Hispanic\n"
        "sub-1824\tPsychosis\t21.5\tF\tWhite\tNot Hispanic\n",
        encoding="utf-8",
    )

    bundle = FEPDS003944BenchmarkSourceAdapter(snapshot_root=working_root).harmonize(working_root)
    diagnoses = {
        row["source_diagnosis_label"] or row["subject_id"]: row
        for row in bundle.tables["diagnoses"]
    }

    assert diagnoses["unknown"]["diagnosis_group"] == "unknown"
    assert "remains unknown" in bundle.tables["subjects"][0]["mapping_note"]


def test_fep_harmonize_requires_staged_saps_sans_totals_and_records_unsupported_fields(tmp_path) -> None:
    source_root = FIXTURE_ROOT / "fep_ds003944"
    working_root = tmp_path / "fep-ds003944"
    shutil.copytree(source_root, working_root)

    phenotype_root = working_root / "phenotype"
    (phenotype_root / "bprs.tsv").write_text(
        "participant_id\tBPRST18\tBPRST19\n"
        "sub-1824\t52\t53\n"
        "sub-1983\t40\t41\n",
        encoding="utf-8",
    )
    (phenotype_root / "saps.tsv").write_text(
        "participant_id\t" + "\t".join(f"SAPS_Q{index}" for index in range(1, 35)) + "\n"
        + "sub-1824\t"
        + "\t".join("1" for _ in range(34))
        + "\n"
        + "sub-1983\t"
        + "\t".join("2" for _ in range(34))
        + "\n",
        encoding="utf-8",
    )
    (phenotype_root / "sans.tsv").write_text(
        "participant_id\t" + "\t".join(f"SANS_Q{index}" for index in range(1, 26)) + "\n"
        + "sub-1824\t"
        + "\t".join("1" for _ in range(25))
        + "\n"
        + "sub-1983\t"
        + "\t".join("2" for _ in range(25))
        + "\n",
        encoding="utf-8",
    )
    (phenotype_root / "matrics.tsv").write_text(
        "participant_id\tOVERALLTSCR\nsub-1448\t46\nsub-1824\t43\nsub-1983\t65\n",
        encoding="utf-8",
    )
    (phenotype_root / "wasi.tsv").write_text(
        "participant_id\tFULL2IQ\nsub-1448\t117\nsub-1824\t128\nsub-1983\t131\n",
        encoding="utf-8",
    )
    (phenotype_root / "gafgas.tsv").write_text(
        "participant_id\tGAS\nsub-1824\t44\nsub-1983\t40\n",
        encoding="utf-8",
    )
    (phenotype_root / "sfs.tsv").write_text(
        "participant_id\tSFS_Q1_LIVEWHERE\nsub-1824\tHouse\nsub-1983\tApartment\n",
        encoding="utf-8",
    )
    (phenotype_root / "medication.tsv").write_text(
        "participant_id\tCPZ_at_scan\nsub-1824\t0\nsub-1983\t113.6363636\n",
        encoding="utf-8",
    )

    bundle = FEPDS003944BenchmarkSourceAdapter(snapshot_root=working_root).harmonize(working_root)

    assert len(bundle.tables["symptom_scores"]) == 2
    assert len(bundle.tables["cognition_scores"]) == 6
    assert len(bundle.tables["functioning_scores"]) == 2
    assert len(bundle.tables["treatment_exposures"]) == 2
    assert len(bundle.tables["outcomes"]) == 2
    assert {row["source_score_label"] for row in bundle.tables["symptom_scores"]} == {"BPRST18"}
    assert {row["source_score_label"] for row in bundle.tables["cognition_scores"]} == {
        "OVERALLTSCR",
        "FULL2IQ",
    }
    assert bundle.tables["functioning_scores"][0]["source_score_label"] == "GAS"
    assert {row["source_treatment_label"] for row in bundle.tables["treatment_exposures"]} == {"CPZ_at_scan"}
    assert (
        bundle.unsupported_fields["functioning_scores"][0]
        == "Staged phenotype file sfs.tsv does not expose a supported staged SFS total column (expected: sfs_total)."
    )
    assert bundle.unsupported_fields["symptom_scores"] == (
        "Staged phenotype file saps.tsv does not expose a supported staged SAPS total column (expected: saps_total).",
        "Staged phenotype file sans.tsv does not expose a supported staged SANS total column (expected: sans_total).",
    )


def test_fep_harmonize_requires_actual_eeg_files_for_modality_rows(tmp_path) -> None:
    source_root = FIXTURE_ROOT / "fep_ds003944"
    working_root = tmp_path / "fep-ds003944"
    shutil.copytree(source_root, working_root)

    junk_subject_root = working_root / "sub-1448"
    junk_subject_root.mkdir()
    (junk_subject_root / "notes.txt").write_text("not eeg", encoding="utf-8")

    bundle_without_eeg = FEPDS003944BenchmarkSourceAdapter(snapshot_root=working_root).harmonize(working_root)
    assert bundle_without_eeg.tables["modality_features"] == ()

    eeg_subject_root = working_root / "sub-1824" / "eeg"
    eeg_subject_root.mkdir(parents=True)
    (eeg_subject_root / "sub-1824_task-rest_eeg.set").write_text("fixture", encoding="utf-8")

    bundle_with_eeg = FEPDS003944BenchmarkSourceAdapter(snapshot_root=working_root).harmonize(working_root)
    assert len(bundle_with_eeg.tables["modality_features"]) == 1
    assert bundle_with_eeg.tables["modality_features"][0]["subject_id"] == "fep-ds003944:sub-1824"


def test_tcp_harmonize_keeps_unknown_participant_labels_unknown(tmp_path) -> None:
    source_root = FIXTURE_ROOT / "tcp_ds005237"
    working_root = tmp_path / "tcp-ds005237"
    shutil.copytree(source_root, working_root)
    (working_root / "participants.tsv").write_text(
        "participant_id\tspecies\tage\tsex\tSite\tGroup\n"
        "sub-NDARINVTEST001\thomo sapiens\t20.33\tF\t1\t\n"
        "sub-NDARINVTEST002\thomo sapiens\t29.33\tM\t2\tPatient\n",
        encoding="utf-8",
    )

    bundle = TCPDS005237BenchmarkSourceAdapter(snapshot_root=working_root).harmonize(working_root)
    diagnoses = {
        row["source_diagnosis_label"] or row["subject_id"]: row
        for row in bundle.tables["diagnoses"]
    }

    assert diagnoses["unknown"]["diagnosis_group"] == "unknown"


def test_tcp_harmonize_requires_actual_bids_imaging_files_for_modality_rows(tmp_path) -> None:
    source_root = FIXTURE_ROOT / "tcp_ds005237"
    working_root = tmp_path / "tcp-ds005237"
    shutil.copytree(source_root, working_root)

    junk_anat_root = working_root / "sub-NDARINVTEST001" / "anat"
    junk_func_root = working_root / "sub-NDARINVTEST001" / "func"
    junk_anat_root.mkdir(parents=True)
    junk_func_root.mkdir(parents=True)
    (junk_anat_root / "notes.txt").write_text("not imaging", encoding="utf-8")
    (junk_func_root / "notes.txt").write_text("not imaging", encoding="utf-8")

    bundle_without_imaging = TCPDS005237BenchmarkSourceAdapter(snapshot_root=working_root).harmonize(working_root)
    assert bundle_without_imaging.tables["modality_features"] == ()

    (junk_anat_root / "sub-NDARINVTEST001_run-01_T1w.json").write_text("{}", encoding="utf-8")
    (junk_func_root / "sub-NDARINVTEST001_task-rest_run-01_bold.json").write_text("{}", encoding="utf-8")

    bundle_with_imaging = TCPDS005237BenchmarkSourceAdapter(snapshot_root=working_root).harmonize(working_root)

    assert {row["modality_type"] for row in bundle_with_imaging.tables["modality_features"]} == {"MRI", "fMRI"}
    assert {row["subject_id"] for row in bundle_with_imaging.tables["modality_features"]} == {
        "tcp-ds005237:sub-NDARINVTEST001"
    }


def test_tcp_harmonize_records_unsupported_fields_when_total_columns_are_missing(tmp_path) -> None:
    source_root = FIXTURE_ROOT / "tcp_ds005237"
    working_root = tmp_path / "tcp-ds005237"
    shutil.copytree(source_root, working_root)

    phenotype_root = working_root / "phenotype"
    (phenotype_root / "panss01.tsv").write_text(
        "participant_id\tPANSS_TOTAL\nsub-NDARINVTEST001\t72\nsub-NDARINVTEST002\t65\n",
        encoding="utf-8",
    )
    (phenotype_root / "lrift01.tsv").write_text(
        "participant_id\tLRIFT_TOTAL\nsub-NDARINVTEST001\t2.6\nsub-NDARINVTEST002\t2.1\n",
        encoding="utf-8",
    )

    bundle = TCPDS005237BenchmarkSourceAdapter(snapshot_root=working_root).harmonize(working_root)

    assert len(bundle.tables["symptom_scores"]) == 2
    assert {row["source_score_label"] for row in bundle.tables["symptom_scores"]} == {"qids_total"}
    assert len(bundle.tables["functioning_scores"]) == 2
    assert {row["source_score_label"] for row in bundle.tables["functioning_scores"]} == {"mcas_total"}
    assert bundle.unsupported_fields["symptom_scores"] == (
        "Staged phenotype file panss01.tsv does not expose the supported PANSS total column (expected: panss_total).",
    )
    assert bundle.unsupported_fields["functioning_scores"] == (
        "Staged phenotype file lrift01.tsv does not expose the supported LIFE-RIFT total column (expected: lrift_total).",
    )


def test_ucla_cnp_harmonize_emits_cross_sectional_rows_without_outcomes() -> None:
    adapter = UCLACNPDS000030BenchmarkSourceAdapter(
        snapshot_root=FIXTURE_ROOT / "ucla_cnp_ds000030"
    )

    bundle = adapter.harmonize(FIXTURE_ROOT / "ucla_cnp_ds000030")

    assert len(bundle.tables["subjects"]) == 4
    assert len(bundle.tables["diagnoses"]) == 4
    assert len(bundle.tables["symptom_scores"]) == 7
    assert len(bundle.tables["cognition_scores"]) == 28
    assert len(bundle.tables["treatment_exposures"]) == 7
    assert len(bundle.tables["modality_features"]) == 14
    assert bundle.tables["functioning_scores"] == ()
    assert bundle.tables["outcomes"] == ()
    assert {
        row["source_score_label"]
        for row in bundle.tables["cognition_scores"]
        if row["instrument"] == "WMS"
    } == {"vr1ir_totalraw", "vr2r_totalraw", "ds_btrs"}
    assert {row["diagnosis_group"] for row in bundle.tables["diagnoses"]} == {
        "adhd",
        "bipolar_disorder",
        "control",
        "schizophrenia",
    }
    assert (
        bundle.unsupported_fields["outcomes"][0]
        == "ucla-cnp-ds000030 remains a cross-sectional representation cohort only; no benchmarkable outcome rows are emitted."
    )


def test_ucla_cnp_harmonize_requires_affirmative_medication_use_flag(tmp_path) -> None:
    source_root = FIXTURE_ROOT / "ucla_cnp_ds000030"
    working_root = tmp_path / "ucla-cnp-ds000030"
    shutil.copytree(source_root, working_root)

    (working_root / "phenotype" / "medication.tsv").write_text(
        "participant_id\tmed_name1\tmed_use1\tmed_dos1\tmed_name2\tmed_use2\tmed_dos2\n"
        "sub-50005\tGeodon/ Ziprasidone\t1\t80\tECT\t0\t\n"
        "sub-60005\tLexapro/ Escitalopram oxalate\t\t20\t\t\t\n",
        encoding="utf-8",
    )

    bundle = UCLACNPDS000030BenchmarkSourceAdapter(snapshot_root=working_root).harmonize(working_root)

    assert len(bundle.tables["treatment_exposures"]) == 1
    assert bundle.tables["treatment_exposures"][0]["treatment_name"] == "Geodon/ Ziprasidone"


def test_ds000115_harmonize_emits_cross_sectional_rows_without_outcomes() -> None:
    adapter = DS000115BenchmarkSourceAdapter(snapshot_root=FIXTURE_ROOT / "ds000115")

    bundle = adapter.harmonize(FIXTURE_ROOT / "ds000115")

    assert len(bundle.tables["subjects"]) == 4
    assert len(bundle.tables["diagnoses"]) == 4
    assert len(bundle.tables["symptom_scores"]) == 12
    assert len(bundle.tables["cognition_scores"]) == 36
    assert bundle.tables["functioning_scores"] == ()
    assert bundle.tables["treatment_exposures"] == ()
    assert bundle.tables["outcomes"] == ()
    assert bundle.tables["modality_features"] == ()
    assert {
        row["source_score_label"] for row in bundle.tables["cognition_scores"]
    } == {
        "DST_SCALE",
        "LNS_SCALE",
        "LOGIALMEMORY_SCALE",
        "TRAILB",
        "WAIS_MATRICS_SCALE",
        "WCSTPSVE",
        "d4prime",
        "nback2_targ",
        "nback2_targ_medrt",
    }
    assert {row["diagnosis_group"] for row in bundle.tables["diagnoses"]} == {
        "control",
        "control_sibling",
        "schizophrenia",
        "schizophrenia_sibling",
    }
    assert (
        bundle.unsupported_fields["outcomes"][0]
        == "ds000115 remains a low-weight cross-sectional representation cohort only; no benchmarkable outcome rows are emitted."
    )

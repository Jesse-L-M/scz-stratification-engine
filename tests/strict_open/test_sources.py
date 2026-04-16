from pathlib import Path

from scz_audit_engine.strict_open.schema import STRICT_OPEN_TABLE_NAMES
from scz_audit_engine.strict_open.sources import TCPDS005237SourceAdapter


FIXTURE_SOURCE_ROOT = Path(__file__).resolve().parents[1] / "fixtures" / "tcp_raw" / "source_input"


def test_tcp_adapter_stages_fixture_tree_with_source_aligned_paths(tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    adapter = TCPDS005237SourceAdapter()

    staged = adapter.stage(raw_root, source_root=FIXTURE_SOURCE_ROOT)

    assert staged.raw_root == raw_root
    assert staged.source == "tcp"
    assert staged.dataset_accession == "ds005237"
    assert (raw_root / "participants.tsv").exists()
    assert (raw_root / "phenotype" / "cogfq01.tsv").exists()
    assert (raw_root / "sub-TCP001" / "anat" / "sub-TCP001_run-01_T1w.nii.gz").exists()
    assert any(record.relative_path == "motion_FD/TCP_FD_rest_AP_1.csv" for record in staged.files)


def test_tcp_adapter_does_not_emit_harmonized_table_names(tmp_path) -> None:
    raw_root = tmp_path / "data" / "raw" / "strict_open" / "tcp"
    adapter = TCPDS005237SourceAdapter()

    staged = adapter.stage(raw_root, source_root=FIXTURE_SOURCE_ROOT)
    staged_paths = {record.relative_path for record in staged.files}

    for table_name in STRICT_OPEN_TABLE_NAMES:
        assert f"{table_name}.tsv" not in staged_paths
        assert f"{table_name}.json" not in staged_paths

"""Audit/profile generation for staged TCP raw inputs."""

from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from .provenance import (
    ProcessedOutputRecord,
    ProvenanceMapping,
    SourceFileRecord,
    build_audit_provenance,
    file_sha256,
    local_file_content_kind,
    load_source_manifest,
    write_audit_provenance,
    write_json_artifact,
)
from .run_manifest import build_run_manifest, write_run_manifest
from .sources.base import inspect_local_tree

COGNITION_INSTRUMENT_CODES = {
    "cerq01",
    "cogfq01",
    "crt01",
    "hammer01",
    "sils01",
    "stroop01",
    "tmb_dsm01",
    "tmb_gradcpt01",
    "tmb_mer01",
    "tmb_mr01",
    "tmb_rt01",
    "tmb_wsap01",
}
SYMPTOM_INSTRUMENT_CODES = {
    "anxsi01",
    "cgi01",
    "cssrs01",
    "dass01",
    "lrift01",
    "madrs01",
    "masq01",
    "mcas01",
    "panss01",
    "pdss01",
    "poms01",
    "pss01",
    "qids01",
    "shaps01",
    "stai01",
    "ymrs01",
}
EXPECTED_MRI_RUNS = ("T1w", "T2w", "restAP", "restPA", "stroopAP", "stroopPA", "hammerAP", "epiAP", "epiPA")
MISSING_VALUE_MARKERS = {"", "999", "999.0", "NA", "N/A", "na", "n/a"}
SUBJECT_ID_COLUMNS = ("participant_id", "subject_id", "src_subject_id", "subjectkey")
VISIT_COLUMNS = ("visit", "visit_label", "eventname", "session_id", "interview_date")


def run_tcp_audit(
    *,
    raw_root: str | Path,
    manifests_root: str | Path,
    profiles_root: str | Path,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
    dataset_version: str | None,
    source_identifier: str = "tcp-ds005237",
    dataset_accession: str = "ds005237",
    source_manifest_path: str | Path | None = None,
) -> dict[str, str]:
    """Generate the TCP audit profile plus provenance artifacts."""

    raw_path = Path(raw_root)
    manifests_path = Path(manifests_root)
    profiles_path = Path(profiles_root)
    source_manifest = None
    effective_dataset_version = dataset_version
    if source_manifest_path is not None and Path(source_manifest_path).exists():
        source_manifest = load_source_manifest(source_manifest_path)
        effective_dataset_version = source_manifest.dataset_version or dataset_version

    local_inventory = _inspect_local_inventory(raw_path)
    file_inventory = _merge_file_inventory(local_inventory, source_manifest.files if source_manifest else ())
    used_inputs: set[str] = set()
    notes_map = _load_notes_map(raw_path / "phenotype" / "notes.tsv", raw_path, used_inputs)

    participant_rows = _read_tsv_rows(raw_path / "participants.tsv", raw_path, used_inputs)
    participant_subjects = _collect_subject_ids(participant_rows)

    phenotype_summaries = _build_phenotype_inventory(
        raw_root=raw_path,
        inventory=file_inventory,
        notes_map=notes_map,
        used_inputs=used_inputs,
    )
    mri_inventory = _build_mri_inventory(
        inventory=file_inventory,
        participant_subjects=participant_subjects,
        raw_root=raw_path,
        used_inputs=used_inputs,
    )
    repeat_visit_availability = _build_repeat_visit_summary(
        participant_subjects=participant_subjects,
        phenotype_summaries=phenotype_summaries,
    )

    audit_profile = {
        "dataset_accession": dataset_accession,
        "dataset_version": effective_dataset_version,
        "diagnosis_breakdown": dict(Counter(row.get("Group", "unknown") or "unknown" for row in participant_rows)),
        "missingness_summary": _build_missingness_summary(
            participant_rows=participant_rows,
            participant_subjects=participant_subjects,
            phenotype_summaries=phenotype_summaries,
            mri_inventory=mri_inventory,
        ),
        "mri_modality_qc_inventory": mri_inventory,
        "repeat_visit_availability": repeat_visit_availability,
        "source": "tcp",
        "source_identifier": source_identifier,
        "subject_counts": {
            "participant_rows": len(participant_rows),
            "participant_subjects": len(participant_subjects),
            "subject_directories": len({subject_id for subject_id in _subject_ids_from_inventory(file_inventory) if subject_id}),
        },
        "cognition_instrument_inventory": phenotype_summaries["cognition"],
        "symptom_instrument_inventory": phenotype_summaries["symptom"],
    }

    profile_path = profiles_path / "tcp_audit_profile.json"
    write_json_artifact(audit_profile, profile_path)

    audit_provenance_path = manifests_path / "tcp_audit_provenance.json"
    audit_provenance = build_audit_provenance(
        source="tcp",
        source_identifier=source_identifier,
        dataset_accession=dataset_accession,
        dataset_version=effective_dataset_version,
        command=command,
        git_sha=git_sha,
        raw_root=raw_path,
        processed_outputs=(
            ProcessedOutputRecord(
                output_name="audit_profile",
                relative_path=profile_path.name,
                sha256=file_sha256(profile_path),
            ),
        ),
        mappings=(
            ProvenanceMapping(
                processed_output=profile_path.name,
                raw_inputs=tuple(sorted(used_inputs)),
            ),
        ),
    )
    write_audit_provenance(audit_provenance, audit_provenance_path)

    run_manifest_path = manifests_path / "tcp_audit_run_manifest.json"
    run_manifest = build_run_manifest(
        dataset_source="tcp",
        dataset_version=effective_dataset_version,
        command=command,
        git_sha=git_sha,
        seed=seed,
        output_paths={
            "audit_profile": profile_path,
            "audit_provenance": audit_provenance_path,
        },
    )
    write_run_manifest(run_manifest, run_manifest_path)

    return {
        "audit_profile": str(profile_path),
        "audit_provenance": str(audit_provenance_path),
        "run_manifest": str(run_manifest_path),
    }


def _inspect_local_inventory(raw_root: Path) -> dict[str, SourceFileRecord]:
    if not raw_root.exists():
        return {}
    return {record.relative_path: record for record in inspect_local_tree(raw_root)}


def _merge_file_inventory(
    local_inventory: dict[str, SourceFileRecord],
    source_manifest_files: tuple[SourceFileRecord, ...],
) -> dict[str, SourceFileRecord]:
    merged = {record.relative_path: record for record in source_manifest_files}
    for relative_path, local_record in local_inventory.items():
        source_record = merged.get(relative_path)
        if source_record is None:
            merged[relative_path] = local_record
            continue
        merged[relative_path] = _merge_source_file_records(source_record, local_record)
    return merged


def _merge_source_file_records(source_record: SourceFileRecord, local_record: SourceFileRecord) -> SourceFileRecord:
    content_kind = local_record.content_kind
    if source_record.content_kind != "file" and local_record.content_kind == "file":
        content_kind = source_record.content_kind

    storage = local_record.storage
    if source_record.storage in {"copied", "staged"}:
        storage = source_record.storage

    return SourceFileRecord(
        relative_path=local_record.relative_path,
        storage=storage,
        size_bytes=local_record.size_bytes if local_record.size_bytes is not None else source_record.size_bytes,
        sha256=local_record.sha256 or source_record.sha256,
        source_url=local_record.source_url or source_record.source_url,
        content_kind=content_kind,
    )


def _load_notes_map(path: Path, raw_root: Path, used_inputs: set[str]) -> dict[str, str]:
    if not path.exists():
        return {}
    rows = _read_tsv_rows(path, raw_root, used_inputs)
    mapping: dict[str, str] = {}
    for row in rows:
        key = (row.get("Survey_Acronym_NDA") or "").strip()
        label = (row.get("Survey_Long_Name") or "").strip()
        if key and label:
            mapping[key] = label
    return mapping


def _read_tsv_rows(path: Path, raw_root: Path, used_inputs: set[str]) -> list[dict[str, str]]:
    if not path.exists():
        return []
    used_inputs.add(path.relative_to(raw_root).as_posix())
    text = path.read_text(encoding="utf-8-sig")
    if local_file_content_kind(path) == "git-annex-pointer":
        return []
    reader = csv.DictReader(text.splitlines(), delimiter="\t")
    return [dict(row) for row in reader]


def _read_csv_rows(path: Path, raw_root: Path, used_inputs: set[str]) -> list[dict[str, str]]:
    if not path.exists():
        return []
    used_inputs.add(path.relative_to(raw_root).as_posix())
    text = path.read_text(encoding="utf-8-sig")
    if local_file_content_kind(path) == "git-annex-pointer":
        return []
    reader = csv.DictReader(text.splitlines())
    return [dict(row) for row in reader]


def _collect_subject_ids(rows: list[dict[str, str]]) -> set[str]:
    subject_ids: set[str] = set()
    for row in rows:
        for column_name in SUBJECT_ID_COLUMNS:
            value = (row.get(column_name) or "").strip()
            if value:
                subject_ids.add(value)
                break
    return subject_ids


def _build_phenotype_inventory(
    *,
    raw_root: Path,
    inventory: dict[str, SourceFileRecord],
    notes_map: dict[str, str],
    used_inputs: set[str],
) -> dict[str, dict[str, Any]]:
    categorized: dict[str, list[dict[str, Any]]] = {"cognition": [], "symptom": []}
    phenotype_prefix = "phenotype/"
    discovered_codes = {
        Path(relative_path).stem.replace("_definitions", "")
        for relative_path in inventory
        if relative_path.startswith(phenotype_prefix)
        and relative_path != "phenotype/notes.tsv"
    }

    for instrument_code in sorted(discovered_codes):
        category = _instrument_category(instrument_code)
        if category is None:
            continue

        data_path = raw_root / "phenotype" / f"{instrument_code}.tsv"
        definition_path = raw_root / "phenotype" / f"{instrument_code}_definitions.tsv"
        data_rows = _read_tsv_rows(data_path, raw_root, used_inputs)
        data_file_record = inventory.get(f"phenotype/{instrument_code}.tsv")
        qc_columns = sorted(
            {
                column_name
                for row in data_rows
                for column_name in row
                if column_name.lower().startswith("qc")
            }
        )
        visit_summary = _summarize_visits(data_rows)
        summary = {
            "data_file": f"phenotype/{instrument_code}.tsv",
            "definition_file": f"phenotype/{instrument_code}_definitions.tsv",
            "definition_present": definition_path.exists() or f"phenotype/{instrument_code}_definitions.tsv" in inventory,
            "instrument": instrument_code,
            "label": notes_map.get(instrument_code, instrument_code),
            "missing_cells": _count_missing_cells(data_rows),
            "qc_columns": qc_columns,
            "rows": len(data_rows) if data_rows else 0,
            "storage": _instrument_storage(data_path, data_file_record),
            "subject_visit_labels": visit_summary["subject_visit_labels"],
            "subjects_with_data": len(_collect_subject_ids(data_rows)),
            "subject_visit_counts": visit_summary["subject_visit_counts"],
            "visit_labels": visit_summary["visit_labels"],
        }
        categorized[category].append(summary)

    return {
        category: {
            "available_instruments": sum(1 for item in items if item["storage"] == "tabular"),
            "instruments": items,
            "unresolved_instruments": [item["instrument"] for item in items if item["storage"] != "tabular"],
        }
        for category, items in categorized.items()
    }


def _instrument_category(instrument_code: str) -> str | None:
    if instrument_code in COGNITION_INSTRUMENT_CODES:
        return "cognition"
    if instrument_code in SYMPTOM_INSTRUMENT_CODES:
        return "symptom"
    return None


def _instrument_storage(data_path: Path, record: SourceFileRecord | None) -> str:
    if data_path.exists():
        if local_file_content_kind(data_path) == "git-annex-pointer":
            return "git-annex-pointer"
        return "tabular"
    if record is not None and record.content_kind == "git-annex-pointer":
        return "git-annex-pointer"
    if record is not None:
        return record.storage
    return "missing"


def _count_missing_cells(rows: list[dict[str, str]]) -> int:
    return sum(1 for row in rows for value in row.values() if (value or "").strip() in MISSING_VALUE_MARKERS)


def _summarize_visits(rows: list[dict[str, str]]) -> dict[str, dict[str, int]]:
    visits_by_subject: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        subject_id = ""
        for column_name in SUBJECT_ID_COLUMNS:
            candidate = (row.get(column_name) or "").strip()
            if candidate:
                subject_id = candidate
                break
        if not subject_id:
            continue

        visit_label = ""
        for column_name in VISIT_COLUMNS:
            candidate = (row.get(column_name) or "").strip()
            if candidate:
                visit_label = candidate
                break
        if not visit_label:
            visit_label = "unspecified"
        visits_by_subject[subject_id].add(visit_label)

    visit_labels = Counter(label for labels in visits_by_subject.values() for label in labels)
    subject_visit_counts = {subject_id: len(labels) for subject_id, labels in visits_by_subject.items()}
    return {
        "subject_visit_labels": {subject_id: sorted(labels) for subject_id, labels in visits_by_subject.items()},
        "subject_visit_counts": subject_visit_counts,
        "visit_labels": dict(visit_labels),
    }


def _build_repeat_visit_summary(
    *,
    participant_subjects: set[str],
    phenotype_summaries: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    visit_counts: Counter[str] = Counter()
    subject_visit_counts: dict[str, int] = {}
    labels_by_subject: dict[str, set[str]] = defaultdict(set)

    for category in ("cognition", "symptom"):
        for item in phenotype_summaries[category]["instruments"]:
            subject_labels = item.get("subject_visit_labels")
            if isinstance(subject_labels, dict):
                for subject_id, labels in subject_labels.items():
                    if isinstance(labels, list):
                        labels_by_subject[subject_id].update(labels)
            subject_counts = item.get("subject_visit_counts")
            if isinstance(subject_counts, dict):
                for subject_id, count in subject_counts.items():
                    if count > subject_visit_counts.get(subject_id, 0):
                        subject_visit_counts[subject_id] = count

    for labels in labels_by_subject.values():
        visit_counts.update(labels)

    subjects_with_repeat_visits = sum(1 for count in subject_visit_counts.values() if count > 1)
    return {
        "max_visit_count_per_subject": max(subject_visit_counts.values(), default=1 if participant_subjects else 0),
        "subjects_with_repeat_visits": subjects_with_repeat_visits,
        "subjects_without_repeat_visits": max(len(participant_subjects) - subjects_with_repeat_visits, 0),
        "visit_labels": dict(visit_counts),
    }


def _build_mri_inventory(
    *,
    inventory: dict[str, SourceFileRecord],
    participant_subjects: set[str],
    raw_root: Path,
    used_inputs: set[str],
) -> dict[str, Any]:
    modality_subjects: dict[str, set[str]] = defaultdict(set)
    functional_runs: dict[str, set[str]] = defaultdict(set)
    qc_rows: dict[str, int] = {}
    qc_file_states: dict[str, str] = {}

    for relative_path in sorted(inventory):
        if relative_path.startswith("sub-"):
            subject_id = _subject_id_from_relative_path(relative_path)
            if subject_id is None:
                continue
            if re.search(r"_T1w\.(nii\.gz|json)$", relative_path):
                modality_subjects["T1w"].add(subject_id)
            elif re.search(r"_T2w\.(nii\.gz|json)$", relative_path):
                modality_subjects["T2w"].add(subject_id)
            elif re.search(r"_dir-ap_epi\.(nii\.gz|json)$", relative_path):
                modality_subjects["epiAP"].add(subject_id)
            elif re.search(r"_dir-pa_epi\.(nii\.gz|json)$", relative_path):
                modality_subjects["epiPA"].add(subject_id)
            else:
                task_match = re.search(r"_task-([A-Za-z0-9]+)_run-\d+_bold\.(nii\.gz|json)$", relative_path)
                if task_match:
                    task_label = task_match.group(1)
                    modality_subjects[task_label].add(subject_id)
                    functional_runs[task_label].add(subject_id)
        elif relative_path.startswith("motion_FD/"):
            csv_path = raw_root / relative_path
            csv_rows = _read_csv_rows(csv_path, raw_root, used_inputs)
            qc_rows[Path(relative_path).name] = len(csv_rows)
            record = inventory[relative_path]
            qc_file_states[Path(relative_path).name] = "tabular" if csv_rows else record.content_kind

    missing_subjects = {
        modality: max(len(participant_subjects) - len(modality_subjects.get(modality, set())), 0)
        for modality in EXPECTED_MRI_RUNS
    }

    return {
        "functional_run_subject_counts": {key: len(value) for key, value in sorted(functional_runs.items())},
        "missing_subjects_by_modality": missing_subjects,
        "modality_subject_counts": {key: len(value) for key, value in sorted(modality_subjects.items())},
        "qc_files": {
            "row_counts": qc_rows,
            "storage": qc_file_states,
        },
    }


def _build_missingness_summary(
    *,
    participant_rows: list[dict[str, str]],
    participant_subjects: set[str],
    phenotype_summaries: dict[str, dict[str, Any]],
    mri_inventory: dict[str, Any],
) -> dict[str, Any]:
    participant_missing: dict[str, int] = Counter()
    for row in participant_rows:
        for column_name, value in row.items():
            if (value or "").strip() in MISSING_VALUE_MARKERS:
                participant_missing[column_name] += 1

    phenotype_missing_cells = 0
    unresolved_tables = 0
    for category in ("cognition", "symptom"):
        for item in phenotype_summaries[category]["instruments"]:
            phenotype_missing_cells += int(item["missing_cells"])
            if item["storage"] != "tabular":
                unresolved_tables += 1

    return {
        "mri_missing_subjects_by_modality": mri_inventory["missing_subjects_by_modality"],
        "participant_missing_by_column": dict(participant_missing),
        "participant_subjects": len(participant_subjects),
        "phenotype_missing_cells": phenotype_missing_cells,
        "phenotype_unresolved_tables": unresolved_tables,
    }


def _subject_id_from_relative_path(relative_path: str) -> str | None:
    match = re.match(r"(sub-[^/]+)/", relative_path)
    if match is None:
        return None
    return match.group(1)


def _subject_ids_from_inventory(inventory: dict[str, SourceFileRecord]) -> set[str]:
    subject_ids = set()
    for relative_path in inventory:
        subject_id = _subject_id_from_relative_path(relative_path)
        if subject_id is not None:
            subject_ids.add(subject_id)
    return subject_ids


__all__ = ["run_tcp_audit"]

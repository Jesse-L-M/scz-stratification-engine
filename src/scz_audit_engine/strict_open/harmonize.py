"""Canonical harmonization for strict-open TCP inputs."""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .provenance import SourceFileRecord, load_source_manifest, local_file_content_kind, write_json_artifact
from .run_manifest import build_run_manifest, write_run_manifest
from .schema import COGNITION_SCORES, MRI_FEATURES, SUBJECTS, SYMPTOM_BEHAVIOR_SCORES, VISITS
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
EXPECTED_MRI_MODALITIES = ("T1w", "T2w", "restAP", "restPA", "stroopAP", "stroopPA", "hammerAP", "epiAP", "epiPA")
MISSING_VALUE_MARKERS = {"", "999", "999.0", "NA", "N/A", "na", "n/a"}
SUBJECT_ID_COLUMNS = ("participant_id", "subject_id", "src_subject_id", "subjectkey")
VISIT_COLUMNS = ("visit", "visit_label", "eventname", "session_id", "interview_date")
NON_SCORE_COLUMNS = set(SUBJECT_ID_COLUMNS) | set(VISIT_COLUMNS) | {
    "species",
    "sex",
    "site",
    "group",
    "age",
}
BASELINE_VISIT_LABEL = "baseline"


@dataclass(frozen=True, slots=True)
class DelimitedTable:
    """Header-plus-row payload for a delimited file."""

    fieldnames: tuple[str, ...]
    rows: tuple[dict[str, str], ...]


@dataclass(slots=True)
class PhenotypeTable:
    """Source-aligned phenotype table plus harmonization metadata."""

    instrument: str
    category: str
    label: str
    storage: str
    data_relative_path: str
    definition_relative_path: str | None
    fieldnames: tuple[str, ...]
    rows: tuple[dict[str, str], ...]
    definitions: dict[str, str]


def run_tcp_harmonization(
    *,
    raw_root: str | Path,
    manifests_root: str | Path,
    harmonized_root: str | Path,
    command: list[str] | tuple[str, ...],
    git_sha: str | None,
    seed: int,
    dataset_version: str | None,
    source_identifier: str = "tcp-ds005237",
    dataset_accession: str = "ds005237",
    source_manifest_path: str | Path | None = None,
) -> dict[str, str]:
    """Transform staged TCP raw inputs into canonical strict-open tables."""

    raw_path = Path(raw_root)
    manifests_path = Path(manifests_root)
    harmonized_path = Path(harmonized_root)
    if not (raw_path / "participants.tsv").exists():
        raise FileNotFoundError(f"Missing participants.tsv under {raw_path}")

    source_manifest = None
    effective_dataset_version = dataset_version
    if source_manifest_path is not None and Path(source_manifest_path).exists():
        source_manifest = load_source_manifest(source_manifest_path)
        effective_dataset_version = source_manifest.dataset_version or dataset_version

    inventory = _merge_file_inventory(
        _inspect_local_inventory(raw_path),
        source_manifest.files if source_manifest is not None else (),
    )
    used_inputs: set[str] = set()
    inaccessible_inputs: set[str] = set()
    notes_map = _load_notes_map(raw_path / "phenotype" / "notes.tsv", raw_path, used_inputs)
    participant_rows = _read_delimited_table(raw_path / "participants.tsv", raw_path, used_inputs, delimiter="\t").rows

    subject_rows, subject_lookup, subject_issues = _build_subject_rows(participant_rows)
    phenotype_tables = _load_phenotype_tables(
        raw_root=raw_path,
        inventory=inventory,
        notes_map=notes_map,
        used_inputs=used_inputs,
        inaccessible_inputs=inaccessible_inputs,
    )
    visit_rows, visit_lookup, baseline_visit_lookup = _build_visit_rows(subject_rows, phenotype_tables)
    cognition_rows, symptom_rows, score_summary = _build_score_rows(
        phenotype_tables=phenotype_tables,
        subject_lookup=subject_lookup,
        visit_lookup=visit_lookup,
    )
    mri_rows, mri_summary = _build_mri_rows(
        inventory=inventory,
        raw_root=raw_path,
        subject_rows=subject_rows,
        subject_lookup=subject_lookup,
        baseline_visit_lookup=baseline_visit_lookup,
        used_inputs=used_inputs,
        inaccessible_inputs=inaccessible_inputs,
    )

    table_payloads = {
        SUBJECTS.name: _sort_rows(subject_rows, ("subject_id",)),
        VISITS.name: _sort_rows(visit_rows, ("subject_id", "visit_index", "visit_id")),
        COGNITION_SCORES.name: _sort_rows(
            cognition_rows,
            ("subject_id", "visit_id", "instrument", "measure"),
        ),
        SYMPTOM_BEHAVIOR_SCORES.name: _sort_rows(
            symptom_rows,
            ("subject_id", "visit_id", "instrument", "measure"),
        ),
        MRI_FEATURES.name: _sort_rows(
            mri_rows,
            ("subject_id", "visit_id", "modality", "feature_name"),
        ),
    }
    output_paths = {
        SUBJECTS.name: _write_csv_table(SUBJECTS.columns, table_payloads[SUBJECTS.name], harmonized_path / "subjects.csv"),
        VISITS.name: _write_csv_table(VISITS.columns, table_payloads[VISITS.name], harmonized_path / "visits.csv"),
        COGNITION_SCORES.name: _write_csv_table(
            COGNITION_SCORES.columns,
            table_payloads[COGNITION_SCORES.name],
            harmonized_path / "cognition_scores.csv",
        ),
        SYMPTOM_BEHAVIOR_SCORES.name: _write_csv_table(
            SYMPTOM_BEHAVIOR_SCORES.columns,
            table_payloads[SYMPTOM_BEHAVIOR_SCORES.name],
            harmonized_path / "symptom_behavior_scores.csv",
        ),
        MRI_FEATURES.name: _write_csv_table(
            MRI_FEATURES.columns,
            table_payloads[MRI_FEATURES.name],
            harmonized_path / "mri_features.csv",
        ),
    }

    harmonization_manifest_path = harmonized_path / "harmonization_manifest.json"
    harmonization_manifest = {
        "command": list(command),
        "dataset_accession": dataset_accession,
        "dataset_version": effective_dataset_version,
        "git_sha": git_sha,
        "input_paths": sorted(used_inputs),
        "inaccessible_inputs": sorted(inaccessible_inputs),
        "mri_summary": mri_summary,
        "mri_visit_policy": (
            "MRI availability and motion-QC rows are attached to the first canonical visit for each subject "
            "because the strict-open TCP public paths do not expose a reliable multi-visit imaging session map."
        ),
        "output_table_paths": {name: str(path) for name, path in output_paths.items()},
        "row_counts": {name: len(rows) for name, rows in table_payloads.items()},
        "raw_root": str(raw_path),
        "score_summary": score_summary,
        "source": "tcp",
        "source_identifier": source_identifier,
        "source_manifest_path": str(source_manifest_path) if source_manifest_path is not None else None,
        "subject_issues": subject_issues,
        "unmapped_fields": score_summary["unmapped_fields"],
    }
    write_json_artifact(harmonization_manifest, harmonization_manifest_path)

    run_manifest_path = manifests_path / "tcp_harmonize_run_manifest.json"
    run_manifest = build_run_manifest(
        dataset_source="tcp",
        dataset_version=effective_dataset_version,
        command=command,
        git_sha=git_sha,
        seed=seed,
        output_paths={
            **output_paths,
            "harmonization_manifest": harmonization_manifest_path,
        },
    )
    write_run_manifest(run_manifest, run_manifest_path)

    return {
        "harmonization_manifest": str(harmonization_manifest_path),
        "harmonized_dir": str(harmonized_path),
        "run_manifest": str(run_manifest_path),
        **{name: str(path) for name, path in output_paths.items()},
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
        storage = local_record.storage
        if source_record.storage in {"copied", "staged"}:
            storage = source_record.storage
        merged[relative_path] = SourceFileRecord(
            relative_path=local_record.relative_path,
            storage=storage,
            size_bytes=local_record.size_bytes if local_record.size_bytes is not None else source_record.size_bytes,
            sha256=local_record.sha256 or source_record.sha256,
            source_url=local_record.source_url or source_record.source_url,
            content_kind=local_record.content_kind,
        )
    return merged


def _load_notes_map(path: Path, raw_root: Path, used_inputs: set[str]) -> dict[str, str]:
    table = _read_delimited_table(path, raw_root, used_inputs, delimiter="\t")
    mapping: dict[str, str] = {}
    for row in table.rows:
        key = (row.get("Survey_Acronym_NDA") or "").strip()
        label = (row.get("Survey_Long_Name") or "").strip()
        if key and label:
            mapping[key] = label
    return mapping


def _read_delimited_table(
    path: Path,
    raw_root: Path,
    used_inputs: set[str],
    *,
    delimiter: str,
    record: SourceFileRecord | None = None,
) -> DelimitedTable:
    if not path.exists():
        return DelimitedTable(fieldnames=(), rows=())
    used_inputs.add(path.relative_to(raw_root).as_posix())
    if _effective_content_kind(path, record) == "git-annex-pointer":
        return DelimitedTable(fieldnames=(), rows=())
    text = path.read_text(encoding="utf-8-sig")
    reader = csv.DictReader(text.splitlines(), delimiter=delimiter)
    rows = tuple(dict(row) for row in reader)
    return DelimitedTable(fieldnames=tuple(reader.fieldnames or ()), rows=rows)


def _effective_content_kind(path: Path, record: SourceFileRecord | None) -> str:
    if path.exists():
        return local_file_content_kind(path)
    if record is not None:
        return record.content_kind
    return "missing"


def _build_subject_rows(
    participant_rows: tuple[dict[str, str], ...],
) -> tuple[list[dict[str, str]], dict[str, dict[str, str]], dict[str, Any]]:
    subject_rows: list[dict[str, str]] = []
    subject_lookup: dict[str, dict[str, str]] = {}
    duplicate_subject_rows: list[str] = []
    missing_subject_rows = 0

    for row in participant_rows:
        source_subject_id = _resolve_subject_id(row)
        if not source_subject_id:
            missing_subject_rows += 1
            continue
        if source_subject_id in subject_lookup:
            duplicate_subject_rows.append(source_subject_id)
            continue

        canonical_row = {
            "subject_id": _canonical_subject_id(source_subject_id),
            "source_dataset": "tcp-ds005237",
            "source_subject_id": source_subject_id,
            "diagnosis": _clean_string(row.get("Group"), default="unknown"),
            "site_id": _clean_string(row.get("Site"), default="unknown"),
            "sex": _clean_string(row.get("sex")),
            "age_years": _clean_numeric_text(row.get("age")),
        }
        subject_lookup[source_subject_id] = canonical_row
        subject_rows.append(canonical_row)

    issues = {
        "duplicate_subject_rows": sorted(duplicate_subject_rows),
        "missing_subject_rows": missing_subject_rows,
    }
    return subject_rows, subject_lookup, issues


def _load_phenotype_tables(
    *,
    raw_root: Path,
    inventory: dict[str, SourceFileRecord],
    notes_map: dict[str, str],
    used_inputs: set[str],
    inaccessible_inputs: set[str],
) -> list[PhenotypeTable]:
    phenotype_tables: list[PhenotypeTable] = []
    instrument_codes = {
        Path(relative_path).stem.replace("_definitions", "")
        for relative_path in inventory
        if relative_path.startswith("phenotype/")
        and relative_path != "phenotype/notes.tsv"
        and not relative_path.endswith("_definitions.tsv")
    }
    for instrument in sorted(instrument_codes):
        category = _instrument_category(instrument)
        if category is None:
            continue
        data_relative_path = f"phenotype/{instrument}.tsv"
        definition_relative_path = f"phenotype/{instrument}_definitions.tsv"
        data_path = raw_root / data_relative_path
        data_record = inventory.get(data_relative_path)
        storage = _tabular_storage(data_path, data_record)
        if storage != "tabular":
            inaccessible_inputs.add(data_relative_path)
        table = _read_delimited_table(data_path, raw_root, used_inputs, delimiter="\t", record=data_record)
        definition_map = _load_definition_map(
            raw_root / definition_relative_path,
            raw_root,
            used_inputs,
            inventory.get(definition_relative_path),
        )
        phenotype_tables.append(
            PhenotypeTable(
                instrument=instrument,
                category=category,
                label=notes_map.get(instrument, instrument),
                storage=storage,
                data_relative_path=data_relative_path,
                definition_relative_path=definition_relative_path
                if definition_relative_path in inventory or (raw_root / definition_relative_path).exists()
                else None,
                fieldnames=table.fieldnames,
                rows=table.rows,
                definitions=definition_map,
            )
        )
    return phenotype_tables


def _load_definition_map(
    path: Path,
    raw_root: Path,
    used_inputs: set[str],
    record: SourceFileRecord | None,
) -> dict[str, str]:
    table = _read_delimited_table(path, raw_root, used_inputs, delimiter="\t", record=record)
    mapping: dict[str, str] = {}
    for row in table.rows:
        key = (row.get("ElementName") or "").strip()
        label = (row.get("ElementDescription") or "").strip()
        if key:
            mapping[key] = label
    return mapping


def _build_visit_rows(
    subject_rows: list[dict[str, str]],
    phenotype_tables: list[PhenotypeTable],
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], str], dict[str, str]]:
    labels_by_subject: dict[str, set[str]] = defaultdict(set)
    for table in phenotype_tables:
        if table.storage != "tabular":
            continue
        for row in table.rows:
            source_subject_id = _resolve_subject_id(row)
            if not source_subject_id:
                continue
            labels_by_subject[source_subject_id].add(_resolve_visit_label(row))

    visit_rows: list[dict[str, Any]] = []
    visit_lookup: dict[tuple[str, str], str] = {}
    baseline_visit_lookup: dict[str, str] = {}
    for subject_row in subject_rows:
        source_subject_id = subject_row["source_subject_id"]
        visit_labels = labels_by_subject.get(source_subject_id) or {BASELINE_VISIT_LABEL}
        ordered_labels = sorted(visit_labels, key=_visit_sort_key)
        baseline_date = next((_parse_visit_date(label) for label in ordered_labels if _parse_visit_date(label) is not None), None)
        for visit_index, visit_label in enumerate(ordered_labels):
            visit_id = _canonical_visit_id(subject_row["subject_id"], visit_index, visit_label)
            visit_rows.append(
                {
                    "visit_id": visit_id,
                    "subject_id": subject_row["subject_id"],
                    "visit_label": visit_label,
                    "visit_index": visit_index,
                    "days_from_baseline": _days_from_baseline(visit_label, baseline_date, visit_index),
                }
            )
            visit_lookup[(source_subject_id, visit_label)] = visit_id
            if visit_index == 0:
                baseline_visit_lookup[source_subject_id] = visit_id
    return visit_rows, visit_lookup, baseline_visit_lookup


def _build_score_rows(
    *,
    phenotype_tables: list[PhenotypeTable],
    subject_lookup: dict[str, dict[str, str]],
    visit_lookup: dict[tuple[str, str], str],
) -> tuple[list[dict[str, str]], list[dict[str, str]], dict[str, Any]]:
    cognition_rows: list[dict[str, str]] = []
    symptom_rows: list[dict[str, str]] = []
    inaccessible_by_category: dict[str, list[str]] = defaultdict(list)
    accessible_by_category: dict[str, list[str]] = defaultdict(list)
    unmapped_fields: dict[str, list[str]] = {}
    unresolved_subject_rows: dict[str, int] = defaultdict(int)

    for table in phenotype_tables:
        if table.storage != "tabular":
            inaccessible_by_category[table.category].append(table.instrument)
            continue

        accessible_by_category[table.category].append(table.instrument)
        measure_columns = _resolve_measure_columns(table)
        if table.fieldnames:
            unmapped = [
                column_name
                for column_name in table.fieldnames
                if column_name not in measure_columns and column_name.lower() not in NON_SCORE_COLUMNS and not column_name.lower().startswith("qc")
            ]
            if unmapped:
                unmapped_fields[table.instrument] = sorted(unmapped)

        for row in table.rows:
            source_subject_id = _resolve_subject_id(row)
            if not source_subject_id or source_subject_id not in subject_lookup:
                unresolved_subject_rows[table.instrument] += 1
                continue

            visit_label = _resolve_visit_label(row)
            visit_id = visit_lookup.get((source_subject_id, visit_label))
            if visit_id is None:
                unresolved_subject_rows[table.instrument] += 1
                continue

            canonical_subject_id = subject_lookup[source_subject_id]["subject_id"]
            target_rows = cognition_rows if table.category == "cognition" else symptom_rows
            for measure in measure_columns:
                value = _clean_string(row.get(measure))
                if value in MISSING_VALUE_MARKERS:
                    continue
                target_rows.append(
                    {
                        "subject_id": canonical_subject_id,
                        "visit_id": visit_id,
                        "instrument": table.instrument,
                        "measure": measure,
                        "score": value,
                    }
                )

    summary = {
        "accessible_instruments": {
            category: sorted(values)
            for category, values in accessible_by_category.items()
        },
        "inaccessible_instruments": {
            category: sorted(values)
            for category, values in inaccessible_by_category.items()
        },
        "unmapped_fields": unmapped_fields,
        "unresolved_subject_rows": dict(unresolved_subject_rows),
    }
    return cognition_rows, symptom_rows, summary


def _resolve_measure_columns(table: PhenotypeTable) -> tuple[str, ...]:
    candidates = [
        column_name
        for column_name in table.fieldnames
        if column_name.lower() not in NON_SCORE_COLUMNS and not column_name.lower().startswith("qc")
    ]
    if not table.definitions:
        return tuple(candidates)
    definition_names = set(table.definitions)
    return tuple(column_name for column_name in candidates if column_name in definition_names)


def _build_mri_rows(
    *,
    inventory: dict[str, SourceFileRecord],
    raw_root: Path,
    subject_rows: list[dict[str, str]],
    subject_lookup: dict[str, dict[str, str]],
    baseline_visit_lookup: dict[str, str],
    used_inputs: set[str],
    inaccessible_inputs: set[str],
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    mri_rows: list[dict[str, str]] = []
    modality_presence: dict[str, set[str]] = defaultdict(set)
    qc_rows_by_modality: dict[str, int] = {}

    for relative_path, record in sorted(inventory.items()):
        subject_id = _subject_id_from_relative_path(relative_path)
        if subject_id is None:
            continue
        modality = _modality_from_image_payload_path(relative_path)
        if modality is None:
            continue
        local_path = raw_root / relative_path
        if not local_path.exists():
            inaccessible_inputs.add(relative_path)
            continue
        used_inputs.add(relative_path)
        if _effective_content_kind(local_path, record) == "git-annex-pointer":
            inaccessible_inputs.add(relative_path)
            continue
        modality_presence[modality].add(subject_id)

    for subject_row in subject_rows:
        source_subject_id = subject_row["source_subject_id"]
        visit_id = baseline_visit_lookup[source_subject_id]
        for modality in EXPECTED_MRI_MODALITIES:
            available = source_subject_id in modality_presence.get(modality, set())
            mri_rows.append(
                {
                    "subject_id": subject_row["subject_id"],
                    "visit_id": visit_id,
                    "modality": modality,
                    "feature_name": "available",
                    "feature_value": "1" if available else "0",
                    "qc_status": "present" if available else "missing",
                }
            )

    for relative_path, record in sorted(inventory.items()):
        if not relative_path.startswith("motion_FD/"):
            continue
        modality = _modality_from_qc_filename(Path(relative_path).name)
        if modality is None:
            continue
        local_path = raw_root / relative_path
        if not local_path.exists():
            inaccessible_inputs.add(relative_path)
            continue
        used_inputs.add(relative_path)
        if _effective_content_kind(local_path, record) == "git-annex-pointer":
            inaccessible_inputs.add(relative_path)
            continue
        table = _read_delimited_table(local_path, raw_root, used_inputs, delimiter=",", record=record)
        qc_rows_by_modality[modality] = len(table.rows)
        for row in table.rows:
            source_subject_id = _resolve_subject_id(row)
            if not source_subject_id or source_subject_id not in subject_lookup:
                continue
            value = _clean_string(row.get("mean_fd"))
            if value in MISSING_VALUE_MARKERS:
                continue
            mri_rows.append(
                {
                    "subject_id": subject_lookup[source_subject_id]["subject_id"],
                    "visit_id": baseline_visit_lookup[source_subject_id],
                    "modality": modality,
                    "feature_name": "mean_fd",
                    "feature_value": value,
                    "qc_status": "tabular_qc",
                }
            )

    summary = {
        "available_subjects_by_modality": {
            modality: len(subject_ids)
            for modality, subject_ids in sorted(modality_presence.items())
        },
        "expected_modalities": list(EXPECTED_MRI_MODALITIES),
        "qc_rows_by_modality": qc_rows_by_modality,
    }
    return mri_rows, summary


def _resolve_subject_id(row: dict[str, str]) -> str:
    for column_name in SUBJECT_ID_COLUMNS:
        candidate = _clean_string(row.get(column_name))
        if candidate:
            return candidate
    return ""


def _resolve_visit_label(row: dict[str, str]) -> str:
    for column_name in VISIT_COLUMNS:
        candidate = _clean_string(row.get(column_name))
        if candidate:
            return candidate
    return BASELINE_VISIT_LABEL


def _canonical_subject_id(source_subject_id: str) -> str:
    return f"tcp-ds005237:{source_subject_id}"


def _canonical_visit_id(subject_id: str, visit_index: int, visit_label: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", visit_label.lower()).strip("-") or "visit"
    return f"{subject_id}:visit-{visit_index:02d}-{slug}"


def _visit_sort_key(visit_label: str) -> tuple[int, str]:
    parsed = _parse_visit_date(visit_label)
    if parsed is not None:
        return (0, parsed.strftime("%Y-%m-%d"))
    if visit_label.lower() == BASELINE_VISIT_LABEL:
        return (1, visit_label.lower())
    return (2, visit_label.lower())


def _parse_visit_date(visit_label: str) -> datetime | None:
    for pattern in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(visit_label, pattern)
        except ValueError:
            continue
    return None


def _days_from_baseline(visit_label: str, baseline_date: datetime | None, visit_index: int) -> int | str:
    if baseline_date is None:
        return 0 if visit_index == 0 else ""
    parsed = _parse_visit_date(visit_label)
    if parsed is None:
        return 0 if visit_index == 0 else ""
    return (parsed.date() - baseline_date.date()).days


def _instrument_category(instrument_code: str) -> str | None:
    if instrument_code in COGNITION_INSTRUMENT_CODES:
        return "cognition"
    if instrument_code in SYMPTOM_INSTRUMENT_CODES:
        return "symptom"
    return None


def _tabular_storage(path: Path, record: SourceFileRecord | None) -> str:
    content_kind = _effective_content_kind(path, record)
    if content_kind == "git-annex-pointer":
        return "git-annex-pointer"
    if path.exists():
        return "tabular"
    if record is not None:
        return record.storage
    return "missing"


def _modality_from_image_payload_path(relative_path: str) -> str | None:
    if re.search(r"_T1w\.nii\.gz$", relative_path):
        return "T1w"
    if re.search(r"_T2w\.nii\.gz$", relative_path):
        return "T2w"
    if re.search(r"_dir-ap_epi\.nii\.gz$", relative_path):
        return "epiAP"
    if re.search(r"_dir-pa_epi\.nii\.gz$", relative_path):
        return "epiPA"
    task_match = re.search(r"_task-([A-Za-z0-9]+)_run-\d+_bold\.nii\.gz$", relative_path)
    if task_match is None:
        return None
    return task_match.group(1)


def _modality_from_qc_filename(filename: str) -> str | None:
    match = re.match(r"TCP_FD_([A-Za-z]+)_([A-Za-z]+)(?:_\d+)?\.csv$", filename)
    if match is None:
        return None
    task_label = match.group(1)
    direction = match.group(2)
    return f"{task_label}{direction}"


def _subject_id_from_relative_path(relative_path: str) -> str | None:
    match = re.match(r"(sub-[^/]+)/", relative_path)
    if match is None:
        return None
    return match.group(1)


def _clean_string(value: str | None, *, default: str = "") -> str:
    cleaned = (value or "").strip()
    if not cleaned:
        return default
    return cleaned


def _clean_numeric_text(value: str | None) -> str:
    cleaned = _clean_string(value)
    if cleaned in MISSING_VALUE_MARKERS:
        return ""
    return cleaned


def _sort_rows(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda row: tuple(str(row.get(key, "")) for key in keys))


def _write_csv_table(columns: tuple[str, ...], rows: list[dict[str, Any]], destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns))
        writer.writeheader()
        for row in rows:
            writer.writerow({column_name: row.get(column_name, "") for column_name in columns})
    return destination


__all__ = ["run_tcp_harmonization"]

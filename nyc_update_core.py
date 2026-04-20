from pathlib import Path

from nyc_open_data_utils import (
    fetch_dataset_metadata,
    format_epoch,
    output_paths,
    read_json,
    utc_now,
    write_json,
)


# Settings to adjust when reusing this update checker in another NYC Open Data project.
DATASET_NAME_KEY = "name"
DATASET_RESOURCE_ID_KEY = "resource_id"
SOURCE_COLUMN_FIELDS = (
    "fieldName",
    "name",
    "dataTypeName",
    "position",
    "description",
)
RAW_DATA_CHANGE_FIELDS = ("rows_updated_at", "columns")
METADATA_CHANGE_FIELDS = (
    "title",
    "description",
    "category",
    "tags",
    "view_last_modified",
)
NEW_DATASET_RAW_FILE_KEY = "raw_csv"


# Keep only the source fields used for update checks.
def summarize_source_metadata(metadata: dict | None) -> dict:
    metadata = metadata or {}
    columns = []
    for column in metadata.get("columns", []):
        columns.append({key: column.get(key) for key in SOURCE_COLUMN_FIELDS})

    return {
        "title": metadata.get("name"),
        "description": metadata.get("description"),
        "category": metadata.get("category"),
        "tags": metadata.get("tags"),
        "rows_updated_at": metadata.get("rowsUpdatedAt"),
        "view_last_modified": metadata.get("viewLastModified"),
        "column_count": len(metadata.get("columns", [])),
        "columns": columns,
    }


# Find which expected local files are missing.
def find_missing_local_files(paths: dict[str, Path]) -> list[str]:
    missing = []
    for name, path in paths.items():
        if name != "source_metadata" and not path.exists():
            missing.append(name)
    return missing


# Detect a newly added dataset with no local source files.
def is_new_dataset_case(local_metadata_exists: bool, missing_local_files: list[str]) -> bool:
    return (not local_metadata_exists) and (NEW_DATASET_RAW_FILE_KEY in missing_local_files)


# Build one refresh decision from local and remote metadata.
def decide_refresh(local: dict, remote: dict, *, local_metadata_exists: bool, missing_local_files: list[str]) -> dict:
    raw_reasons = []
    metadata_reasons = []

    if is_new_dataset_case(local_metadata_exists, missing_local_files):
        raw_reasons.append("new_dataset")
    elif not local_metadata_exists:
        raw_reasons.append("local_metadata_missing")
    for name in missing_local_files:
        raw_reasons.append(f"missing_{name}")

    for field in RAW_DATA_CHANGE_FIELDS:
        if local.get(field) != remote.get(field):
            raw_reasons.append(field)

    for field in METADATA_CHANGE_FIELDS:
        if local.get(field) != remote.get(field):
            metadata_reasons.append(field)

    if raw_reasons:
        return {
            "status": "raw_data_changed",
            "action": "refresh_raw_data",
            "needs_refresh": True,
            "changes_vs_local": raw_reasons,
        }

    if metadata_reasons:
        return {
            "status": "metadata_changed",
            "action": "refresh_metadata",
            "needs_refresh": True,
            "changes_vs_local": metadata_reasons,
        }

    return {
        "status": "unchanged",
        "action": "no_action",
        "needs_refresh": False,
        "changes_vs_local": [],
    }


# Count datasets by status.
def summarize_results(results: list[dict]) -> dict:
    counts = {}
    for item in results:
        counts[item["status"]] = counts.get(item["status"], 0) + 1
    return counts


# Add readable time strings to report rows.
def add_readable_times(results: list[dict]) -> list[dict]:
    time_fields = ("rows_updated_at", "view_last_modified")
    sources = ("local", "remote")
    updated = []

    for item in results:
        row = dict(item)
        for source in sources:
            for field in time_fields:
                key = f"{source}_{field}"
                row[f"{key}_readable"] = format_epoch(row.get(key))
        updated.append(row)

    return updated


# Check one dataset against local and remote state.
def check_dataset(
    *,
    dataset: dict,
    local_metadata: dict | None,
    local_metadata_exists: bool,
    missing_local_files: list[str],
    metadata_fetcher=fetch_dataset_metadata,
) -> dict:
    dataset_name = dataset[DATASET_NAME_KEY]
    local = summarize_source_metadata(local_metadata)
    new_dataset = is_new_dataset_case(local_metadata_exists, missing_local_files)

    try:
        remote_raw = metadata_fetcher(dataset[DATASET_RESOURCE_ID_KEY])
        remote = summarize_source_metadata(remote_raw)
        error = None
    except Exception as exc:
        remote_raw = {}
        remote = {}
        error = str(exc)

    if error:
        decision = {
            "status": "error",
            "action": "retry_check",
            "needs_refresh": False,
            "changes_vs_local": [],
        }
    else:
        decision = decide_refresh(
            local,
            remote,
            local_metadata_exists=local_metadata_exists,
            missing_local_files=missing_local_files,
        )

    return {
        "dataset_name": dataset_name,
        "resource_id": dataset[DATASET_RESOURCE_ID_KEY],
        "status": decision["status"],
        "action": decision["action"],
        "needs_refresh": decision["needs_refresh"],
        "changes_vs_local": decision["changes_vs_local"],
        "missing_local_files": missing_local_files,
        "is_new_dataset": new_dataset,
        "error": error,
        "local_rows_updated_at": local.get("rows_updated_at"),
        "remote_rows_updated_at": remote.get("rows_updated_at"),
        "local_view_last_modified": local.get("view_last_modified"),
        "remote_view_last_modified": remote.get("view_last_modified"),
        "local_column_count": local.get("column_count"),
        "remote_column_count": remote.get("column_count"),
        "local_title": local.get("title"),
        "remote_title": remote.get("title"),
        "local_description": local.get("description"),
        "remote_description": remote.get("description"),
        "remote_metadata": remote_raw,
    }


# Build a Markdown report from check results.
def build_markdown_report(checked_at: str, results: list[dict]) -> str:
    lines = [
        "# NYC Open Data Update Check",
        "",
        f"- Checked at: `{checked_at}`",
        f"- Datasets: `{len(results)}`",
        "",
        "| Dataset | Status | Action | Needs Refresh | New Dataset | Changes vs Local |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for item in results:
        lines.append(
            f"| {item['dataset_name']} | {item['status']} | {item['action']} | "
            f"{item['needs_refresh']} | "
            f"{item['is_new_dataset']} | "
            f"{', '.join(item['changes_vs_local']) or 'none'} |"
        )
    return "\n".join(lines) + "\n"


# Write the update report as CSV.
def write_report_csv(path: Path, results: list[dict]) -> None:
    import csv

    fields = [
        "dataset_name",
        "resource_id",
        "status",
        "action",
        "needs_refresh",
        "is_new_dataset",
        "changes_vs_local",
        "local_rows_updated_at",
        "remote_rows_updated_at",
        "local_view_last_modified",
        "remote_view_last_modified",
        "local_rows_updated_at_readable",
        "remote_rows_updated_at_readable",
        "local_view_last_modified_readable",
        "remote_view_last_modified_readable",
        "local_column_count",
        "remote_column_count",
        "error",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for item in results:
            row = dict(item)
            row["changes_vs_local"] = ",".join(item["changes_vs_local"])
            writer.writerow({field: row.get(field) for field in fields})


# Run a full update check for all datasets.
def run_update_check(
    datasets: list[dict],
    *,
    report_json_path: Path | None = None,
    report_csv_path: Path | None = None,
    report_md_path: Path | None = None,
    paths_builder=output_paths,
    metadata_fetcher=fetch_dataset_metadata,
) -> dict:
    results = []

    for dataset in datasets:
        paths = paths_builder(dataset[DATASET_NAME_KEY])
        local_metadata = read_json(paths["source_metadata"])
        results.append(
            check_dataset(
                dataset=dataset,
                local_metadata=local_metadata,
                local_metadata_exists=paths["source_metadata"].exists(),
                missing_local_files=find_missing_local_files(paths),
                metadata_fetcher=metadata_fetcher,
            )
        )

    results.sort(key=lambda item: item["dataset_name"])
    results = add_readable_times(results)
    checked_at = utc_now()
    payload = {
        "checked_at": checked_at,
        "summary": summarize_results(results),
        "datasets": results,
    }

    if report_json_path is not None:
        write_json(report_json_path, payload)
    if report_csv_path is not None:
        write_report_csv(report_csv_path, results)
    if report_md_path is not None:
        report_md_path.write_text(
            build_markdown_report(checked_at, results),
            encoding="utf-8",
        )

    return payload

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
TIMESTAMP_CHANGE_FIELDS = (
    ("rows_updated_at", "rows_updated_at"),
    ("view_last_modified", "view_last_modified"),
    ("columns", "schema"),
)
SOURCE_METADATA_FIELDS = ("title", "description", "category", "tags")

# Expected inputs passed in from the project adapter.
PREVIOUS_REMOTE_STATE_KEY = "previous_remote"
LOCAL_METADATA_KEY = "local_metadata"
LOCAL_METADATA_EXISTS_KEY = "local_metadata_exists"
MISSING_LOCAL_FILES_KEY = "missing_local_files"


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


# Convert state differences into refresh triggers.
def compare_states(
    old: dict,
    new: dict,
    *,
    local_missing: bool = False,
    missing: list[str] | None = None,
) -> list[str]:
    changes = []
    if local_missing:
        changes.append("missing_local_metadata")

    for name in missing or []:
        changes.append(f"missing_{name}")

    for field, label in TIMESTAMP_CHANGE_FIELDS:
        if old.get(field) != new.get(field):
            changes.append(label)

    if any(old.get(field) != new.get(field) for field in SOURCE_METADATA_FIELDS):
        changes.append("source_metadata")

    return changes


# Map refresh triggers to a status label.
def classify_status(changes: list[str]) -> str:
    if not changes:
        return "unchanged"
    if any(change.startswith("missing_") for change in changes):
        return "missing_local_files"
    if "schema" in changes:
        return "schema_changed"
    if "rows_updated_at" in changes:
        return "data_changed"
    return "metadata_changed"


# Map refresh triggers to a pipeline action.
def classify_action(changes: list[str]) -> str:
    if not changes:
        return "no_action"

    has_missing_files = any(change.startswith("missing_") for change in changes)
    if "schema" in changes or "rows_updated_at" in changes or has_missing_files:
        return "refresh_data_and_reprofile"

    return "refresh_source_metadata_and_rebuild_lake"


# Count datasets by status.
def summarize_results(results: list[dict]) -> dict:
    counts = {}
    for item in results:
        counts[item["status"]] = counts.get(item["status"], 0) + 1
    return counts


# Add readable time strings to report rows.
def add_readable_times(results: list[dict]) -> list[dict]:
    updated = []
    for item in results:
        row = dict(item)
        for prefix in ("local", "remote"):
            row[f"{prefix}_rows_updated_at_readable"] = format_epoch(
                row.get(f"{prefix}_rows_updated_at")
            )
            row[f"{prefix}_view_last_modified_readable"] = format_epoch(
                row.get(f"{prefix}_view_last_modified")
            )
        updated.append(row)
    return updated


# Check one dataset against local and remote state.
def check_dataset(
    *,
    dataset: dict,
    previous_remote: dict,
    local_metadata: dict | None,
    local_metadata_exists: bool,
    missing_local_files: list[str],
    metadata_fetcher=fetch_dataset_metadata,
) -> dict:
    dataset_name = dataset[DATASET_NAME_KEY]
    local = summarize_source_metadata(local_metadata)

    try:
        remote_raw = metadata_fetcher(dataset[DATASET_RESOURCE_ID_KEY])
        remote = summarize_source_metadata(remote_raw)
        error = None
    except Exception as exc:
        remote = {}
        error = str(exc)

    if error:
        changes_vs_local = []
        changes_since_last_check = []
    else:
        changes_vs_local = compare_states(
            local,
            remote,
            local_missing=not local_metadata_exists,
            missing=missing_local_files,
        )
        changes_since_last_check = compare_states(previous_remote or {}, remote)

    result = {
        "dataset_name": dataset_name,
        "resource_id": dataset[DATASET_RESOURCE_ID_KEY],
        "status": "error" if error else classify_status(changes_vs_local),
        "action": "retry_check" if error else classify_action(changes_vs_local),
        "changes_vs_local": changes_vs_local,
        "changes_since_last_check": changes_since_last_check,
        "missing_local_files": missing_local_files,
        "error": error,
        "remote_state": remote,
    }

    for prefix, state in [("local", local), ("remote", remote)]:
        result[f"{prefix}_rows_updated_at"] = state.get("rows_updated_at")
        result[f"{prefix}_view_last_modified"] = state.get("view_last_modified")
        result[f"{prefix}_column_count"] = state.get("column_count")
        result[f"{prefix}_title"] = state.get("title")
        result[f"{prefix}_description_hash"] = state.get("description")

    return result


# Build a Markdown report from check results.
def build_markdown_report(checked_at: str, results: list[dict]) -> str:
    lines = [
        "# NYC Open Data Update Check",
        "",
        f"- Checked at: `{checked_at}`",
        f"- Datasets: `{len(results)}`",
        "",
        "| Dataset | Status | Action | Changes vs Local | Changes Since Last Check |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in results:
        lines.append(
            f"| {item['dataset_name']} | {item['status']} | {item['action']} | "
            f"{', '.join(item['changes_vs_local']) or 'none'} | "
            f"{', '.join(item['changes_since_last_check']) or 'none'} |"
        )
    return "\n".join(lines) + "\n"


# Find which expected local files are missing.
def find_missing_local_files(paths: dict[str, Path]) -> list[str]:
    missing = []
    for name, path in paths.items():
        if name != "source_metadata" and not path.exists():
            missing.append(name)
    return missing


# Write the update report as CSV.
def write_report_csv(path: Path, results: list[dict]) -> None:
    import csv

    fields = [
        "dataset_name",
        "resource_id",
        "status",
        "action",
        "changes_vs_local",
        "changes_since_last_check",
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
            row["changes_since_last_check"] = ",".join(
                item["changes_since_last_check"]
            )
            writer.writerow({field: row.get(field) for field in fields})


# Run a full update check for all datasets.
def run_update_check(
    datasets: list[dict],
    *,
    state_path: Path,
    report_json_path: Path | None = None,
    report_csv_path: Path | None = None,
    report_md_path: Path | None = None,
    paths_builder=output_paths,
    metadata_fetcher=fetch_dataset_metadata,
) -> dict:
    previous_state = read_json(state_path, {})
    previous_remote = previous_state.get("remote_state", {})
    results = []

    for dataset in datasets:
        paths = paths_builder(dataset[DATASET_NAME_KEY])
        local_metadata = read_json(paths["source_metadata"])
        previous_dataset_state = previous_remote.get(dataset[DATASET_NAME_KEY], {})
        results.append(
            check_dataset(
                dataset=dataset,
                previous_remote=previous_dataset_state,
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

    write_json(
        state_path,
        {
            "checked_at": checked_at,
            "remote_state": {
                item["dataset_name"]: item["remote_state"]
                for item in results
            },
        },
    )
    return payload

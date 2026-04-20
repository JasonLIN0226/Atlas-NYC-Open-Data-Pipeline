from datetime import UTC, datetime
from pathlib import Path

import build_lake
from nyc_atlas_core import DEFAULT_GEO_THRESHOLD, process_dataset_outputs
from nyc_open_data_utils import ROOT, UPDATE_DIR, load_datasets, utc_now, write_json
from nyc_refresh_core import refresh_changed_datasets, split_refresh_targets
from nyc_update_core import (
    run_update_check as run_core_update_check,
)


REFRESH_LOG = UPDATE_DIR / "latest_refresh_log.json"
CHANGE_DIR = UPDATE_DIR / "change_details"
REPORT_PATH = UPDATE_DIR / "latest_report.json"
REPORT_CSV = UPDATE_DIR / "latest_report.csv"
REPORT_MD = UPDATE_DIR / "latest_report.md"


# Run the full refresh flow for this project.
def main() -> None:
    datasets = load_datasets()
    report = run_update_check(datasets)
    pending, errored = split_refresh_targets(report)

    refresh_log = {
        "refreshed_at": utc_now(),
        "trigger_report": REPORT_PATH.relative_to(ROOT).as_posix(),
        "changed_datasets": [],
        "errored_datasets": errored,
        "lake_rebuilt": False,
    }

    for item in refresh_changed_datasets(datasets, report):
        refresh_log["changed_datasets"].append(refresh_dataset(item))

    if pending:
        run_update_check(datasets)

    build_lake.main()
    refresh_log["lake_rebuilt"] = True
    refresh_log["final_report"] = REPORT_PATH.relative_to(ROOT).as_posix()
    write_json(REFRESH_LOG, refresh_log)

    changed = len(refresh_log["changed_datasets"])
    message = (
        "No dataset updates detected. Datalake unchanged."
        if not changed
        else f"Refreshed {changed} dataset(s) and rebuilt the datalake."
    )
    print(message)


# Build the latest update report for this project.
def run_update_check(datasets: list[dict]) -> dict:
    UPDATE_DIR.mkdir(parents=True, exist_ok=True)
    payload = run_core_update_check(
        datasets,
        report_json_path=REPORT_PATH,
        report_csv_path=REPORT_CSV,
        report_md_path=REPORT_MD,
    )
    print(f"Checked {len(payload['datasets'])} datasets")
    print(f"Report JSON: {REPORT_PATH}")
    print(f"Report CSV:  {REPORT_CSV}")
    print(f"Report MD:   {REPORT_MD}")
    return payload


# Refresh one dataset for this project.
def refresh_dataset(source_item: dict) -> dict:
    dataset = source_item["dataset"]
    report_item = source_item["report_item"]
    paths = source_item["paths"]
    name = dataset["name"]
    remote_metadata = source_item["remote_metadata"]
    limit = source_item["csv_limit"]
    current_csv = source_item["current_csv"]
    before_metadata = source_item["before_metadata"]
    before_csv = source_item["before_csv"]
    detail = build_change_summary(
        name,
        report_item,
        before_metadata,
        remote_metadata,
        before_csv,
        current_csv,
    )

    if limit is not None:
        detail["data_change"]["current_csv"] = current_csv
        process_dataset_outputs(
            paths["raw_csv"],
            geo_threshold=DEFAULT_GEO_THRESHOLD,
            print_details=False,
        )

    artifacts = write_change_summary_file(name, detail)

    raw_data_path = paths["raw_csv"].relative_to(ROOT).as_posix()
    source_metadata_path = paths["source_metadata"].relative_to(ROOT).as_posix()
    final_metadata_path = paths["final_metadata"].relative_to(ROOT).as_posix()
    final_geo_results_path = paths["final_geo_results"].relative_to(ROOT).as_posix()

    return {
        "dataset_name": name,
        "resource_id": dataset["resource_id"],
        "reason": report_item["changes_vs_local"],
        "download_limit": limit,
        "raw_data_path": raw_data_path,
        "source_metadata_path": source_metadata_path,
        "final_metadata_path": final_metadata_path,
        "final_geo_results_path": final_geo_results_path,
        **artifacts,
        "change_detail": detail,
    }


# Build one compact change summary.
def build_change_summary(
    dataset_name: str,
    report_item: dict,
    before_metadata: dict | None,
    after_metadata: dict,
    before_csv: dict | None,
    after_csv: dict | None,
) -> dict:
    before_metadata = before_metadata or {}
    metadata_fields = [
        "name",
        "description",
        "category",
        "tags",
        "rowsUpdatedAt",
        "viewLastModified",
        "displayType",
        "assetType",
        "publicationDate",
        "publicationStage",
    ]
    changed_fields = [
        field
        for field in metadata_fields
        if before_metadata.get(field) != after_metadata.get(field)
    ]
    old_columns = {
        column.get("fieldName")
        for column in before_metadata.get("columns", [])
        if column.get("fieldName")
    }
    new_columns = {
        column.get("fieldName")
        for column in after_metadata.get("columns", [])
        if column.get("fieldName")
    }
    return {
        "dataset_name": dataset_name,
        "status": report_item["status"],
        "action": report_item["action"],
        "changes_vs_local": report_item["changes_vs_local"],
        "data_change": {
            "rows_updated_at_changed": before_metadata.get("rowsUpdatedAt") != after_metadata.get("rowsUpdatedAt"),
            "csv_hash_changed": (before_csv or {}).get("sha256") != (after_csv or {}).get("sha256"),
            "previous_csv": before_csv,
            "current_csv": after_csv,
        },
        "metadata_change": {
            "changed_fields": changed_fields,
            "added_columns": sorted(new_columns - old_columns),
            "removed_columns": sorted(old_columns - new_columns),
        },
    }


# Save one compact change summary.
def write_change_summary_file(dataset_name: str, detail: dict) -> dict:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    CHANGE_DIR.mkdir(parents=True, exist_ok=True)
    detail_json = CHANGE_DIR / f"{stamp}_{dataset_name}_change_summary.json"
    write_json(detail_json, detail)
    return {
        "change_summary_json": detail_json.relative_to(ROOT).as_posix(),
    }

if __name__ == "__main__":
    main()

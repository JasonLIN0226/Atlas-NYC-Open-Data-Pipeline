import hashlib
from datetime import UTC, datetime
from pathlib import Path

import build_lake
from nyc_update_core import (
    run_update_check as run_core_update_check,
)
from nyc_refresh_core import refresh_changed_datasets, split_refresh_targets
from nyc_open_data_utils import (
    ROOT,
    UPDATE_DIR,
    count_rows,
    load_datasets,
    utc_now,
    write_json,
)
from test import GEO_THRESHOLD, OUT_DIR, run_dataset


REFRESH_LOG = UPDATE_DIR / "latest_refresh_log.json"
CHANGE_DIR = UPDATE_DIR / "change_details"
HISTORY_DIR = UPDATE_DIR / "history"
REPORT_PATH = UPDATE_DIR / "latest_report.json"
REPORT_CSV = UPDATE_DIR / "latest_report.csv"
REPORT_MD = UPDATE_DIR / "latest_report.md"
STATE_PATH = UPDATE_DIR / "last_check_state.json"


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
        state_path=STATE_PATH,
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
    before_metadata = source_item["before_metadata"]
    before_csv = source_item["before_csv"]
    detail = build_change_detail(
        name,
        report_item,
        before_metadata,
        remote_metadata,
        before_csv,
        before_csv,
    )

    if limit is not None:
        csv_path = paths["raw_csv"]
        current_csv = file_stats(csv_path)
        detail["data_change"]["current_csv"] = current_csv
        detail["data_change"]["csv_hash_changed"] = (
            (before_csv or {}).get("sha256") != current_csv["sha256"]
        )
        profile_dataset(csv_path)

    artifacts = write_change_artifacts(name, before_metadata, remote_metadata, detail)

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


# Run Atlas for raw output and wrapped output.
def profile_dataset(path: Path) -> None:
    for use_wrapper in (False, True):
        run_dataset(
            str(path),
            use_wrapper=use_wrapper,
            geo_threshold=GEO_THRESHOLD,
            out_dir=OUT_DIR,
            print_details=False,
        )


# Build one change detail record.
def build_change_detail(
    dataset_name: str,
    report_item: dict,
    before_metadata: dict | None,
    after_metadata: dict,
    before_csv: dict | None,
    after_csv: dict | None,
) -> dict:
    before_metadata = before_metadata or {}
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
        "source_metadata_change": diff_source_metadata(before_metadata, after_metadata),
    }


# Diff source metadata before and after refresh.
def diff_source_metadata(before: dict, after: dict) -> dict:
    keys = [
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
    top_level = []
    for key in keys:
        if before.get(key) != after.get(key):
            top_level.append(
                {"field": key, "before": before.get(key), "after": after.get(key)}
            )

    old_columns = {col.get("fieldName"): col for col in before.get("columns", []) if col.get("fieldName")}
    new_columns = {col.get("fieldName"): col for col in after.get("columns", []) if col.get("fieldName")}
    changed = []
    shared_names = sorted(old_columns.keys() & new_columns.keys())

    for name in shared_names:
        edits = [
            {
                "field": field,
                "before": old_columns[name].get(field),
                "after": new_columns[name].get(field),
            }
            for field in ["name", "dataTypeName", "position", "description", "renderTypeName"]
            if old_columns[name].get(field) != new_columns[name].get(field)
        ]
        if edits:
            changed.append({"column": name, "changes": edits})
    return {
        "top_level_fields_changed": top_level,
        "schema": {
            "added_columns": sorted(new_columns.keys() - old_columns.keys()),
            "removed_columns": sorted(old_columns.keys() - new_columns.keys()),
            "changed_columns": changed,
        },
    }


# Save refresh snapshots and reports.
def write_change_artifacts(dataset_name: str, before: dict | None, after: dict, detail: dict) -> dict:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    history_dir = HISTORY_DIR / dataset_name
    history_dir.mkdir(parents=True, exist_ok=True)
    CHANGE_DIR.mkdir(parents=True, exist_ok=True)
    before_path = after_path = None
    if before:
        before_path = history_dir / f"{stamp}_before_source_metadata.json"
        write_json(before_path, before)
    after_path = history_dir / f"{stamp}_after_source_metadata.json"
    detail_json = CHANGE_DIR / f"{stamp}_{dataset_name}.json"
    detail_md = CHANGE_DIR / f"{stamp}_{dataset_name}.md"
    write_json(after_path, after)
    write_json(detail_json, detail)
    detail_md.write_text(build_change_markdown(detail), encoding="utf-8")
    return {
        "detail_report_json": detail_json.relative_to(ROOT).as_posix(),
        "detail_report_md": detail_md.relative_to(ROOT).as_posix(),
        "before_metadata_snapshot": before_path.relative_to(ROOT).as_posix() if before_path else None,
        "after_metadata_snapshot": after_path.relative_to(ROOT).as_posix(),
    }


# Build a Markdown summary for one refresh.
def build_change_markdown(detail: dict) -> str:
    schema = detail["source_metadata_change"]["schema"]
    data_change = detail["data_change"]
    lines = [
        f"# Update Detail: {detail['dataset_name']}",
        "",
        f"- Status: `{detail['status']}`",
        f"- Action: `{detail['action']}`",
        f"- Change triggers: `{', '.join(detail['changes_vs_local']) or 'none'}`",
        "",
        "## Data Change",
        "",
        f"- `rowsUpdatedAt` changed: `{data_change['rows_updated_at_changed']}`",
        f"- CSV hash changed: `{data_change['csv_hash_changed']}`",
        f"- Previous CSV rows: `{(data_change['previous_csv'] or {}).get('row_count')}`",
        f"- Current CSV rows: `{(data_change['current_csv'] or {}).get('row_count')}`",
        "",
        "## Source Metadata Change",
        "",
    ]
    top_level = detail["source_metadata_change"]["top_level_fields_changed"]
    if top_level:
        lines += ["### Top-level Fields", ""] + [
            f"- `{item['field']}`: `{short(item['before'])}` -> `{short(item['after'])}`" for item in top_level
        ] + [""]
    else:
        lines += ["- No top-level metadata field changes detected.", ""]
    lines += [
        "### Schema",
        "",
        f"- Added columns: `{', '.join(schema['added_columns']) or 'none'}`",
        f"- Removed columns: `{', '.join(schema['removed_columns']) or 'none'}`",
    ]
    if schema["changed_columns"]:
        lines.append("- Changed columns:")
        for column in schema["changed_columns"]:
            parts = ", ".join(
                f"{item['field']}: {short(item['before'])} -> {short(item['after'])}"
                for item in column["changes"]
            )
            lines.append(f"  - `{column['column']}`: {parts}")
    else:
        lines.append("- Changed columns: `none`")
    return "\n".join(lines) + "\n"


# Shorten long values for reports.
def short(value) -> str:
    text = "null" if value is None else str(value).replace("\n", " ").strip()
    return text if len(text) <= 120 else text[:117] + "..."


# Build a small fingerprint for one file.
def file_stats(path: Path) -> dict | None:
    if not path.exists():
        return None
    raw = path.read_bytes()
    return {"size_bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(), "row_count": count_rows(path)}


if __name__ == "__main__":
    main()

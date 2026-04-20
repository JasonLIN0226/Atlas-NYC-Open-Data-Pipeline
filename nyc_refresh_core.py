import hashlib
from pathlib import Path

from nyc_open_data_utils import count_rows, fetch_dataset_metadata, fetch_url, output_paths, read_json, write_json


# Settings to adjust when reusing this source refresh logic in another NYC Open Data project.
DEFAULT_LIMIT = 5000
NYC_OPEN_DATA_BASE_URL = "https://data.cityofnewyork.us"
CSV_RESOURCE_PATH = "/resource/{resource_id}.csv?$limit={limit}"
METADATA_TIMEOUT_SECONDS = 20
CSV_TIMEOUT_SECONDS = 60

# Expected path keys passed in from the project adapter.
SOURCE_METADATA_PATH_KEY = "source_metadata"
RAW_CSV_PATH_KEY = "raw_csv"


# Reuse the current row count for refresh size.
def infer_csv_limit(path: Path, default_limit: int = DEFAULT_LIMIT) -> int:
    return count_rows(path) or default_limit


# Download CSV data for one dataset.
def fetch_dataset_csv(resource_id: str, limit: int, timeout: int = CSV_TIMEOUT_SECONDS) -> bytes:
    url = NYC_OPEN_DATA_BASE_URL + CSV_RESOURCE_PATH.format(
        resource_id=resource_id,
        limit=limit,
    )
    return fetch_url(url, timeout)


# Build a small fingerprint for one file.
def file_stats(path: Path) -> dict | None:
    if not path.exists():
        return None
    raw = path.read_bytes()
    return {
        "size_bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "row_count": count_rows(path),
    }


# Refresh source metadata and raw CSV files.
def refresh_source_assets(
    *,
    dataset: dict,
    report_item: dict,
    paths: dict,
    default_limit: int = DEFAULT_LIMIT,
) -> dict:
    before_metadata = read_json(paths[SOURCE_METADATA_PATH_KEY])
    before_csv = file_stats(paths[RAW_CSV_PATH_KEY])
    remote_metadata = fetch_dataset_metadata(
        dataset["resource_id"],
        timeout=METADATA_TIMEOUT_SECONDS,
    )
    write_json(paths[SOURCE_METADATA_PATH_KEY], remote_metadata)

    csv_limit = None
    if report_item["action"] == "refresh_raw_data":
        csv_limit = infer_csv_limit(paths[RAW_CSV_PATH_KEY], default_limit)
        csv_bytes = fetch_dataset_csv(remote_metadata["_resolved_view_id"], csv_limit)
        paths[RAW_CSV_PATH_KEY].write_bytes(csv_bytes)

    current_csv = file_stats(paths[RAW_CSV_PATH_KEY])

    return {
        "paths": paths,
        "before_metadata": before_metadata,
        "before_csv": before_csv,
        "remote_metadata": remote_metadata,
        "csv_limit": csv_limit,
        "current_csv": current_csv,
    }


# Split one report into pending and errored datasets.
def split_refresh_targets(report: dict) -> tuple[list[dict], list[str]]:
    pending = []
    errored = []
    for item in report.get("datasets", []):
        status = item.get("status")
        if status == "error":
            errored.append(item["dataset_name"])
        elif item.get("needs_refresh"):
            pending.append(item)
    return pending, errored


# Refresh source data for all changed datasets.
def refresh_changed_datasets(
    datasets: list[dict],
    report: dict,
    *,
    paths_builder=output_paths,
    default_limit: int = DEFAULT_LIMIT,
) -> list[dict]:
    dataset_lookup = {item["name"]: item for item in datasets}
    pending, _ = split_refresh_targets(report)
    refreshed = []

    for item in pending:
        dataset = dataset_lookup[item["dataset_name"]]
        paths = paths_builder(dataset["name"])
        source_refresh = refresh_source_assets(
            dataset=dataset,
            report_item=item,
            paths=paths,
            default_limit=default_limit,
        )
        refreshed.append(
            {
                "dataset": dataset,
                "report_item": item,
                **source_refresh,
            }
        )

    return refreshed

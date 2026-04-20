import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parent
DATASETS_PATH = ROOT / "nyc_open_data_datasets.json"
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"
SOURCE_METADATA_DIR = ROOT / "source_metadata" / "nyc_open_data"
UPDATE_DIR = ROOT / "update_checks" / "nyc_open_data"


# Read JSON from disk.
def read_json(path: Path, default=None):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


# Write JSON to disk.
def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


# Return the current UTC time string.
def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


# Convert an epoch value to a UTC time string.
def format_epoch(value) -> str | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value), UTC).replace(microsecond=0).isoformat()
    except (TypeError, ValueError, OSError):
        return str(value)


# Count data rows in a CSV file.
def count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


# Return a repo relative path string.
def rel_repo_path(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


# Load the dataset config list.
def load_datasets() -> list[dict]:
    return read_json(DATASETS_PATH, [])

# Return all local file paths for one dataset.
def output_paths(dataset_name: str) -> dict[str, Path]:
    return {
        "raw_csv": DATA_DIR / f"{dataset_name}.csv",
        "source_metadata": SOURCE_METADATA_DIR / f"{dataset_name}.json",
        "final_metadata": OUTPUT_DIR / f"metadata_{dataset_name}_wrapped.json",
        "final_geo_results": OUTPUT_DIR / f"geo_classifier_results_{dataset_name}_wrapped.csv",
        "raw_metadata": OUTPUT_DIR / f"metadata_{dataset_name}_raw.json",
        "raw_geo_results": OUTPUT_DIR / f"geo_classifier_results_{dataset_name}_raw.csv",
    }


# Fetch raw bytes from a URL.
def fetch_url(url: str, timeout: int) -> bytes:
    try:
        with urlopen(url, timeout=timeout) as response:
            return response.read()
    except HTTPError as exc:
        raise RuntimeError(f"{url}: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"{url}: {exc.reason}") from exc


# Fetch JSON from a URL.
def fetch_json(url: str, timeout: int) -> dict:
    return json.loads(fetch_url(url, timeout).decode("utf-8"))

# Fetch source metadata for one NYC Open Data dataset.
def fetch_dataset_metadata(resource_id: str, timeout: int = 20) -> dict:
    metadata = fetch_json(f"https://data.cityofnewyork.us/api/views/{resource_id}", timeout)
    resolved_id = metadata.get("modifyingViewUid")
    if resolved_id:
        metadata = fetch_json(f"https://data.cityofnewyork.us/api/views/{resolved_id}", timeout)
    metadata["_requested_view_id"] = resource_id
    metadata["_resolved_view_id"] = resolved_id or resource_id
    return metadata

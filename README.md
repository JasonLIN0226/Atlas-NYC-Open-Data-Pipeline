# Atlas NYC Open Data Pipeline

This project downloads NYC Open Data datasets then runs Atlas plus a wrapper layer then builds a local static data lake.

If you only want to use this project you mainly need:

- `python refresh_nyc_datalake.py`
- `open lake/site/index.html`

You can ignore the core files unless you want to reuse the update logic in another project.

## Quick Start

### 1. Open the web UI

```bash
open lake/site/index.html
```

This opens the local data lake in your browser.

### 2. Update everything that needs updating

```bash
python refresh_nyc_datalake.py
```

This command:

1. checks all configured NYC Open Data datasets
2. refreshes only the datasets that changed
3. reruns Atlas plus the wrapper through `nyc_atlas_core.py` when needed
4. rebuilds the web data lake

You do not need to run `build_lake.py` after this.

### 3. Add a new dataset

Add one item to `nyc_open_data_datasets.json`:

```json
{"name": "my_dataset", "resource_id": "abcd-1234"}
```

Then run:

```bash
python refresh_nyc_datalake.py
```

The new dataset will be downloaded then profiled then added to the lake.

## The Main Commands

### Normal update command

```bash
python refresh_nyc_datalake.py
```

Use this for normal day to day work.

### Rebuild the web UI only

```bash
python build_lake.py
```

Use this when:

- local data already exists
- output files already exist
- you only want to rebuild the static site

### Run Atlas manually

```bash
python test.py
```

At the top of `test.py` you can change:

- `RUN_ALL_DATASETS`
- `GEO_THRESHOLD`
- `DATA_PATH`

## What You Will See In The Web UI

Homepage:

- dataset cards
- latest update report
- latest refresh log

Dataset detail page:

- basic dataset info
- sample rows
- dataset metadata
- column metadata
- type analysis
- links to final files

Main entry file:

- `lake/site/index.html`

## Main Folders

- `data/`
  - raw CSV files
- `source_metadata/nyc_open_data/`
  - source metadata from NYC Open Data
- `output/`
  - Atlas raw and wrapped outputs
- `lake/`
  - static web data lake
- `update_checks/nyc_open_data/`
  - update reports and refresh logs

## Main Files

- `refresh_nyc_datalake.py`
  - main project entry point
- `build_lake.py`
  - rebuilds the web UI
- `test.py`
  - runs `nyc_atlas_core.py` manually
- `nyc_atlas_core.py`
  - runs Atlas plus the wrapper and writes output files
- `atlas_wrapper.py`
  - improves final Atlas labels
- `nyc_open_data_datasets.json`
  - list of tracked datasets
- `nyc_update_core.py`
  - reusable update check core
- `nyc_refresh_core.py`
  - reusable source refresh core

## How This Project Updates Data

This project uses four steps:

1. `nyc_update_core.py`
   - checks what changed
2. `nyc_refresh_core.py`
   - updates local source metadata and local raw CSV files
3. `nyc_atlas_core.py`
   - reruns Atlas plus wrapper
   - writes raw and wrapped output files
4. `refresh_nyc_datalake.py`
   - calls the update core
   - calls the refresh core
   - calls the Atlas core
   - writes refresh artifacts
   - rebuilds the web data lake

The main update cases are:

- `unchanged`
  - nothing important changed
- `metadata_changed`
  - only source metadata changed
- `raw_data_changed`
  - raw rows changed
  - schema changed
  - or local source files are missing
  - this also covers a newly added dataset with no local files yet
- `error`
  - the remote metadata check failed

## Final Output

For each dataset the main final files are:

- `data/<dataset>.csv`
- `source_metadata/nyc_open_data/<dataset>.json`
- `output/metadata_<dataset>_wrapped.json`
- `output/geo_classifier_results_<dataset>_wrapped.csv`
- `lake/site/tables/<dataset>.html`

## Atlas Processing Core

`nyc_atlas_core.py` is the only file that runs Atlas plus the wrapper and writes results into `output/`.

It writes these files for each dataset:

- `output/metadata_<dataset>_raw.json`
- `output/geo_classifier_results_<dataset>_raw.csv`
- `output/metadata_<dataset>_wrapped.json`
- `output/geo_classifier_results_<dataset>_wrapped.csv`

You can use it in two ways:

- manual use through `python test.py`
- automatic use through `python refresh_nyc_datalake.py`

## Advanced Use

You only need this section if you want to reuse the NYC Open Data update logic in another project.

### `nyc_update_core.py`

Use this file when you want to answer:

**What changed and what should happen next**

Main function:

```python
run_update_check(...)
```

What it does:

- reads local source metadata
- fetches remote NYC Open Data metadata
- compares local and remote state
- decides `status`
- decides `action`
- builds a full update report

Important settings at the top of the file:

| Setting | Current value | Used for |
| --- | --- | --- |
| `DATASET_NAME_KEY` | `"name"` | dataset name key in the config |
| `DATASET_RESOURCE_ID_KEY` | `"resource_id"` | dataset id key in the config |
| `SOURCE_COLUMN_FIELDS` | `("fieldName", "name", "dataTypeName", "position", "description")` | schema comparison fields |
| `RAW_DATA_CHANGE_FIELDS` | `("rows_updated_at", "columns")` | fields used to detect raw data changes |
| `METADATA_CHANGE_FIELDS` | `("title", "description", "category", "tags", "view_last_modified")` | fields used to detect metadata only changes |

Example:

```python
from pathlib import Path
from nyc_update_core import run_update_check

datasets = [
    {"name": "nyc311", "resource_id": "76ig-c548"},
]

report = run_update_check(
    datasets,
    report_json_path=Path("latest_report.json"),
    report_csv_path=Path("latest_report.csv"),
    report_md_path=Path("latest_report.md"),
)
```

The result looks like:

```python
{
    "checked_at": "...",
    "summary": {
        "unchanged": 1,
    },
    "datasets": [
        {
            "dataset_name": "nyc311",
            "resource_id": "76ig-c548",
            "status": "unchanged",
            "action": "no_action",
            "needs_refresh": False,
            "is_new_dataset": False,
            "changes_vs_local": [],
            "missing_local_files": [],
        }
    ],
}
```

Possible `status` values:

- `unchanged`
- `metadata_changed`
- `raw_data_changed`
- `error`

Possible `action` values:

- `no_action`
- `refresh_metadata`
- `refresh_raw_data`
- `retry_check`

If you add a new dataset to `nyc_open_data_datasets.json` and there are no local files yet then:

- `status` will be `raw_data_changed`
- `action` will be `refresh_raw_data`
- `is_new_dataset` will be `True`
- `changes_vs_local` will include `new_dataset`

### `nyc_refresh_core.py`

Use this file when you want to answer:

**How do I update the local source files for the datasets that changed**

Main functions:

```python
refresh_source_assets(...)
split_refresh_targets(...)
refresh_changed_datasets(...)
```

What it does:

- finds which datasets still need work
- separates failed checks from refreshable datasets
- updates local source metadata files
- updates local raw CSV files when needed
- returns structured refresh results

Where the path keys come from:

- `refresh_changed_datasets(...)` uses a `paths_builder`
- by default it uses `output_paths(...)` from `nyc_open_data_utils.py`
- that function returns a dictionary with keys like:
  - `raw_csv`
  - `source_metadata`
  - `final_metadata`
  - `final_geo_results`
  - `raw_metadata`
  - `raw_geo_results`
- inside `nyc_refresh_core.py` only these two keys are required:
  - `raw_csv`
  - `source_metadata`

So the path key settings at the top of `nyc_refresh_core.py` must match the keys returned by your `paths_builder`.

Important settings at the top of the file:

| Setting | Current value | Used for |
| --- | --- | --- |
| `DEFAULT_LIMIT` | `5000` | default CSV row limit for new datasets |
| `NYC_OPEN_DATA_BASE_URL` | `"https://data.cityofnewyork.us"` | base URL for NYC Open Data |
| `CSV_RESOURCE_PATH` | `"/resource/{resource_id}.csv?$limit={limit}"` | CSV download URL template |
| `METADATA_TIMEOUT_SECONDS` | `20` | timeout for source metadata requests |
| `CSV_TIMEOUT_SECONDS` | `60` | timeout for CSV downloads |
| `SOURCE_METADATA_PATH_KEY` | `"source_metadata"` | key used for the local source metadata path |
| `RAW_CSV_PATH_KEY` | `"raw_csv"` | key used for the local raw CSV path |

Example:

```python
from nyc_refresh_core import refresh_changed_datasets

refreshed = refresh_changed_datasets(datasets, report)
```

This works because the default `paths_builder` already returns:

```python
{
    "raw_csv": Path("data/nyc311.csv"),
    "source_metadata": Path("source_metadata/nyc_open_data/nyc311.json"),
    ...
}
```

If another project uses different local folders then it should pass its own `paths_builder`.

Example:

```python
from pathlib import Path

from nyc_refresh_core import refresh_changed_datasets

def my_paths_builder(dataset_name: str) -> dict:
    return {
        "raw_csv": Path("raw") / f"{dataset_name}.csv",
        "source_metadata": Path("metadata") / f"{dataset_name}.json",
    }

refreshed = refresh_changed_datasets(
    datasets,
    report,
    paths_builder=my_paths_builder,
)
```

Each item in `refreshed` looks like:

```python
{
    "dataset": {"name": "nyc311", "resource_id": "76ig-c548"},
    "report_item": {...},
    "paths": {
        "raw_csv": Path("data/nyc311.csv"),
        "source_metadata": Path("source_metadata/nyc_open_data/nyc311.json"),
        ...
    },
    "before_metadata": {...},
    "before_csv": {
        "size_bytes": 123456,
        "sha256": "...",
        "row_count": 5000,
    },
    "remote_metadata": {...},
    "csv_limit": 5000,
    "current_csv": {
        "size_bytes": 123999,
        "sha256": "...",
        "row_count": 5000,
    },
}
```

If the dataset only had a metadata change:

- `remote_metadata` is updated
- `csv_limit` is `None`
- the local raw CSV is not downloaded again

If the dataset had a data or schema change:

- `remote_metadata` is updated
- the local raw CSV is overwritten with the new download
- `current_csv` describes the new local file

### Reuse Pattern

Another project can reuse the two cores like this:

```python
from nyc_update_core import run_update_check
from nyc_refresh_core import refresh_changed_datasets

datasets = [
    {"name": "nyc311", "resource_id": "76ig-c548"},
]

report = run_update_check(
    datasets,
)

refreshed = refresh_changed_datasets(datasets, report)

for item in refreshed:
    # Run your own next step here.
    pass
```

So the split is:

- `nyc_update_core.py`
  - decide what changed
- `nyc_refresh_core.py`
  - update local source files
- your own code
  - do whatever should happen after the source files are updated

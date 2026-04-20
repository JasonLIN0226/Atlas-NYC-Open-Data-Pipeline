# Atlas NYC Open Data Pipeline

This project does four things:

1. downloads NYC Open Data datasets
2. stores source metadata and raw CSV files
3. runs Atlas plus a wrapper layer
4. builds a local static data lake

## Main Folders

- `data/`
  - raw CSV files
- `source_metadata/nyc_open_data/`
  - source metadata from NYC Open Data
- `output/`
  - final Atlas outputs
- `lake/`
  - the local static data lake site
- `update_checks/nyc_open_data/`
  - update reports and refresh logs

## Main Files

- `nyc_open_data_datasets.json`
  - list of datasets to track
- `nyc_update_core.py`
  - reusable update check core
- `nyc_refresh_core.py`
  - reusable source refresh core
- `refresh_nyc_datalake.py`
  - project adapter for this data lake
- `test.py`
  - Atlas runner
- `atlas_wrapper.py`
  - post processing layer for Atlas
- `build_lake.py`
  - static site builder

## How To Open The Web UI

Open this file in your browser:

`lake/site/index.html`

Or run:

```bash
open lake/site/index.html
```

The homepage shows:

- dataset cards
- latest update report
- latest refresh log

Each dataset page shows:

- basic dataset info
- sample rows
- dataset metadata
- column metadata
- analysis tables and plots
- links to the final output files

## How To Update Data

Run:

```bash
python refresh_nyc_datalake.py
```

This is the normal command for this project.

It will:

1. check all configured NYC Open Data datasets
2. detect which datasets changed
3. refresh only the datasets that need work
4. rerun Atlas and the wrapper when needed
5. rebuild the data lake

You do not need to run `build_lake.py` after this.

The main update cases are:

- `unchanged`
  - nothing important changed
- `metadata_changed`
  - source metadata changed
  - the pipeline updates source metadata and rebuilds the lake
- `data_changed`
  - table rows changed
  - the pipeline refreshes CSV files then reruns Atlas and rebuilds the lake
- `schema_changed`
  - table columns changed
  - the pipeline refreshes CSV files then reruns Atlas and rebuilds the lake
- `missing_local_files`
  - local source files or final outputs are missing
  - the pipeline rebuilds the missing parts

## How This Data Lake Updates

This project uses three layers:

- `nyc_update_core.py`
  - checks remote NYC Open Data metadata
  - decides what changed
  - writes the update report

- `nyc_refresh_core.py`
  - refreshes source metadata
  - refreshes raw CSV files when needed
  - selects which datasets need source refresh

- `refresh_nyc_datalake.py`
  - uses the two cores above
  - reruns Atlas and the wrapper for changed datasets
  - writes change artifacts
  - rebuilds the web data lake

So the full project flow is:

1. check updates
2. refresh source data
3. rerun Atlas plus wrapper
4. rebuild the web data lake

## How To Use The Two Cores In Another Project

The two core files are meant for a project that also uses NYC Open Data but does not need this exact Atlas data lake.

Use them in this order:

1. `nyc_update_core.py`
2. `nyc_refresh_core.py`
3. your own project code

### `nyc_update_core.py`

This file answers one question:

**What changed and what should happen next**

It does these jobs:

- reads local source metadata
- fetches the latest remote NYC Open Data metadata
- compares local and remote state
- decides the dataset status
- decides the next action
- builds one full update report

#### Main function

```python
run_update_check(...)
```

#### What you can change

At the top of `nyc_update_core.py` you can change:

- `DATASET_NAME_KEY`
- `DATASET_RESOURCE_ID_KEY`
- `SOURCE_COLUMN_FIELDS`
- `TIMESTAMP_CHANGE_FIELDS`
- `SOURCE_METADATA_FIELDS`

These control:

- which keys are used in the dataset config
- which column fields are used for schema comparison
- which metadata fields count as source metadata changes
- which remote fields count as data or schema changes

#### Main inputs

```python
from pathlib import Path
from nyc_update_core import run_update_check

datasets = [
    {"name": "nyc311", "resource_id": "76ig-c548"},
]

report = run_update_check(
    datasets,
    state_path=Path("state.json"),
    report_json_path=Path("latest_report.json"),
    report_csv_path=Path("latest_report.csv"),
    report_md_path=Path("latest_report.md"),
)
```

#### Main output

It returns one report dictionary with:

- `checked_at`
- `summary`
- `datasets`

The `summary` looks like:

```python
{
    "unchanged": 9,
    "metadata_changed": 1,
    "missing_local_files": 1,
}
```

Each item in `datasets` looks like:

```python
{
    "dataset_name": "nyc311",
    "resource_id": "76ig-c548",
    "status": "unchanged",
    "action": "no_action",
    "changes_vs_local": [],
    "changes_since_last_check": [],
    "missing_local_files": [],
}
```

Possible `status` values:

- `unchanged`
- `metadata_changed`
- `data_changed`
- `schema_changed`
- `missing_local_files`
- `error`

Possible `action` values:

- `no_action`
- `refresh_source_metadata_and_rebuild_lake`
- `refresh_data_and_reprofile`
- `retry_check`

### `nyc_refresh_core.py`

This file answers one question:

**How do I refresh the source files for the datasets that changed**

It does these jobs:

- finds which datasets in the report still need work
- separates failed checks from datasets that can be refreshed
- refreshes source metadata
- refreshes raw CSV files when the action requires it
- returns structured refresh results for your project code

#### Main functions

```python
split_refresh_targets(...)
refresh_changed_datasets(...)
```

#### What you can change

At the top of `nyc_refresh_core.py` you can change:

- `DEFAULT_LIMIT`
- `NYC_OPEN_DATA_BASE_URL`
- `CSV_RESOURCE_PATH`
- `METADATA_TIMEOUT_SECONDS`
- `CSV_TIMEOUT_SECONDS`
- `SOURCE_METADATA_PATH_KEY`
- `RAW_CSV_PATH_KEY`

These control:

- how many rows to fetch by default
- where the NYC Open Data API is
- how CSV download URLs are built
- request timeout values
- which keys must exist in the `paths` dictionary

#### Main inputs

```python
from nyc_refresh_core import refresh_changed_datasets

refreshed = refresh_changed_datasets(datasets, report)
```

This expects:

- `datasets`
  - your dataset config list
- `report`
  - the report returned by `run_update_check(...)`

#### Main output

It returns one item per dataset that was actually refreshed.

Each item looks like:

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
}
```

If the dataset only had a metadata change then:

- `remote_metadata` will still be updated
- `csv_limit` will be `None`
- the raw CSV will not be downloaded again

### Simple Reuse Pattern

A different project can reuse the two cores like this:

```python
from pathlib import Path

from nyc_update_core import run_update_check
from nyc_refresh_core import refresh_changed_datasets

datasets = [
    {"name": "nyc311", "resource_id": "76ig-c548"},
]

report = run_update_check(
    datasets,
    state_path=Path("state.json"),
)

refreshed = refresh_changed_datasets(datasets, report)

for item in refreshed:
    # Do your own project step here.
    # Example:
    # - run another profiler
    # - update a database
    # - rebuild a website
    pass
```

So the split is:

- `nyc_update_core.py`
  - decide what changed
- `nyc_refresh_core.py`
  - refresh local source files
- your own code
  - do whatever should happen after the source files are updated

## How To Use These Cores In This Project

This project does not call the cores by hand.

Instead it uses:

```bash
python refresh_nyc_datalake.py
```

That file already:

- calls `nyc_update_core.py`
- calls `nyc_refresh_core.py`
- reruns Atlas
- applies the wrapper
- rebuilds `lake/`

## Rebuild The Lake Only

Run:

```bash
python build_lake.py
```

Use this when:

- local data already exists
- output files already exist
- you only want to rebuild the static site

## Run Atlas Manually

Run:

```bash
python test.py
```

At the top of `test.py` you can change:

- `RUN_ALL_DATASETS`
- `USE_WRAPPER`
- `GEO_THRESHOLD`
- `DATA_PATH`

This writes Atlas outputs to `output/`.

## Add A New Dataset

Add one item to `nyc_open_data_datasets.json`:

```json
{"name": "my_dataset", "resource_id": "abcd-1234"}
```

Then run:

```bash
python refresh_nyc_datalake.py
```

The pipeline will:

1. fetch source metadata
2. download the CSV
3. run Atlas
4. apply the wrapper
5. rebuild the data lake

## Final Output

For each dataset the main final files are:

- `data/<dataset>.csv`
- `source_metadata/nyc_open_data/<dataset>.json`
- `output/metadata_<dataset>_wrapped.json`
- `output/geo_classifier_results_<dataset>_wrapped.csv`
- `lake/site/tables/<dataset>.html`

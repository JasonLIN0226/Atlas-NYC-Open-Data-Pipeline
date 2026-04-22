# Atlas NYC Open Data Pipeline

This project:

- downloads selected NYC Open Data datasets
- stores NYC source metadata
- runs Atlas plus the wrapper
- builds a local static data lake

## Quick Start

Update everything:

```bash
python refresh_nyc_datalake.py
```

Open the web UI:

```bash
open lake/site/index.html
```

## Main Commands

Full update:

```bash
python refresh_nyc_datalake.py
```

This command:

1. checks NYC Open Data for changes
2. refreshes only changed datasets
3. reruns Atlas plus the wrapper when needed
4. rebuilds the lake

Rebuild the lake only:

```bash
python build_lake.py
```

Run Atlas manually:

```bash
python test.py
```

## Add A New Dataset

Add one item to `nyc_open_data_datasets.json`:

```json
{"name": "my_dataset", "resource_id": "abcd-1234"}
```

Then run:

```bash
python refresh_nyc_datalake.py
```

## Web UI

Main page:

- `lake/site/index.html`

Homepage includes:

- dataset cards
- latest update info
- month based time range search

Detail page includes:

- basic dataset info
- sample rows
- dataset metadata
- column metadata
- type analysis
- links to final files

## Time Range Search

The homepage search is month based.

A dataset matches if it has at least one covered month inside the selected date range.

The search uses saved metadata only. It does not rescan raw CSV files while you search.

Temporal metadata is stored in:

- `lake/profiles/<dataset>.json`
- `lake/catalog/datasets.json`
- `lake/catalog/datasets.csv`

Each temporal summary includes:

- `has_temporal_data`
- `temporal_columns`
- `temporal_start`
- `temporal_end`
- `month_coverage`

## Important Folders

- `data/`
  - raw CSV files
- `source_metadata/nyc_open_data/`
  - source metadata from NYC Open Data
- `output/`
  - Atlas raw and wrapped outputs
- `lake/`
  - static data lake
- `lake/profiles/`
  - metadata only dataset profiles
- `update_checks/nyc_open_data/`
  - update reports and refresh logs

## Important Files

- `refresh_nyc_datalake.py`
  - main project entry point
- `build_lake.py`
  - builds profiles and the static lake
- `nyc_atlas_core.py`
  - runs Atlas plus the wrapper
- `atlas_wrapper.py`
  - improves final Atlas labels
- `nyc_open_data_datasets.json`
  - list of tracked datasets

## Core Flow

- `nyc_update_core.py`
  - checks what changed
- `nyc_refresh_core.py`
  - refreshes source metadata and raw CSV files
- `nyc_atlas_core.py`
  - generates Atlas raw and wrapped outputs
- `build_lake.py`
  - builds profiles catalog and web pages

## Main Output Per Dataset

- `data/<dataset>.csv`
- `source_metadata/nyc_open_data/<dataset>.json`
- `output/metadata_<dataset>_wrapped.json`
- `output/geo_classifier_results_<dataset>_wrapped.csv`
- `lake/profiles/<dataset>.json`
- `lake/site/tables/<dataset>.html`

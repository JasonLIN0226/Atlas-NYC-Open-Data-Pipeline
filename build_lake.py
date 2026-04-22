import csv
import json
import os
import shutil
from datetime import datetime
from html import escape
from pathlib import Path

from nyc_temporal_core import build_temporal_metadata
from nyc_open_data_utils import (
    DATA_DIR,
    ROOT,
    UPDATE_DIR as UPDATE_CHECK_DIR,
    count_rows,
    output_paths,
    read_json as load_json,
    rel_repo_path,
    write_json,
)
LATEST_UPDATE_REPORT = UPDATE_CHECK_DIR / "latest_report.json"
LATEST_REFRESH_LOG = UPDATE_CHECK_DIR / "latest_refresh_log.json"
LAKE_DIR = ROOT / "lake"
CATALOG_DIR = LAKE_DIR / "catalog"
PROFILES_DIR = LAKE_DIR / "profiles"
TABLES_DIR = LAKE_DIR / "tables"
SITE_DIR = LAKE_DIR / "site"
SITE_TABLES_DIR = SITE_DIR / "tables"

SAMPLE_ROWS = 10
MISSING = "missing"
NON_SPATIAL = "non_spatial"
NO_UPDATE_REPORT = "No update report available."
NO_REFRESH_LOG = "No refresh log available."
NO_DATASET_METADATA = "No dataset-level metadata."
NO_COLUMN_METADATA = "No column metadata."
NO_CONTENT_SUMMARY = "No geo/profile summary available"
RAW_CSV_LABEL = "Raw CSV"
FINAL_METADATA_LABEL = "Final metadata JSON"
FINAL_GEO_RESULTS_LABEL = "Final geo results CSV"
SUMMARY_JSON_LABEL = "Summary JSON"


# Rebuild the static lake site.
def main() -> None:
    reset_lake_dirs()
    profile_paths = []
    for data_path in sorted(DATA_DIR.glob("*.csv")):
        dataset_name = data_path.stem
        profile_paths.append(build_dataset_profile(dataset_name, data_path))

    datasets = []
    for profile_path in sorted(profile_paths):
        profile = load_json(profile_path, {})
        if not profile:
            continue
        datasets.append(build_dataset_entry(profile))

    write_catalog(datasets)
    write_site(datasets)

    print(f"Built lake for {len(datasets)} datasets at {LAKE_DIR}")


# Reset the generated lake folders.
def reset_lake_dirs() -> None:
    if LAKE_DIR.exists():
        shutil.rmtree(LAKE_DIR)
    for path in [CATALOG_DIR, PROFILES_DIR, TABLES_DIR, SITE_TABLES_DIR]:
        path.mkdir(parents=True, exist_ok=True)


# Build one metadata only profile for one dataset.
def build_dataset_profile(dataset_name: str, data_path: Path) -> Path:
    paths = output_paths(dataset_name)
    source_metadata_path = paths["source_metadata"]
    raw_metadata_path = paths["raw_metadata"]
    wrapped_metadata_path = paths["final_metadata"]

    source_metadata = load_json(source_metadata_path)
    raw_metadata = load_json(raw_metadata_path)
    wrapped_metadata = load_json(wrapped_metadata_path)
    chosen_metadata = wrapped_metadata or raw_metadata or {}
    header = read_csv_header(data_path)
    wrapped_geo_columns = extract_geo_columns(wrapped_metadata)
    combined_columns = combined_column_metadata_rows(source_metadata, chosen_metadata, header)
    temporal_metadata = build_temporal_metadata(data_path, chosen_metadata)

    profile = {
        "dataset_name": dataset_name,
        "source_metadata": source_profile(source_metadata),
        "atlas_metadata": atlas_profile(chosen_metadata, wrapped_metadata, wrapped_geo_columns, combined_columns),
        "temporal_metadata": temporal_metadata,
    }

    profile_path = PROFILES_DIR / f"{dataset_name}.json"
    write_json(profile_path, profile)
    return profile_path


# Build one lake entry from one saved profile.
def build_dataset_entry(profile: dict) -> dict:
    dataset_name = profile["dataset_name"]
    data_path = DATA_DIR / f"{dataset_name}.csv"
    paths = output_paths(dataset_name)
    header, sample_rows = read_sample_rows(data_path, SAMPLE_ROWS)
    table_dir = TABLES_DIR / dataset_name
    table_dir.mkdir(parents=True, exist_ok=True)

    sample_path = table_dir / "sample.csv"
    write_sample_csv(sample_path, header, sample_rows)

    source_meta = profile.get("source_metadata", {})
    atlas_meta = profile.get("atlas_metadata", {})
    temporal_meta = profile.get("temporal_metadata", {})

    row_count = atlas_meta.get("row_count", count_rows(data_path))
    column_count = atlas_meta.get("column_count", len(header))
    summary = {
        "dataset_name": dataset_name,
        "row_count": row_count,
        "column_count": column_count,
        "geo_labels_wrapped": atlas_meta.get("geo_labels_wrapped", []),
        "wrapper_changed_count": atlas_meta.get("wrapper_changed_count", 0),
        "temporal_metadata": temporal_meta,
    }
    manifest = {
        "dataset_name": dataset_name,
        "source_metadata_path": rel_repo_path(paths["source_metadata"])
        if source_meta
        else None,
        "raw_data_path": rel_repo_path(data_path),
        "metadata_raw_path": rel_repo_path(paths["raw_metadata"])
        if paths["raw_metadata"].exists()
        else None,
        "metadata_wrapped_path": rel_repo_path(paths["final_metadata"])
        if paths["final_metadata"].exists()
        else None,
        "geo_results_wrapped_path": rel_repo_path(paths["final_geo_results"])
        if paths["final_geo_results"].exists()
        else None,
        "sample_path": rel_repo_path(sample_path),
        "nb_rows": row_count,
        "nb_columns": column_count,
        "has_wrapped_metadata": paths["final_metadata"].exists(),
        "has_raw_metadata": paths["raw_metadata"].exists(),
        "has_source_metadata": bool(source_meta),
    }

    summary_path = table_dir / "summary.json"
    manifest_path = table_dir / "manifest.json"
    write_json(summary_path, summary)
    write_json(manifest_path, manifest)

    entry = {
        "dataset_name": dataset_name,
        "source_title": source_meta.get("title"),
        "source_id": source_meta.get("id"),
        "source_rows_updated_at": source_meta.get("rows_updated_at"),
        "source_view_last_modified": source_meta.get("view_last_modified"),
        "source_description": source_meta.get("description"),
        "raw_data_path": rel_repo_path(data_path),
        "source_metadata_path": rel_repo_path(paths["source_metadata"]) if source_meta else None,
        "metadata_wrapped_path": rel_repo_path(paths["final_metadata"]) if paths["final_metadata"].exists() else None,
        "geo_results_wrapped_path": rel_repo_path(paths["final_geo_results"]) if paths["final_geo_results"].exists() else None,
        "row_count": row_count,
        "column_count": column_count,
        "wrapper_changed_count": atlas_meta.get("wrapper_changed_count", 0),
        "wrapped_geo_column_count": atlas_meta.get("wrapped_geo_column_count", 0),
        "summary_path": rel_repo_path(summary_path),
        "sample_header": header,
        "sample_rows": sample_rows,
        "dataset_metadata_overview": dataset_metadata_overview(atlas_meta, temporal_meta),
        "combined_column_metadata_rows": atlas_meta.get("combined_column_metadata_rows", []),
        "source_type_breakdown_rows": atlas_meta.get("source_type_breakdown_rows", []),
        "atlas_type_breakdown_rows": atlas_meta.get("atlas_type_breakdown_rows", []),
        "content_summary": atlas_meta.get("content_summary"),
        "temporal_metadata": temporal_meta,
    }

    write_dataset_page(entry)
    return entry


# Build source metadata for one dataset profile.
def source_profile(source_metadata: dict | None) -> dict:
    if not source_metadata:
        return {
            "title": None,
            "id": None,
            "rows_updated_at": None,
            "view_last_modified": None,
            "description": None,
            "columns": [],
        }
    columns = []
    for column in source_metadata.get("columns", []):
        columns.append(
            {
                "name": column.get("name"),
                "field_name": column.get("fieldName"),
                "description": column.get("description"),
                "source_type": pretty_type(column.get("dataTypeName")),
                "position": column.get("position"),
            }
        )
    return {
        "title": source_metadata.get("name"),
        "id": source_metadata.get("id"),
        "rows_updated_at": source_metadata.get("rowsUpdatedAt"),
        "view_last_modified": source_metadata.get("viewLastModified"),
        "description": source_metadata.get("description"),
        "columns": columns,
    }


# Build Atlas metadata for one dataset profile.
def atlas_profile(metadata: dict, wrapped_metadata: dict | None, wrapped_geo_columns: list[dict], combined_columns: list[dict]) -> dict:
    wrapper_summary = (wrapped_metadata or {}).get("_wrapper_summary", {})
    return {
        "row_count": metadata.get("nb_rows"),
        "profiled_rows": metadata.get("nb_profiled_rows"),
        "column_count": metadata.get("nb_columns"),
        "wrapped_geo_column_count": len(wrapped_geo_columns),
        "geo_labels_wrapped": sorted({item["label"] for item in wrapped_geo_columns}),
        "wrapper_changed_count": wrapper_summary.get("changed_count", 0),
        "dataset_metadata_overview": base_dataset_metadata_overview(metadata),
        "combined_column_metadata_rows": combined_columns,
        "source_type_breakdown_rows": source_type_breakdown_rows(combined_columns),
        "atlas_type_breakdown_rows": atlas_type_breakdown_rows(combined_columns),
        "content_summary": content_summary(metadata, wrapped_geo_columns),
        "final_columns": metadata.get("columns", []),
    }


# Build base dataset metadata rows from Atlas metadata.
def base_dataset_metadata_overview(metadata: dict) -> list[tuple[str, str]]:
    fields = [
        ("Rows", metadata.get("nb_rows")),
        ("Profiled Rows", metadata.get("nb_profiled_rows")),
        ("Columns", metadata.get("nb_columns")),
        ("Spatial Columns", metadata.get("nb_spatial_columns")),
        ("Temporal Columns", metadata.get("nb_temporal_columns")),
        ("Categorical Columns", metadata.get("nb_categorical_columns")),
        ("Numerical Columns", metadata.get("nb_numerical_columns")),
    ]
    rows = []
    for label, value in fields:
        if value is not None:
            rows.append((label, str(value)))
    types_value = metadata.get("types")
    if types_value:
        rows.append(("Dataset Types", ", ".join(map(str, types_value))))
    keywords = metadata.get("attribute_keywords")
    if keywords:
        rows.append(("Attribute Keywords", ", ".join(map(str, keywords[:12]))))
    return rows


# Build full dataset metadata rows for the detail page.
def dataset_metadata_overview(atlas_metadata: dict, temporal_metadata: dict) -> list[tuple[str, str]]:
    rows = list(atlas_metadata.get("dataset_metadata_overview", []))
    rows.append(("Has Temporal Data", str(temporal_metadata.get("has_temporal_data", False))))
    if temporal_metadata.get("temporal_columns"):
        rows.append(("Temporal Columns", ", ".join(temporal_metadata["temporal_columns"])))
    if temporal_metadata.get("temporal_start"):
        rows.append(("Temporal Start", temporal_metadata["temporal_start"]))
    if temporal_metadata.get("temporal_end"):
        rows.append(("Temporal End", temporal_metadata["temporal_end"]))
    return rows


# Join source metadata with final Atlas metadata.
def combined_column_metadata_rows(
    source_metadata: dict | None, metadata: dict, header: list[str]
) -> list[dict]:
    source_columns = source_metadata.get("columns", []) if source_metadata else []
    source_lookup = {}
    for column in source_columns:
        for key in [column.get("fieldName"), column.get("name")]:
            if key:
                source_lookup.setdefault(str(key).lower(), column)

    atlas_lookup = {
        str(column.get("name")).lower(): column
        for column in metadata.get("columns", [])
        if column.get("name")
    }

    ordered_names = list(header)
    extra_names = [
        column.get("name")
        for column in metadata.get("columns", [])
        if column.get("name") and column.get("name") not in ordered_names
    ]
    ordered_names.extend(extra_names)

    rows = []
    for name in ordered_names:
        key = str(name).lower()
        source_column = source_lookup.get(key, {})
        atlas_column = atlas_lookup.get(key, {})
        geo = atlas_column.get("geo_classifier") or {}
        semantic_types = ", ".join(pretty_type(value) for value in atlas_column.get("semantic_types", []))
        final_type = (
            geo.get("label")
            or pretty_type(atlas_column.get("structural_type"))
            or pretty_type(source_column.get("dataTypeName"))
            or "untyped"
        )
        rows.append(
            {
                "name": name,
                "final_type": str(final_type),
                "source_type": pretty_type(source_column.get("dataTypeName")),
                "source_description": source_column.get("description"),
                "structural_type": pretty_type(atlas_column.get("structural_type")),
                "semantic_types": semantic_types,
                "geo_label": geo.get("label"),
                "confidence": geo.get("confidence"),
            }
        )
    return rows


# Count values by type label.
def count_breakdown_rows(values: list[str]) -> list[dict]:
    counts = {}
    for value in values:
        key = value or MISSING
        counts[key] = counts.get(key, 0) + 1
    return [
        {"type": key, "count": counts[key]}
        for key in sorted(counts, key=lambda item: (-counts[item], item))
    ]


# Count source metadata types.
def source_type_breakdown_rows(rows: list[dict]) -> list[dict]:
    return count_breakdown_rows([row.get("source_type") or MISSING for row in rows])


# Map Atlas labels to supported types.
def normalize_atlas_supported_type(label: str | None) -> str:
    if not label:
        return NON_SPATIAL
    text = str(label)
    if text in {"zip5", "zip9", "zip_code"}:
        return "zip_code"
    if text == "borough":
        return "borough_code"
    supported = {
        "latitude",
        "longitude",
        "x_coord",
        "y_coord",
        "bbl",
        "bin",
        "zip_code",
        "borough_code",
        "city",
        "state",
        "address",
        "point",
        "line",
        "polygon",
        "multi-polygon",
        "multi-line",
        NON_SPATIAL,
    }
    return text if text in supported else NON_SPATIAL


# Count Atlas supported types.
def atlas_type_breakdown_rows(rows: list[dict]) -> list[dict]:
    return count_breakdown_rows(
        [normalize_atlas_supported_type(row.get("geo_label")) for row in rows]
    )


# Build a short content summary.
def content_summary(metadata: dict, wrapped_geo_columns: list[dict]) -> str:
    labels = []
    for item in wrapped_geo_columns:
        label = item["label"]
        if label not in labels:
            labels.append(label)
    if labels:
        return ", ".join(labels[:8])
    types_value = metadata.get("types") or []
    if types_value:
        return ", ".join(pretty_type(value) for value in types_value[:8])
    return NO_CONTENT_SUMMARY


# Read only the CSV header.
def read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        return next(reader, [])


# Read sample rows from a CSV file.
def read_sample_rows(path: Path, limit: int) -> tuple[list[str], list[list[str]]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        rows = []
        for idx, row in enumerate(reader):
            if idx >= limit:
                break
            rows.append(row)
    return header, rows


# Write sample rows to CSV.
def write_sample_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


# Extract final geo labeled columns.
def extract_geo_columns(metadata: dict | None) -> list[dict]:
    if not metadata:
        return []
    results = []
    for column in metadata.get("columns", []):
        geo = column.get("geo_classifier")
        if not geo:
            continue
        results.append(
            {
                "name": column["name"],
                "label": geo.get("label"),
                "confidence": geo.get("confidence"),
                "source": geo.get("source"),
                "wrapper_reason": column.get("wrapper_reason"),
            }
        )
    return results


# Write catalog files for the lake.
def write_catalog(datasets: list[dict]) -> None:
    catalog_json = []
    for item in datasets:
        catalog_json.append(
            {
                "dataset_name": item["dataset_name"],
                "source_title": item["source_title"],
                "source_id": item["source_id"],
                "source_rows_updated_at": item["source_rows_updated_at"],
                "source_view_last_modified": item["source_view_last_modified"],
                "raw_data_path": item["raw_data_path"],
                "source_metadata_path": item["source_metadata_path"],
                "metadata_wrapped_path": item["metadata_wrapped_path"],
                "geo_results_wrapped_path": item["geo_results_wrapped_path"],
                "row_count": item["row_count"],
                "column_count": item["column_count"],
                "wrapper_changed_count": item["wrapper_changed_count"],
                "wrapped_geo_column_count": item["wrapped_geo_column_count"],
                "has_temporal_data": item["temporal_metadata"].get("has_temporal_data"),
                "temporal_columns": ", ".join(item["temporal_metadata"].get("temporal_columns", [])),
                "temporal_start": item["temporal_metadata"].get("temporal_start"),
                "temporal_end": item["temporal_metadata"].get("temporal_end"),
                "temporal_month_coverage": item["temporal_metadata"].get("month_coverage", {}),
            }
        )

    write_json(CATALOG_DIR / "datasets.json", catalog_json)

    with (CATALOG_DIR / "datasets.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "dataset_name",
                "source_title",
                "source_id",
                "source_rows_updated_at",
                "source_view_last_modified",
                "raw_data_path",
                "source_metadata_path",
                "metadata_wrapped_path",
                "geo_results_wrapped_path",
                "row_count",
                "column_count",
                "wrapper_changed_count",
                "wrapped_geo_column_count",
                "has_temporal_data",
                "temporal_columns",
                "temporal_start",
                "temporal_end",
                "temporal_month_coverage",
            ],
        )
        writer.writeheader()
        for row in catalog_json:
            csv_row = dict(row)
            csv_row["temporal_month_coverage"] = json.dumps(
                row.get("temporal_month_coverage", {}),
                sort_keys=True,
            )
            writer.writerow(csv_row)


# Write the lake site files.
def write_site(datasets: list[dict]) -> None:
    total_rows = sum(item["row_count"] for item in datasets)
    total_geo = sum(item["wrapped_geo_column_count"] for item in datasets)
    latest_report = load_json(LATEST_UPDATE_REPORT)
    latest_refresh_log = load_json(LATEST_REFRESH_LOG)
    cards = []
    for item in datasets:
        cards.append(render_index_card(item))

    html = (
        html_head("Atlas Lake")
        + "<section class='hero'>"
        + "<div>"
        + "<p class='eyebrow'>Static Data Lake</p>"
        + "<h1>Atlas Lake</h1>"
        + "<p class='lead'>Browse each dataset, open its metadata files, inspect wrapped geo results, and preview sample rows without running any server.</p>"
        + "</div>"
        + "<div class='hero-stats'>"
        + stat_box("Datasets", str(len(datasets)))
        + stat_box("Total Rows", f"{total_rows:,}")
        + stat_box("Wrapped Geo Labels", str(total_geo))
        + "</div>"
        + "</section>"
        + render_update_panel(latest_report, latest_refresh_log)
        + render_temporal_search_panel(datasets)
        + "<section class='section'>"
        + "<h2>Dataset Catalog</h2>"
        + "<p class='section-copy'>Each card shows the core table facts, metadata access points, and the main geo fields currently captured in wrapped output.</p>"
        + "<div class='card-grid' id='dataset-card-grid'>"
        + "".join(cards)
        + "</div>"
        + "</section>"
        + html_tail()
    )
    (SITE_DIR / "index.html").write_text(html, encoding="utf-8")


# Render one dataset card on the index page.
def render_index_card(item: dict) -> str:
    page_href = f"tables/{escape(item['dataset_name'])}.html"
    description = truncate_text(item.get("source_description") or "", 180)
    temporal = item.get("temporal_metadata", {})
    has_temporal = temporal.get("has_temporal_data", False)
    temporal_start = temporal.get("temporal_start") or ""
    temporal_end = temporal.get("temporal_end") or ""
    temporal_columns = ", ".join(temporal.get("temporal_columns", []))
    temporal_months = flatten_month_keys(temporal.get("month_coverage", {}))
    card_open = (
        "<article class='dataset-card'"
        + f" data-dataset-name='{escape(item['dataset_name'])}'"
        + f" data-has-temporal='{str(has_temporal).lower()}'"
        + f" data-temporal-start='{escape(temporal_start)}'"
        + f" data-temporal-end='{escape(temporal_end)}'"
        + f" data-temporal-months='{escape(temporal_months)}'"
        + ">"
    )

    return (
        card_open
        + f"<div class='card-top'><h3><a href='{page_href}'>{escape(item['dataset_name'])}</a></h3>"
        + f"<p class='subtle'>{escape(item.get('source_title') or item['raw_data_path'])}</p>"
        + "</div>"
        + "<div class='metric-row'>"
        + metric_chip("Rows", f"{item['row_count']:,}")
        + metric_chip("Columns", str(item["column_count"]))
        + "</div>"
        + "<div class='card-section'><h4>Description</h4>"
        + f"<p>{escape(description or 'No source description.')}</p></div>"
        + "<div class='card-section'><h4>Source Update Info</h4><div class='compact-grid'>"
        + compact_meta("Rows Updated", format_timestamp(item.get("source_rows_updated_at")))
        + compact_meta("View Modified", format_timestamp(item.get("source_view_last_modified")))
        + compact_meta("Contains", item.get("content_summary"))
        + "</div></div>"
        + "<div class='card-section'><h4>Temporal Coverage</h4><div class='compact-grid'>"
        + compact_meta("Has Time Data", "Yes" if has_temporal else "No")
        + compact_meta("Time Columns", temporal_columns or MISSING)
        + compact_meta("Start", temporal_start or MISSING)
        + compact_meta("End", temporal_end or MISSING)
        + "</div></div>"
        + f"<p class='card-action'><a href='{page_href}'>Open details</a></p>"
        + "</article>"
    )


# Render the temporal search panel on the index page.
def render_temporal_search_panel(datasets: list[dict]) -> str:
    temporal_count = sum(
        1 for item in datasets if item.get("temporal_metadata", {}).get("has_temporal_data")
    )
    return (
        "<section class='section action-panel'>"
        + "<h2>Time Range Search</h2>"
        + "<p class='section-copy'>This search is month based and returns datasets that have at least one month with data in the requested date range.</p>"
        + "<div class='search-grid'>"
        + "<label class='search-field'><span>Start Date</span><input id='temporal-start-date' type='date'></label>"
        + "<label class='search-field'><span>End Date</span><input id='temporal-end-date' type='date'></label>"
        + "</div>"
        + "<div class='action-row'>"
        + "<button class='action-button' type='button' onclick='runTemporalSearch()'>Search</button>"
        + "<button class='action-button secondary-button' type='button' onclick='clearTemporalSearch()'>Clear</button>"
        + "</div>"
        + f"<p class='section-copy'>Datasets with temporal coverage: {temporal_count}</p>"
        + "<p id='temporal-search-status' class='section-copy'>No time range filter is active.</p>"
        + "</section>"
    )


# Flatten month coverage into month keys for the search UI.
def flatten_month_keys(month_coverage: dict) -> str:
    month_keys = []
    for year in sorted(month_coverage, key=int):
        for month in month_coverage[year]:
            month_keys.append(f"{year}-{int(month):02d}")
    return "|".join(month_keys)


# Render one metric chip.
def metric_chip(label: str, value: str) -> str:
    return (
        "<div class='metric-chip'>"
        + f"<span class='label'>{escape(label)}</span>"
        + f"<span class='value'>{escape(value)}</span>"
        + "</div>"
    )


# Render the update panel on the index page.
def render_update_panel(report: dict | None, refresh_log: dict | None) -> str:
    checked_at = (report or {}).get("checked_at") or MISSING
    report_summary = (report or {}).get("summary") or {}
    report_summary_text = ", ".join(
        f"{key}: {value}" for key, value in sorted(report_summary.items())
    ) or "No report summary"
    refreshed_at = (refresh_log or {}).get("refreshed_at") or MISSING
    changed_count = len((refresh_log or {}).get("changed_datasets", []))
    errored_count = len((refresh_log or {}).get("errored_datasets", []))
    rebuilt = (refresh_log or {}).get("lake_rebuilt")
    return (
        "<section class='section action-panel'>"
        + "<h2>Update Control</h2>"
        + "<p class='section-copy'>Run one command to check NYC Open Data metadata, refresh only changed datasets, rerun Atlas + wrapper, and rebuild the lake.</p>"
        + "<div class='action-row'>"
        + "<button class='action-button' type='button' onclick=\"toggleLakePanel('update-run-panel')\">Check &amp; Update</button>"
        + "</div>"
        + "<div id='update-run-panel' class='action-detail is-hidden'>"
        + "<div class='status-grid'>"
        + status_card("Last Checked", str(checked_at))
        + status_card("Report Summary", report_summary_text)
        + status_card("Last Refreshed", str(refreshed_at))
        + status_card("Changed Datasets", str(changed_count))
        + status_card("Errored Datasets", str(errored_count))
        + status_card("Lake Rebuilt", str(rebuilt))
        + "</div>"
        + "<p class='section-copy'>This site is static, so the button cannot run the pipeline directly. Run the command below in the project root:</p>"
        + "<pre class='mono'>python refresh_nyc_datalake.py</pre>"
        + "</div>"
        + render_latest_report(report)
        + render_latest_refresh_log(refresh_log)
        + "</section>"
    )


# Render one status card row.
def status_card(label: str, value: str) -> str:
    return (
        "<div class='status-card'>"
        + f"<span class='label'>{escape(label)}</span>"
        + f"<span class='value'>{escape(value)}</span>"
        + "</div>"
    )


# Render the latest update report block.
def render_latest_report(report: dict | None) -> str:
    if not report:
        return (
            "<details class='inline-report'><summary>Latest report</summary>"
            + f"<p class='muted'>{NO_UPDATE_REPORT}</p></details>"
        )
    rows = []
    for item in report.get("datasets", []):
        rows.append(
            "<tr>"
            + f"<td>{escape(str(item.get('dataset_name') or ''))}</td>"
            + f"<td>{escape(str(item.get('status') or ''))}</td>"
            + f"<td>{escape(str(item.get('action') or ''))}</td>"
            + f"<td>{escape(', '.join(item.get('changes_vs_local', [])) or 'none')}</td>"
            + "</tr>"
        )
    summary = report.get("summary", {})
    summary_line = ", ".join(f"{key}: {value}" for key, value in summary.items()) or "No summary"
    return (
        "<details class='inline-report'>"
        + "<summary>Latest report</summary>"
        + f"<p class='section-copy'>Checked at: {escape(str(report.get('checked_at') or MISSING))}</p>"
        + f"<p class='section-copy'>Summary: {escape(summary_line)}</p>"
        + "<div class='table-wrap'><table><thead><tr><th>Dataset</th><th>Status</th><th>Action</th><th>Changes vs Local</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></div>"
        + "</details>"
    )


# Render the latest refresh log block.
def render_latest_refresh_log(refresh_log: dict | None) -> str:
    if not refresh_log:
        return (
            "<details class='inline-report'><summary>Latest refresh log</summary>"
            + f"<p class='muted'>{NO_REFRESH_LOG}</p></details>"
        )
    changed_rows = []
    for item in refresh_log.get("changed_datasets", []):
        changed_rows.append(
            "<tr>"
            + f"<td>{escape(str(item.get('dataset_name') or ''))}</td>"
            + f"<td>{escape(', '.join(item.get('reason', [])) or 'none')}</td>"
            + f"<td>{escape(str(item.get('download_limit') or ''))}</td>"
            + "</tr>"
        )
    if not changed_rows:
        changed_rows.append("<tr><td colspan='3' class='muted'>No datasets refreshed in the latest run.</td></tr>")
    return (
        "<details class='inline-report'>"
        + "<summary>Latest refresh log</summary>"
        + f"<p class='section-copy'>Refreshed at: {escape(str(refresh_log.get('refreshed_at') or MISSING))}</p>"
        + f"<p class='section-copy'>Lake rebuilt: {escape(str(refresh_log.get('lake_rebuilt')))}</p>"
        + f"<p class='section-copy'>Errored datasets: {escape(', '.join(refresh_log.get('errored_datasets', [])) or 'none')}</p>"
        + "<div class='table-wrap'><table><thead><tr><th>Dataset</th><th>Reason</th><th>Download Limit</th></tr></thead><tbody>"
        + "".join(changed_rows)
        + "</tbody></table></div>"
        + "</details>"
    )


# Compact a metadata value for display.
def compact_meta(label: str, value) -> str:
    text = MISSING if value in (None, "") else str(value)
    return (
        "<div class='compact-meta'>"
        + f"<span class='label'>{escape(label)}</span>"
        + f"<span class='value'>{escape(text)}</span>"
        + "</div>"
    )


# Render one stat box.
def stat_box(label: str, value: str) -> str:
    return (
        "<div class='stat-box'>"
        + f"<span class='label'>{escape(label)}</span>"
        + f"<span class='value'>{escape(value)}</span>"
        + "</div>"
    )


# Truncate long text for the index page.
def truncate_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "…"


# Format a timestamp for display.
def format_timestamp(value) -> str:
    if value in (None, ""):
        return MISSING
    try:
        return datetime.fromtimestamp(int(value)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        return str(value)


# Make one type label easier to read.
def pretty_type(value) -> str:
    if value in (None, ""):
        return ""
    text = str(value)
    if "/" in text:
        text = text.rstrip("/").rsplit("/", 1)[-1]
    if "#" in text:
        text = text.rsplit("#", 1)[-1]
    return text


# Write one dataset detail page.
def write_dataset_page(item: dict) -> None:
    page_path = SITE_TABLES_DIR / f"{item['dataset_name']}.html"

    basic_info = (
        "<h2>Basic Info</h2><table>"
        f"<tr><th>Dataset</th><td>{escape(item['dataset_name'])}</td></tr>"
        f"<tr><th>Source Title</th><td>{escape(str(item.get('source_title') or MISSING))}</td></tr>"
        f"<tr><th>Source Dataset ID</th><td>{escape(str(item.get('source_id') or MISSING))}</td></tr>"
        f"<tr><th>Rows</th><td>{item['row_count']}</td></tr>"
        f"<tr><th>Columns</th><td>{item['column_count']}</td></tr>"
        f"<tr><th>Description</th><td>{escape(str(item.get('source_description') or MISSING))}</td></tr>"
        f"<tr><th>Rows Updated</th><td>{escape(format_timestamp(item.get('source_rows_updated_at')))}</td></tr>"
        f"<tr><th>View Modified</th><td>{escape(format_timestamp(item.get('source_view_last_modified')))}</td></tr>"
        f"<tr><th>Contains</th><td>{escape(str(item.get('content_summary') or MISSING))}</td></tr>"
        "</table>"
    )

    dataset_metadata = (
        "<h2>Dataset Metadata</h2>"
        + render_dataset_metadata_table(item["dataset_metadata_overview"])
    )

    sample_rows = (
        "<h2>Sample Data</h2>"
        + "<p>First 10 rows from the full dataset.</p>"
        + render_sample_table(item["sample_header"], item["sample_rows"])
    )

    analysis = (
        "<h2>Analysis</h2>"
        + "<h3>NYC Open Data Column Types</h3>"
        + "<p>This analysis counts each column by the original NYC Open Data source type.</p>"
        + render_type_breakdown(item["source_type_breakdown_rows"])
        + "<h3>Atlas + Wrapper Spatial Types</h3>"
        + f"<p>This analysis counts each column by the final Atlas + wrapper spatial type. Columns without a final spatial label are counted as <code>{NON_SPATIAL}</code>.</p>"
        + render_type_breakdown(item["atlas_type_breakdown_rows"])
    )

    column_metadata = (
        "<h2>Combined Column Metadata</h2>"
        + "<p>This table combines NYC Open Data column metadata with the final Atlas + wrapper result for every column.</p>"
        + render_combined_column_metadata_table(item["combined_column_metadata_rows"])
    )

    files = (
        "<h2>Files</h2><table>"
        f"<tr><th>Raw Data</th><td>{file_link(page_path, item['raw_data_path'], RAW_CSV_LABEL)}</td></tr>"
        f"<tr><th>Final Metadata</th><td>{file_link(page_path, item['metadata_wrapped_path'], FINAL_METADATA_LABEL)}</td></tr>"
        f"<tr><th>Final Geo Results</th><td>{file_link(page_path, item['geo_results_wrapped_path'], FINAL_GEO_RESULTS_LABEL)}</td></tr>"
        f"<tr><th>Summary</th><td>{file_link(page_path, item['summary_path'], SUMMARY_JSON_LABEL)}</td></tr>"
        "</table>"
    )

    html = (
        html_head(f"Atlas Lake - {item['dataset_name']}")
        + f"<p><a href='../index.html'>&larr; Back to index</a></p>"
        + f"<h1>{escape(item['dataset_name'])}</h1>"
        + basic_info
        + dataset_metadata
        + sample_rows
        + analysis
        + column_metadata
        + files
        + html_tail()
    )
    page_path.write_text(html, encoding="utf-8")


# Build one file link.
def file_link(page_path: Path, repo_relative_path: str | None, label: str | None = None) -> str:
    if not repo_relative_path:
        return f"<span class='muted'>{MISSING}</span>"
    target = ROOT / repo_relative_path
    href = os.path.relpath(target, start=page_path.parent).replace(os.sep, "/")
    title = label or Path(repo_relative_path).name
    return f"<a href='{escape(href)}'>{escape(title)}</a>"


# Render the dataset metadata table.
def render_dataset_metadata_table(rows: list[tuple[str, str]]) -> str:
    if not rows:
        return f"<p class='muted'>{NO_DATASET_METADATA}</p>"
    html_rows = "".join(
        f"<tr><th>{escape(label)}</th><td>{escape(value)}</td></tr>" for label, value in rows
    )
    return "<table>" + html_rows + "</table>"


# Render the combined column metadata table.
def render_combined_column_metadata_table(rows: list[dict]) -> str:
    if not rows:
        return f"<p class='muted'>{NO_COLUMN_METADATA}</p>"
    body = []
    for row in rows:
        body.append(
            "<tr>"
            f"<td>{escape(str(row['name'] or ''))}</td>"
            f"<td>{escape(str(row['final_type'] or ''))}</td>"
            f"<td>{escape(str(row['source_type'] or ''))}</td>"
            f"<td>{escape(str(row['structural_type'] or ''))}</td>"
            f"<td>{escape(str(row['semantic_types'] or ''))}</td>"
            f"<td>{escape(str(row['geo_label'] or ''))}</td>"
            f"<td>{escape(str(row['confidence'] or ''))}</td>"
            f"<td>{escape(str(row['source_description'] or ''))}</td>"
            "</tr>"
        )
    return (
        "<div class='table-wrap'><table><thead><tr><th>Column</th><th>Final Type</th><th>NYC Source Type</th><th>Atlas Structural Type</th><th>Semantic Types</th><th>Geo Label</th><th>Confidence</th><th>NYC Source Description</th></tr></thead><tbody>"
        + "".join(body)
        + "</tbody></table></div>"
    )


# Render a sample data table.
def render_sample_table(
    header: list[str], rows: list[list[str]], selected_header: list[str] | None = None
) -> str:
    use_header = selected_header or header
    if not use_header:
        return "<p class='muted'>No geo-labeled sample columns.</p>"
    index_map = [header.index(name) for name in use_header if name in header]
    head_html = "".join(f"<th>{escape(name)}</th>" for name in use_header)
    body = []
    for row in rows:
        picked = [row[idx] if idx < len(row) else "" for idx in index_map]
        body.append("<tr>" + "".join(f"<td>{escape(cell)}</td>" for cell in picked) + "</tr>")
    return (
        "<div class='table-wrap'>"
        + "<table><thead><tr>"
        + head_html
        + "</tr></thead><tbody>"
        + "".join(body)
        + "</tbody></table></div>"
    )


# Render a type breakdown section.
def render_type_breakdown(rows: list[dict]) -> str:
    if not rows:
        return "<p class='muted'>No type analysis available.</p>"
    max_count = max(row["count"] for row in rows) or 1
    chart = []
    table_rows = []
    for row in rows:
        width = max(8, round((row["count"] / max_count) * 100))
        chart.append(
            "<div class='bar-row'>"
            + f"<span class='bar-label'>{escape(str(row['type']))}</span>"
            + f"<div class='bar-track'><div class='bar-fill' style='width:{width}%'></div></div>"
            + f"<span class='bar-value'>{row['count']}</span>"
            + "</div>"
        )
        table_rows.append(
            f"<tr><td>{escape(str(row['type']))}</td><td>{row['count']}</td></tr>"
        )
    return (
        "<div class='analysis-grid'>"
        + "<div>"
        + "<h3>Plot</h3>"
        + "<div class='bar-chart'>"
        + "".join(chart)
        + "</div></div>"
        + "<div>"
        + "<h3>Table</h3>"
        + "<table><thead><tr><th>Type</th><th>Columns</th></tr></thead><tbody>"
        + "".join(table_rows)
        + "</tbody></table></div>"
        + "</div>"
    )


# Render the shared HTML head.
def html_head(title: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 32px;
      line-height: 1.45;
      color: #1f2937;
      background: #f8fafc;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      margin: 12px 0 24px;
    }}
    th, td {{
      border: 1px solid #d1d5db;
      padding: 8px 10px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: #f3f4f6;
    }}
    .muted {{
      color: #6b7280;
    }}
    .table-wrap {{
      overflow-x: auto;
    }}
    .hero {{
      display: grid;
      grid-template-columns: minmax(0, 1.6fr) minmax(280px, 1fr);
      gap: 24px;
      align-items: start;
      margin-bottom: 28px;
    }}
    .hero-stats {{
      display: grid;
      gap: 12px;
    }}
    .stat-box, .metric-chip {{
      border: 1px solid #d1d5db;
      background: #ffffff;
      border-radius: 12px;
      padding: 12px 14px;
    }}
    .stat-box .label, .metric-chip .label, .link-list .label {{
      display: block;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: #6b7280;
      margin-bottom: 4px;
    }}
    .stat-box .value, .metric-chip .value {{
      font-size: 22px;
      font-weight: 600;
      color: #111827;
    }}
    .section {{
      margin-top: 24px;
    }}
    .section-copy, .lead, .subtle {{
      color: #4b5563;
    }}
    .eyebrow {{
      margin: 0 0 8px;
      color: #0f766e;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-weight: 700;
    }}
    h1, h2, h3, h4 {{
      color: #111827;
    }}
    .card-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 18px;
    }}
    .dataset-card {{
      background: #ffffff;
      border: 1px solid #d1d5db;
      border-radius: 16px;
      padding: 18px;
      box-shadow: 0 6px 18px rgba(15, 23, 42, 0.04);
    }}
    .card-top {{
      margin-bottom: 14px;
    }}
    .card-top h3 {{
      margin: 0 0 4px;
    }}
    .subtle {{
      margin: 0;
      font-size: 13px;
      word-break: break-all;
    }}
    .metric-row {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 16px;
    }}
    .card-section {{
      margin-top: 14px;
    }}
    .card-section p {{
      margin: 0;
    }}
    .card-section h4 {{
      margin: 0 0 8px;
      font-size: 14px;
    }}
    .link-list, .geo-list {{
      margin: 0;
      padding-left: 18px;
    }}
    .link-list li, .geo-list li {{
      margin: 6px 0;
    }}
    .mono {{
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 13px;
      background: #f3f4f6;
      padding: 8px 10px;
      border-radius: 10px;
      overflow-x: auto;
      margin: 0;
    }}
    .compact-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
    }}
    .compact-meta {{
      border: 1px solid #e5e7eb;
      border-radius: 10px;
      background: #f8fafc;
      padding: 10px 12px;
    }}
    .compact-meta .label {{
      display: block;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: #6b7280;
      margin-bottom: 4px;
    }}
    .compact-meta .value {{
      color: #111827;
      font-size: 14px;
      word-break: break-word;
    }}
    .search-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      margin-top: 12px;
    }}
    .search-field {{
      display: grid;
      gap: 6px;
      color: #374151;
      font-size: 14px;
      font-weight: 600;
    }}
    .search-field input {{
      border: 1px solid #d1d5db;
      border-radius: 10px;
      padding: 10px 12px;
      font: inherit;
      background: #ffffff;
      color: #111827;
    }}
    .card-action {{
      margin-top: 16px;
      font-weight: 600;
    }}
    .action-panel {{
      background: #ffffff;
      border: 1px solid #d1d5db;
      border-radius: 16px;
      padding: 18px;
      box-shadow: 0 6px 18px rgba(15, 23, 42, 0.04);
    }}
    .action-row {{
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      margin-top: 12px;
    }}
    .action-button {{
      border: 0;
      border-radius: 999px;
      background: #0f766e;
      color: #ffffff;
      padding: 10px 16px;
      font-size: 14px;
      font-weight: 600;
      cursor: pointer;
    }}
    .action-button:hover {{
      background: #115e59;
    }}
    .secondary-button {{
      background: #475569;
    }}
    .secondary-button:hover {{
      background: #334155;
    }}
    .action-detail {{
      margin-top: 14px;
      padding: 14px;
      border: 1px solid #d1d5db;
      border-radius: 12px;
      background: #f8fafc;
    }}
    .status-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }}
    .status-card {{
      border: 1px solid #d1d5db;
      border-radius: 12px;
      background: #ffffff;
      padding: 12px 14px;
    }}
    .status-card .label {{
      display: block;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: #6b7280;
      margin-bottom: 6px;
    }}
    .status-card .value {{
      color: #111827;
      font-size: 14px;
      word-break: break-word;
    }}
    .inline-report {{
      margin-top: 12px;
      border: 1px solid #d1d5db;
      border-radius: 12px;
      background: #ffffff;
      padding: 12px 14px;
    }}
    .inline-report summary {{
      cursor: pointer;
      font-weight: 600;
      color: #111827;
    }}
    .is-hidden {{
      display: none;
    }}
    .analysis-grid {{
      display: grid;
      grid-template-columns: minmax(320px, 1.2fr) minmax(240px, 0.8fr);
      gap: 20px;
      align-items: start;
    }}
    .bar-chart {{
      display: grid;
      gap: 10px;
    }}
    .bar-row {{
      display: grid;
      grid-template-columns: minmax(120px, 180px) minmax(140px, 1fr) 56px;
      gap: 10px;
      align-items: center;
    }}
    .bar-label, .bar-value {{
      font-size: 13px;
      color: #374151;
    }}
    .bar-track {{
      background: #e5e7eb;
      border-radius: 999px;
      height: 10px;
      overflow: hidden;
    }}
    .bar-fill {{
      background: #0f766e;
      height: 100%;
      border-radius: 999px;
    }}
    a {{
      color: #0f766e;
      text-decoration: none;
    }}
    a:hover {{
      text-decoration: underline;
    }}
    @media (max-width: 900px) {{
      .hero {{
        grid-template-columns: 1fr;
      }}
      .analysis-grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
"""


# Render the shared HTML tail.
def html_tail() -> str:
    return """<script>
function toggleLakePanel(id) {
  const node = document.getElementById(id);
  if (!node) return;
  node.classList.toggle('is-hidden');
}

function monthKeysInRange(startValue, endValue) {
  const monthKeys = [];
  const start = new Date(startValue + 'T00:00:00');
  const end = new Date(endValue + 'T00:00:00');
  let year = start.getFullYear();
  let month = start.getMonth() + 1;
  const endYear = end.getFullYear();
  const endMonth = end.getMonth() + 1;

  while (year < endYear || (year === endYear && month <= endMonth)) {
    monthKeys.push(String(year) + '-' + String(month).padStart(2, '0'));
    month += 1;
    if (month > 12) {
      month = 1;
      year += 1;
    }
  }

  return monthKeys;
}

function runTemporalSearch() {
  const startNode = document.getElementById('temporal-start-date');
  const endNode = document.getElementById('temporal-end-date');
  const statusNode = document.getElementById('temporal-search-status');
  const cards = document.querySelectorAll('.dataset-card');

  const startValue = startNode ? startNode.value : '';
  const endValue = endNode ? endNode.value : '';

  if (!startValue || !endValue) {
    if (statusNode) {
      statusNode.textContent = 'Enter both a start date and an end date.';
    }
    return;
  }

  if (startValue > endValue) {
    if (statusNode) {
      statusNode.textContent = 'Start date must be on or before end date.';
    }
    return;
  }

  const queryStart = new Date(startValue + 'T00:00:00');
  const queryEnd = new Date(endValue + 'T23:59:59');
  const queryMonths = monthKeysInRange(startValue, endValue);
  let shownCount = 0;

  cards.forEach((card) => {
    const hasTemporal = card.dataset.hasTemporal === 'true';
    const startText = card.dataset.temporalStart;
    const endText = card.dataset.temporalEnd;
    const datasetMonths = new Set((card.dataset.temporalMonths || '').split('|').filter(Boolean));
    let showCard = false;

    if (hasTemporal && startText && endText) {
      const datasetStart = new Date(startText);
      const datasetEnd = new Date(endText);
      const overlapsRange = datasetStart <= queryEnd && datasetEnd >= queryStart;
      const hasMonthInRange = queryMonths.some((monthKey) => datasetMonths.has(monthKey));
      showCard = overlapsRange && hasMonthInRange;
    }

    card.style.display = showCard ? '' : 'none';
    if (showCard) {
      shownCount += 1;
    }
  });

  if (statusNode) {
    statusNode.textContent = 'Showing ' + shownCount + ' dataset(s) with data in ' + startValue + ' to ' + endValue + '.';
  }
}

function clearTemporalSearch() {
  const startNode = document.getElementById('temporal-start-date');
  const endNode = document.getElementById('temporal-end-date');
  const statusNode = document.getElementById('temporal-search-status');
  const cards = document.querySelectorAll('.dataset-card');

  if (startNode) startNode.value = '';
  if (endNode) endNode.value = '';

  cards.forEach((card) => {
    card.style.display = '';
  });

  if (statusNode) {
    statusNode.textContent = 'No time range filter is active.';
  }
}
</script></body></html>
"""


if __name__ == "__main__":
    main()

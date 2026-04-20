import csv
import json
from pathlib import Path

from atlas_profiler import process_dataset

from atlas_wrapper import apply_wrapper
from nyc_open_data_utils import DATA_DIR, output_paths


# Settings to adjust when reusing this Atlas processing core in another project.
DEFAULT_GEO_THRESHOLD = 0.6
RAW_METADATA_PATH_KEY = "raw_metadata"
RAW_GEO_RESULTS_PATH_KEY = "raw_geo_results"
FINAL_METADATA_PATH_KEY = "final_metadata"
FINAL_GEO_RESULTS_PATH_KEY = "final_geo_results"
PROFILE_OPTIONS = {
    "geo_classifier": True,
    "include_sample": True,
    "coverage": True,
    "plots": False,
    "indexes": True,
    "load_max_size": None,
    "metadata": None,
    "nominatim": None,
}


# Return the output mode label.
def output_mode(use_wrapper: bool) -> str:
    return "wrapped" if use_wrapper else "raw"


# Return the metadata path for one mode.
def metadata_output_path(paths: dict[str, Path], use_wrapper: bool) -> Path:
    key = FINAL_METADATA_PATH_KEY if use_wrapper else RAW_METADATA_PATH_KEY
    return paths[key]


# Return the geo results path for one mode.
def geo_results_output_path(paths: dict[str, Path], use_wrapper: bool) -> Path:
    key = FINAL_GEO_RESULTS_PATH_KEY if use_wrapper else RAW_GEO_RESULTS_PATH_KEY
    return paths[key]


# Print a short summary for one processed dataset.
def print_dataset_details(metadata: dict, *, dataset_name: str, mode: str, geo_threshold: float) -> None:
    columns = metadata.get("columns", [])
    interesting = [
        column
        for column in columns
        if column.get("geo_classifier") or column.get("wrapper_reason")
    ]

    print("Dataset:", dataset_name)
    print("Mode:", mode)
    print("Geo Threshold:", geo_threshold)
    print("Top-level keys:", list(metadata.keys()))
    print(f"Detected {len(columns)} columns")

    if mode == "wrapped":
        summary = metadata.get("_wrapper_summary", {})
        changes = summary.get("changes", [])
        print(f"Wrapper changes: {summary.get('changed_count', 0)}")
        for change in changes:
            print(
                f"  - {change['name']}: {change['old_label']} -> "
                f"{change['new_label']} ({change['reason']})"
            )

    for column in interesting:
        print("=" * 80)
        print("column:", column.get("name"))
        if column.get("geo_classifier") is not None:
            print("geo_classifier:", column.get("geo_classifier"))
        if column.get("wrapper_reason") is not None:
            print("wrapper_reason:", column.get("wrapper_reason"))
        if column.get("semantic_types") is not None:
            print("semantic_types:", column.get("semantic_types"))


# Write final labeled columns to one CSV file.
def write_geo_results(path: Path, dataset_name: str, columns: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["dataset_name", "name", "label", "confidence", "wrapper_reason"],
        )
        writer.writeheader()
        writer.writerows(
            {
                "dataset_name": dataset_name,
                "name": column.get("name"),
                "label": geo.get("label"),
                "confidence": geo.get("confidence"),
                "wrapper_reason": column.get("wrapper_reason", ""),
            }
            for column in columns
            if (geo := column.get("geo_classifier"))
        )


# Run Atlas for one dataset and one mode.
def process_dataset_once(
    data_path: str | Path,
    *,
    use_wrapper: bool,
    geo_threshold: float = DEFAULT_GEO_THRESHOLD,
    print_details: bool = True,
    paths_builder=output_paths,
) -> dict:
    data_path = Path(data_path)
    dataset_name = data_path.stem
    paths = paths_builder(dataset_name)
    mode = output_mode(use_wrapper)
    metadata_path = metadata_output_path(paths, use_wrapper)
    geo_results_path = geo_results_output_path(paths, use_wrapper)

    metadata = process_dataset(
        str(data_path),
        geo_classifier_threshold=geo_threshold,
        **PROFILE_OPTIONS,
    )

    if use_wrapper:
        metadata = apply_wrapper(str(data_path), metadata)

    metadata["dataset_name"] = dataset_name
    metadata["output_mode"] = mode

    if print_details:
        print_dataset_details(
            metadata,
            dataset_name=dataset_name,
            mode=mode,
            geo_threshold=geo_threshold,
        )

    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_geo_results(geo_results_path, dataset_name, metadata.get("columns", []))

    if print_details:
        print(f"\nSaved full metadata to {metadata_path}")
        print(f"Saved geo classifier results to {geo_results_path}")

    return {
        "dataset_name": dataset_name,
        "mode": mode,
        "metadata_path": str(metadata_path),
        "geo_results_path": str(geo_results_path),
    }


# Run Atlas for raw output and wrapped output.
def process_dataset_outputs(
    data_path: str | Path,
    *,
    geo_threshold: float = DEFAULT_GEO_THRESHOLD,
    print_details: bool = True,
    paths_builder=output_paths,
) -> dict:
    raw = process_dataset_once(
        data_path,
        use_wrapper=False,
        geo_threshold=geo_threshold,
        print_details=print_details,
        paths_builder=paths_builder,
    )
    wrapped = process_dataset_once(
        data_path,
        use_wrapper=True,
        geo_threshold=geo_threshold,
        print_details=print_details,
        paths_builder=paths_builder,
    )
    return {
        "dataset_name": Path(data_path).stem,
        "raw": raw,
        "wrapped": wrapped,
    }


# Run Atlas for every dataset in one folder.
def process_all_datasets(
    *,
    data_dir: Path = DATA_DIR,
    geo_threshold: float = DEFAULT_GEO_THRESHOLD,
    print_details: bool = True,
    paths_builder=output_paths,
) -> list[dict]:
    results = []
    for path in sorted(data_dir.glob("*.csv")):
        if print_details:
            print("\n" + "#" * 80)
        results.append(
            process_dataset_outputs(
                path,
                geo_threshold=geo_threshold,
                print_details=print_details,
                paths_builder=paths_builder,
            )
        )
    return results

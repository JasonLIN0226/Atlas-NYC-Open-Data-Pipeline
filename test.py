import csv
import json
from pathlib import Path

from atlas_profiler import process_dataset

from atlas_wrapper import apply_wrapper

DATA_PATH = "data/street_trees.csv"  # RUN_ALL_DATASETS=False 时使用
RUN_ALL_DATASETS = True  # True: 跑 data/ 目录下全部 csv
USE_WRAPPER = True  # True: 接入 wrapper, False: 只看 Atlas 原始输出
GEO_THRESHOLD = 0.6

OUT_DIR = Path("output")
OUT_DIR.mkdir(exist_ok=True)
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


# Run Atlas for one dataset.
def run_dataset(
    data_path: str,
    *,
    use_wrapper: bool = USE_WRAPPER,
    geo_threshold: float = GEO_THRESHOLD,
    out_dir: Path = OUT_DIR,
    print_details: bool = True,
) -> dict:
    dataset_name = Path(data_path).stem
    mode = output_mode(use_wrapper)
    metadata_path = out_dir / f"metadata_{dataset_name}_{mode}.json"
    geo_results_path = out_dir / f"geo_classifier_results_{dataset_name}_{mode}.csv"

    if geo_results_path.exists():
        geo_results_path.unlink()

    metadata = process_dataset(data_path, geo_classifier_threshold=geo_threshold, **PROFILE_OPTIONS)

    if use_wrapper:
        metadata = apply_wrapper(data_path, metadata)

    metadata["dataset_name"] = dataset_name
    metadata["output_mode"] = mode

    if print_details:
        print("Dataset:", dataset_name)
        print("Mode:", mode)
        print("Geo Threshold:", geo_threshold)
        print("Top-level keys:", list(metadata.keys()))

    columns = metadata.get("columns", [])
    interesting = [
        col for col in columns if col.get("geo_classifier") or col.get("wrapper_reason")
    ]
    if print_details:
        print(f"Detected {len(columns)} columns")

    if use_wrapper and print_details:
        summary = metadata.get("_wrapper_summary", {})
        changes = summary.get("changes", [])
        print(f"Wrapper changes: {summary.get('changed_count', 0)}")
        for change in changes:
            print(
                f"  - {change['name']}: {change['old_label']} -> "
                f"{change['new_label']} ({change['reason']})"
            )

    if print_details:
        for col in interesting:
            name = col.get("name")
            geo = col.get("geo_classifier")
            reason = col.get("wrapper_reason")
            types_ = col.get("semantic_types")
            print("=" * 80)
            print("column:", name)
            if geo is not None:
                print("geo_classifier:", geo)
            if reason is not None:
                print("wrapper_reason:", reason)
            if types_ is not None:
                print("semantic_types:", types_)

    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    write_geo_results(geo_results_path, dataset_name, columns)

    if print_details:
        print(f"\nSaved full metadata to {metadata_path}")
        print(f"Saved geo classifier results to {geo_results_path}")

    return {
        "dataset_name": dataset_name,
        "mode": mode,
        "metadata_path": str(metadata_path),
        "geo_results_path": str(geo_results_path),
    }


# Write final labeled columns to CSV.
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


# Run Atlas for every dataset in the data folder.
def run_all_datasets(
    *,
    use_wrapper: bool = USE_WRAPPER,
    geo_threshold: float = GEO_THRESHOLD,
    out_dir: Path = OUT_DIR,
    print_details: bool = True,
) -> list[dict]:
    results = []
    for path in sorted(Path("data").glob("*.csv")):
        if print_details:
            print("\n" + "#" * 80)
        results.append(
            run_dataset(
                str(path),
                use_wrapper=use_wrapper,
                geo_threshold=geo_threshold,
                out_dir=out_dir,
                print_details=print_details,
            )
        )
    return results


# Run the configured Atlas entry point.
def main() -> None:
    if RUN_ALL_DATASETS:
        run_all_datasets(
            use_wrapper=USE_WRAPPER,
            geo_threshold=GEO_THRESHOLD,
            out_dir=OUT_DIR,
            print_details=True,
        )
    else:
        run_dataset(
            DATA_PATH,
            use_wrapper=USE_WRAPPER,
            geo_threshold=GEO_THRESHOLD,
            out_dir=OUT_DIR,
            print_details=True,
        )


if __name__ == "__main__":
    main()

from nyc_atlas_core import DEFAULT_GEO_THRESHOLD, process_all_datasets, process_dataset_outputs


DATA_PATH = "data/street_trees.csv"
RUN_ALL_DATASETS = True
GEO_THRESHOLD = DEFAULT_GEO_THRESHOLD


# Run the configured Atlas entry point.
def main() -> None:
    if RUN_ALL_DATASETS:
        process_all_datasets(
            geo_threshold=GEO_THRESHOLD,
            print_details=True,
        )
    else:
        process_dataset_outputs(
            DATA_PATH,
            geo_threshold=GEO_THRESHOLD,
            print_details=True,
        )


if __name__ == "__main__":
    main()

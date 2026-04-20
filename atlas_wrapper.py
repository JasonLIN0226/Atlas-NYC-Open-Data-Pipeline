import re
from pathlib import Path

import pandas as pd
from profiler import types
from profiler.core import GEO_CLASSIFIER_SPATIAL_MAP


_ADMIN_LIKE_NAMES = {
    "borough",
    "boroughcode",
    "borocode",
    "borocd",
    "boroname",
    "boro",
    "boro_ct",
    "park_borough",
    "taxi_company_borough",
    "council_district",
    "councildistrict",
    "community_district",
    "community_board",
    "communityboard",
    "nta",
    "nta_code",
    "ntacode",
    "nta_name",
    "ntaname",
    "coundist",
    "census_tract",
    "police_precinct",
    "city",
    "zip_city",
}
_BOROUGH_NAMES = {
    "BRONX",
    "BROOKLYN",
    "MANHATTAN",
    "QUEENS",
    "STATEN ISLAND",
    "NEW YORK",
    "UNSPECIFIED",
}
_BOROUGH_CODES = {"1", "2", "3", "4", "5"}
_BOROUGH_SHORT_CODES = {"B", "Q", "M", "R", "X", "BX", "BK", "MN", "QN", "SI"}


# Apply wrapper rules to Atlas output.
def apply_wrapper(data_path: str, metadata: dict) -> dict:
    samples = _load_samples(data_path)
    changes = []
    pair_flags = _detect_xy_pairs(samples)

    for column in metadata.get("columns", []):
        name = column["name"]
        norm = _normalize(name)
        values = samples.get(name, [])
        label = column.get("geo_classifier", {}).get("label")
        hint = _coord_name_hint(norm)

        if _city_name_hint(norm) and _looks_city_like(values) and label != "city":
            _set_geo(column, "city", 0.95, "city_name_and_values", changes)
            continue

        if _admin_name_hint(norm) and _looks_admin_like(values) and label not in {
            "borough",
            "borough_code",
        }:
            _set_geo(column, "borough_code", 0.95, "admin_name_and_values", changes)
            continue

        if pair_flags.get(norm) == "x" and label != "x_coord":
            _set_geo(column, "x_coord", 0.95, "x_coord_xy_pair", changes)
            continue

        if pair_flags.get(norm) == "y" and label != "y_coord":
            _set_geo(column, "y_coord", 0.95, "y_coord_xy_pair", changes)
            continue

        if norm == "bin" and _is_bin(values):
            _set_geo(column, "bin", 0.99, "bin_name_and_pattern", changes)
            continue

        if norm == "bbl" and _is_bbl(values):
            _set_geo(column, "bbl", 0.99, "bbl_name_and_pattern", changes)
            continue

        if "zip" in norm and _is_zip5(values):
            _set_geo(column, "zip5", 0.99, "zip_name_and_pattern", changes)
            continue

        if hint == "x" and _looks_projected_coord(values) and label != "x_coord":
            _set_geo(column, "x_coord", 0.95, "x_coord_name_and_range", changes)
            continue

        if hint == "y" and _looks_projected_coord(values) and label != "y_coord":
            _set_geo(column, "y_coord", 0.95, "y_coord_name_and_range", changes)
            continue

        if norm == "primaryaddresspointid" and label == "x_coord":
            _clear_geo(column, "clear_false_x_coord_identifier", changes)
            column["structural_type"] = types.INTEGER
            column["semantic_types"] = _merge_semantic_types(column, [types.ID])
            continue

        if norm == "boro_ct" and label == "x_coord":
            _clear_geo(column, "clear_false_x_coord_admin_code", changes)
            column["structural_type"] = types.INTEGER
            continue

        if label in {"x_coord", "y_coord"} and not hint and _looks_code_like(values):
            _clear_geo(column, "clear_coord_on_code_like_values", changes)
            continue

        if label in {"borough", "borough_code"} and not _keep_admin_like(norm, values):
            _clear_geo(column, "clear_false_admin_like", changes)
            column["structural_type"] = types.TEXT

    metadata["_wrapper_summary"] = {
        "changed_count": len(changes),
        "changes": changes,
    }
    return metadata


# Load sample values from one dataset.
def _load_samples(data_path: str, nrows: int = 300, max_values: int = 10) -> dict:
    frame = pd.read_csv(Path(data_path), dtype=str, nrows=nrows).fillna("")
    samples = {}
    for name in frame.columns:
        values = []
        for value in frame[name]:
            value = value.strip()
            if value and value not in values:
                values.append(value)
            if len(values) >= max_values:
                break
        samples[name] = values
    return samples


# Normalize one column name.
def _normalize(name: str) -> str:
    return name.strip().lower()


# Check whether values look like BINs.
def _is_bin(values: list[str]) -> bool:
    return bool(values) and all(re.fullmatch(r"[1-5]\d{6}", value) for value in values[:5])


# Check whether values look like BBLs.
def _is_bbl(values: list[str]) -> bool:
    return bool(values) and all(re.fullmatch(r"[1-5]\d{9}", value) for value in values[:5])


# Check whether values look like ZIP codes.
def _is_zip5(values: list[str]) -> bool:
    return bool(values) and all(re.fullmatch(r"\d{5}", value) for value in values[:5])


# Infer whether a name hints at X or Y coordinates.
def _coord_name_hint(name: str) -> str | None:
    if name.startswith("x_coordinate") or name in {"x_sp", "x_coord", "xcoord"}:
        return "x"
    if name.startswith("y_coordinate") or name in {"y_sp", "y_coord", "ycoord"}:
        return "y"
    return None


# Check whether a name hints at city values.
def _city_name_hint(name: str) -> bool:
    return name in {"city", "zip_city"} or name.endswith("_city")


# Check whether a name hints at admin values.
def _admin_name_hint(name: str) -> bool:
    if name in _ADMIN_LIKE_NAMES:
        return True
    return any(
        token in name
        for token in ("borough", "district", "board", "precinct", "tract", "nta", "boro")
    )


# Detect simple X and Y column pairs.
def _detect_xy_pairs(samples: dict[str, list[str]]) -> dict[str, str]:
    x_values = samples.get("x", [])
    y_values = samples.get("y", [])
    if _looks_projected_coord(x_values) and _looks_projected_coord(y_values):
        return {"x": "x", "y": "y"}
    return {}


# Check whether values look like projected coordinates.
def _looks_projected_coord(values: list[str]) -> bool:
    if not values:
        return False
    numbers = []
    for value in values[:5]:
        try:
            numbers.append(abs(float(value)))
        except ValueError:
            return False
    return all(10_000 <= number <= 2_000_000 for number in numbers)


# Check whether values look like city names.
def _looks_city_like(values: list[str]) -> bool:
    if not values:
        return False
    for value in values[:5]:
        upper = value.upper().strip()
        if not re.fullmatch(r"[A-Z][A-Z .'-]{1,40}", upper):
            return False
    return True


# Check whether values look admin like.
def _looks_admin_like(values: list[str]) -> bool:
    if not values:
        return False
    upper_values = [value.upper().strip() for value in values[:5]]
    if set(upper_values) <= (_BOROUGH_NAMES | _BOROUGH_CODES | _BOROUGH_SHORT_CODES):
        return True
    patterns = [
        r"\d{1,3}",
        r"\d{1,3}\.0+",
        r"\d{1,2}\s+[A-Z ]+",
        r"PRECINCT\s+\d{1,3}",
        r"[A-Z]{2}\d{2}",
        r"\d{3,6}",
        r"\d{3,6}\.0+",
    ]
    for value in upper_values:
        if re.fullmatch(r"[A-Z][A-Z .'-]{2,50}", value):
            continue
        if any(re.fullmatch(pattern, value) for pattern in patterns):
            continue
        return False
    return True


# Check whether values look like compact codes.
def _looks_code_like(values: list[str]) -> bool:
    if not values:
        return False
    return all(re.fullmatch(r"[A-Z]{2}\d{2}|\d{4,8}", value.upper()) for value in values[:5])


# Decide whether an admin label should be kept.
def _keep_admin_like(name: str, values: list[str]) -> bool:
    if name == "steward":
        return False
    if name in _ADMIN_LIKE_NAMES:
        return True
    if any(token in name for token in ("borough", "district", "board", "community", "nta")):
        return True

    upper_values = {value.upper() for value in values[:5]}
    if upper_values and upper_values <= (
        _BOROUGH_NAMES | _BOROUGH_CODES | _BOROUGH_SHORT_CODES
    ):
        return True
    if all(re.fullmatch(r"\d{1,2}\s+[A-Z ]+", value.upper()) for value in values[:5]):
        return True
    if all(re.fullmatch(r"[A-Z]{2}\d{2}", value.upper()) for value in values[:5]):
        return True
    return False


# Set a final geo label on one column.
def _set_geo(column: dict, label: str, confidence: float, reason: str, changes: list[dict]) -> None:
    previous = column.get("geo_classifier", {}).get("label")
    structural_type, semantic_types = GEO_CLASSIFIER_SPATIAL_MAP[label]
    if previous in GEO_CLASSIFIER_SPATIAL_MAP:
        _, previous_semantic_types = GEO_CLASSIFIER_SPATIAL_MAP[previous]
        current = column.get("semantic_types", [])
        column["semantic_types"] = [
            item for item in current if item not in previous_semantic_types
        ]
    column["geo_classifier"] = {
        "label": label,
        "confidence": confidence,
        "source": "wrapper",
    }
    column["structural_type"] = structural_type
    column["semantic_types"] = _merge_semantic_types(column, semantic_types)
    column["wrapper_reason"] = reason
    _record_change(changes, column["name"], "set", previous, label, reason)


# Clear a geo label from one column.
def _clear_geo(column: dict, reason: str, changes: list[dict]) -> None:
    previous = column.get("geo_classifier", {}).get("label")
    if previous in GEO_CLASSIFIER_SPATIAL_MAP:
        _, semantic_types = GEO_CLASSIFIER_SPATIAL_MAP[previous]
        current = column.get("semantic_types", [])
        column["semantic_types"] = [item for item in current if item not in semantic_types]
    column.pop("geo_classifier", None)
    column["wrapper_reason"] = reason
    _record_change(changes, column["name"], "clear", previous, None, reason)


# Merge semantic types without duplicates.
def _merge_semantic_types(column: dict, new_types: list[str]) -> list[str]:
    merged = list(column.get("semantic_types", []))
    for semantic_type in new_types:
        if semantic_type not in merged:
            merged.append(semantic_type)
    return merged


# Record one wrapper change.
def _record_change(
    changes: list[dict],
    name: str,
    action: str,
    old_label: str | None,
    new_label: str | None,
    reason: str,
) -> None:
    changes.append(
        {
            "name": name,
            "action": action,
            "old_label": old_label,
            "new_label": new_label,
            "reason": reason,
        }
    )

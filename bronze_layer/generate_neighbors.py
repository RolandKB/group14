#!/usr/bin/env python3
"""Generate a municipality-neighbor JSON file from Kartverket GeoJSON.

Input may be the downloaded .zip or an extracted .geojson file.
Requires: geopandas and shapely

Output shape:
{
  "0301": ["0302", "0303"],
  "0302": ["0301"]
}

Adjacency means the official municipality geometries touch. This includes
shared land/water boundaries represented by the source geometries and
point-only contacts. It does not infer adjacency just because two places
are across the same fjord.
"""

import argparse
import hashlib
import json
import os
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import geopandas as gpd
from shapely.strtree import STRtree

DEFAULT_BRONZE_ROOT = Path("data/bronze")
DATASET = "geonorge/administrative_enheter_kommuner"


def sha256_file(path: Path) -> str:
    """Return the SHA-256 checksum for a file without loading it all into memory."""
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()

def load_geodata(path: Path):
    """Read a GeoJSON directly or find and extract one from a ZIP archive."""
    if path.suffix.lower() != ".zip":
        return gpd.read_file(path)

    with zipfile.ZipFile(path) as archive:
        geojson_names = [
            name for name in archive.namelist()
            if name.lower().endswith((".geojson", ".json"))
        ]
        if len(geojson_names) != 1:
            raise ValueError(
                f"Expected one GeoJSON file in ZIP; found {geojson_names}"
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            extracted = Path(temp_dir) / Path(geojson_names[0]).name
            extracted.write_bytes(archive.read(geojson_names[0]))
            # Read while the temporary extracted file still exists.
            return gpd.read_file(extracted)


def normalize_code(value):
    if value is None:
        raise ValueError("A municipality feature has no kommunenummer.")

    code = str(value).strip()
    if code.endswith(".0"):
        code = code[:-2]

    if not code.isdigit() or len(code) > 4:
        raise ValueError(f"Unexpected municipality code: {value!r}")

    return code.zfill(4)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Create a JSON map of Norwegian municipality neighbors."
    )
    parser.add_argument("input", type=Path, help="Kartverket .zip or .geojson")
    parser.add_argument(
        "-o", "--output", type=Path,
        help="Output JSON path (default: <bronze-root>/geonorge/administrative_enheter_kommuner/municipality-neighbors.json)",
    )
    parser.add_argument(
        "--bronze-root", type=Path, default=DEFAULT_BRONZE_ROOT,
        help="Bronze data root (default: data/bronze)",
    )
    parser.add_argument(
        "--code-column", default="kommunenummer",
        help="Municipality-code column (default: kommunenummer)",
    )
    parser.add_argument(
        "--type-column", default="objtype",
        help="Feature-type column (default: objtype)",
    )
    parser.add_argument(
        "--municipality-type", default="Kommune",
        help="Value identifying municipality polygons (default: Kommune)",
    )
    args = parser.parse_args(argv)
    output = args.output or (
        args.bronze_root / DATASET / "municipality-neighbors.json"
    )

    if not args.input.exists():
        sys.exit(f"Input file not found: {args.input}")

    try:
        gdf = load_geodata(args.input)
    except Exception as exc:
        sys.exit(f"Could not read input dataset: {exc}")

    for column in (args.code_column, args.type_column):
        if column not in gdf.columns:
            sys.exit(
                f"Required column {column!r} not found. "
                f"Available columns: {list(gdf.columns)}"
            )

    # Kartverket's file contains both municipality polygons and thousands
    # of boundary-line features. Only use the municipality polygon records.
    gdf = gdf[gdf[args.type_column] == args.municipality_type].copy()
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()

    if gdf.empty:
        sys.exit(
            f"No features where {args.type_column!r} == "
            f"{args.municipality_type!r}. Check the input or options."
        )

    gdf["code"] = gdf[args.code_column].map(normalize_code)

    if gdf["code"].duplicated().any():
        duplicates = sorted(
            gdf.loc[gdf["code"].duplicated(False), "code"].unique()
        )
        sys.exit(
            "Duplicate municipality polygons found for codes: "
            f"{duplicates}. Check that only municipality features are selected."
        )

    # Use a projected CRS for geometry calculations.
    if gdf.crs is None:
        sys.exit("Input has no coordinate reference system (CRS).")
    gdf = gdf.to_crs("EPSG:25833")

    codes = gdf["code"].tolist()
    geometries = gdf.geometry.tolist()
    tree = STRtree(geometries)
    neighbors = {code: set() for code in codes}

    for i, geom in enumerate(geometries):
        for candidate in tree.query(geom):
            j = int(candidate)
            if i != j and geom.touches(geometries[j]):
                neighbors[codes[i]].add(codes[j])

    result = {
        code: sorted(neighbors[code])
        for code in sorted(neighbors)
    }

    # Validate that every relationship is symmetric.
    asymmetric = [
        (a, b)
        for a, adjacent in result.items()
        for b in adjacent
        if a not in result.get(b, [])
    ]
    if asymmetric:
        sys.exit(f"Symmetry check failed: {asymmetric[:20]}")

    output_payload = json.dumps(result, indent=2, ensure_ascii=False) + "\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(output_payload, encoding="utf-8")

    metadata = {
        "source": "Geonorge",
        "dataset": "Administrative enheter kommuner - municipality neighbors",
        "source_file": str(args.input),
        "source_sha256": sha256_file(args.input),
        "source_metadata_file": (
            str(args.input.with_name(args.input.name + ".metadata.json"))
            if args.input.with_name(args.input.name + ".metadata.json").exists()
            else None
        ),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "filename": output.name,
        "format": "json",
        "size_bytes": output.stat().st_size,
        "sha256": sha256_file(output),
        "geographic_coverage": "Norway",
        "coordinate_reference_system": "EPSG:25833",
        "adjacency_definition": "Municipality geometries touch; includes point-only contacts.",
        "municipality_count": len(result),
        "unique_adjacency_pair_count": sum(map(len, result.values())) // 2,
        "municipality_codes_without_neighbors": [
            code for code, adjacent in result.items() if not adjacent
        ],
        "parameters": {
            "code_column": args.code_column,
            "type_column": args.type_column,
            "municipality_type": args.municipality_type,
        },
    }
    metadata_path = output.with_name(output.name + ".metadata.json")
    # write_json_atomic(metadata_path, metadata)metadata_path.parent.mkdir(parents=True, exist_ok=True)
    with metadata_path.open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2, ensure_ascii=False)
        handle.write("\n")

    pair_count = sum(len(adjacent) for adjacent in result.values()) // 2
    no_neighbors = [code for code, adjacent in result.items() if not adjacent]
    print(f"Wrote {len(result)} municipalities to {output}")
    print(f"Metadata: {metadata_path}")
    print(f"Unique adjacency pairs: {pair_count}")
    print(f"Symmetry check: passed")
    if no_neighbors:
        print(f"Warning: {len(no_neighbors)} municipalities have no neighbors: "
              f"{', '.join(no_neighbors)}")


if __name__ == "__main__":
    main()

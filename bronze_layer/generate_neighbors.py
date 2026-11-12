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

import hashlib
import logging
import tempfile
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd
from shapely.strtree import STRtree

from bronze_layer.bronze_common import write_json

LOG = logging.getLogger(__name__)
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
            name
            for name in archive.namelist()
            if name.lower().endswith((".geojson", ".json"))
        ]
        if len(geojson_names) != 1:
            raise ValueError(f"Expected one GeoJSON file in ZIP; found {geojson_names}")

        with tempfile.TemporaryDirectory() as temp_dir:
            extracted = Path(temp_dir) / Path(geojson_names[0]).name
            extracted.write_bytes(archive.read(geojson_names[0]))
            # Read while the temporary extracted file still exists.
            return gpd.read_file(extracted)


def normalize_code(value):
    if value is None:
        raise ValueError("A municipality feature has no kommunenummer.")

    code = str(value).strip()
    code = code.removesuffix(".0")

    if not code.isdigit() or len(code) > 4:
        raise ValueError(f"Unexpected municipality code: {value!r}")

    return code.zfill(4)


def generate_neighbors(
    input_path: Path,
    output: Path | None = None,
    bronze_root: Path = DEFAULT_BRONZE_ROOT,
    code_column: str = "kommunenummer",
    type_column: str = "objtype",
    municipality_type: str = "Kommune",
) -> Path:
    """Create and write a JSON map of Norwegian municipality neighbors."""
    LOG.info("Generating municipality neighbors from %s", input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    try:
        gdf = load_geodata(input_path)
    except Exception as exc:
        raise ValueError(f"Could not read input dataset: {exc}") from exc

    output = output or bronze_root / DATASET / "municipality-neighbors.json"

    for column in (code_column, type_column):
        if column not in gdf.columns:
            raise ValueError(
                f"Required column {column!r} not found. "
                f"Available columns: {list(gdf.columns)}"
            )

    # Kartverket's file contains both municipality polygons and thousands
    # of boundary-line features. Only use the municipality polygon records.
    gdf = gdf[gdf[type_column] == municipality_type].copy()
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()

    if gdf.empty:
        raise ValueError(
            f"No features where {type_column!r} == "
            f"{municipality_type!r}. Check the input or options."
        )

    gdf["code"] = gdf[code_column].map(normalize_code)

    if gdf["code"].duplicated().any():
        duplicates = sorted(gdf.loc[gdf["code"].duplicated(False), "code"].unique())
        raise ValueError(
            "Duplicate municipality polygons found for codes: "
            f"{duplicates}. Check that only municipality features are selected."
        )

    # Use a projected CRS for geometry calculations.
    if gdf.crs is None:
        raise ValueError("Input has no coordinate reference system (CRS).")
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

    result = {code: sorted(neighbors[code]) for code in sorted(neighbors)}

    # Validate that every relationship is symmetric.
    asymmetric = [
        (a, b)
        for a, adjacent in result.items()
        for b in adjacent
        if a not in result.get(b, [])
    ]
    if asymmetric:
        raise ValueError(f"Symmetry check failed: {asymmetric[:20]}")

    write_json(output, result)

    metadata = {
        "source": "Geonorge",
        "dataset": "Administrative enheter kommuner - municipality neighbors",
        "source_file": str(input_path),
        "source_sha256": sha256_file(input_path),
        "source_metadata_file": (
            str(input_path.with_name(input_path.name + ".metadata.json"))
            if input_path.with_name(input_path.name + ".metadata.json").exists()
            else None
        ),
        "generated_at_utc": datetime.now(UTC).isoformat(),
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
            "code_column": code_column,
            "type_column": type_column,
            "municipality_type": municipality_type,
        },
    }
    metadata_path = output.with_name(output.name + ".metadata.json")
    write_json(metadata_path, metadata)
    LOG.info(
        "Wrote neighbors for %d municipalities and %d adjacency pairs to %s",
        len(result),
        sum(map(len, result.values())) // 2,
        output,
    )

    return output

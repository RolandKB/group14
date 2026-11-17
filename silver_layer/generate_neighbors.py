"""Generate a municipality-neighbor JSON file from Kartverket GeoJSON.

Input: a GeoJSON file or a ZIP containing exactly one GeoJSON.
Adjacency: geometries touch, including point-only contacts.
"""

import hashlib
import json
import logging
import tempfile
from uuid import uuid4
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import geopandas as gpd

LOG = logging.getLogger(__name__)
DEFAULT_SILVER_ROOT = Path("data/silver")
DATASET = "geonorge/administrative_enheter_kommuner"


def ensure_run_id(run_id: str | None = None) -> str:
    return run_id or uuid4().hex


def write_json(path: Path, payload: object) -> None:
    print(f"Writing {path}...")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")

def sha256_file(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def load_geodata(path: Path) -> gpd.GeoDataFrame:
    if path.suffix.lower() != ".zip":
        return gpd.read_file(path)

    with zipfile.ZipFile(path) as archive:
        names = [
            info.filename
            for info in archive.infolist()
            if not info.is_dir() and info.filename.lower().endswith((".geojson", ".json"))
        ]
        if len(names) != 1:
            raise ValueError(f"Expected exactly one GeoJSON in ZIP; found {names}")

        with tempfile.TemporaryDirectory() as directory:
            extracted = Path(directory) / "municipalities.geojson"
            with archive.open(names[0]) as source, extracted.open("wb") as target:
                import shutil
                shutil.copyfileobj(source, target)
            return gpd.read_file(extracted)


def normalize_code(value: object) -> str:
    if value is None:
        raise ValueError("A municipality feature has no kommunenummer")
    code = str(value).strip().removesuffix(".0")
    if not code.isdigit() or len(code) > 4:
        raise ValueError(f"Unexpected municipality code: {value!r}")
    return code.zfill(4)


def generate_neighbors(
    input_path: Path,
    output: Path | None = None,
    silver_root: Path = DEFAULT_SILVER_ROOT,
    code_column: str = "kommunenummer",
    type_column: str = "objtype",
    municipality_type: str = "Kommune",
    run_id: str | None = None,
) -> Path:
    """Write a symmetric mapping of municipality codes to touching municipalities."""
    run_id = ensure_run_id(run_id)
    input_path = Path(input_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    try:
        gdf = load_geodata(input_path)
    except Exception as exc:
        raise ValueError(f"Could not read input dataset: {exc}") from exc

    for column in (code_column, type_column):
        if column not in gdf.columns:
            raise ValueError(f"Required column {column!r} not found; available: {list(gdf.columns)}")

    gdf = gdf.loc[gdf[type_column].eq(municipality_type)].copy()
    gdf = gdf.loc[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    if gdf.empty:
        raise ValueError(f"No nonempty municipality geometries with {type_column}={municipality_type!r}")
    if gdf.crs is None:
        raise ValueError("Input has no coordinate reference system (CRS)")

    gdf["code"] = gdf[code_column].map(normalize_code)
    if gdf["code"].duplicated().any():
        duplicates = sorted(gdf.loc[gdf["code"].duplicated(keep=False), "code"].unique())
        raise ValueError(f"Duplicate municipality codes: {duplicates}")
    if not gdf.geometry.is_valid.all():
        invalid = gdf.loc[~gdf.geometry.is_valid, "code"].tolist()
        raise ValueError(f"Invalid municipality geometries: {invalid}")

    polygons = gdf[["code", "geometry"]]
    pairs = gpd.sjoin(polygons, polygons, how="inner", predicate="touches", lsuffix="left", rsuffix="right")
    neighbors = {code: set() for code in polygons["code"]}
    for a, b in zip(pairs["code_left"], pairs["code_right"]):
        if a != b:
            neighbors[a].add(b)
            neighbors[b].add(a)

    result = {code: sorted(neighbors[code]) for code in sorted(neighbors)}
    if any(a not in result.get(b, []) for a, adjacent in result.items() for b in adjacent):
        raise ValueError("Symmetry validation failed")

    output = Path(output) if output is not None else silver_root / DATASET / "municipality_neighbors.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    write_json(output, result)

    metadata = {
        "run_id": run_id,
        "source": "Kartverket / Geonorge",
        "dataset": DATASET,
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
        "coordinate_reference_system": str(gdf.crs),
        "adjacency_definition": "Municipality geometries touch; includes point-only contacts.",
        "municipality_count": len(result),
        "unique_adjacency_pair_count": sum(map(len, result.values())) // 2,
        "municipality_codes_without_neighbors": [code for code, adjacent in result.items() if not adjacent],
        "parameters": {
            "code_column": code_column,
            "type_column": type_column,
            "municipality_type": municipality_type,
        },
    }
    write_json(output.with_name(output.name + ".metadata.json"), metadata)
    LOG.info("Run %s: wrote %d municipalities and %d pairs to %s", run_id, len(result), metadata["unique_adjacency_pair_count"], output)
    return output

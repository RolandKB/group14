"""Create a municipality-neighbor mapping from Kartverket GeoJSON."""

import hashlib
import json
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import geopandas as gpd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze" / "kartverket"
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
DATASET = "geonorge/administrative_enheter_kommuner"


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
        file.write("\n")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_geodata(path: Path) -> gpd.GeoDataFrame:
    """Read a GeoJSON file, or the single GeoJSON inside a ZIP archive."""
    if path.suffix.lower() != ".zip":
        return gpd.read_file(path)

    with zipfile.ZipFile(path) as archive:
        geojson_files = [
            name
            for name in archive.namelist()
            if not name.endswith("/") and name.lower().endswith((".geojson", ".json"))
        ]
        if len(geojson_files) != 1:
            raise ValueError(f"Expected one GeoJSON in ZIP, found: {geojson_files}")

        with tempfile.TemporaryDirectory() as directory:
            extracted = Path(directory) / "municipalities.geojson"
            with (
                archive.open(geojson_files[0]) as source,
                extracted.open("wb") as target,
            ):
                shutil.copyfileobj(source, target)
            return gpd.read_file(extracted)


def normalize_code(value: object) -> str:
    """Convert municipality codes such as 301 into '0301'."""
    if value is None:
        raise ValueError("Missing municipality code")
    code = str(value).strip().removesuffix(".0")
    if not code.isdigit() or len(code) > 4:
        raise ValueError(f"Invalid municipality code: {value!r}")
    return code.zfill(4)


def generate_neighbors(
    input_path: str | Path,
    output: str | Path | None = None,
    silver_root: str | Path = SILVER_ROOT,
    code_column: str = "kommunenummer",
    type_column: str = "objtype",
    municipality_type: str = "Kommune",
    run_id: str | None = None,
) -> Path:
    """Generate and save a symmetric municipality-to-neighbors JSON mapping."""
    run_id = run_id or uuid4().hex
    input_path = Path(input_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"Kartverket file not found: {input_path}")

    gdf = load_geodata(input_path)
    for column in (code_column, type_column):
        if column not in gdf.columns:
            raise ValueError(
                f"Missing column {column!r}. Available: {list(gdf.columns)}"
            )

    gdf = gdf.loc[gdf[type_column] == municipality_type].copy()
    gdf = gdf.loc[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    if gdf.empty:
        raise ValueError("No municipality geometries found")
    if gdf.crs is None:
        raise ValueError("Input GeoJSON has no coordinate reference system (CRS)")

    gdf["code"] = gdf[code_column].map(normalize_code)
    if gdf["code"].duplicated().any():
        duplicates = sorted(
            gdf.loc[gdf["code"].duplicated(keep=False), "code"].unique()
        )
        raise ValueError(f"Duplicate municipality codes: {duplicates}")
    if not gdf.geometry.is_valid.all():
        invalid = gdf.loc[~gdf.geometry.is_valid, "code"].tolist()
        raise ValueError(f"Invalid municipality geometries: {invalid}")

    polygons = gdf[["code", "geometry"]]
    touching = gpd.sjoin(
        polygons,
        polygons,
        how="inner",
        predicate="touches",
        lsuffix="left",
        rsuffix="right",
    )

    neighbors = {code: set() for code in polygons["code"]}
    for left, right in zip(touching["code_left"], touching["code_right"]):
        if left != right:
            neighbors[left].add(right)
            neighbors[right].add(left)

    result = {code: sorted(neighbors[code]) for code in sorted(neighbors)}
    if any(
        a not in result.get(b, []) for a, adjacent in result.items() for b in adjacent
    ):
        raise ValueError("Neighbor symmetry check failed")

    if output is None:
        output = Path(silver_root) / DATASET / "municipality_neighbors.json"
    output = Path(output)
    write_json(output, result)

    metadata = {
        "run_id": run_id,
        "source": "Kartverket / Geonorge",
        "dataset": DATASET,
        "source_file": str(input_path),
        "source_sha256": sha256_file(input_path),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "filename": output.name,
        "sha256": sha256_file(output),
        "size_bytes": output.stat().st_size,
        "coordinate_reference_system": str(gdf.crs),
        "adjacency_definition": "Geometries touch, including point-only contacts",
        "municipality_count": len(result),
        "unique_adjacency_pair_count": sum(len(items) for items in result.values())
        // 2,
        "municipality_codes_without_neighbors": [
            code for code, items in result.items() if not items
        ],
        "parameters": {
            "code_column": code_column,
            "type_column": type_column,
            "municipality_type": municipality_type,
        },
    }
    write_json(output.with_name(output.name + ".metadata.json"), metadata)
    print(f"Saved {len(result)} municipalities to {output}")
    return output

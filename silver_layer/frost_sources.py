"""Frost station metadata: Bronze to Silver station Parquet."""

import json
import shutil
from pathlib import Path

from pyspark.sql import types as T

from silver_layer import cache

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
DEFAULT_SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
DATASET = "frost_sources"
VERSION = "frost-sources-v1"

STATION_SCHEMA = T.StructType(
    [
        T.StructField("station_id", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("short_name", T.StringType()),
        T.StructField("municipality_code", T.StringType()),
        T.StructField("municipality_name", T.StringType()),
        T.StructField("county_code", T.StringType()),
        T.StructField("county_name", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("elevation_m", T.DoubleType()),
        T.StructField("valid_from", T.StringType()),
        T.StructField("valid_to", T.StringType()),
    ]
)


QUARANTINE_SCHEMA = T.StructType(
    STATION_SCHEMA.fields
    + [
        T.StructField("quarantine_reason", T.StringType()),
        T.StructField("_source_file", T.StringType()),
    ]
)


def source_file(bronze_root):
    path = Path(bronze_root) / "frost" / "sources" / "country=NO" / "response.json"
    if not path.is_file():
        raise FileNotFoundError(f"Missing Frost Bronze sources: {path}")
    return path


def clean_text(value):
    return str(value).strip() if value is not None else None


def number(value):
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def parse_station(item):
    """Extract one Frost source. GeoJSON coordinates are [longitude, latitude]."""
    geometry = item.get("geometry") or {}
    coords = geometry.get("coordinates") if isinstance(geometry, dict) else None
    if not isinstance(coords, list) or len(coords) < 2:
        coords = [None, None]
    # Frost uses municipalityId/countyId for codes, and municipality/county for names.
    municipality_code = clean_text(item.get("municipalityId"))
    county_code = clean_text(item.get("countyId"))
    return (
        clean_text(item.get("id")),
        clean_text(item.get("name")),
        clean_text(item.get("shortName")),
        municipality_code,
        clean_text(item.get("municipality")),
        county_code,
        clean_text(item.get("county")),
        clean_text(item.get("countryCode")),
        number(coords[1]),
        number(coords[0]),
        number(item.get("masl")),
        clean_text(item.get("validFrom")),
        clean_text(item.get("validTo")),
    )


def validate_station(row):
    station_id, _, _, municipality, _, _, _, country, lat, lon, *_ = row
    if not station_id or not station_id.startswith("SN"):
        return "INVALID_STATION_ID"
    if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return "INVALID_COORDINATES"
    if country and country.upper() != "NO":
        return "NON_NORWEGIAN_STATION"
    if municipality and (not municipality.isdigit() or len(municipality) > 4):
        return "INVALID_MUNICIPALITY"
    return None


def prepare(payload, path):
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise ValueError(f"Invalid Frost sources JSON structure: {path}")
    accepted, rejected, seen = [], [], set()
    for item in payload["data"]:
        if not isinstance(item, dict):
            rejected.append(
                (None,) * len(STATION_SCHEMA.fields)
                + ("INVALID_SOURCE_RECORD", str(path))
            )
            continue
        row = parse_station(item)
        reason = validate_station(row)
        if reason is None and row[0] in seen:
            reason = "DUPLICATE_STATION_ID"
        if reason:
            rejected.append(row + (reason, str(path)))
        else:
            accepted.append(row)
            seen.add(row[0])
    return accepted, rejected


def quarantine_cached(output, fingerprint):
    """An empty quarantine is a valid result."""
    output = Path(output)
    if not (output / "_SUCCESS").is_file() or not any(output.glob("*.parquet")):
        return False
    try:
        manifest = cache.read_manifest(output)
        return (
            manifest.get("fingerprint") == fingerprint
            and isinstance(manifest.get("rows"), int)
            and manifest["rows"] >= 0
        )
    except (OSError, ValueError, TypeError):
        return False


def ingest(
    bronze_root=DEFAULT_BRONZE_ROOT,
    silver_root=DEFAULT_SILVER_ROOT,
    start="2024-01-01",
    end="2026-01-01",
    spark=None,
    run_id=None,
):
    """Write station metadata and quarantine. Return a dict with the result and the number of rows written."""
    if spark is None:
        raise ValueError("Pass an existing SparkSession using spark=...")

    if start >= end:
        raise ValueError("start must precede end")

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    path = source_file(bronze_root)
    output = Path(silver_root) / "frost" / "stations"
    quarantine_output = Path(silver_root) / "quarantine" / "frost_sources"
    fingerprint = cache.fingerprint(path, VERSION, start, end)

    if cache.is_cached(output, fingerprint) and quarantine_cached(
        quarantine_output, fingerprint
    ):
        manifest = cache.read_manifest(output)
        q_manifest = cache.read_manifest(quarantine_output)

        return {
            "dataset": DATASET,
            "status": "cached",
            "rows": manifest["rows"],
            "quarantined_rows": q_manifest["rows"],
            "silver_path": str(output),
            "quarantine_path": str(quarantine_output),
        }

    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)

    accepted, rejected = prepare(payload, path)
    if not accepted:
        raise ValueError("No valid Frost stations in Bronze")

    stations = spark.createDataFrame(accepted, STATION_SCHEMA)
    quarantine = spark.createDataFrame(rejected, QUARANTINE_SCHEMA)
    stages = [
        cache.new_staging_directory(output),
        cache.new_staging_directory(quarantine_output),
    ]

    try:
        stations.coalesce(1).write.mode("overwrite").parquet(str(stages[0]))
        quarantine.coalesce(1).write.mode("overwrite").parquet(str(stages[1]))

        # Publish each completed dataset to the cache.
        for stage, dest, count in zip(
            stages, (output, quarantine_output), (len(accepted), len(rejected))
        ):
            cache.publish(
                stage,
                dest,
                {
                    "dataset": DATASET,
                    "fingerprint": fingerprint,
                    "run_id": run_id,
                    "rows": count,
                },
            )
    finally:
        for stage in stages:
            if stage.exists():
                shutil.rmtree(stage)

    result = {
        "dataset": DATASET,
        "status": "success",
        "rows": len(accepted),
        "quarantined_rows": len(rejected),
        "silver_path": str(output),
        "quarantine_path": str(quarantine_output),
    }

    print(json.dumps(result, indent=2), flush=True)

    return result

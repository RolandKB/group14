"""Sime municipality-price-area relationships. Also validated against Kartverket."""

import hashlib
import json
import shutil
import zipfile
from collections import Counter
from pathlib import Path

from pyspark.sql import types as T
from silver_layer import cache

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
DEFAULT_SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
DATASET = "municipality_price_areas"
VERSION = "municipality-price-areas-v1"
AREAS = {"NO1", "NO2", "NO3", "NO4", "NO5"}

SILVER_SCHEMA = T.StructType(
    [
        T.StructField("municipality_code", T.StringType(), False),
        T.StructField("municipality_name", T.StringType(), True),
        T.StructField("price_area", T.StringType(), False),
        T.StructField("primary_price_area", T.StringType(), False),
        T.StructField("is_overlapping", T.BooleanType(), False),
    ]
)

QUARANTINE_SCHEMA = T.StructType(
    [
        T.StructField("municipality_code", T.StringType(), True),
        T.StructField("municipality_name", T.StringType(), True),
        T.StructField("primary_price_area", T.StringType(), True),
        T.StructField("price_areas", T.StringType(), True),
        T.StructField("quarantine_reason", T.StringType(), False),
        T.StructField("_source_file", T.StringType(), False),
    ]
)


def source_paths(bronze_root):
    root = Path(bronze_root)
    sime = root / "sime" / "municipalities" / "response.json"
    kartverket = root / "kartverket" / "Basisdata_0000_Norge_25833_Kommuner_GeoJSON.zip"
    for path in (sime, kartverket):
        if not path.is_file():
            raise FileNotFoundError(f"Missing Bronze source: {path}")
    return sime, kartverket


def source_fingerprint(sime, kartverket, start, end):
    digest = hashlib.sha256()

    for path in (sime, kartverket):
        digest.update(path.name.encode("utf-8"))
        digest.update(bytes.fromhex(cache.sha256_file(path)))

    return {
        "source_sha256": digest.hexdigest(),
        "transformation_version": VERSION,
        "start": start,
        "end_exclusive": end,
    }


def municipality_codes(zip_path):
    """Read Kartverket GeoJSON and return a set of valid municipality codes."""

    with zipfile.ZipFile(zip_path) as archive:
        names = [
            name
            for name in archive.namelist()
            if name.lower().endswith((".geojson", ".json"))
        ]

        if len(names) != 1:
            raise ValueError(f"Expected one Kartverket GeoJSON, found {len(names)}")

        with archive.open(names[0]) as handle:
            payload = json.load(handle)

    if payload.get("type") != "FeatureCollection" or not isinstance(
        payload.get("features"), list
    ):
        raise ValueError("Invalid Kartverket GeoJSON FeatureCollection")

    codes = []
    for feature in payload["features"]:
        properties = feature.get("properties") or {}
        if properties.get("objtype") != "Kommune":
            continue

        code = str(properties.get("kommunenummer") or "").strip()
        if len(code) != 4 or not code.isdigit():
            raise ValueError(f"Invalid Kartverket municipality code: {code!r}")

        codes.append(code)
    if not codes or len(codes) != len(set(codes)):
        raise ValueError("Kartverket municipality codes are empty or duplicated")

    return set(codes)


def prepare(sime_payload, kartverket_codes, source_path):
    """Quarantine a whole Sime municipality if its mapping is invalid."""
    if not isinstance(sime_payload, dict) or not isinstance(
        sime_payload.get("kommuner"), list
    ):
        raise ValueError("Invalid Sime JSON: expected a 'kommuner' array")

    accepted, rejected = [], []
    records = sime_payload["kommuner"]
    counts = Counter(
        str(item.get("number") or "").strip()
        for item in records
        if isinstance(item, dict)
    )

    for item in records:
        if not isinstance(item, dict):
            item = {}

        code = str(item.get("number") or "").strip() or None
        name = str(item.get("name") or "").strip() or None
        primary = str(item.get("priceArea") or "").strip() or None
        areas = item.get("priceAreas")
        area_text = json.dumps(areas, ensure_ascii=False)

        if not code or len(code) != 4 or not code.isdigit():
            reason = "INVALID_MUNICIPALITY_CODE"
        elif counts[code] > 1:
            reason = "DUPLICATE_MUNICIPALITY"
        elif code not in kartverket_codes:
            reason = "NOT_IN_KARTVERKET"
        elif not name:
            reason = "MISSING_MUNICIPALITY_NAME"
        elif (
            not isinstance(areas, list)
            or not areas
            or any(not isinstance(a, str) or a not in AREAS for a in areas)
        ):
            reason = "INVALID_PRICE_AREAS"
        elif len(areas) != len(set(areas)):
            reason = "DUPLICATE_PRICE_AREA"
        elif primary not in AREAS or primary not in areas:
            reason = "PRIMARY_AREA_MISMATCH"
        else:
            reason = None

        if reason:
            rejected.append((code, name, primary, area_text, reason, str(source_path)))
        else:
            for area in areas:
                accepted.append((code, name, area, primary, len(areas) > 1))

    return accepted, rejected


def _quarantine_cached(output, fingerprint):
    """Check if the quarantine dataset is cached and valid. Returns True if cached."""
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
    """Publish Silver mapping and row-level quarantine using the shared cache."""
    if spark is None:
        raise ValueError("Pass an existing SparkSession using spark=...")
    if start >= end:
        raise ValueError("start must precede end")

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    sime_path, kartverket_path = source_paths(bronze_root)
    output = Path(silver_root) / "geography" / "municipality_price_areas"
    quarantine_output = Path(silver_root) / "quarantine" / DATASET
    fingerprint = source_fingerprint(sime_path, kartverket_path, start, end)

    if cache.is_cached(output, fingerprint) and _quarantine_cached(
        quarantine_output, fingerprint
    ):
        return {
            "dataset": DATASET,
            "status": "cached",
            "rows": cache.read_manifest(output)["rows"],
            "quarantined_rows": cache.read_manifest(quarantine_output)["rows"],
            "silver_path": str(output),
            "quarantine_path": str(quarantine_output),
        }

    with sime_path.open(encoding="utf-8") as handle:
        payload = json.load(handle)

    codes = municipality_codes(kartverket_path)
    accepted, rejected = prepare(payload, codes, sime_path)

    if not accepted:
        raise ValueError("No valid municipality-price-area relationships")

    stations = spark.createDataFrame(accepted, SILVER_SCHEMA)
    quarantine = spark.createDataFrame(rejected, QUARANTINE_SCHEMA)
    stages = [
        cache.new_staging_directory(output),
        cache.new_staging_directory(quarantine_output),
    ]
    try:
        stations.coalesce(1).write.mode("overwrite").parquet(str(stages[0]))
        quarantine.coalesce(1).write.mode("overwrite").parquet(str(stages[1]))
        for stage, destination, rows in zip(
            stages, (output, quarantine_output), (len(accepted), len(rejected))
        ):
            cache.publish(
                stage,
                destination,
                {
                    "dataset": DATASET,
                    "fingerprint": fingerprint,
                    "run_id": run_id,
                    "rows": rows,
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

"""Transform Elhub hourly price-area consumption into Silver Parquet."""

import json
import shutil
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from pyspark.sql import Window, functions as F, types as T

from silver_layer import cache

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
DEFAULT_SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
DATASET = "CONSUMPTION_PER_GROUP_MBA_HOUR"
OUTPUT_NAME = "price_area_consumption"
TRANSFORMATION_VERSION = "price-area-v1"
COLUMNS = (
    "START_TIME",
    "END_TIME",
    "PRICE_AREA",
    "EIC",
    "CONSUMPTION_GROUP",
    "QUANTITY_KWH",
    "METERING_POINT_COUNT",
    "LAST_UPDATE_TIME",
)
TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
VALID_AREAS = ("NO1", "NO2", "NO3", "NO4", "NO5")
VALID_GROUPS = ("cabin", "household", "primary", "secondary", "tertiary")


def latest_archive(bronze_root):
    directory = Path(bronze_root) / "elhub" / DATASET
    archives = sorted(
        directory.glob("*.zip"),
        key=lambda p: (p.stat().st_mtime_ns, p.name),
        reverse=True,
    )
    if not archives:
        raise FileNotFoundError(f"No ZIP archive found in {directory}")
    if len(archives) > 1:
        print(f"Found {len(archives)} archives. Using newest: {archives[0].name}", flush=True)
    return archives[0]


def extract_csvs(archive_path, target_dir):
    """Extract ZIP CSV filenames without modifying Bronze."""
    paths = []
    with zipfile.ZipFile(archive_path) as archive:
        members = [
            member for member in archive.infolist()
            if not member.is_dir() and member.filename.lower().endswith(".csv")
        ]
        if not members:
            raise ValueError(f"No CSV files found inside {archive_path}")
        for index, member in enumerate(members):
            destination = Path(target_dir) / f"part-{index:05d}.csv"
            with archive.open(member) as source, destination.open("wb") as target:
                shutil.copyfileobj(source, target, length=8 * 1024 * 1024)
            paths.append(str(destination))
    return paths


def boundary_utc(date_text):
    local = datetime.strptime(date_text, "%Y-%m-%d").replace(
        tzinfo=ZoneInfo("Europe/Oslo")
    )
    return local.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def transform(raw, start="2024-01-01", end="2026-01-01"):
    """Return deduplicated Silver rows and invalid candidate rows."""
    missing = set(COLUMNS) - set(raw.columns)
    if missing:
        raise ValueError(f"Missing Elhub columns: {sorted(missing)}")

    start_utc, end_utc = boundary_utc(start), boundary_utc(end)
    if start_utc >= end_utc:
        raise ValueError("start must precede end")

    df = raw.select(
        F.to_timestamp("START_TIME", TIME_PATTERN).alias("start_time"),
        F.to_timestamp("END_TIME", TIME_PATTERN).alias("end_time"),
        F.upper(F.trim("PRICE_AREA")).alias("price_area"),
        F.trim("EIC").alias("eic"),
        F.lower(F.trim("CONSUMPTION_GROUP")).alias("consumption_group"),
        F.col("QUANTITY_KWH").cast(T.DecimalType(18, 6)).alias("quantity_kwh"),
        F.col("METERING_POINT_COUNT").cast("int").alias("metering_point_count"),
        F.to_timestamp("LAST_UPDATE_TIME", TIME_PATTERN).alias("last_update_time"),
    )

    in_period = (
        (F.col("start_time") >= F.lit(start_utc).cast("timestamp"))
        & (F.col("start_time") < F.lit(end_utc).cast("timestamp"))
    )
    # Keep rows with unparseable start times so validation can report them.
    candidates = df.where(in_period | F.col("start_time").isNull())
    invalid_condition = (
        F.col("start_time").isNull()
        | F.col("end_time").isNull()
        | F.col("last_update_time").isNull()
        | F.col("price_area").isNull()
        | (~F.col("price_area").isin(*VALID_AREAS))
        | F.col("eic").isNull()
        | (F.length("eic") == 0)
        | F.col("consumption_group").isNull()
        | (~F.col("consumption_group").isin(*VALID_GROUPS))
        | F.col("quantity_kwh").isNull()
        | F.col("metering_point_count").isNull()
        | (F.col("metering_point_count") < 0)
        | (F.col("end_time") != F.col("start_time") + F.expr("INTERVAL 1 HOUR"))
    )
    invalid = candidates.where(invalid_condition)
    valid = candidates.where(~invalid_condition)

    window = Window.partitionBy(
        "start_time", "price_area", "consumption_group", "eic"
    ).orderBy(
        F.col("last_update_time").desc(),
        F.col("end_time").desc(),
        F.col("quantity_kwh").desc(),
        F.col("metering_point_count").desc(),
    )
    silver = (
        valid.withColumn("_row_number", F.row_number().over(window))
        .where(F.col("_row_number") == 1)
        .drop("_row_number")
    )
    return silver, invalid


def ingest(
    bronze_root=DEFAULT_BRONZE_ROOT,
    silver_root=DEFAULT_SILVER_ROOT,
    start="2024-01-01",
    end="2026-01-01",
    spark=None,
    run_id=None,
):
    """Build or reuse Silver data using the caller's Spark session."""
    if spark is None:
        raise ValueError("Pass an existing SparkSession using spark=...")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    archive = latest_archive(bronze_root)
    output = Path(silver_root) / "elhub" / OUTPUT_NAME
    print(f"Checking Silver cache: {output}", flush=True)
    fingerprint = cache.fingerprint(archive, TRANSFORMATION_VERSION, start, end)
    if cache.is_cached(output, fingerprint):
        previous = cache.read_manifest(output)
        result = {
            "dataset": DATASET,
            "status": "cached",
            "rows": previous["rows"],
            "silver_path": str(output),
            "bronze_archive": str(archive),
        }
        print(json.dumps(result, indent=2), flush=True)
        return result

    started = time.perf_counter()
    staging = cache.new_staging_directory(output)
    try:
        with tempfile.TemporaryDirectory(prefix="dat535-elhub-price-area-") as tmp:
            paths = extract_csvs(archive, tmp)
            print(f"Extracted {len(paths)} CSV file(s). Processing with Spark", flush=True)
            schema = T.StructType([
                T.StructField(column, T.StringType(), True) for column in COLUMNS
            ])
            raw = (
                spark.read.option("header", "true")
                .option("mode", "FAILFAST")
                .schema(schema)
                .csv(paths)
            )
            silver, invalid = transform(raw, start=start, end=end)
            if invalid.limit(1).count():
                invalid.show(10, truncate=False)
                raise ValueError("Invalid Elhub price-area rows. Silver not published")

            rows = silver.count()
            if rows == 0:
                raise ValueError("No price-area Silver rows in requested period")
            print(f"Writing {rows:,} rows to staging directory", flush=True)
            silver.write.mode("errorifexists").parquet(str(staging))

            elapsed = round(time.perf_counter() - started, 2)
            manifest = {
                "dataset": DATASET,
                "fingerprint": fingerprint,
                "rows": rows,
                "bronze_archive": str(archive),
                "csv_files": len(paths),
                "run_id": run_id,
                "processing_seconds": elapsed,
            }
            cache.publish(staging, output, manifest)
            result = {
                "dataset": DATASET,
                "status": "success",
                "rows": rows,
                "csv_files": len(paths),
                "processing_seconds": elapsed,
                "silver_path": str(output),
                "bronze_archive": str(archive),
            }
            print(json.dumps(result, indent=2), flush=True)
            return result
    finally:
        if staging.exists():
            shutil.rmtree(staging)

"""Transform Elhub hourly municipality consumption into Silver Parquet."""

import json
import shutil
import tempfile
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from silver_layer import cache

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
DEFAULT_SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
DATASET = "CONSUMPTION_PER_GROUP_MUNICIPALITY_HOUR"
OUTPUT_NAME = "municipality_consumption"
TRANSFORMATION_VERSION = "municipality-v2"
COLUMNS = (
    "START_TIME",
    "END_TIME",
    "MUNICIPALITY_NUMBER",
    "NAME",
    "CONSUMPTION_GROUP",
    "QUANTITY_KWH",
    "METERING_POINT_COUNT",
    "LAST_UPDATE_TIME",
)
TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"


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
        print(
            f"Found {len(archives)} archives. Using newest: {archives[0].name}",
            flush=True,
        )
    return archives[0]


def extract_csvs(archive_path, target_dir):
    """Extract ZIP CSV filenames without modifying Bronze."""
    paths = []
    with zipfile.ZipFile(archive_path) as archive:
        members = [
            member
            for member in archive.infolist()
            if not member.is_dir() and member.filename.lower().endswith(".csv")
        ]
        if not members:
            raise ValueError(f"No CSV members in {archive_path}")
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
    return local.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def transform(raw, start="2024-01-01", end="2026-01-01"):
    """Return (valid Silver rows, invalid rows) for an all-string raw DataFrame.

    Invalid rows are detected before filtering.

    Rows outside the requested period are excluded.
    """
    if not set(COLUMNS).issubset(raw.columns):
        raise ValueError(f"Missing columns: {sorted(set(COLUMNS) - set(raw.columns))}")
    start_utc, end_utc = boundary_utc(start), boundary_utc(end)
    if start_utc >= end_utc:
        raise ValueError("start must precede end")

    df = raw.select(
        F.to_timestamp("START_TIME", TIME_PATTERN).alias("start_time"),
        F.to_timestamp("END_TIME", TIME_PATTERN).alias("end_time"),
        F.trim("MUNICIPALITY_NUMBER").alias("municipality_number"),
        F.trim("NAME").alias("name"),
        F.trim("CONSUMPTION_GROUP").alias("consumption_group"),
        F.col("QUANTITY_KWH").cast(T.DecimalType(18, 6)).alias("quantity_kwh"),
        F.col("METERING_POINT_COUNT").cast("int").alias("metering_point_count"),
        F.to_timestamp("LAST_UPDATE_TIME", TIME_PATTERN).alias("last_update_time"),
    )

    in_period = (F.col("start_time") >= F.lit(start_utc).cast("timestamp")) & (
        F.col("start_time") < F.lit(end_utc).cast("timestamp")
    )
    # Keep rows with unparseable start times so validation can report them.
    candidates = df.where(in_period | F.col("start_time").isNull())
    invalid_condition = (
        F.col("start_time").isNull()
        | F.col("end_time").isNull()
        | F.col("last_update_time").isNull()
        | F.col("municipality_number").isNull()
        | (~F.col("municipality_number").rlike(r"^[0-9]{4}$"))
        | F.col("name").isNull()
        | (F.length("name") == 0)
        | F.col("consumption_group").isNull()
        | (~F.col("consumption_group").isin("business", "industry", "private"))
        | F.col("quantity_kwh").isNull()
        | F.col("metering_point_count").isNull()
        | (F.col("metering_point_count") < F.lit(0))
        | (F.col("end_time") != F.col("start_time") + F.expr("INTERVAL 1 HOUR"))
    )
    invalid = candidates.where(invalid_condition)
    valid = candidates.where(~invalid_condition)

    window = Window.partitionBy(
        "start_time", "municipality_number", "consumption_group"
    ).orderBy(
        F.col("last_update_time").desc(),
        F.col("end_time").desc(),
        F.col("quantity_kwh").desc(),
        F.col("metering_point_count").desc(),
        F.col("name").desc(),
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
    master=None,
    driver_memory=None,
    run_id=None,
):
    """Build or reuse Silver data, creating a Spark session if needed."""

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

    owns_spark = spark is None
    started = time.perf_counter()
    staging = cache.new_staging_directory(output)
    if owns_spark:
        spark = make_spark(master=master, driver_memory=driver_memory)
    else:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        with tempfile.TemporaryDirectory(prefix="dat535-elhub-municipality-") as tmp:
            paths = extract_csvs(archive, tmp)
            print(
                f"Extracted {len(paths)} CSV files. Starting Spark transformation",
                flush=True,
            )
            schema = T.StructType(
                [T.StructField(column, T.StringType(), True) for column in COLUMNS]
            )
            raw = (
                spark.read.option("header", "true")
                .option("mode", "FAILFAST")
                .schema(schema)
                .csv(paths)
            )
            silver, invalid = transform(raw, start=start, end=end)
            if invalid.limit(1).count():
                invalid.show(10, truncate=False)
                raise ValueError(
                    "Invalid Elhub rows found. Silver output not published"
                )

            rows = silver.count()
            if rows == 0:
                raise ValueError("No Silver rows in requested period")
            print(f"Writing {rows:,} Silver rows to staging directory", flush=True)
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
        if owns_spark:
            spark.stop()

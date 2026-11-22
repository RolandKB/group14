"""Aggregate Silver Elhub price-area consumption into Norwegian daily totals.

Each Gold row represents one local date, price area and consumer group.
"""

import hashlib
import shutil
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F
from pyspark.sql import types as T

from silver_layer import cache

VERSION = "gold-consumption-v1"
AREAS = ("NO1", "NO2", "NO3", "NO4", "NO5")
GROUPS = ("cabin", "household", "primary", "secondary", "tertiary")
OSLO = ZoneInfo("Europe/Oslo")


def _calendar(spark, first, last):
    """Expected UTC hourly intervals in each Norwegian local calendar day."""
    rows = []
    current = first

    while current < last:
        following = current + timedelta(days=1)
        start = datetime.combine(current, time.min, OSLO).astimezone(UTC)
        end = datetime.combine(following, time.min, OSLO).astimezone(UTC)
        rows.append((current, int((end - start).total_seconds() // 3600)))
        current = following

    schema = T.StructType([
        T.StructField("date", T.DateType(), False),
        T.StructField("expected_hours", T.IntegerType(), False),
    ])

    return spark.createDataFrame(rows, schema)


def ingest(silver_root, gold_root, start="2024-01-01", end="2026-01-01", spark=None, run_id=None):
    if spark is None:
        raise ValueError("Pass an existing SparkSession as spark=...")
    
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first >= last:
        raise ValueError("start must precede end")
    
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    source = Path(silver_root) / "elhub" / "price_area_consumption"
    manifest = source / cache.MANIFEST_NAME
    
    if not source.is_dir() or not (source / "_SUCCESS").is_file() or not manifest.is_file():
        raise FileNotFoundError(f"Completed Silver price-area consumption not found: {source}")

    output = Path(gold_root) / "consumption" / "daily"
    
    fingerprint = {
        "silver_manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
        "transformation_version": VERSION,
        "start_local": start,
        "end_exclusive_local": end,
    }

    if cache.is_cached(output, fingerprint):
        return {"dataset": "daily_consumption", "status": "cached",
                "rows": cache.read_manifest(output)["rows"], "gold_path": str(output)}

    start_epoch = int(datetime.combine(first, time.min, OSLO).timestamp())
    end_epoch = int(datetime.combine(last, time.min, OSLO).timestamp())

    silver = spark.read.parquet(str(source))
    required = {"start_time", "end_time", "price_area", "consumption_group", "quantity_kwh"}
    if not required.issubset(silver.columns):
        raise ValueError(f"Missing Silver columns: {sorted(required - set(silver.columns))}")

    hourly = (
        silver.filter((F.col("start_time").cast("long") >= start_epoch)
                      & (F.col("start_time").cast("long") < end_epoch))
        .select("start_time", "end_time", "price_area", "consumption_group", "quantity_kwh")
    )

    invalid = hourly.filter(
        F.col("start_time").isNull() | F.col("end_time").isNull()
        | F.col("quantity_kwh").isNull()
        | ~F.col("price_area").isin(*AREAS)
        | ~F.col("consumption_group").isin(*GROUPS)
        | ((F.col("end_time").cast("long") - F.col("start_time").cast("long")) != 3600)
    )
    if invalid.limit(1).count():
        raise ValueError("Invalid hourly Silver records found; Gold not published")

    # Silver is already deduplicated. If there are duplicates, it's a sign of serious problems.
    duplicates = hourly.groupBy("start_time", "price_area", "consumption_group") \
        .count().filter(F.col("count") != 1)
    if duplicates.limit(1).count():
        raise ValueError("Duplicate hourly area/group keys found; Gold not published")

    daily = (
        hourly.withColumn("date", F.to_date(F.from_utc_timestamp("start_time", "Europe/Oslo")))
        .groupBy("date", "price_area", "consumption_group")
        .agg(F.sum("quantity_kwh").alias("consumption_kwh"),
             F.count("*").cast("int").alias("hour_count"))
        .join(F.broadcast(_calendar(spark, first, last)), "date", "inner")
        .withColumn("is_complete", F.col("hour_count") == F.col("expected_hours"))
        .select("date", "price_area", "consumption_group", "consumption_kwh",
                "hour_count", "expected_hours", "is_complete")
    )

    rows = daily.count()
    if rows == 0:
        raise ValueError("No daily consumption records found")
    if daily.filter(F.col("hour_count") > F.col("expected_hours")).limit(1).count():
        raise ValueError("More hourly records than expected in a local day")

    staging = cache.new_staging_directory(output)
    try:
        daily.repartition(5, "price_area").write.mode("errorifexists").parquet(str(staging))
        cache.publish(staging, output, {
            "dataset": "daily_consumption",
            "fingerprint": fingerprint,
            "rows": rows,
            "run_id": run_id,
            "silver_path": str(source),
        })
    finally:
        if staging.exists():
            shutil.rmtree(staging)

    return {"dataset": "daily_consumption", "status": "success",
            "rows": rows, "gold_path": str(output)}

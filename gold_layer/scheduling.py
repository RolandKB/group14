"""
Transform Silver spot prices into Gold scheduling prices.

Reads validated hourly spot prices from Silver and creates a dataset
for the historical operating scheduler.

Gold contains only complete unique one-hour price intervals.
"""

import hashlib
from datetime import UTC
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from silver_layer import cache

VERSION = "gold-scheduling-v1"
AREAS = ("NO1", "NO2", "NO3", "NO4", "NO5")


def ingest(
    silver_root,
    gold_root,
    start="2024-01-01",
    end="2026-01-01",
    spark=None,
    run_id=None,
):
    """Build Gold scheduling prices. Dates describe the Norwegian date range."""
    if spark is None:
        raise ValueError("Pass an existing SparkSession as spark")

    from datetime import date, datetime, time
    from zoneinfo import ZoneInfo

    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first >= last:
        raise ValueError("start must be before end")

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    source = Path(silver_root) / "spot_prices"
    if not source.is_dir():
        raise FileNotFoundError(f"Silver spot-price Parquet not found: {source}")

    manifest = source / cache.MANIFEST_NAME
    if not manifest.is_file() or not (source / "_SUCCESS").is_file():
        raise ValueError(f"Silver spot prices are not a completed dataset: {source}")

    output = Path(gold_root) / "scheduling" / "prices"
    source_hash = hashlib.sha256(manifest.read_bytes()).hexdigest()
    fingerprint = {
        "silver_manifest_sha256": source_hash,
        "transformation_version": VERSION,
        "start_local": start,
        "end_exclusive_local": end,
    }
    if cache.is_cached(output, fingerprint):
        rows = cache.read_manifest(output)["rows"]
        return {
            "dataset": "scheduling_prices",
            "status": "cached",
            "rows": rows,
            "gold_path": str(output),
        }

    oslo = ZoneInfo("Europe/Oslo")
    start_utc = datetime.combine(first, time.min, oslo).astimezone(UTC)
    end_utc = datetime.combine(last, time.min, oslo).astimezone(UTC)

    # Timestamp comparisons use UTC because the shared SparkSession uses UTC.
    start_epoch = int(start_utc.timestamp())
    end_epoch = int(end_utc.timestamp())

    prices = spark.read.parquet(str(source))
    required = {"price_area", "start_time", "end_time", "nok_per_kwh"}
    if not required.issubset(prices.columns):
        raise ValueError(
            f"Missing Silver columns: {sorted(required - set(prices.columns))}"
        )

    # Source is already validated in Silver. We only need to filter by time range.
    within_range = prices.filter(
        (F.col("start_time").cast("long") >= start_epoch)
        & (F.col("end_time").cast("long") <= end_epoch)
    )

    valid = within_range.filter(
        F.col("price_area").isin(*AREAS)
        & F.col("start_time").isNotNull()
        & F.col("end_time").isNotNull()
        & F.col("nok_per_kwh").isNotNull()
        & ((F.col("end_time").cast("long") - F.col("start_time").cast("long")) == 3600)
    ).select("price_area", "start_time", "end_time", "nok_per_kwh")

    # Exclude every duplicate key rather than arbitrarily choosing a price.
    counts = Window.partitionBy("price_area", "start_time")
    unique = (
        valid.withColumn("key_count", F.count("*").over(counts))
        .filter(F.col("key_count") == 1)
        .drop("key_count")
    )

    rows = unique.count()
    if rows == 0:
        raise ValueError("No valid hourly price intervals found")

    staging = cache.new_staging_directory(output)

    try:
        unique.repartition(5, "price_area").write.mode("errorifexists").parquet(
            str(staging)
        )
        cache.publish(
            staging,
            output,
            {
                "dataset": "scheduling_prices",
                "fingerprint": fingerprint,
                "rows": rows,
                "source": str(source),
                "rule": "Unique one-hour UTC price intervals; missing hours remain missing",
            },
        )

    finally:
        if staging.exists():
            import shutil

            shutil.rmtree(staging)

    return {
        "dataset": "scheduling_prices",
        "status": "success",
        "rows": rows,
        "gold_path": str(output),
    }

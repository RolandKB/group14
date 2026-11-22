"""Build daily price-area temperature indicators from Silver Frost observations.

Use quality-code 0 and 2 m observations. Prefer PT1H per station/hour,
but fall back to the mean of at least five distinct PT10M timestamps.
"""

import hashlib
import shutil
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F

from silver_layer import cache

VERSION = "gold-temperature-v1"
AREAS = ("NO1", "NO2", "NO3", "NO4", "NO5")


def _required_parquet(path):
    if not path.is_dir() or not (path / "_SUCCESS").is_file():
        raise FileNotFoundError(f"Completed Silver Parquet not found: {path}")
    manifest = path / cache.MANIFEST_NAME

    if not manifest.is_file():
        raise ValueError(f"Missing Silver manifest: {manifest}")
    return hashlib.sha256(manifest.read_bytes()).hexdigest()


def ingest(
    silver_root,
    gold_root,
    start="2024-01-01",
    end="2026-01-01",
    spark=None,
    run_id=None,
):
    """Create Gold daily temperatures for Norwegian local dates.
    Requires the Spark session timezone to be UTC.
    """

    if spark is None:
        raise ValueError("Pass an existing SparkSession as spark")

    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first >= last:
        raise ValueError("start must be before end")

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    root = Path(silver_root)

    paths = {
        "observations": root / "frost" / "observations",
        "stations": root / "frost" / "stations",
        "mapping": root / "geography" / "municipality_price_areas",
    }

    hashes = {name: _required_parquet(path) for name, path in paths.items()}
    output = Path(gold_root) / "temperature" / "daily"
    fingerprint = {
        "silver_manifest_sha256": hashes,
        "transformation_version": VERSION,
        "start_local": start,
        "end_exclusive_local": end,
        "quality_code": "0",
        "height_m": 2.0,
        "fallback_min_distinct_10m": 5,
        "daily_min_coverage": 0.75,
    }

    if cache.is_cached(output, fingerprint):
        return {
            "dataset": "daily_temperature",
            "status": "cached",
            "rows": cache.read_manifest(output)["rows"],
            "gold_path": str(output),
        }

    mapping = spark.read.parquet(str(paths["mapping"]))
    stations = spark.read.parquet(str(paths["stations"]))
    obs = spark.read.parquet(str(paths["observations"]))

    # Municipalities spanning two areas are excluded. Do not duplicate stations.
    unique_areas = (
        mapping.groupBy("municipality_code")
        .agg(
            F.countDistinct("price_area").alias("n_areas"),
            F.first("price_area").alias("price_area"),
        )
        .filter((F.col("n_areas") == 1) & F.col("price_area").isin(*AREAS))
        .select("municipality_code", "price_area")
    )

    station_areas = (
        stations.withColumn("municipality_code", F.lpad("municipality_code", 4, "0"))
        .join(F.broadcast(unique_areas), "municipality_code", "inner")
        .select("station_id", "price_area")
        .dropDuplicates(["station_id"])
    )

    # Source timestamps are UTC. Both Frost resolutions use PT0H offsets.
    filtered = (
        obs.filter(F.col("time_resolution").isin("PT1H", "PT10M"))
        .filter(F.col("height_m") == 2.0)
        .filter(F.col("quality_code").cast("string") == "0")
        .filter(
            F.col("temperature_c").isNotNull() & F.col("reference_time").isNotNull()
        )
        .filter(F.col("time_offset") == "PT0H")
        .select("station_id", "reference_time", "time_resolution", "temperature_c")
        .join(F.broadcast(station_areas), "station_id", "inner")
        .withColumn("hour_utc", F.date_trunc("hour", "reference_time"))
    )

    # Multiple sensors/series at the same station, height, timestamp and resolution are reduced to one value.
    per_timestamp = filtered.groupBy(
        "station_id", "price_area", "time_resolution", "reference_time", "hour_utc"
    ).agg(F.avg("temperature_c").alias("temperature_c"))

    hourly_candidates = (
        per_timestamp.groupBy("station_id", "price_area", "time_resolution", "hour_utc")
        .agg(
            F.avg("temperature_c").alias("temperature_c"),
            F.count("*").alias("readings"),
        )
        .filter(
            (F.col("time_resolution") == "PT1H")
            | ((F.col("time_resolution") == "PT10M") & (F.col("readings") >= 5))
        )
    )

    # Choose first PT1H when available and PT10M as fallback.
    hourly_1h = (
        hourly_candidates.filter(F.col("time_resolution") == "PT1H")
        .select("station_id", "price_area", "hour_utc", "temperature_c")
        .withColumn("source_resolution", F.lit("PT1H"))
    )

    hourly_10m = (
        hourly_candidates.filter(F.col("time_resolution") == "PT10M")
        .select("station_id", "price_area", "hour_utc", "temperature_c")
        .withColumn("source_resolution", F.lit("PT10M"))
    )

    hourly = hourly_1h.unionByName(
        hourly_10m.join(
            hourly_1h.select("station_id", "hour_utc"),
            ["station_id", "hour_utc"],
            "left_anti",
        )
    )

    oslo = ZoneInfo("Europe/Oslo")
    calendar_rows = []
    day = first

    from datetime import timedelta

    while day < last:
        next_day = day + timedelta(days=1)
        start_local = datetime.combine(day, time.min, oslo)
        end_local = datetime.combine(next_day, time.min, oslo)
        expected = round((end_local.timestamp() - start_local.timestamp()) / 3600)
        calendar_rows.append((day, int(expected)))
        day = next_day
    calendar = spark.createDataFrame(calendar_rows, "date date, expected_hours int")

    station_days = (
        hourly.withColumn(
            "date", F.to_date(F.from_utc_timestamp("hour_utc", "Europe/Oslo"))
        )
        .filter((F.col("date") >= F.lit(start)) & (F.col("date") < F.lit(end)))
        .groupBy("date", "price_area", "station_id")
        .agg(
            F.avg("temperature_c").alias("station_avg_temperature_c"),
            F.count("*").alias("hours_observed"),
            F.sum(F.when(F.col("source_resolution") == "PT1H", 1).otherwise(0)).alias(
                "hours_pt1h"
            ),
            F.sum(F.when(F.col("source_resolution") == "PT10M", 1).otherwise(0)).alias(
                "hours_pt10m_fallback"
            ),
        )
        .join(F.broadcast(calendar), "date")
        .filter(
            F.col("hours_observed") >= F.ceil(F.col("expected_hours") * F.lit(0.75))
        )
    )

    daily = (
        station_days.groupBy("date", "price_area")
        .agg(
            F.avg("station_avg_temperature_c").alias("avg_temperature_c"),
            F.count("*").alias("station_count"),
            F.sum("hours_pt1h").alias("station_hours_pt1h"),
            F.sum("hours_pt10m_fallback").alias("station_hours_pt10m_fallback"),
            F.avg("hours_observed").alias("avg_station_hours"),
        )
        .orderBy("date", "price_area")
    )

    rows = daily.count()
    if rows == 0:
        raise ValueError("No Gold temperature days passed the coverage threshold")

    staging = cache.new_staging_directory(output)
    try:
        daily.repartition(5, "price_area").write.mode("errorifexists").parquet(
            str(staging)
        )
        cache.publish(
            staging,
            output,
            {
                "dataset": "daily_temperature",
                "fingerprint": fingerprint,
                "rows": rows,
                "source": {name: str(path) for name, path in paths.items()},
                "method": "2m quality=0; PT1H preferred; >=5 PT10M fallback; >=75% local-day hours; equal station weights",
            },
        )

    finally:
        if staging.exists():
            shutil.rmtree(staging)

    return {
        "dataset": "daily_temperature",
        "status": "success",
        "rows": rows,
        "gold_path": str(output),
    }

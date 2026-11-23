"""Join daily consumption, temperature and spot prices for historical analysis.

One row per Norwegian local date, price area and Elhub consumer group.
Daily spot prices average available one-hour intervals.
"""

import hashlib
import shutil
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F
from pyspark.sql import types as T

from silver_layer import cache

VERSION = "gold-analytics-v1"
AREAS = ("NO1", "NO2", "NO3", "NO4", "NO5")
GROUPS = ("cabin", "household", "primary", "secondary", "tertiary")
OSLO = ZoneInfo("Europe/Oslo")


def _source_hash(path):
    manifest = path / cache.MANIFEST_NAME
    if not path.is_dir() or not (path / "_SUCCESS").is_file() or not manifest.is_file():
        raise FileNotFoundError(f"Completed input dataset not found: {path}")
    return hashlib.sha256(manifest.read_bytes()).hexdigest()


def _calendar(spark, first, last):
    rows = []
    day = first
    while day < last:
        next_day = day + timedelta(days=1)
        begin = datetime.combine(day, time.min, OSLO).astimezone(UTC)
        end = datetime.combine(next_day, time.min, OSLO).astimezone(UTC)
        rows.append((day, int((end - begin).total_seconds() // 3600)))
        day = next_day

    schema = T.StructType(
        [
            T.StructField("date", T.DateType(), False),
            T.StructField("expected_hours", T.IntegerType(), False),
        ]
    )

    return spark.createDataFrame(rows, schema)


def ingest(
    silver_root,
    gold_root,
    start="2024-01-01",
    end="2026-01-01",
    spark=None,
    run_id=None,
):
    """Build the daily analytical dataset for local dates start to end."""
    if spark is None:
        raise ValueError("Pass an existing SparkSession as spark=...")

    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first >= last:
        raise ValueError("start must precede end")

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    sources = {
        "consumption": Path(gold_root) / "consumption" / "daily",
        "temperature": Path(gold_root) / "temperature" / "daily",
        "spot_prices": Path(silver_root) / "spot_prices",
    }

    hashes = {name: _source_hash(path) for name, path in sources.items()}
    output = Path(gold_root) / "analytics" / "daily"
    fingerprint = {
        "input_manifest_sha256": hashes,
        "transformation_version": VERSION,
        "start_local": start,
        "end_exclusive_local": end,
    }

    if cache.is_cached(output, fingerprint):
        return {
            "dataset": "daily_analytics",
            "status": "cached",
            "rows": cache.read_manifest(output)["rows"],
            "gold_path": str(output),
        }

    consumption = spark.read.parquet(str(sources["consumption"]))
    temperature = spark.read.parquet(str(sources["temperature"]))
    prices = spark.read.parquet(str(sources["spot_prices"]))

    required = {
        "consumption": {
            "date",
            "price_area",
            "consumption_group",
            "consumption_kwh",
            "hour_count",
            "is_complete",
        },
        "temperature": {"date", "price_area", "avg_temperature_c", "station_count"},
        "spot_prices": {"start_time", "end_time", "price_area", "nok_per_kwh"},
    }

    for name, df in (
        ("consumption", consumption),
        ("temperature", temperature),
        ("spot_prices", prices),
    ):
        missing = required[name] - set(df.columns)
        if missing:
            raise ValueError(f"Missing {name} columns: {sorted(missing)}")

    consumption = consumption.filter(
        (F.col("date") >= F.lit(start)) & (F.col("date") < F.lit(end))
    )
    temperature = temperature.filter(
        (F.col("date") >= F.lit(start)) & (F.col("date") < F.lit(end))
    )

    for name, df, keys in (
        ("consumption", consumption, ["date", "price_area", "consumption_group"]),
        ("temperature", temperature, ["date", "price_area"]),
    ):
        if df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError(f"Duplicate {name} keys: {keys}")

    start_epoch = int(datetime.combine(first, time.min, OSLO).timestamp())
    end_epoch = int(datetime.combine(last, time.min, OSLO).timestamp())

    hourly_prices = prices.filter(
        (F.col("start_time").cast("long") >= start_epoch)
        & (F.col("start_time").cast("long") < end_epoch)
    ).select("start_time", "end_time", "price_area", "nok_per_kwh")

    invalid = hourly_prices.filter(
        F.col("start_time").isNull()
        | F.col("end_time").isNull()
        | F.col("nok_per_kwh").isNull()
        | ~F.col("price_area").isin(*AREAS)
        | ((F.col("end_time").cast("long") - F.col("start_time").cast("long")) != 3600)
    )

    if invalid.limit(1).count():
        raise ValueError("Invalid Silver spot-price intervals; Gold not published")
    if (
        hourly_prices.groupBy("price_area", "start_time")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError(
            "Duplicate Silver spot-price area/hour keys; Gold not published"
        )

    daily_prices = (
        hourly_prices.withColumn(
            "date", F.to_date(F.from_utc_timestamp("start_time", "Europe/Oslo"))
        )
        .groupBy("date", "price_area")
        .agg(
            F.avg("nok_per_kwh").cast("double").alias("avg_spot_price_nok_kwh"),
            F.count("*").cast("int").alias("price_hour_count"),
        )
        .join(F.broadcast(_calendar(spark, first, last)), "date", "inner")
        .withColumn(
            "price_is_complete", F.col("price_hour_count") == F.col("expected_hours")
        )
        .select(
            "date",
            "price_area",
            "avg_spot_price_nok_kwh",
            "price_hour_count",
            "price_is_complete",
        )
    )

    if daily_prices.filter(F.col("price_hour_count") > 25).limit(1).count():
        raise ValueError("Unexpected number of spot-price hours in a local day")

    result = (
        consumption.select(
            "date",
            "price_area",
            "consumption_group",
            "consumption_kwh",
            F.col("hour_count").alias("consumption_hour_count"),
            F.col("is_complete").alias("consumption_is_complete"),
        )
        .join(
            temperature.select(
                "date", "price_area", "avg_temperature_c", "station_count"
            ),
            ["date", "price_area"],
            "left",
        )
        .join(daily_prices, ["date", "price_area"], "left")
        .select(
            "date",
            "price_area",
            "consumption_group",
            "consumption_kwh",
            "avg_temperature_c",
            "avg_spot_price_nok_kwh",
            "consumption_hour_count",
            "consumption_is_complete",
            "station_count",
            "price_hour_count",
            "price_is_complete",
        )
    )

    rows = result.count()
    if rows == 0:
        raise ValueError("No analytical rows produced")
    if (
        result.groupBy("date", "price_area", "consumption_group")
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError("Duplicate analytical keys; Gold not published")

    staging = cache.new_staging_directory(output)
    try:
        result.repartition(5, "price_area").write.mode("errorifexists").parquet(
            str(staging)
        )
        cache.publish(
            staging,
            output,
            {
                "dataset": "daily_analytics",
                "fingerprint": fingerprint,
                "rows": rows,
                "run_id": run_id,
                "sources": {name: str(path) for name, path in sources.items()},
                "method": "Local daily totals and means; left joins retain consumption rows; missing price hours not filled",
            },
        )

    finally:
        if staging.exists():
            shutil.rmtree(staging)

    return {
        "dataset": "daily_analytics",
        "status": "success",
        "rows": rows,
        "gold_path": str(output),
    }

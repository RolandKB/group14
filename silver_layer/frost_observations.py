"""Frost air-temperature observations: Bronze to Silver.

Invalid readings are quarantined.

invalid JSON files get a quarantine metadata row. Times are UTC.
"""

import hashlib
import json
import shutil
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F, types as T
from tqdm.auto import tqdm

from silver_layer import cache

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
DEFAULT_SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
DATASET = "frost_observations"
VERSION = "frost-observations-v2"
MIN_TEMPERATURE_C = -60
MAX_TEMPERATURE_C = 50

OBSERVATION = T.StructType(
    [
        T.StructField("elementId", T.StringType()),
        T.StructField("value", T.StringType()),
        T.StructField("unit", T.StringType()),
        T.StructField("timeOffset", T.StringType()),
        T.StructField("timeResolution", T.StringType()),
        T.StructField("timeSeriesId", T.StringType()),
        T.StructField("qualityCode", T.StringType()),
        T.StructField(
            "level",
            T.StructType(
                [
                    T.StructField("levelType", T.StringType()),
                    T.StructField("unit", T.StringType()),
                    T.StructField("value", T.StringType()),
                ]
            ),
        ),
    ]
)

RAW_SCHEMA = T.StructType(
    [
        T.StructField(
            "data",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("sourceId", T.StringType()),
                        T.StructField("referenceTime", T.StringType()),
                        T.StructField("observations", T.ArrayType(OBSERVATION)),
                    ]
                )
            ),
        ),
    ]
)

SILVER_COLUMNS = [
    "station_id",
    "source_id",
    "reference_time",
    "observation_time",
    "temperature_c",
    "quality_code",
    "time_resolution",
    "time_offset",
    "time_series_id",
    "height_m",
]


def source_files(bronze_root):
    """Discover successful station-month responses; skip explicit no-data markers."""
    root = Path(bronze_root) / "frost" / "observations"
    if not root.is_dir():
        raise FileNotFoundError(root)
    files = sorted(root.glob("station=*/period=*/response.json"))
    if not files:
        raise ValueError(f"No Frost observation responses found under {root}")
    return files


def source_fingerprint(files, start, end):
    digest = hashlib.sha256()

    for path in tqdm(
        files, desc="Hashing Frost files", unit="file", disable=not sys.stderr.isatty()
    ):
        digest.update(str(path.parent.parent.name + "/" + path.parent.name).encode())
        digest.update(bytes.fromhex(cache.sha256_file(path)))

    return {
        "source_sha256": digest.hexdigest(),
        "source_files": len(files),
        "start": start,
        "end_exclusive": end,
        "transformation_version": VERSION,
    }


def classify_files(files, start, end):
    """Check JSON structure and retain only station-months within the date range."""
    valid, rejected = [], []

    for path in tqdm(
        files, desc="Checking Frost JSON", unit="file", disable=not sys.stderr.isatty()
    ):
        period = path.parent.name.removeprefix("period=")

        try:
            first, last = period.split("_to_")
            if not (start <= first < end and first < last <= end):
                continue

            with path.open(encoding="utf-8") as handle:
                payload = json.load(handle)

            if not isinstance(payload, dict) or not isinstance(
                payload.get("data"), list
            ):
                reason = "INVALID_JSON_STRUCTURE"

            elif not payload["data"]:
                reason = "EMPTY_DATA"

            elif any(
                not isinstance(row, dict)
                or not isinstance(row.get("observations"), list)
                for row in payload["data"]
            ):
                reason = "INVALID_JSON_STRUCTURE"

            else:
                valid.append(path)
                continue

        except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            reason = f"MALFORMED_JSON: {exc}"

        rejected.append(
            {
                "station_id": path.parent.parent.name.removeprefix("station="),
                "_source_file": str(path.resolve()),
                "quarantine_reason": reason,
            }
        )

    return valid, rejected


def parse(raw):
    rows = raw.select("_source_file", F.explode("data").alias("item"))
    rows = rows.select(
        "_source_file",
        "item.sourceId",
        "item.referenceTime",
        F.explode_outer("item.observations").alias("obs"),
    )

    # Calculate the offset in seconds
    offset = F.col("obs.timeOffset")

    # ISO-8601 time durations: PT0H, PT20M, PT1H30M, PT30S.
    offset_ok = offset.rlike(r"^PT(?=\d+[HMS]|\d+M)(?:\d+H)?(?:\d+M)?(?:\d+S)?$")
    hours = F.regexp_extract(offset, r"(\d+)H", 1)
    minutes = F.regexp_extract(offset, r"(\d+)M", 1)
    seconds = F.regexp_extract(offset, r"(\d+)S", 1)

    def number(value):
        return F.when(value == "", F.lit(0)).otherwise(value.cast("long"))

    offset_seconds = number(hours) * 3600 + number(minutes) * 60 + number(seconds)
    reference_time = F.to_timestamp("referenceTime", "yyyy-MM-dd'T'HH:mm:ss.SSSX")

    return rows.select(
        F.regexp_extract("_source_file", r"(?:^|/)station=([^/]+)(?:/|$)", 1).alias(
            "station_id"
        ),
        F.col("sourceId").alias("source_id"),
        reference_time.alias("reference_time"),
        F.when(
            offset_ok & reference_time.isNotNull(),
            F.timestamp_seconds(reference_time.cast("long") + offset_seconds),
        ).alias("observation_time"),
        F.col("obs.value").cast("decimal(18,6)").alias("temperature_c"),
        F.col("obs.qualityCode").cast("int").alias("quality_code"),
        F.col("obs.timeResolution").alias("time_resolution"),
        offset.alias("time_offset"),
        F.col("obs.timeSeriesId").cast("int").alias("time_series_id"),
        F.col("obs.level.value").cast("decimal(18,6)").alias("height_m"),
        F.col("obs.elementId").alias("element_id"),
        F.col("obs.unit").alias("unit"),
        F.col("obs.level.levelType").alias("level_type"),
        F.col("obs.level.unit").alias("level_unit"),
        F.col("_source_file"),
    )


def validate(parsed, start, end):
    start_ts = F.lit(f"{start} 00:00:00").cast("timestamp")
    end_ts = F.lit(f"{end} 00:00:00").cast("timestamp")
    invalid = (
        (F.col("station_id") == "")
        | F.col("source_id").isNull()
        | ~F.col("source_id").startswith(F.col("station_id"))
        | F.col("reference_time").isNull()
        | (F.col("reference_time") < start_ts)
        | (F.col("reference_time") >= end_ts)
        | F.col("observation_time").isNull()
        | F.col("temperature_c").isNull()
        | (F.col("element_id") != "air_temperature")
        | (F.col("unit") != "degC")
        | F.col("time_resolution").isNull()
        | F.col("time_series_id").isNull()
        | F.col("quality_code").isNull()
        | (
            F.col("level_type").isNotNull()
            & (F.col("level_type") != "height_above_ground")
        )
        | (F.col("level_unit").isNotNull() & (F.col("level_unit") != "m"))
    )
    # Selected plausibility limits for Norwegian outdoor air temperature.
    implausible = (F.col("temperature_c") < MIN_TEMPERATURE_C) | (
        F.col("temperature_c") > MAX_TEMPERATURE_C
    )

    marked = parsed.withColumn(
        "quarantine_reason",
        F.when(F.coalesce(invalid, F.lit(True)), "INVALID_OBSERVATION").when(
            implausible, "IMPLAUSIBLE_TEMPERATURE"
        ),
    )

    quarantine = marked.where("quarantine_reason IS NOT NULL")
    silver = marked.where("quarantine_reason IS NULL").select(*SILVER_COLUMNS)

    # Only exact identical rows are duplicates.
    silver = silver.dropDuplicates(SILVER_COLUMNS)
    return silver, quarantine


def ingest(
    bronze_root=DEFAULT_BRONZE_ROOT,
    silver_root=DEFAULT_SILVER_ROOT,
    start="2024-01-01",
    end="2026-01-01",
    spark=None,
    run_id=None,
):
    """Transform Frost observations. Return Silver/quarantine paths and counts."""
    if spark is None:
        raise ValueError("A SparkSession is required")

    if date.fromisoformat(start) >= date.fromisoformat(end):
        raise ValueError("start must precede end")

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    files = source_files(bronze_root)
    output = Path(silver_root) / "frost" / "observations"
    quarantine_output = Path(silver_root) / "quarantine" / "frost_observations"
    fingerprint = source_fingerprint(files, start, end)

    if cache.is_cached(output, fingerprint) and cache.is_cached(
        quarantine_output, fingerprint
    ):
        print("Frost observations already processed")
        return {
            "dataset": DATASET,
            "status": "cached",
            "silver_path": str(output),
            "quarantine_path": str(quarantine_output),
        }

    print("Checking Frost JSON files...", flush=True)
    valid_files, rejected_files = classify_files(files, start, end)
    print(
        f"Checked {len(files):,} files; {len(valid_files):,} valid, {len(rejected_files):,} rejected",
        flush=True,
    )

    if not valid_files:
        raise ValueError("No valid Frost JSON files")

    raw = (
        spark.read.schema(RAW_SCHEMA)
        .option("multiLine", "true")
        .option("mode", "FAILFAST")
        .json([str(p) for p in valid_files])
        .withColumn("_source_file", F.input_file_name())
    )

    silver, quarantine = validate(parse(raw), start, end)

    if rejected_files:
        records = []

        for item in rejected_files:
            record = {field.name: None for field in quarantine.schema}
            record.update(item)
            records.append(tuple(record[field.name] for field in quarantine.schema))

        quarantine = quarantine.unionByName(
            spark.createDataFrame(records, quarantine.schema)
        )

    print("Running Spark validation and writing Silver outputs...", flush=True)
    silver.persist()
    quarantine.persist()

    try:
        silver_rows, quarantine_rows = silver.count(), quarantine.count()
        if not silver_rows:
            raise ValueError("No valid Frost observations")
        silver_stage = cache.new_staging_directory(output)
        quarantine_stage = cache.new_staging_directory(quarantine_output)

        try:
            silver.write.mode("overwrite").parquet(str(silver_stage))
            quarantine.write.mode("overwrite").parquet(str(quarantine_stage))

            for staging, destination, count in (
                (silver_stage, output, silver_rows),
                (quarantine_stage, quarantine_output, quarantine_rows),
            ):
                manifest = {
                    "dataset": DATASET,
                    "fingerprint": fingerprint,
                    "run_id": run_id,
                    "rows": count,
                    "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                (staging / cache.MANIFEST_NAME).write_text(
                    json.dumps(manifest, indent=2)
                )

            # Publish only after both datasets are written successfully.
            for destination in (output, quarantine_output):
                if destination.exists():
                    shutil.rmtree(destination)

            silver_stage.replace(output)
            quarantine_stage.replace(quarantine_output)

        finally:
            for staging in (silver_stage, quarantine_stage):
                if staging.exists():
                    shutil.rmtree(staging)

    finally:
        silver.unpersist()
        quarantine.unpersist()

    print(f"Silver rows: {silver_rows:,}")
    print(f"Quarantine rows: {quarantine_rows:,}")
    return {
        "dataset": DATASET,
        "status": "success",
        "rows": silver_rows,
        "quarantined_rows": quarantine_rows,
        "silver_path": str(output),
        "quarantine_path": str(quarantine_output),
    }

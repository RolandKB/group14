"""Spot prices: Bronze to Silver."""

import hashlib
import json
import os
import shutil
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F, types as T

from silver_layer import cache

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
DEFAULT_SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
DATASET = "spot_prices"
TRANSFORMATION_VERSION = "spot-prices-v1"
AREAS = ("NO1", "NO2", "NO3", "NO4", "NO5")
TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ssXXX"

RAW_SCHEMA = T.StructType(
    [
        T.StructField("NOK_per_kWh", T.StringType()),
        T.StructField("EUR_per_kWh", T.StringType()),
        T.StructField("EXR", T.StringType()),
        T.StructField("time_start", T.StringType()),
        T.StructField("time_end", T.StringType()),
    ]
)


def source_files(bronze_root, start, end):
    """Require one Bronze file per area/local date."""
    first, last = date.fromisoformat(start), date.fromisoformat(end)

    if first >= last:
        raise ValueError("start must precede end")

    root = Path(bronze_root) / "spot_prices"
    files = []
    day = first

    while day < last:
        for area in AREAS:
            path = root / f"area={area}" / f"date={day.isoformat()}" / "response.json"
            if not path.is_file():
                raise FileNotFoundError(f"Missing Bronze response: {path}")
            files.append(path)
        day += timedelta(days=1)

    return files


def validate_json_file(path):
    """Return a source rejection reason."""
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return f"MALFORMED_JSON: {type(exc).__name__}: {exc}"

    if not isinstance(payload, list):
        return "INVALID_JSON_STRUCTURE: expected array of objects"

    if not payload:
        return "EMPTY_JSON_ARRAY"

    if any(not isinstance(record, dict) for record in payload):
        return "INVALID_JSON_STRUCTURE: array contains non-object elements"

    return None


def classify_source_files(files):
    """Separate valid source files from rejected."""
    valid, rejected = [], []
    for path in files:
        reason = validate_json_file(path)
        if reason is None:
            valid.append(path)
        else:
            rejected.append(
                {
                    "price_area": path.parent.parent.name.split("=", 1)[1],
                    "source_date": path.parent.name.split("=", 1)[1],
                    "_source_file": str(path.resolve()),
                    "quarantine_reason": reason,
                }
            )
    return valid, rejected


def source_fingerprint(files, start, end):
    digest = hashlib.sha256()

    for path in files:
        digest.update(
            f"{path.parent.parent.name}/{path.parent.name}/{path.name}\n".encode()
        )
        digest.update(bytes.fromhex(cache.sha256_file(path)))

    return {
        "source_sha256": digest.hexdigest(),
        "source_files": len(files),
        "transformation_version": TRANSFORMATION_VERSION,
        "start_local": start,
        "end_exclusive_local": end,
    }


def local_boundary_utc(day):
    return datetime.combine(
        date.fromisoformat(day), datetime.min.time(), tzinfo=ZoneInfo("Europe/Oslo")
    ).astimezone(timezone.utc)


def _parsed(raw):
    required = set(RAW_SCHEMA.fieldNames()) | {"_source_file"}
    missing = required - set(raw.columns)

    if missing:
        raise ValueError(f"Missing spot-price columns: {sorted(missing)}")
    path = F.col("_source_file")

    return raw.select(
        F.to_timestamp("time_start", TIME_PATTERN).alias("start_time"),
        F.to_timestamp("time_end", TIME_PATTERN).alias("end_time"),
        F.regexp_extract(path, r"(?:^|/)area=(NO[1-5])(?:/|$)", 1).alias("price_area"),
        F.regexp_extract(path, r"(?:^|/)date=(\d{4}-\d{2}-\d{2})(?:/|$)", 1).alias(
            "source_date"
        ),
        F.col("NOK_per_kWh").cast(T.DecimalType(18, 6)).alias("nok_per_kwh"),
        F.col("EUR_per_kWh").cast(T.DecimalType(18, 6)).alias("eur_per_kwh"),
        F.col("EXR").cast(T.DecimalType(18, 6)).alias("exchange_rate"),
        F.col("time_start").alias("raw_time_start"),
        F.col("time_end").alias("raw_time_end"),
        path.alias("_source_file"),
    )


def invalid_rows(df, start, end):
    """Return area-days containing invalid values or timestamps."""
    first_utc = local_boundary_utc(start).strftime("%Y-%m-%d %H:%M:%S")
    last_utc = local_boundary_utc(end).strftime("%Y-%m-%d %H:%M:%S")

    # Source files are grouped by Norwegian date. Spark timestamps use UTC.
    local_date = F.date_format(
        F.from_utc_timestamp("start_time", "Europe/Oslo"), "yyyy-MM-dd"
    )

    invalid = (
        F.col("start_time").isNull()
        | F.col("end_time").isNull()
        | ~F.col("price_area").isin(*AREAS)
        | (F.col("source_date") == "")
        | F.col("nok_per_kwh").isNull()
        | F.col("eur_per_kwh").isNull()
        | F.col("exchange_rate").isNull()
        | (F.col("exchange_rate") <= 0)
        | (F.col("end_time") <= F.col("start_time"))
        | (F.col("end_time") != F.col("start_time") + F.expr("INTERVAL 1 HOUR"))
        | (local_date != F.col("source_date"))
        | (F.col("start_time") < F.lit(first_utc).cast("timestamp"))
        | (F.col("end_time") > F.lit(last_utc).cast("timestamp"))
    )

    # Treat unknown validation results as invalid too.
    return (
        df.where(F.coalesce(invalid, F.lit(True)))
        .select("price_area", "source_date")
        .distinct()
        .withColumn("reason", F.lit("INVALID_INTERVAL_OR_VALUE"))
    )


def overlapping_days(df):
    """Return both area-days involved in any overlapping time intervals."""
    # Compare every pair in the same area. Detects a long malformed interval overlapping several shorter intervals.
    valid_times = df.where(
        F.col("start_time").isNotNull() & F.col("end_time").isNotNull()
    ).withColumn("_row_id", F.monotonically_increasing_id())

    overlaps = valid_times.alias("a").join(
        valid_times.alias("b"),
        (F.col("a.price_area") == F.col("b.price_area"))
        & (F.col("a._row_id") < F.col("b._row_id"))
        & (F.col("a.start_time") < F.col("b.end_time"))
        & (F.col("b.start_time") < F.col("a.end_time")),
        "inner",
    )

    # An overlap can cross file boundaries so quarantine both source days.
    return (
        overlaps.select(
            F.col("a.price_area").alias("price_area"),
            F.col("a.source_date").alias("source_date"),
        )
        .unionByName(
            overlaps.select(
                F.col("b.price_area").alias("price_area"),
                F.col("b.source_date").alias("source_date"),
            )
        )
        .distinct()
        .withColumn("reason", F.lit("OVERLAPPING_INTERVAL"))
    )


def incorrect_day_counts(df):
    """Return area-days with an incorrect number of hourly records."""
    counts = df.groupBy("price_area", "source_date").count()

    # Norwegian days have 24 hours normally and 23/25 at DST transitions.
    begin = F.to_utc_timestamp(F.to_timestamp("source_date"), "Europe/Oslo")
    next_day = F.to_utc_timestamp(
        F.date_add(F.to_date("source_date"), 1).cast("timestamp"),
        "Europe/Oslo",
    )
    expected = ((next_day.cast("long") - begin.cast("long")) / 3600).cast("int")

    return (
        counts.withColumn("expected", expected)
        .where(F.col("count") != F.col("expected"))
        .select("price_area", "source_date")
        .withColumn("reason", F.lit("UNEXPECTED_DAY_ROW_COUNT"))
    )


def _validation(parsed, start, end):
    """Split complete area-day batches into Silver and quarantine."""
    # Validate the full requested period before removing any bad batches.
    bad_batches = (
        invalid_rows(parsed, start, end)
        .unionByName(overlapping_days(parsed))
        .unionByName(incorrect_day_counts(parsed))
    )

    # A batch can fail multiple checks; preserve all its rejection reasons.
    affected = bad_batches.groupBy("price_area", "source_date").agg(
        F.concat_ws(",", F.sort_array(F.collect_set("reason"))).alias(
            "quarantine_reason"
        )
    )
    marked = parsed.join(affected, ["price_area", "source_date"], "left")

    # Reject whole area-day batches, not just individual invalid rows.
    quarantine = marked.where(F.col("quarantine_reason").isNotNull())
    silver = marked.where(F.col("quarantine_reason").isNull()).select(
        "start_time",
        "end_time",
        "price_area",
        "nok_per_kwh",
        "eur_per_kwh",
        "exchange_rate",
    )
    return silver, quarantine


def _publish_pair(
    silver_staging,
    quarantine_staging,
    silver_output,
    quarantine_output,
    silver_manifest,
    quarantine_manifest,
):
    """Publish both datasets with rollback if either publish fails."""
    outputs = [
        (Path(silver_staging), Path(silver_output), silver_manifest),
        (Path(quarantine_staging), Path(quarantine_output), quarantine_manifest),
    ]

    backups = []
    published = []

    try:
        # First prepare manifests and validate both staging outputs.
        for staging, _, manifest in outputs:
            if not (staging / "_SUCCESS").is_file():
                raise ValueError(f"Incomplete Spark output: {staging}")

            # Empty quarantine output is allowed; Spark still writes _SUCCESS.
            payload = dict(manifest)
            payload["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
            (staging / cache.MANIFEST_NAME).write_text(
                json.dumps(payload, indent=2, sort_keys=True)
            )

        for _, output, _ in outputs:
            backup = output.with_name(f".{output.name}.backup-{uuid.uuid4().hex}")
            if output.exists():
                os.replace(output, backup)
                backups.append((output, backup))

        for staging, output, _ in outputs:
            os.replace(staging, output)
            published.append(output)

    except Exception:
        for output in reversed(published):
            if output.exists():
                shutil.rmtree(output)

        for output, backup in reversed(backups):
            if backup.exists():
                os.replace(backup, output)

        raise
    for _, backup in backups:
        shutil.rmtree(backup)


def ingest(
    bronze_root=DEFAULT_BRONZE_ROOT,
    silver_root=DEFAULT_SILVER_ROOT,
    start="2024-01-01",
    end="2026-01-01",
    spark=None,
    run_id=None,
):
    """Convert Bronze spot prices to Silver and quarantine bad days."""
    if spark is None:
        raise ValueError("A SparkSession is required")

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    files = source_files(bronze_root, start, end)
    output = Path(silver_root) / "spot_prices"
    quarantine_output = Path(silver_root) / "quarantine" / "spot_prices"
    fingerprint = source_fingerprint(files, start, end)

    # Use previous results if both outputs are cached.
    if cache.is_cached(output, fingerprint) and cache.is_cached(
        quarantine_output, fingerprint
    ):
        print("Spot prices already processed")
        return {
            "dataset": DATASET,
            "status": "cached",
            "silver_path": str(output),
            "quarantine_path": str(quarantine_output),
        }

    # Check JSON files before reading them with Spark.
    valid_files, rejected_files = classify_source_files(files)

    if valid_files:
        raw = (
            spark.read.schema(RAW_SCHEMA)
            .option("multiLine", "true")
            .option("mode", "FAILFAST")
            .json([str(path) for path in valid_files])
            .withColumn("_source_file", F.input_file_name())
        )
    else:
        raw = spark.createDataFrame([], RAW_SCHEMA).withColumn(
            "_source_file", F.lit(None).cast("string")
        )

    # Parse raw JSON fields into timestamps, decimals, and area/date.
    parsed = _parsed(raw)

    # Validate every daily batch. A bad batch goes to quarantine. Fully valid batches are written to Silver.
    silver, quarantine = _validation(parsed, start, end)

    # Add files that could not be parsed to quarantine.
    if rejected_files:
        records = []
        for item in rejected_files:
            record = {field.name: None for field in quarantine.schema}
            record.update(item)
            records.append(record)

        rejected = spark.createDataFrame(
            [tuple(row[field.name] for field in quarantine.schema) for row in records],
            schema=quarantine.schema,
        )
        quarantine = quarantine.unionByName(rejected)

    silver = silver.persist()
    quarantine = quarantine.persist()

    try:
        silver_rows = silver.count()
        quarantine_rows = quarantine.count()

        if silver_rows == 0:
            raise ValueError("No valid spot-price data")

        silver_staging = cache.new_staging_directory(output)
        quarantine_staging = cache.new_staging_directory(quarantine_output)

        try:
            silver.write.parquet(str(silver_staging))
            quarantine.write.parquet(str(quarantine_staging))

            # Publish both datasets and their cache manifests.
            shared = {
                "dataset": DATASET,
                "fingerprint": fingerprint,
                "run_id": run_id,
            }

            _publish_pair(
                silver_staging,
                quarantine_staging,
                output,
                quarantine_output,
                {**shared, "rows": silver_rows},
                {**shared, "rows": quarantine_rows},
            )

        finally:
            for staging in (silver_staging, quarantine_staging):
                if staging.exists():
                    shutil.rmtree(staging)
    finally:
        silver.unpersist()
        quarantine.unpersist()

    print(f"Silver rows: {silver_rows:,}")
    print(f"Quarantine rows: {quarantine_rows:,}")

    return {
        "dataset": DATASET,
        "status": "success_with_quarantine" if quarantine_rows else "success",
        "rows": silver_rows,
        "quarantined_rows": quarantine_rows,
        "silver_path": str(output),
        "quarantine_path": str(quarantine_output),
    }

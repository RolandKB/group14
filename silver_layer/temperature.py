"""Validate Frost observations without mixing sensors or sampling frequencies.

Frost time and quality definitions:
https://frost.met.no/concepts2.html
https://frost.met.no/dataclarifications.html
"""

import hashlib
import json
import os
import re
import shutil
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from pyspark.sql import Window, functions as F

RULE_VERSION = "temperature-v1"
RESOLUTIONS = {"PT1M": 60, "PT10M": 600, "PT30M": 1800,
               "PT1H": 3600, "PT3H": 10800, "PT6H": 21600}
SERIES_KEY = ["source_id", "element_id", "time_resolution", "time_offset",
              "time_series_id", "level_type", "level_unit", "level_value"]
BUSINESS_KEY = SERIES_KEY + ["observed_utc"]
RAW_SCHEMA = """
    sourceId STRING, referenceTime STRING,
    observations ARRAY<STRUCT<elementId: STRING, value: STRING, unit: STRING,
        timeOffset: STRING, timeResolution: STRING, timeSeriesId: STRING,
        qualityCode: STRING, performanceCategory: STRING, exposureCategory: STRING,
        level: STRUCT<levelType: STRING, unit: STRING, value: STRING>>>,
    source_file STRING, source_sha256 STRING, retrieval_manifest STRING,
    retrieved_at_utc STRING, requested_station STRING,
    period_start STRING, period_end STRING, record_index LONG
"""
SERIES_SCHEMA = """
    source_id STRING, element_id STRING, time_resolution STRING, time_offset STRING,
    time_series_id LONG, level_type STRING, level_unit STRING, level_value DECIMAL(12,4),
    first_second LONG, last_second LONG, step_seconds LONG
"""


def save_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{uuid4().hex}.tmp")
    try:
        temporary.write_text(json.dumps(payload, indent=2) + "\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def epoch(value):
    """Require an explicit offset before interpreting a source timestamp."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError("Timestamp must contain an offset")
    return int(parsed.timestamp())


def monthly_periods(start, end):
    """Use complete UTC months to match the existing Bronze request folders."""
    first, limit = date.fromisoformat(start), date.fromisoformat(end)
    if first >= limit or first.day != 1 or limit.day != 1:
        raise ValueError("Use month boundaries with start before exclusive end")
    periods = []
    while first < limit:
        following = date(first.year + (first.month == 12), first.month % 12 + 1, 1)
        periods.append((str(first), str(following)))
        first = following
    return periods


def validate_stations(stations):
    if not stations or any(not re.fullmatch(r"SN[0-9]+", s) for s in stations):
        raise ValueError("Supply one or more station IDs such as SN18700")
    return sorted(set(stations))


def load_response(folder, station, period=None):
    """Verify the latest retrieval. Corrupt latest inputs never fall back silently."""
    candidates = []
    for manifest in sorted(folder.glob("retrieval_*.metadata.json")):
        metadata = json.loads(manifest.read_text())
        epoch(metadata["retrieved_at_utc"])
        retrieved = datetime.fromisoformat(metadata["retrieved_at_utc"].replace("Z", "+00:00"))
        candidates.append((retrieved, str(manifest), metadata))
    if not candidates:
        raise ValueError("Missing retrieval manifest")
    _, manifest, metadata = max(candidates, key=lambda item: (item[0], item[1]))
    if metadata["source"] != "Frost" or metadata["station_id"] != station:
        raise ValueError("Unexpected source or station in manifest")
    if period and (metadata["period_start"], metadata["period_end_exclusive"]) != period:
        raise ValueError("Manifest does not match the requested period")
    path = (folder / metadata["response_file"]).resolve()
    if path.parent != folder.resolve():
        raise ValueError("Response path escapes its Bronze folder")
    body = path.read_bytes()
    if hashlib.sha256(body).hexdigest() != metadata["sha256"]:
        raise ValueError("Response checksum mismatch")
    if len(body) != metadata["size_bytes"]:
        raise ValueError("Response size mismatch")
    # Decode decimal tokens as strings to avoid losing precision during staging.
    payload = json.loads(body, parse_float=str)
    if not isinstance(payload, dict) or payload.get("apiVersion") != "v0":
        raise ValueError("Expected a Frost v0 response object")
    rows = payload.get("data")
    status, http_status = metadata["status"], metadata["http_status"]
    if status == "empty" and http_status in (404, 412):
        error = payload.get("error", {})
        if str(error.get("code")) != str(http_status):
            raise ValueError("No-data error code does not match HTTP status")
        if http_status == 412 and not error.get("reason", "").startswith("No time series found for this combination of parameters"):
            raise ValueError("Unexpected HTTP 412 error")
        if rows:
            raise ValueError("No-data response unexpectedly contains measurements")
        rows = []
    elif http_status == 200 and status in ("success", "empty"):
        if not isinstance(rows, list) or payload.get("error"):
            raise ValueError("Expected a data array without an API error")
        if (payload.get("currentItemCount") != len(rows)
                or payload.get("totalItemCount") != len(rows)
                or payload.get("offset") != 0 or payload.get("nextLink")):
            raise ValueError("Incomplete or inconsistent response pagination")
        if (status == "empty") != (len(rows) == 0):
            raise ValueError("Manifest status does not match response data")
    else:
        raise ValueError("Unexpected retrieval status")
    lineage = {
        "source_file": str(path), "source_sha256": metadata["sha256"],
        "retrieval_manifest": manifest, "retrieved_at_utc": metadata["retrieved_at_utc"],
        "requested_station": station, "status": status,
    }
    return rows, lineage


def stage_month(bronze_root, staging, start, end, stations):
    """Validate one response at a time and stage one parent record per line."""
    staging = Path(staging)
    staging.mkdir(parents=True, exist_ok=False)
    selected, failures = [], []
    paths = []
    for station in validate_stations(stations):
        folder = Path(bronze_root) / "observations" / f"station={station}" / f"period={start}_to_{end}"
        target = staging / f"{station}.jsonl"
        try:
            rows, lineage = load_response(folder, station, (start, end))
            observations = 0
            # Reject malformed parent structures as an input failure, not an empty month.
            # Missing scalar values are kept for Spark's row-level quarantine.
            with target.open("w") as handle:
                for index, record in enumerate(rows):
                    if not isinstance(record, dict) or not isinstance(record.get("observations"), list) or not record["observations"]:
                        raise ValueError("Missing, malformed or empty observations array")
                    for observation in record["observations"]:
                        if not isinstance(observation, dict):
                            raise ValueError("Observation must be an object")
                        level = observation.get("level")
                        if level is not None and not isinstance(level, dict):
                            raise ValueError("Sensor level must be an object")
                        for key, value in observation.items():
                            if key != "level" and isinstance(value, (dict, list)):
                                raise ValueError(f"Unexpected nested observation field: {key}")
                        if level and any(isinstance(v, (dict, list)) for v in level.values()):
                            raise ValueError("Sensor level fields must be scalar")
                    if any(isinstance(record.get(k), (dict, list)) for k in ("sourceId", "referenceTime")):
                        raise ValueError("Source identifier and timestamp must be scalar")
                    observations += len(record["observations"])
                    staged = dict(record)
                    staged.update(lineage)
                    staged.update(period_start=start, period_end=end, record_index=index)
                    handle.write(json.dumps(staged, separators=(",", ":")) + "\n")
            selected.append({**lineage, "source_records": len(rows), "source_observations": observations})
            if rows:
                paths.append(str(target.resolve()))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            target.unlink(missing_ok=True)
            failures.append({"station_id": station, "period_start": start, "error": str(exc)})
    return paths, selected, failures


def read_temperature(spark, paths):
    """Expand observations while retaining parent identity and input lineage."""
    if paths:
        raw = spark.read.schema(RAW_SCHEMA).option("mode", "FAILFAST").json(paths)
    else:
        raw = spark.createDataFrame([], RAW_SCHEMA)
    return raw.select("*", F.posexplode("observations").alias("observation_index", "observation")).drop("observations")


def normalize_temperature(raw):
    """Keep raw values alongside UTC timestamps, exact decimals and quality flags."""
    rows = raw.select(
        "source_file", "source_sha256", "retrieval_manifest", "retrieved_at_utc",
        "requested_station", "period_start", "period_end", "record_index", "observation_index",
        F.col("sourceId").alias("source_id"), F.col("referenceTime").alias("source_time"),
        F.col("observation").alias("source_observation"),
        F.col("observation.elementId").alias("element_id"),
        F.col("observation.timeResolution").alias("time_resolution"),
        F.col("observation.timeOffset").alias("time_offset"),
        F.col("observation.unit").alias("unit"),
        F.col("observation.level.levelType").alias("level_type"),
        F.col("observation.level.unit").alias("level_unit"),
        F.col("observation.performanceCategory").alias("performance_category"),
        F.col("observation.exposureCategory").alias("exposure_category"),
    )
    rows = (
        rows.withColumn("station_id", F.regexp_extract("source_id", r"^(SN[0-9]+):[0-9]+$", 1))
        .withColumn("observed_utc", F.try_to_timestamp("source_time"))
        .withColumn("temperature_c", F.expr("try_cast(source_observation.value AS DECIMAL(16,6))"))
        .withColumn("level_value", F.expr("try_cast(source_observation.level.value AS DECIMAL(12,4))"))
        .withColumn("time_series_id", F.expr("try_cast(source_observation.timeSeriesId AS BIGINT)"))
        .withColumn("quality_code", F.expr("try_cast(source_observation.qualityCode AS INT)"))
        .withColumn("quality_uncertain", F.col("quality_code").isin(2, 4))
        .withColumn("local_date", F.to_date(F.from_utc_timestamp("observed_utc", "Europe/Oslo")))
    )
    # Subdaily referenceTime already is the observation time. Never add timeOffset.
    checks = [
        (F.col("source_time").rlike(r"(Z|[+-][0-9]{2}:[0-9]{2})$") & F.col("observed_utc").isNotNull(), "invalid_timestamp"),
        (F.col("source_id").rlike(r"^SN[0-9]+:[0-9]+$") & (F.col("station_id") == F.col("requested_station")), "invalid_source_id"),
        ((F.col("observed_utc") >= F.to_timestamp("period_start")) & (F.col("observed_utc") < F.to_timestamp("period_end")), "outside_requested_month"),
        (F.col("element_id") == "air_temperature", "unexpected_element"),
        (F.col("unit") == "degC", "missing_or_unexpected_unit"),
        (F.col("temperature_c").isNotNull(), "invalid_temperature"),
        (F.col("source_observation.value").rlike(r"^[+-]?[0-9]+(?:\.[0-9]{1,6}0*)?$"), "temperature_precision_or_format"),
        (F.col("time_resolution").isin(*RESOLUTIONS), "unsupported_resolution"),
        (F.col("time_offset").rlike(r"^PT(?:[0-9]+H)?(?:[0-9]+M)?(?:[0-9]+S)?$") & (F.col("time_offset") != "PT"), "invalid_offset"),
        ((F.col("time_series_id") >= 0) & F.col("source_observation.timeSeriesId").rlike(r"^[0-9]+$"), "invalid_series_id"),
        ((F.col("level_type") == "height_above_ground") & (F.col("level_unit") == "m") & (F.col("level_value") >= 0)
         & F.col("source_observation.level.value").rlike(r"^[+-]?[0-9]+(?:\.[0-9]{1,4}0*)?$"), "invalid_sensor_level"),
        (F.col("source_observation.qualityCode").isin("0", "1", "2", "4"), "unaccepted_quality_code"),
    ]
    # Keep every applicable reason so a missing unit cannot hide another issue.
    reasons = []
    for condition, reason in checks:
        reasons.append(F.when(~F.coalesce(condition, F.lit(False)), F.lit(reason)))
    return rows.withColumn("issues", F.filter(F.array(*reasons), lambda issue: issue.isNotNull()))


def classify_temperature(rows):
    """Keep distinct series and account for duplicates without choosing conflicting values."""
    invalid = rows.filter(F.size("issues") > 0).withColumn("disposition_reason", F.lit("invalid_observation"))
    valid = rows.filter(F.size("issues") == 0)
    # Include quality and sensor metadata when deciding if two observations agree.
    value = F.struct("temperature_c", "unit", "quality_code", "performance_category", "exposure_category")
    variants = valid.groupBy(*BUSINESS_KEY).agg(F.countDistinct(value).alias("_variants"))
    inspected = valid.join(variants, BUSINESS_KEY)
    conflicts = inspected.filter("_variants > 1").drop("_variants")
    conflicts = conflicts.withColumn("disposition_reason", F.lit("conflicting_observations"))
    copies = Window.partitionBy(*BUSINESS_KEY).orderBy("source_file", "record_index", "observation_index")
    equivalent = inspected.filter("_variants = 1").drop("_variants").withColumn("_copy", F.row_number().over(copies))
    accepted = equivalent.filter("_copy = 1").drop("_copy").withColumn("disposition_reason", F.lit(None).cast("string"))
    duplicates = equivalent.filter("_copy > 1").drop("_copy").withColumn("disposition_reason", F.lit("duplicate_observation"))
    return accepted, invalid.unionByName(conflicts), duplicates


def duration_seconds(value):
    match = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", value)
    if not match or not any(match.groups()):
        raise ValueError("Unsupported duration")
    hours, minutes, seconds = match.groups()
    return int(hours or 0) * 3600 + int(minutes or 0) * 60 + int(seconds or 0)


def load_series(bronze_root, stations, start, end):
    """Read validity metadata for nominal coverage, without assuming every station is hourly."""
    first, limit = epoch(start + "T00:00:00Z"), epoch(end + "T00:00:00Z")
    intervals, selected, failures = [], [], []
    for station in validate_stations(stations):
        folder = Path(bronze_root) / "series" / f"station={station}"
        try:
            records, lineage = load_response(folder, station)
            station_intervals = []
            for record in records:
                if record.get("elementId") != "air_temperature":
                    continue
                lower = max(first, epoch(record["validFrom"]))
                upper = min(limit, epoch(record["validTo"]) if record.get("validTo") else limit)
                if lower >= upper:
                    continue
                step = RESOLUTIONS[record["timeResolution"]]
                offset = duration_seconds(record["timeOffset"])
                source = record["sourceId"]
                if not re.fullmatch(station + r":[0-9]+", source):
                    raise ValueError("Series source does not match requested station")
                level = record["level"]
                height = Decimal(str(level["value"]))
                if not height.is_finite() or height != height.quantize(Decimal("0.0001")) or abs(height) >= 100000000:
                    raise ValueError("Invalid series height")
                first_tick = lower + (offset - lower) % step
                if first_tick < upper:
                    station_intervals.append((source, "air_temperature", record["timeResolution"],
                        record["timeOffset"], int(record["timeSeriesId"]), level["levelType"], level["unit"],
                        height, first_tick, upper - 1, step))
            intervals.extend(station_intervals)
            selected.append(lineage)
        except (OSError, ValueError, KeyError, TypeError, ArithmeticError) as exc:
            failures.append({"station_id": station, "error": str(exc)})
    return intervals, selected, failures


def coverage_report(spark, accepted, intervals):
    """Report nominal sampling gaps, not invented observations or hourly completeness.

    Validity windows are treated as [validFrom, validTo). Frequencies can vary
    within a series, so nominal gaps are diagnostics rather than publication gates.
    """
    schedules = spark.createDataFrame(intervals, SERIES_SCHEMA)
    expected = schedules.select(
        *SERIES_KEY,
        F.explode(F.sequence("first_second", "last_second", "step_seconds")).alias("second"),
    ).select(*SERIES_KEY, F.col("second").cast("timestamp").alias("observed_utc")).distinct()
    observed = accepted.select(*BUSINESS_KEY)
    missing = expected.join(observed, BUSINESS_KEY, "left_anti")
    unexpected = observed.join(expected, BUSINESS_KEY, "left_anti")
    report = expected.groupBy(*SERIES_KEY).count().withColumnRenamed("count", "nominal_expected")
    actual = observed.groupBy(*SERIES_KEY).count().withColumnRenamed("count", "accepted_observations")
    report = report.join(actual, SERIES_KEY, "full")
    report = report.join(missing.groupBy(*SERIES_KEY).count().withColumnRenamed("count", "nominal_missing"), SERIES_KEY, "left")
    report = report.join(unexpected.groupBy(*SERIES_KEY).count().withColumnRenamed("count", "outside_nominal_schedule"), SERIES_KEY, "left")
    return report.fillna(0, subset=["accepted_observations", "nominal_missing", "outside_nominal_schedule"])


def process_month(spark, bronze_root, batch, start, end, stations):
    """Write one bounded monthly batch and release staging only after Spark finishes."""
    batch = Path(batch)
    batch.mkdir(parents=True)
    staging = batch / "_staged"
    cached = []
    try:
        paths, selected, failures = stage_month(bronze_root, staging, start, end, stations)
        save_json(batch / "inputs.json", {"selected": selected, "failures": failures})
        normalized = normalize_temperature(read_temperature(spark, paths)).cache()
        cached.append(normalized)
        input_rows = normalized.count()
        expected_input = sum(item["source_observations"] for item in selected)
        if input_rows != expected_input:
            raise ValueError("Expanded Spark rows do not reconcile with source observations")
        accepted, quarantine, duplicates = classify_temperature(normalized)
        outputs = {"accepted": accepted, "quarantine": quarantine, "duplicates": duplicates}
        counts = {}
        for name, frame in outputs.items():
            frame.cache()
            cached.append(frame)
            counts[name] = frame.count()
        if sum(counts.values()) != input_rows:
            raise ValueError("Row dispositions do not reconcile")
        if accepted.groupBy(*BUSINESS_KEY).count().filter("count > 1").count():
            raise ValueError("Accepted observation keys are not unique")

        intervals, series_selected, series_failures = load_series(bronze_root, stations, start, end)
        save_json(batch / "series_inputs.json", {"selected": series_selected, "failures": series_failures})
        coverage = coverage_report(spark, accepted, intervals).cache()
        cached.append(coverage)
        issues = {}
        for row in quarantine.select(F.explode("issues").alias("issue")).groupBy("issue").count().collect():
            issues[row["issue"]] = row["count"]
        conflict_rows = quarantine.filter("disposition_reason = 'conflicting_observations'").count()
        coverage_counts = coverage.agg(F.sum("nominal_missing"), F.sum("outside_nominal_schedule")).first()
        quality = {
            "source_records": sum(item["source_records"] for item in selected),
            "input_observations": input_rows, "accepted_rows": counts["accepted"],
            "quarantined_rows": counts["quarantine"], "duplicate_rows": counts["duplicates"],
            "conflicting_rows": conflict_rows, "issue_counts": issues,
            "uncertain_accepted_rows": accepted.filter("quality_uncertain").count(),
            "negative_accepted_rows": accepted.filter("temperature_c < 0").count(),
            "requested_station_months": len(set(stations)),
            "empty_station_months": sum(item["status"] == "empty" for item in selected),
            "input_failures": len(failures), "series_metadata_failures": len(series_failures),
            "nominal_missing_observations": coverage_counts[0] or 0,
            "outside_nominal_schedule": coverage_counts[1] or 0,
        }
        for name, frame in outputs.items():
            frame.write.parquet(str(batch / name))
        coverage.write.parquet(str(batch / "coverage"))
        save_json(batch / "quality.json", quality)
        return quality
    finally:
        for frame in cached:
            frame.unpersist(blocking=True)
        if staging.exists():
            shutil.rmtree(staging)


def run(bronze_root, output_root, start, end, stations, spark):
    """Process full UTC months with a caller-owned Spark session and explicit station list."""
    periods = monthly_periods(start, end)
    stations = validate_stations(stations)
    bronze_root, output_root = Path(bronze_root).resolve(), Path(output_root).resolve()
    if output_root == bronze_root or bronze_root in output_root.parents:
        raise ValueError("Silver output must not be inside Bronze")
    output = output_root / uuid4().hex
    output.mkdir(parents=True)
    info = {"status": "started", "rule_version": RULE_VERSION,
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "start_date": start, "end_date_exclusive": end, "date_timezone": "UTC",
            "stations": stations, "spark_version": spark.version,
            "scope_note": "Dates use UTC months, not Norwegian calendar-year boundaries.",
            "completed_months": []}
    save_json(output / "run.json", info)
    reports = []
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        for first, limit in periods:
            print(f"Silver temperature: processing {first} to {limit}", flush=True)
            batch = output / f"month={first[:7]}"
            quality = process_month(spark, bronze_root, batch, first, limit, stations)
            reports.append({"month": first[:7], **quality})
            info["completed_months"].append(first[:7])
            save_json(output / "run.json", info)
        totals = {}
        for key in reports[0]:
            if isinstance(reports[0][key], int):
                totals[key] = sum(report[key] for report in reports)
        save_json(output / "quality.json", {"totals": totals, "months": reports})
        info["status"] = "partial" if any(totals[k] for k in ("quarantined_rows", "input_failures", "series_metadata_failures")) else "success"
        # Operational success never implies complete or geographically representative coverage.
        info["coverage_requires_review"] = any(totals[k] for k in ("empty_station_months", "nominal_missing_observations", "outside_nominal_schedule"))
    except BaseException as exc:
        info.update(status="failed", error=str(exc), error_type=type(exc).__name__)
        raise
    finally:
        info["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        save_json(output / "run.json", info)
    print(f"Silver temperature output: {output}")
    return output

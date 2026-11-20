"""Run all Silver transformations sequentially with one SparkSession."""

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

from pyspark.sql import SparkSession

from silver_layer import (
    elhub_municipality,
    elhub_price_area,
    frost_observations,
    frost_sources,
    municipality_price_areas,
    spot_prices,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def save_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")


def ingest_all(
    bronze_root,
    silver_root,
    start="2024-01-01",
    end="2026-01-01",
    master="local[2]",
    driver_memory="6g",
    shuffle_partitions=16,
):
    """Run datasets in order, stop on failure, and always write a run report."""
    if date.fromisoformat(start) >= date.fromisoformat(end):
        raise ValueError("start must be before end (exclusive)")

    bronze_root = Path(bronze_root).resolve()
    silver_root = Path(silver_root).resolve()
    if not bronze_root.is_dir():
        raise FileNotFoundError(f"Bronze root does not exist: {bronze_root}")

    run_id = uuid4().hex
    report_path = silver_root / "_runs" / f"{run_id}.json"
    report = {
        "run_id": run_id,
        "status": "running",
        "bronze_root": str(bronze_root),
        "silver_root": str(silver_root),
        "start": start,
        "end_exclusive": end,
        "spark_master": master,
        "spark_driver_memory": driver_memory,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "steps": {},
    }

    spark = None
    steps = [
        ("elhub_municipality", elhub_municipality.ingest),
        ("elhub_price_area", elhub_price_area.ingest),
        ("spot_prices", spot_prices.ingest),
        ("frost_sources", frost_sources.ingest),
        ("frost_observations", frost_observations.ingest),
        ("municipality_price_areas", municipality_price_areas.ingest),
    ]

    try:
        spark = (
            SparkSession.builder.appName("DAT535 Silver Pipeline")
            .master(master)
            .config("spark.driver.memory", driver_memory)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .getOrCreate()
        )

        for name, function in steps:
            print(f"\n=== Silver: {name} ===", flush=True)
            started = datetime.now(timezone.utc)
            try:
                result = function(
                    bronze_root=bronze_root,
                    silver_root=silver_root,
                    start=start,
                    end=end,
                    spark=spark,
                    run_id=run_id,
                )

                if not isinstance(result, dict):
                    raise TypeError(f"{name}.ingest() must return a dictionary")
                if result.get("status") not in (
                    "success",
                    "cached",
                    "success_with_quarantine",
                ):
                    raise RuntimeError(
                        f"Unexpected status for {name}: {result.get('status')}"
                    )

                report["steps"][name] = {
                    "status": result["status"],
                    "seconds": round(
                        (datetime.now(timezone.utc) - started).total_seconds(), 2
                    ),
                    "result": result,
                }

                print(f"Silver {name}: {result['status']}", flush=True)

            except Exception as exc:
                report["steps"][name] = {
                    "status": "failed",
                    "seconds": round(
                        (datetime.now(timezone.utc) - started).total_seconds(), 2
                    ),
                    "error": str(exc),
                }
                raise

        report["status"] = "success"
        return report

    except Exception as exc:
        report["status"] = "failed"
        report["error"] = str(exc)
        raise

    finally:
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        save_report(report_path, report)
        print(
            f"\nSilver pipeline: {report['status']} | Report: {report_path}", flush=True
        )
        if spark is not None:
            spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bronze-root", type=Path, default=PROJECT_ROOT / "data" / "bronze"
    )
    parser.add_argument(
        "--silver-root", type=Path, default=PROJECT_ROOT / "data" / "silver"
    )
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end", default="2026-01-01", help="Exclusive end date")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--driver-memory", default="6g")
    parser.add_argument("--shuffle-partitions", type=int, default=16)

    args = parser.parse_args()

    if args.shuffle_partitions < 1:
        parser.error("--shuffle-partitions must be positive")

    ingest_all(
        args.bronze_root,
        args.silver_root,
        args.start,
        args.end,
        args.master,
        args.driver_memory,
        args.shuffle_partitions,
    )


if __name__ == "__main__":
    main()

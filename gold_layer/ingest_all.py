"""Run all Gold layers sequentially with one Spark session."""

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession

from gold_layer import analytics, consumption, scheduling, temperature


def main():
    parser = argparse.ArgumentParser(description="Build all DAT535 Gold datasets")
    parser.add_argument("--silver-root", default="data/silver")
    parser.add_argument("--gold-root", default="data/gold")
    parser.add_argument(
        "--start", default="2024-01-01", help="First Norwegian local date (inclusive)"
    )
    parser.add_argument(
        "--end", default="2026-01-01", help="Last Norwegian local date (exclusive)"
    )
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--driver-memory", default="6g")
    args = parser.parse_args()

    silver_root = Path(args.silver_root).expanduser().resolve()
    gold_root = Path(args.gold_root).expanduser().resolve()
    reports_dir = gold_root / "_runs"
    reports_dir.mkdir(parents=True, exist_ok=True)

    run_id = os.environ.get(
        "GITHUB_RUN_ID", datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    )
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    report_path = reports_dir / f"gold-{run_id}-{attempt}.json"
    report = {
        "run_id": run_id,
        "attempt": attempt,
        "started_at": datetime.now(UTC).isoformat(),
        "silver_root": str(silver_root),
        "gold_root": str(gold_root),
        "start": args.start,
        "end": args.end,
        "results": [],
        "status": "running",
    }

    spark = None
    try:
        spark = (
            SparkSession.builder.appName("DAT535-Gold")
            .master(args.master)
            .config("spark.driver.memory", args.driver_memory)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        # Analytics depends on the temperature and consumption Gold outputs.
        for name, module in (
            ("scheduling", scheduling),
            ("temperature", temperature),
            ("consumption", consumption),
            ("analytics", analytics),
        ):
            print(f"\n=== Gold: {name} ===", flush=True)
            result = module.ingest(
                silver_root=silver_root,
                gold_root=gold_root,
                start=args.start,
                end=args.end,
                spark=spark,
                run_id=run_id,
            )
            if result.get("status") not in {"success", "cached"}:
                raise RuntimeError(f"Unexpected Gold status for {name}: {result}")

            report["results"].append(result)
            print(json.dumps(result, default=str, indent=2), flush=True)

        report["status"] = "success"
        print("\nAll Gold datasets completed successfully.", flush=True)

    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise

    finally:
        report["finished_at"] = datetime.now(UTC).isoformat()
        report_path.write_text(
            json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8"
        )
        print(f"Gold report: {report_path}", flush=True)
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

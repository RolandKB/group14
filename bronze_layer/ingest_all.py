"""Run all Bronze ingestions using their existing caches.

Run from the project root:
    python -m bronze_layer.ingest_all --bronze-root data/bronze
"""

import argparse
import json
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

from bronze_layer import (
    elhub,
    frost_observations,
    frost_series,
    frost_sources,
    sime_municipalities,
    spot_prices,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str) + "\n", encoding="utf-8")


def has_failures(result):
    """Our ingestion modules use slightly different failure field names."""
    if not isinstance(result, dict):
        return False
    return bool(
        result.get("failed_count", 0)
        or result.get("failed")
        or result.get("failures")
        or result.get("status") in ("partial", "failed")
        or (isinstance(result.get("results"), dict) and has_failures(result["results"]))
    )


def ingest_all(bronze_root, start, end, refresh=False):
    """Run independent sources first, then Frost sources -> series -> observations."""
    bronze_root = Path(bronze_root).resolve()
    run_id = uuid4().hex
    report = {
        "run_id": run_id,
        "start": start,
        "end_exclusive": end,
        "started_at_utc": datetime.now(UTC).isoformat(),
        "steps": {},
    }

    def run(name, function, **kwargs):
        print(f"\n=== {name} ===", flush=True)
        try:
            result = function(run_id=run_id, **kwargs)
            status = "partial" if has_failures(result) else "success"
            report["steps"][name] = {"status": status, "result": result}
            print(f"{name}: {status}", flush=True)
        except Exception as exc:
            report["steps"][name] = {"status": "failed", "error": str(exc)}
            raise

    try:
        # The Kartverket ZIP is downloaded manually; neighbors are generated in Silver.
        kartverket = bronze_root / "kartverket"
        if not any(kartverket.glob("*.zip")) and not any(kartverket.glob("*.geojson")):
            raise FileNotFoundError(f"No Kartverket ZIP or GeoJSON in {kartverket}")

        run("elhub", elhub.ingest, root=bronze_root / "elhub", refresh=refresh)

        run(
            "sime",
            sime_municipalities.ingest,
            root=bronze_root / "sime/municipalities",
            refresh=refresh,
        )
        run(
            "spot_prices",
            spot_prices.ingest,
            start=start,
            end=end,
            root=bronze_root / "spot_prices",
            refresh=refresh,
        )

        run(
            "frost_sources",
            frost_sources.ingest,
            root=bronze_root / "frost/sources",
            refresh=refresh,
        )
        run(
            "frost_series",
            frost_series.ingest,
            start=start,
            end=end,
            sources_file=bronze_root / "frost/sources/country=NO/response.json",
            root=bronze_root / "frost/series",
            refresh=refresh,
        )
        run(
            "frost_observations",
            frost_observations.ingest,
            start=start,
            end=end,
            series_root=bronze_root / "frost/series",
            root=bronze_root / "frost/observations",
            refresh=refresh,
        )
    except Exception as exc:
        report["error"] = str(exc)
        report["status"] = "failed"
        raise
    finally:
        report.setdefault(
            "status",
            "partial"
            if any(step["status"] != "success" for step in report["steps"].values())
            else "success",
        )
        report["finished_at_utc"] = datetime.now(UTC).isoformat()
        output = bronze_root / "_runs" / f"{run_id}.json"
        save_json(output, report)
        print(f"\nBronze run: {report['status']} | Report: {output}", flush=True)

    if report["status"] != "success":
        raise SystemExit(1)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bronze-root", type=Path, default=PROJECT_ROOT / "data/bronze"
    )
    parser.add_argument("--start", default="2024-01-01")
    parser.add_argument("--end", default="2026-01-01", help="Exclusive end date")
    parser.add_argument(
        "--refresh", action="store_true", help="Redownload cached API responses"
    )
    args = parser.parse_args()
    if date.fromisoformat(args.start) >= date.fromisoformat(args.end):
        parser.error("--start must be earlier than --end")
    ingest_all(args.bronze_root, args.start, args.end, args.refresh)


if __name__ == "__main__":
    main()

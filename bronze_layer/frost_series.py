"""Download Frost time-series metadata and identify temperature-series candidates."""

import json
import os
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4
from tqdm.auto import tqdm

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parent.parent 
SOURCES_FILE = PROJECT_ROOT / "data/bronze/frost/sources/country=NO/response.json"
DEFAULT_ROOT = PROJECT_ROOT / "data/bronze/frost/series"
API_URL = "https://frost.met.no/observations/availableTimeSeries/v0.jsonld"


def save_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def load_station_ids(sources_file=SOURCES_FILE):
    """Read station IDs from the raw Frost sources response saved in Bronze."""
    path = Path(sources_file)
    if not path.is_file():
        raise FileNotFoundError(f"Run frost_sources ingestion first. Missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    stations = payload.get("data")
    if not isinstance(stations, list):
        raise ValueError("Sources JSON has no valid 'data' list")
    return sorted({row["id"] for row in stations if row.get("id")})



# Finds all air_temperature series where validity overlaps the requested period.
def temperature_candidates(payload, start, end):
    """Find air_temperature series whose advertised validity overlaps [start, end)."""
    candidates = []
    for row in payload.get("data", []):
        if row.get("elementId") != "air_temperature":
            continue
        valid_from = (row.get("validFrom") or "0001-01-01")[:10]
        valid_to = (row.get("validTo") or "9999-12-31")[:10]
        if valid_from < end and valid_to >= start:
            candidates.append({
                "source_id": row.get("sourceId"),
                "resolution": row.get("timeResolution"),
                "valid_from": row.get("validFrom"),
                "valid_to": row.get("validTo"),
                "time_series_id": row.get("timeSeriesId"),
                "level": row.get("level"),
            })
    return candidates


def ingest(start="2024-01-01", end="2026-01-01", station_ids=None,
           root=DEFAULT_ROOT, sources_file=SOURCES_FILE, refresh=False, run_id=None):
    """Save raw series metadata for stations and summarize eligible temperatures.

    station_ids=None loads ALL station IDs from the saved Frost sources JSON.
    Pass station_ids=["SN40", ...] for a test.
    The end date is exclusive.
    """
    if date.fromisoformat(start) >= date.fromisoformat(end):
        raise ValueError("start must be earlier than end")
    run_id = run_id or uuid4().hex
    root = Path(root)
    stations = sorted(set(station_ids)) if station_ids is not None else load_station_ids(sources_file)
    results = {}
    downloaded = cached = eligible = 0
    failed = []

    # Retries are for temporary API/network problems, not invalid requests.
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        for index, station in enumerate(tqdm(stations, desc="Frost series", unit="station"), 1):
            raw_file = root / f"station={station}" / "response.json"
            try:
                if raw_file.is_file() and not refresh:
                    payload = json.loads(raw_file.read_text(encoding="utf-8"))
                    status = "cached"
                    cached += 1
                else:
                    client_id = os.getenv("FROST_CLIENT_ID")
                    if not client_id:
                        raise ValueError("Set FROST_CLIENT_ID before downloading Frost data")
                    response = session.get(
                        API_URL,
                        params={"sources": f"{station}:all"},
                        auth=(client_id, ""),
                        timeout=60,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if payload.get("@type") == "ErrorResponse" or not isinstance(payload.get("data"), list):
                        raise ValueError("Invalid Frost series response")
                    save_json(raw_file, payload)
                    status = "downloaded"
                    downloaded += 1

                if not isinstance(payload.get("data"), list):
                    raise ValueError("Cached series response has no valid data list")
                candidates = temperature_candidates(payload, start, end)
                if candidates:
                    eligible += 1
                results[station] = {
                    "status": status,
                    "candidate_count": len(candidates),
                    "candidates": candidates,
                }
                print("%d/%d %s: %s, %d candidate series", index, len(stations),
                         station, status, len(candidates))
            except (requests.RequestException, ValueError, OSError, json.JSONDecodeError) as exc:
                print("%d/%d %s: failed: %s", index, len(stations), station, exc)
                failed.append({"station": station, "error": str(exc)})
                results[station] = {"status": "failed", "candidate_count": 0, "candidates": []}
    finally:
        session.close()

    summary = {
        "run_id": run_id,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "period_start": start,
        "period_end_exclusive": end,
        "station_count": len(stations),
        "eligible_count": eligible,
        "downloaded_count": downloaded,
        "cached_count": cached,
        "failed_count": len(failed),
        "failed": failed,
        "results": results,
    }
    save_json(root / "_runs" / f"{run_id}.json", summary)
    return summary

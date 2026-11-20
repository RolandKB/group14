"""Ingest monthly Frost air-temperature observations into Bronze.
The end date is exclusive.
"""

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import requests
from requests.adapters import HTTPAdapter
from tqdm.auto import tqdm
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SERIES_ROOT = PROJECT_ROOT / "data/bronze/frost/series"
DEFAULT_ROOT = PROJECT_ROOT / "data/bronze/frost/observations"
API_URL = "https://frost.met.no/observations/v0.jsonld"


def save_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temporary file first to avoid partial cache files.
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    temporary.replace(path)


def months(start, end):
    """Yield calendar-month chunks covering start to end."""
    current = date.fromisoformat(start)
    limit = date.fromisoformat(end)
    if current >= limit:
        raise ValueError("start must be earlier than end")
    while current < limit:
        following = date(
            current.year + (current.month == 12), current.month % 12 + 1, 1
        )
        stop = min(following, limit)
        yield current.isoformat(), stop.isoformat()
        current = stop


def temperature_candidates(payload, start, end):
    """Return advertised temperature series overlapping start to end."""
    matches = []
    for row in payload.get("data", []):
        if row.get("elementId") != "air_temperature":
            continue
        valid_from = (row.get("validFrom") or "0001-01-01")[:10]
        valid_to = (row.get("validTo") or "9999-12-31")[:10]
        if valid_from < end and valid_to >= start:
            matches.append(row)
    return matches


def eligible_stations(start, end, series_root=SERIES_ROOT):
    """Discover eligible stations using cached responses."""
    series_root = Path(series_root)
    if not series_root.is_dir():
        raise FileNotFoundError(f"Run frost_series ingestion first: {series_root}")
    stations = []
    for raw_file in sorted(series_root.glob("station=*/response.json")):
        payload = json.loads(raw_file.read_text(encoding="utf-8"))
        if not isinstance(payload.get("data"), list):
            raise TypeError(f"Invalid series cache: {raw_file}")
        if temperature_candidates(payload, start, end):
            stations.append(raw_file.parent.name.removeprefix("station="))
    if not list(series_root.glob("station=*/response.json")):
        raise ValueError(f"No station series responses found in {series_root}")
    return sorted(set(stations))


def is_no_data_response(response):
    """Recognize Frost's explicit no-data errors, not arbitrary HTTP errors."""
    if response.status_code not in (404, 412):
        return False
    try:
        reason = response.json().get("error", {}).get("reason", "")
    except (ValueError, AttributeError, TypeError):
        return False
    if not isinstance(reason, str):
        return False
    return (response.status_code == 404 and reason.startswith("No data found")) or (
        response.status_code == 412 and reason.startswith("No time series found")
    )


def ingest(
    start="2024-01-01",
    end="2026-01-01",
    station_ids=None,
    root=DEFAULT_ROOT,
    series_root=SERIES_ROOT,
    refresh=False,
    run_id=None,
):
    """Download monthly air-temperature responses for selected or eligible stations.

    station_ids=None: automatically use eligible stations from frost_series cache.
    station_ids=[...]: test a small subset.
    A cached month is not downloaded again unless refresh=True.
    """
    periods = list(months(start, end))
    stations = (
        sorted(set(station_ids))
        if station_ids is not None
        else eligible_stations(start, end, series_root)
    )
    run_id = run_id or uuid4().hex
    root = Path(root)
    counts = {"downloaded": 0, "cached": 0, "empty": 0, "failed": 0}
    failures = []
    total = len(stations) * len(periods)

    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    client_id = os.getenv("FROST_CLIENT_ID")

    progress = tqdm(total=total, desc="Frost observations", unit="month")
    try:
        for station in stations:
            for first, last in periods:
                raw_file = (
                    root
                    / f"station={station}"
                    / f"period={first}_to_{last}"
                    / "response.json"
                )
                no_data_file = raw_file.with_name("no_data.json")
                try:
                    if no_data_file.is_file() and not refresh:
                        counts["empty"] += 1
                        continue
                    if raw_file.is_file() and not refresh:
                        payload = json.loads(raw_file.read_text(encoding="utf-8"))
                        if not isinstance(payload.get("data"), list):
                            raise ValueError("Invalid cached observations response")
                        counts["cached"] += 1
                    else:
                        if not client_id:
                            raise ValueError(
                                "Set FROST_CLIENT_ID before downloading Frost data"
                            )
                        response = session.get(
                            API_URL,
                            params={
                                "sources": station
                                if ":" in station
                                else f"{station}:all",
                                "referencetime": f"{first}/{last}",
                                "elements": "air_temperature",
                            },
                            auth=(client_id, ""),
                            timeout=120,
                        )
                        if is_no_data_response(response):
                            save_json(no_data_file, response.json())
                            counts["empty"] += 1
                            continue
                        response.raise_for_status()
                        payload = response.json()
                        if not isinstance(payload.get("data"), list):
                            raise ValueError("Frost response has no valid data list")
                        save_json(raw_file, payload)
                        counts["downloaded"] += 1
                    if not payload["data"]:
                        counts["empty"] += 1
                except (
                    requests.RequestException,
                    ValueError,
                    OSError,
                    json.JSONDecodeError,
                ) as exc:
                    counts["failed"] += 1
                    failures.append(
                        {
                            "station": station,
                            "start": first,
                            "end": last,
                            "error": str(exc),
                        }
                    )
                finally:
                    progress.update(1)
                    progress.set_postfix(
                        downloaded=counts["downloaded"],
                        cached=counts["cached"],
                        empty=counts["empty"],
                        failed=counts["failed"],
                    )
    finally:
        progress.close()
        session.close()

    summary = {
        "run_id": run_id,
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "period_start": start,
        "period_end_exclusive": end,
        "station_count": len(stations),
        "months_per_station": len(periods),
        "total_station_months": total,
        "downloaded_count": counts["downloaded"],
        "cached_count": counts["cached"],
        "empty_count": counts["empty"],
        "failed_count": counts["failed"],
        "failed": failures,
    }
    save_json(root / "_runs" / f"{run_id}.json", summary)
    return summary

"""Download and cache raw metadata for Norwegian Frost weather stations."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import uuid
import requests
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "bronze" / "frost" / "sources"

API_URL = "https://frost.met.no/sources/v0.jsonld"
DEFAULT_ROOT = Path("data/bronze/frost/sources")


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def ingest(root=DEFAULT_OUTPUT, refresh=False, run_id=None):
    """Fetch Norwegian station metadata, or reuse the saved response."""
    root = Path(root)
    raw_file = root / "country=NO" / "response.json"

    if run_id is None:
        run_id = uuid.uuid4().hex

    if raw_file.exists() and not refresh:
        payload = json.loads(raw_file.read_text(encoding="utf-8"))
        status = "cached"
        print(f"Using cached Frost sources: {raw_file}")
    else:
        client_id = os.getenv("FROST_CLIENT_ID")
        if not client_id:
            raise ValueError("Set FROST_CLIENT_ID before downloading Frost data")

        response = requests.get(
            API_URL,
            params={"country": "NO", "types": "SensorSystem"},
            auth=(client_id, ""),
            timeout=60,
        )

        response.raise_for_status()  # Raise an error if the request failed
        payload = response.json()

        if payload.get("@type") == "ErrorResponse" or not isinstance(
            payload.get("data"), list
        ):
            raise ValueError("Frost did not return a valid sources response")

        save_json(raw_file, payload)
        status = "downloaded"
        print(f"Saved Frost sources: {raw_file}")

    stations = payload.get("data", [])

    if not isinstance(stations, list):
        raise ValueError("Cached Frost response has no valid data list")

    station_ids = sorted({station["id"] for station in stations if station.get("id")})
    total = payload.get("totalItemCount")

    if total is not None and int(total) != len(stations):
        print(
            f"Warning: Frost reports {total} items, but the response contains {len(stations)}"
        )

    result = {
        "run_id": run_id,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "station_count": len(station_ids),
        "station_ids": station_ids,
        "raw_file": str(raw_file),
        "returned_items": len(stations),
        "reported_total_items": total,
    }

    save_json(root / "_runs" / f"{run_id}.json", result)
    return result

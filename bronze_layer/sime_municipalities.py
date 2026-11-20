"""Download Sime's municipality-to-electricity-price-area mapping into Bronze."""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ROOT = PROJECT_ROOT / "data/bronze/sime/municipalities"
URL = "https://sime.no/api/stromdata/v1/kommuner"
PRICE_AREAS = {"NO1", "NO2", "NO3", "NO4", "NO5"}


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    try:
        temporary.write_text(
            json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def validate(payload):
    """Check the fields needed by the downstream Silver join."""
    if not isinstance(payload, dict) or not isinstance(payload.get("kommuner"), list):
        raise ValueError("Expected an object with a 'kommuner' list")
    if not payload["kommuner"]:
        raise ValueError("Municipality list is empty")

    seen = set()
    multi_area = []
    for municipality in payload["kommuner"]:
        if not isinstance(municipality, dict):
            raise ValueError("Each municipality must be a JSON object")
        code = municipality.get("number")
        area = municipality.get("priceArea")
        areas = municipality.get("priceAreas")
        if not isinstance(code, str) or len(code) != 4 or not code.isdigit():
            raise ValueError(f"Invalid municipality code: {code!r}")
        if code in seen:
            raise ValueError(f"Duplicate municipality code: {code}")
        seen.add(code)
        if area not in PRICE_AREAS or not isinstance(areas, list) or not areas:
            raise ValueError(f"Invalid price area for municipality {code}")
        if any(value not in PRICE_AREAS for value in areas) or area not in areas:
            raise ValueError(f"Inconsistent price areas for municipality {code}")
        if len(areas) > 1:
            multi_area.append(code)

    return {
        "municipality_count": len(seen),
        "multi_area_count": len(multi_area),
        "multi_area_codes": multi_area,
    }


def download(session):
    """Retry temporary HTTP/network failures."""
    for attempt in range(4):
        try:
            response = session.get(URL, timeout=60)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            status = exc.response.status_code if exc.response is not None else None
            if attempt == 3 or (
                status is not None and status not in (429, 500, 502, 503, 504)
            ):
                raise
            time.sleep(2**attempt)


def ingest(root=DEFAULT_ROOT, refresh=False, run_id=None):
    """Save the raw response, metadata, and a summary of this ingestion run."""
    root = Path(root)
    run_id = run_id or uuid4().hex
    raw_path = root / "response.json"
    result = {
        "run_id": run_id,
        "status": "started",
        "url": URL,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    try:
        if raw_path.exists() and not refresh:
            try:
                cached = json.loads(raw_path.read_text(encoding="utf-8"))
                stats = validate(cached)
            except (OSError, ValueError, TypeError):
                print("Cached response is invalid; downloading again.")
            else:
                result.update(status="cached", **stats)
                print(f"Cached: {stats['municipality_count']} municipalities")
                return result

        with requests.Session() as session:
            response = download(session)
            payload = response.json()
            stats = validate(payload)

        # Preserve the exact response bytes, including the provider's attribution.
        root.mkdir(parents=True, exist_ok=True)
        temporary = root / "response.json.tmp"
        try:
            temporary.write_bytes(response.content)
            temporary.replace(raw_path)
        finally:
            temporary.unlink(missing_ok=True)

        metadata = {
            "source": "Sime",
            "url": URL,
            "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id,
            "response_file": raw_path.name,
            "http_status": response.status_code,
            "size_bytes": len(response.content),
            **stats,
            "attribution": payload.get("attribution"),
        }
        save_json(root / "metadata.json", metadata)
        result.update(status="downloaded", **stats)
        print(
            f"Downloaded: {stats['municipality_count']} municipalities "
            f"({stats['multi_area_count']} with multiple price areas)"
        )
    except (requests.RequestException, ValueError, TypeError, OSError) as exc:
        result.update(status="failed", error=str(exc))
        raise
    finally:
        result["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        save_json(root / "_runs" / f"{run_id}.json", result)

    return result

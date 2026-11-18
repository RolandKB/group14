"""Bronze ingestion of Norwegian spot prices from Hva koster strømmen."""

import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import requests
from tqdm.auto import tqdm

BASE_URL = "https://www.hvakosterstrommen.no/api/v1/prices"
PRICE_AREAS = ("NO1", "NO2", "NO3", "NO4", "NO5")
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ROOT = PROJECT_ROOT / "data" / "bronze" / "spot_prices"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def save_json(path, data):
    """Save a JSON summary or metadata file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def download(session, url, attempts=4):
    """Retry temporary network errors, rate limits and server errors."""
    for attempt in range(attempts):
        try:
            response = session.get(url, timeout=60)
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            status = error.response.status_code if error.response is not None else None
            retryable = status is None or status in (429, 500, 502, 503, 504)
            if not retryable or attempt == attempts - 1:
                raise
            time.sleep(2 ** attempt)


def ingest(start, end, areas=PRICE_AREAS, root=DEFAULT_ROOT,
           refresh=False, run_id=None):
    """Download daily spot prices for start to end, with end exclusive.

    Example: ingest("2024-01-01", "2024-01-03", areas=["NO2"])
    downloads January 1 and January 2 for NO2.
    """
    first = date.fromisoformat(start)
    last = date.fromisoformat(end)
    if first >= last:
        raise ValueError("start must be earlier than end (exclusive)")
    if first < date(2021, 12, 1):
        raise ValueError("This API documents prices from 2021-12-01")

    areas = list(dict.fromkeys(areas))  # Remove duplicates, preserve order.
    if not areas or any(area not in PRICE_AREAS for area in areas):
        raise ValueError("areas must contain one or more of NO1, NO2, NO3, NO4, NO5")

    root = Path(root)
    run_id = run_id or uuid4().hex
    results = {
        "run_id": run_id,
        "total_requests": (last - first).days * len(areas),
        "downloaded_count": 0,
        "cached_count": 0,
        "failed_count": 0,
        "failures": [],
    }
    started_at = utc_now()
    status = "success"

    try:
        with requests.Session() as session:
            session.headers.update({
                "Accept": "application/json",
                "User-Agent": "DAT535-group14/1.0",
            })
            with tqdm(total=results["total_requests"], desc="Spot prices", unit="day-area") as bar:
                day = first
                while day < last:
                    for area in areas:
                        folder = root / f"area={area}" / f"date={day.isoformat()}"
                        raw_path = folder / "response.json"
                        metadata_path = folder / "metadata.json"
                        url = f"{BASE_URL}/{day.year}/{day:%m-%d}_{area}.json"

                        try:
                            # Cache only a nonempty, valid JSON list.
                            if raw_path.exists() and not refresh:
                                cached = json.loads(raw_path.read_text(encoding="utf-8"))
                                if isinstance(cached, list) and cached:
                                    results["cached_count"] += 1
                                    continue

                            response = download(session, url)
                            payload = response.json()
                            if not isinstance(payload, list) or not payload:
                                raise ValueError("Expected a nonempty JSON list of price intervals")

                            folder.mkdir(parents=True, exist_ok=True)
                            # Write a temporary file first.
                            temp_path = folder / f".{uuid4().hex}.tmp"
                            try:
                                temp_path.write_bytes(response.content)
                                temp_path.replace(raw_path)
                            finally:
                                temp_path.unlink(missing_ok=True)

                            save_json(metadata_path, {
                                "source": "Hva koster strømmen",
                                "url": response.url,
                                "retrieved_at_utc": utc_now(),
                                "run_id": run_id,
                                "price_area": area,
                                "date": day.isoformat(),
                                "timezone": "Europe/Oslo",
                                "record_count": len(payload),
                            })
                            results["downloaded_count"] += 1

                        except (requests.RequestException, ValueError, OSError) as error:
                            results["failed_count"] += 1
                            results["failures"].append({
                                "date": day.isoformat(),
                                "area": area,
                                "url": url,
                                "error": str(error),
                            })
                        finally:
                            bar.update(1)
                            bar.set_postfix(
                                downloaded=results["downloaded_count"],
                                cached=results["cached_count"],
                                failed=results["failed_count"],
                            )
                    day += timedelta(days=1)
        if results["failed_count"]:
            status = "partial"
    except BaseException:
        status = "failed"
        raise
    finally:
        save_json(root / "_runs" / f"{run_id}.json", {
            "run_id": run_id,
            "started_at_utc": started_at,
            "finished_at_utc": utc_now(),
            "status": status,
            "period_start": start,
            "period_end_exclusive": end,
            "areas": areas,
            "refresh": refresh,
            "results": results,
        })

    return results

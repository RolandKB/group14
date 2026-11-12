"""Shared Frost authentication, raw response versioning and retrieval manifests."""

import hashlib
import json
import os
import time
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import requests
from dotenv import load_dotenv

from bronze_layer.bronze_common import write_json

BASE = "https://frost.met.no"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def now():
    return datetime.now(UTC).isoformat()


def validate_period(start, end):
    if date.fromisoformat(start) >= date.fromisoformat(end):
        raise ValueError("start must precede exclusive end")


def frost_session():
    load_dotenv(PROJECT_ROOT / ".env")
    token = (
        os.getenv("FROST_CLIENT_ID")
        or os.getenv("FROST_API_TOKEN")
        or os.getenv("CLIENT_ID")
    )
    if not token or token == "your_client_id_here":
        raise RuntimeError(
            "Set FROST_CLIENT_ID (or CLIENT_ID / FROST_API_TOKEN) in .env or the environment"
        )
    session = requests.Session()
    session.auth = (token, "")
    session.headers.update(
        {"Accept": "application/json", "User-Agent": "DAT535-group14/1.0"}
    )
    return session


class RawPayload(dict):
    """Parsed data for discovery plus unchanged HTTP response bytes for Bronze."""

    def __init__(self, response):
        super().__init__(response.json())
        self.raw = response.content
        self.http_status = response.status_code


def get_json(session, endpoint, params, attempts=4):
    for attempt in range(attempts):
        try:
            response = session.get(BASE + endpoint, params=params, timeout=(15, 90))
            # Preserve documented no-data responses, including months outside
            # a station's series coverage. Do not suppress unrelated 412 errors.
            if response.status_code in (404, 412):
                payload = RawPayload(response)
                error = payload.get("error", {})
                no_series = (
                    endpoint == "/observations/v0.jsonld"
                    and str(error.get("code")) == "412"
                    and error.get("reason", "").startswith(
                        "No time series found for this combination of parameters"
                    )
                )
                if (
                    response.status_code == 404 and str(error.get("code")) == "404"
                ) or no_series:
                    return payload, response.url
            response.raise_for_status()
            payload = RawPayload(response)
            if not isinstance(payload.get("data"), list):
                raise TypeError("Frost response is missing a data list")
            return payload, response.url
        except requests.RequestException as exc:
            status = exc.response.status_code if exc.response is not None else None
            if attempt == attempts - 1 or (
                status is not None and status not in (429, 500, 502, 503, 504)
            ):
                raise
            time.sleep(min(2**attempt, 8))
    raise RuntimeError("Request retries exhausted")


def store_response(root: Path, payload, url, extra=None):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    body = payload.raw
    digest = hashlib.sha256(body).hexdigest()
    target = root / f"response_{digest}.json"
    if not target.exists():
        temp = root / f".{uuid4().hex}.tmp"
        try:
            temp.write_bytes(body)
            os.replace(temp, target)
        finally:
            temp.unlink(missing_ok=True)
    elif hashlib.sha256(target.read_bytes()).hexdigest() != digest:
        raise ValueError(f"Corrupt cached response: {target}")
    run_id = uuid4().hex
    meta = {
        "source": "Frost",
        "url": url,
        "retrieved_at_utc": now(),
        "sha256": digest,
        "size_bytes": len(body),
        "response_file": target.name,
        "run_id": run_id,
        "status": "empty"
        if payload.http_status in (404, 412) or not payload.get("data")
        else "success",
        "http_status": payload.http_status,
        **(extra or {}),
    }
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    write_json(root / f"retrieval_{stamp}_{run_id}.metadata.json", meta)
    return target


def cached_response(root):
    """Use the latest retrieval, never the alphabetical order of checksums."""
    manifests = sorted(Path(root).glob("retrieval_*.metadata.json"))
    if not manifests:
        return None
    try:
        meta = json.loads(manifests[-1].read_text())
        target = Path(root) / meta["response_file"]
        if hashlib.sha256(target.read_bytes()).hexdigest() == meta["sha256"]:
            return target
    except (OSError, ValueError, KeyError):
        pass
    return None


def write_run(root, results):
    run_id = uuid4().hex
    write_json(
        Path(root) / "_runs" / f"{run_id}.json",
        {
            "run_id": run_id,
            "finished_at_utc": now(),
            "results": results,
        },
    )

#!/usr/bin/env python3
"""Download and version raw Elhub consumption archives in Bronze."""

import argparse
import hashlib
import json
import logging
import os
import re
import tempfile
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse

import requests

DATASET = "CONSUMPTION_PER_GROUP_MBA_HOUR"
SOURCE_URL = (
    "https://api.elhub.no/energy-data/v0/price-areas.csv"
    f"?dataset={DATASET}"
)
DEFAULT_BRONZE_ROOT = Path("data/bronze/elhub")
DATE_RANGE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})-to-(\d{4}-\d{2}-\d{2})")


def utc_now():
    return datetime.now(timezone.utc)


def extract_advertised_period(final_url):
    """Read the advertised date range from the redirect target filename."""
    name = unquote(PurePosixPath(urlparse(final_url).path).name)
    match = DATE_RANGE_RE.search(name)
    if not match:
        return name, None, None
    from datetime import date
    start = date.fromisoformat(match.group(1))
    end = date.fromisoformat(match.group(2))
    if start > end:
        raise ValueError(f"Archive has invalid advertised period: {name}")
    return name, start, end


def write_json_atomic(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
        os.replace(temp_path, path)
    finally:
        temp_path.unlink(missing_ok=True)


def ingest(bronze_root=DEFAULT_BRONZE_ROOT, timeout=180):
    dataset_dir = Path(bronze_root) / DATASET
    dataset_dir.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    started_at = utc_now()
    runs_dir = dataset_dir / "_runs"
    run_log = runs_dir / f"{started_at.strftime('%Y%m%dT%H%M%SZ')}_{run_id}.json"

    run_info = {
        "run_id": run_id,
        "dataset": DATASET,
        "source_url": SOURCE_URL,
        "started_at_utc": started_at.isoformat(),
        "status": "started",
    }
    temp_path = None

    try:
        with requests.get(SOURCE_URL, stream=True, timeout=(15, timeout)) as response:
            response.raise_for_status()
            final_url = response.url
            original_filename, period_start, period_end = extract_advertised_period(final_url)

            # Stream bytes to disk and hash them incrementally; avoid holding ZIP in RAM.
            digest = hashlib.sha256()
            size = 0
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=".elhub_", suffix=".download", dir=dataset_dir, delete=False
            ) as handle:
                temp_path = Path(handle.name)
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)

        if size == 0:
            raise ValueError("Elhub returned an empty download")
        if not zipfile.is_zipfile(temp_path):
            raise ValueError("Expected a ZIP archive, but downloaded content is not a valid ZIP")
        with zipfile.ZipFile(temp_path) as archive:
            bad_member = archive.testzip()
            if bad_member is not None:
                raise ValueError(f"ZIP CRC validation failed for {bad_member}")
            csv_members = [
                item.filename for item in archive.infolist()
                if not item.is_dir() and item.filename.lower().endswith(".csv")
            ]
            if not csv_members:
                raise ValueError("ZIP archive does not contain any CSV files")

        checksum = digest.hexdigest()
        filename = f"{DATASET}_{checksum[:16]}.zip"
        destination = dataset_dir / filename
        metadata_path = dataset_dir / f"{filename}.metadata.json"
        downloaded_at = utc_now()
        expected_end = downloaded_at.date() - timedelta(days=1)
        matches_expected = period_end == expected_end if period_end is not None else None

        if period_start is None:
            logging.warning("No advertised date range found in ZIP filename: %s", original_filename)
        elif not matches_expected:
            logging.warning(
                "Archive ends %s; expected %s (yesterday UTC). Preserving download anyway.",
                period_end, expected_end,
            )

        if destination.exists():
            # Full checksum comparison avoids treating a prefix collision as identical.
            existing_hash = hashlib.sha256()
            with destination.open("rb") as existing:
                for chunk in iter(lambda: existing.read(1024 * 1024), b""):
                    existing_hash.update(chunk)
            if existing_hash.hexdigest() != checksum:
                raise RuntimeError(f"Hash-prefix collision or modified existing file: {destination}")
            status = "unchanged"
        else:
            os.replace(temp_path, destination)
            temp_path = None
            status = "downloaded"

        metadata = {
            "source": "Elhub",
            "dataset": DATASET,
            "source_url": SOURCE_URL,
            "final_url": final_url,
            "original_filename": original_filename,
            "filename": filename,
            "downloaded_at_utc": downloaded_at.isoformat(),
            "sha256": checksum,
            "size_bytes": size,
            "format": "zip",
            "csv_members": csv_members,
            "advertised_period_start": period_start.isoformat() if period_start else None,
            "advertised_period_end": period_end.isoformat() if period_end else None,
            "period_source": "archive_filename" if period_start else None,
            "period_verified": False,
            "expected_period_end_utc": expected_end.isoformat(),
            "period_end_matches_expected": matches_expected,
            "first_seen_run_id": run_id,
            "status": "success",
        }
        # Preserve original version metadata on repeat downloads.
        if not metadata_path.exists():
            write_json_atomic(metadata_path, metadata)

        run_info.update({
            "status": status,
            "finished_at_utc": utc_now().isoformat(),
            "file_path": str(destination),
            "metadata_path": str(metadata_path),
            "sha256": checksum,
            "advertised_period_start": metadata["advertised_period_start"],
            "advertised_period_end": metadata["advertised_period_end"],
            "period_end_matches_expected": matches_expected,
        })
        write_json_atomic(run_log, run_info)
        print(f"Status: {status}")
        print(f"Archive: {destination}")
        print(f"Metadata: {metadata_path}")
        print(f"Run log: {run_log}")
        print(f"Advertised coverage: {period_start} to {period_end}")
        print(f"Size: {size / (1024 * 1024):.2f} MiB")
        return destination

    except Exception as exc:
        run_info.update({
            "status": "failed",
            "finished_at_utc": utc_now().isoformat(),
            "error_type": type(exc).__name__,
            "error": str(exc),
        })
        write_json_atomic(run_log, run_info)
        logging.error("Ingestion failed: %s", exc)
        raise
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-root", type=Path, default=DEFAULT_BRONZE_ROOT)
    parser.add_argument("--timeout", type=int, default=180, help="Read timeout in seconds")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    ingest(args.bronze_root, args.timeout)


if __name__ == "__main__":
    main()

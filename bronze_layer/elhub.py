"""Download and cache the two raw Elhub consumption datasets."""

import hashlib
import json
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse
from uuid import uuid4

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ROOT = PROJECT_ROOT / "data/bronze/elhub"
DATASETS = {
    "CONSUMPTION_PER_GROUP_MBA_HOUR": "price-areas.csv",
    "CONSUMPTION_PER_GROUP_MUNICIPALITY_HOUR": "municipalities.csv",
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_zip(path):
    if not zipfile.is_zipfile(path):
        raise ValueError(f"Not a ZIP archive: {path}")
    with zipfile.ZipFile(path) as archive:
        damaged = archive.testzip()
        if damaged:
            raise ValueError(f"Damaged ZIP member: {damaged}")
        csv_files = [
            name
            for name in archive.namelist()
            if name.lower().endswith(".csv") and not name.endswith("/")
        ]
    if not csv_files:
        raise ValueError(f"ZIP contains no CSV files: {path}")
    return csv_files


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def ingest_dataset(dataset, root=DEFAULT_ROOT, refresh=False, run_id=None):
    """Ingest one Elhub dataset, returning its archive and metadata paths.

    An existing valid archive is reused unless refresh=True. Elhub publishes
    snapshot ZIPs, so use refresh=True to check for a newer snapshot.
    """
    if dataset not in DATASETS:
        raise ValueError(f"Unknown Elhub dataset: {dataset}")

    run_id = run_id or uuid4().hex
    directory = Path(root) / dataset
    directory.mkdir(parents=True, exist_ok=True)
    source_url = (
        f"https://api.elhub.no/energy-data/v0/{DATASETS[dataset]}?dataset={dataset}"
    )
    started = utc_now()
    existing = sorted(
        directory.glob(f"{dataset}_*.zip"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    archive_path = None
    status = "cached"
    final_url = None
    original_name = None
    temp_path = None
    succeeded = False

    try:
        if not refresh:
            for candidate in existing:
                try:
                    inspect_zip(candidate)
                    if sha256_file(candidate).startswith(
                        candidate.stem.removeprefix(dataset + "_")
                    ):
                        archive_path = candidate
                        break
                except (ValueError, OSError, zipfile.BadZipFile):
                    continue

        if archive_path is None:
            with tempfile.NamedTemporaryFile(
                dir=directory, prefix="download_", suffix=".tmp", delete=False
            ) as temp:
                temp_path = Path(temp.name)
            print(f"Downloading {dataset}...")
            with requests.get(source_url, stream=True, timeout=(15, 180)) as response:
                response.raise_for_status()
                final_url = response.url
                original_name = unquote(Path(urlparse(final_url).path).name)
                with temp_path.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            output.write(chunk)
            if temp_path.stat().st_size == 0:
                raise ValueError("Elhub returned an empty file")
            inspect_zip(temp_path)
            checksum = sha256_file(temp_path)
            archive_path = directory / f"{dataset}_{checksum[:16]}.zip"
            if archive_path.exists():
                if sha256_file(archive_path) != checksum:
                    raise ValueError(f"Checksum collision: {archive_path}")
                status = "unchanged"
            else:
                temp_path.replace(archive_path)
                temp_path = None
                status = "downloaded"

        csv_files = inspect_zip(archive_path)
        checksum = sha256_file(archive_path)
        metadata_path = directory / f"{archive_path.name}.metadata.json"
        if not metadata_path.exists():
            match = re.search(
                r"(\d{4}-\d{2}-\d{2})-to-(\d{4}-\d{2}-\d{2})", original_name or ""
            )
            write_json(
                metadata_path,
                {
                    "dataset": dataset,
                    "source_url": source_url,
                    "final_url": final_url,
                    "original_filename": original_name,
                    "saved_filename": archive_path.name,
                    "sha256": checksum,
                    "size_bytes": archive_path.stat().st_size,
                    "csv_files": csv_files,
                    "advertised_period_start": match.group(1) if match else None,
                    "advertised_period_end": match.group(2) if match else None,
                    "period_verified": False,
                    "first_seen_utc": utc_now(),
                },
            )
        result = {
            "dataset": dataset,
            "status": status,
            "archive_path": str(archive_path),
            "metadata_path": str(metadata_path),
            "csv_count": len(csv_files),
            "sha256": checksum,
        }
        print(f"{dataset}: {status} ({len(csv_files)} CSV files)")
        succeeded = True
        return result
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        # A failed ingestion must still propagate to the orchestrator.
        # Successful runs are logged separately from immutable archive metadata.
        write_json(
            directory / "_runs" / f"{run_id}.json",
            {
                "run_id": run_id,
                "started_at_utc": started,
                "finished_at_utc": utc_now(),
                "dataset": dataset,
                "status": status if succeeded else "failed",
                "archive_path": str(archive_path) if archive_path else None,
            },
        )


def ingest(root=DEFAULT_ROOT, refresh=False, run_id=None):
    """Ingest both required Elhub datasets."""
    run_id = run_id or uuid4().hex
    return {
        dataset: ingest_dataset(dataset, root=root, refresh=refresh, run_id=run_id)
        for dataset in DATASETS
    }

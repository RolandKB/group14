"""Read Elhub municipality codes directly from CSV member(s) inside a ZIP."""

import logging
import zipfile
from pathlib import Path

import pandas as pd

from common.runs import ensure_run_id
from common.storage import write_json

LOG = logging.getLogger(__name__)


def discover(
    archive_path: Path, output: Path | None = None, run_id: str | None = None
) -> list[str]:
    """Read municipality codes from CSV members in an Elhub ZIP archive."""
    run_id = ensure_run_id(run_id)
    with zipfile.ZipFile(archive_path) as archive:
        csv_files = [
            member
            for member in archive.namelist()
            if member.lower().endswith(".csv") and not member.endswith("/")
        ]
        frames = [pd.read_csv(archive.open(member)) for member in csv_files]

    if not frames:
        raise ValueError(f"No CSV files found in archive: {archive_path}")

    codes = sorted(
        pd.concat(frames)["MUNICIPALITY_NUMBER"]
        .astype(str)
        .str.zfill(4)
        .unique()
        .tolist()
    )
    if output:
        write_json(output, codes)
        write_json(
            output.with_name(output.name + ".metadata.json"),
            {
                "run_id": run_id,
                "source": "Elhub",
                "source_file": str(archive_path),
                "output_file": str(output),
                "municipality_count": len(codes),
            },
        )
    LOG.info(
        "Run %s: discovered %d municipality codes from %s",
        run_id,
        len(codes),
        archive_path,
    )
    return codes

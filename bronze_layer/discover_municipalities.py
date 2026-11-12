"""Read Elhub municipality codes directly from CSV member(s) inside a ZIP."""
import zipfile
import pandas as pd
from pathlib import Path

from bronze_layer.bronze_common import write_json


def discover(archive_path: Path, output: Path | None = None) -> list[str]:
    """Read municipality codes from CSV members in an Elhub ZIP archive."""
    with zipfile.ZipFile(archive_path) as archive:
        csv_files = [
            member for member in archive.namelist()
            if member.lower().endswith('.csv') and not member.endswith('/')
        ]
        frames = [pd.read_csv(archive.open(member)) for member in csv_files]

    if not frames:
        raise ValueError(f'No CSV files found in archive: {archive_path}')

    codes = sorted(
        pd.concat(frames)['MUNICIPALITY_NUMBER']
        .astype(str)
        .str.zfill(4)
        .unique()
        .tolist()
    )
    if output:
        write_json(output, codes)
    return codes

"""Shared helpers for Bronze-layer ingestion."""
import json
from pathlib import Path


def write_json(path: Path, payload) -> None:
    """Write a JSON payload, creating its parent directory when needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
        handle.write("\n")

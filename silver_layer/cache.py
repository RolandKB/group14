"""Dataset-level Silver cache with safe directory publication."""

import hashlib
import json
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

MANIFEST_NAME = "_silver_manifest.json"


def sha256_file(path, chunk_size=8 * 1024 * 1024):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fingerprint(archive, version, start, end):
    """Hash source bytes and transformation parameters; stable across runs."""
    return {
        "source_sha256": sha256_file(archive),
        "transformation_version": version,
        "start_local": start,
        "end_exclusive_local": end,
    }


def is_cached(output, expected):
    output = Path(output)
    manifest_path = output / MANIFEST_NAME
    if (
        not output.is_dir()
        or not (output / "_SUCCESS").is_file()
        or not manifest_path.is_file()
    ):
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        return (
            manifest.get("fingerprint") == expected
            and isinstance(manifest.get("rows"), int)
            and manifest["rows"] > 0
            and any(output.glob("*.parquet"))
        )
    except (OSError, ValueError, TypeError):
        return False


def read_manifest(output):
    return json.loads((Path(output) / MANIFEST_NAME).read_text(encoding="utf-8"))


def new_staging_directory(output):
    """Return a unique sibling path for Spark to create."""
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    return output.with_name(f".{output.name}.staging-{uuid.uuid4().hex}")


def publish(staging, output, manifest):
    """Publish a validated local directory, restoring previous output on failure.

    Both directories must be on the same filesystem. Publication uses rename,
    with a short interval between moving the old and new directories.
    """
    staging, output = Path(staging), Path(output)
    if not (staging / "_SUCCESS").is_file() or not any(staging.glob("*.parquet")):
        raise ValueError(f"Incomplete Spark output: {staging}")
    payload = dict(manifest)
    payload["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    temporary_manifest = staging / (MANIFEST_NAME + ".tmp")
    temporary_manifest.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    os.replace(temporary_manifest, staging / MANIFEST_NAME)

    backup = output.with_name(f".{output.name}.backup-{uuid.uuid4().hex}")
    moved_old = False
    try:
        if output.exists():
            os.replace(output, backup)
            moved_old = True
        os.replace(staging, output)
    except Exception:
        if moved_old and not output.exists() and backup.exists():
            os.replace(backup, output)
        raise
    if moved_old:
        shutil.rmtree(backup)

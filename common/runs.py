"""Shared pipeline run identifiers."""

from uuid import uuid4


def new_run_id() -> str:
    """Create a unique identifier for one end-to-end pipeline run."""
    return uuid4().hex


def ensure_run_id(run_id: str | None = None) -> str:
    """Use a supplied pipeline ID or create one for standalone execution."""
    return run_id or new_run_id()

"""Download monthly Frost air-temperature observations for selected stations."""

import json
import logging
import time
from collections import deque
from datetime import date
from pathlib import Path

if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bronze_layer.frost_common import (
    cached_response,
    frost_session,
    get_json,
    store_response,
    validate_period,
    write_run,
)

LOG = logging.getLogger(__name__)


def months(start: str, end: str):
    current = date.fromisoformat(start)
    limit = date.fromisoformat(end)
    if current >= limit:
        raise ValueError("start must precede exclusive end")
    while current < limit:
        next_month = date(
            current.year + (current.month == 12), current.month % 12 + 1, 1
        )
        stop = min(next_month, limit)
        yield current.isoformat(), stop.isoformat()
        current = stop


def ingest(
    station_ids, start, end, root=Path("data/bronze/frost/observations"), refresh=False
):
    validate_period(start, end)
    results = {"downloaded": 0, "cached": 0, "empty": 0, "failed": []}
    stations = sorted(set(station_ids))
    periods = list(months(start, end))
    total = len(stations) * len(periods)
    completed = 0
    started = time.monotonic()
    request_times = deque(maxlen=100)
    LOG.info(
        "Observations: %d stations x %d periods = %d requests; output=%s",
        len(stations),
        len(periods),
        total,
        Path(root).resolve(),
    )
    with frost_session() as session:
        for station_index, station in enumerate(stations, 1):
            LOG.info(
                "Station %d/%d: %s; completed %d/%d periods",
                station_index,
                len(stations),
                station,
                completed,
                total,
            )
            for first, last in periods:
                request_started = time.monotonic()
                outcome = "failed"
                record_count = 0
                folder = Path(root) / f"station={station}" / f"period={first}_to_{last}"
                cached = cached_response(folder) if not refresh else None
                if cached:
                    results["cached"] += 1
                    if not json.loads(cached.read_text(encoding="utf-8")).get("data"):
                        results["empty"] += 1
                    completed += 1
                    if completed % 100 == 0 or (first, last) == periods[-1]:
                        LOG.info(
                            "Cache progress: %d/%d (%.1f%%); station %d/%d %s through %s; cached=%d",
                            completed,
                            total,
                            100 * completed / total,
                            station_index,
                            len(stations),
                            station,
                            last,
                            results["cached"],
                        )
                    continue
                try:
                    payload, url = get_json(
                        session,
                        "/observations/v0.jsonld",
                        {
                            "sources": station + ":all"
                            if ":" not in station
                            else station,
                            "referencetime": f"{first}/{last}",
                            "elements": "air_temperature",
                        },
                    )
                    store_response(
                        folder,
                        payload,
                        url,
                        {
                            "station_id": station,
                            "period_start": first,
                            "period_end_exclusive": last,
                        },
                    )
                    if not payload.get("data"):
                        results["empty"] += 1
                        outcome = "empty"
                    else:
                        results["downloaded"] += 1
                        outcome = "downloaded"
                        record_count = len(payload["data"])
                except Exception as exc:
                    LOG.error("Observations %s %s: %s", station, first, exc)
                    results["failed"].append(
                        {
                            "station": station,
                            "start": first,
                            "end": last,
                            "error": str(exc),
                        }
                    )
                completed += 1
                request_times.append(time.monotonic() - request_started)
                average = sum(request_times) / len(request_times)
                eta = (
                    f"{(total - completed) * average / 3600:.2f}h"
                    if len(request_times) >= 10
                    else "estimating"
                )
                LOG.info(
                    "Observations %d/%d (%.1f%%) | station %d/%d %s | %s to %s | "
                    "%s; records=%d | downloaded=%d cached=%d empty=%d failed=%d | "
                    "elapsed=%.1fmin ETA~%s (remaining cache hits may shorten ETA)",
                    completed,
                    total,
                    100 * completed / total,
                    station_index,
                    len(stations),
                    station,
                    first,
                    last,
                    outcome,
                    record_count,
                    results["downloaded"],
                    results["cached"],
                    results["empty"],
                    len(results["failed"]),
                    (time.monotonic() - started) / 60,
                    eta,
                )
    LOG.info(
        "Observations finished in %.1fmin: downloaded=%d cached=%d empty=%d failed=%d",
        (time.monotonic() - started) / 60,
        results["downloaded"],
        results["cached"],
        results["empty"],
        len(results["failed"]),
    )
    write_run(root, results)
    return results

"""Discover available Frost air-temperature series and preserve raw responses."""
import json
import logging
from pathlib import Path
if __package__ in (None, ''):
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bronze_layer.frost_common import frost_session, get_json, store_response, cached_response, validate_period, write_run

LOG = logging.getLogger(__name__)


def series_candidates(payload, start, end):
    candidates = []
    for row in payload.get('data', []):
        if row.get('elementId') != 'air_temperature':
            continue
        valid_from = (row.get('validFrom') or '')[:10]
        valid_to = (row.get('validTo') or '9999-12-31')[:10]
        if valid_from and valid_from < end and valid_to >= start:
            candidates.append({'source_id': row.get('sourceId'), 'resolution': row.get('timeResolution'),
                               'valid_from': valid_from, 'valid_to': valid_to})
    return candidates


def ingest(station_ids, start, end, root=Path('data/bronze/frost/series'), refresh=False):
    validate_period(start, end)
    results = {}
    stations = sorted(set(station_ids))
    LOG.info('Series discovery: %d stations; period=%s to %s; output=%s',
             len(stations), start, end, Path(root).resolve())
    with frost_session() as session:
        for index, station in enumerate(stations, 1):
            folder = Path(root) / f'station={station}'
            cached = cached_response(folder)
            if cached and not refresh:
                payload = json.loads(cached.read_text(encoding='utf-8'))
            else:
                try:
                    payload, url = get_json(session, '/observations/availableTimeSeries/v0.jsonld', {'sources': station + ':all' if ':' not in station else station})
                    store_response(folder, payload, url, {'station_id': station})
                except Exception as exc:
                    LOG.error('Series for %s: %s', station, exc)
                    results[station] = {'status': 'failed', 'error': str(exc), 'candidates': []}
                    continue
            candidates = series_candidates(payload, start, end)
            results[station] = {'status': 'ok', 'candidates': candidates}
            LOG.info('Series %d/%d (%.1f%%): %s | %s | %d temperature series candidates',
                     index, len(stations), 100 * index / len(stations), station,
                     'cached' if cached and not refresh else 'downloaded', len(candidates))
    write_run(root, results)
    return results

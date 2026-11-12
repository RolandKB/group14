"""Run the Bronze workflow: Elhub, municipality references, and Frost temperature."""
import argparse
import json
import logging
from pathlib import Path
from uuid import uuid4

if __package__ in (None, ''):
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bronze_layer.bronze_common import write_json
from bronze_layer.ingest_elhub import ingest as ingest_elhub
from bronze_layer.discover_municipalities import discover
from bronze_layer.generate_neighbors import generate_neighbors
from bronze_layer.ingest_frost_sources import ingest as ingest_sources
from bronze_layer.ingest_frost_series import ingest as ingest_series
from bronze_layer.ingest_frost_observations import ingest as ingest_observations
from bronze_layer.frost_common import PROJECT_ROOT, frost_session, now, validate_period

LOG = logging.getLogger(__name__)
DEFAULT_GEODATA = Path(__file__).resolve().parent / 'Basisdata_0000_Norge_25833_Kommuner_GeoJSON.zip'


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bronze-root', type=Path, default=PROJECT_ROOT / 'data/bronze')
    p.add_argument('--municipality-archive', type=Path,
                   help='Optional municipality-level Elhub ZIP; restrict Frost to its municipalities')
    p.add_argument('--geodata', type=Path, default=DEFAULT_GEODATA,
                   help='Kartverket ZIP or GeoJSON for municipality neighbors')
    p.add_argument('--start-date', default='2024-01-01')
    p.add_argument('--end-date', default='2026-01-01', help='Exclusive end date')
    p.add_argument('--skip-elhub', action='store_true')
    p.add_argument('--skip-neighbors', action='store_true')
    p.add_argument('--refresh-sources', action='store_true')
    p.add_argument('--refresh-series', action='store_true')
    p.add_argument('--refresh-observations', action='store_true')
    p.add_argument('--skip-observations', action='store_true')
    p.add_argument('--max-stations', type=int, default=0, help='Pilot limit; 0 means all eligible stations')
    p.add_argument('--allow-partial', action='store_true', help='Exit successfully despite failed Frost requests')
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    try:
        validate_period(a.start_date, a.end_date)
    except ValueError as exc:
        p.error(str(exc))
    if a.max_stations < 0:
        p.error('--max-stations must be nonnegative')
    if a.municipality_archive and not a.municipality_archive.is_file():
        p.error(f'Municipality archive not found: {a.municipality_archive}')
    if not a.skip_neighbors and not a.geodata.is_file():
        p.error(f'Geodata not found: {a.geodata}; supply --geodata or --skip-neighbors')
    # Fail before a potentially large Elhub download if credentials are missing.
    with frost_session():
        pass
    run_id = uuid4().hex
    summary = {'run_id': run_id, 'started_at_utc': now(), 'status': 'started',
               'period_start': a.start_date, 'period_end_exclusive': a.end_date,
               'configuration': {k: str(v) if isinstance(v, Path) else v for k, v in vars(a).items()}}
    manifest = a.bronze_root / '_runs' / f'{run_id}.json'
    LOG.info('Bronze run %s: %s to %s; output=%s; manifest=%s',
             run_id, a.start_date, a.end_date, a.bronze_root.resolve(), manifest)
    try:
        if not a.skip_elhub:
            LOG.info('Starting Elhub ingestion')
            summary['elhub_archive'] = str(ingest_elhub(a.bronze_root / 'elhub'))
        if not a.skip_neighbors:
            LOG.info('Generating municipality neighbors from %s', a.geodata)
            generate_neighbors(a.geodata, bronze_root=a.bronze_root)
        codes = discover(a.municipality_archive) if a.municipality_archive else None
        if codes is not None:
            if not codes:
                raise ValueError('Municipality archive contains no municipality codes')
            write_json(a.bronze_root / 'elhub/municipalities.json', codes)
        # Country-wide discovery includes historical stations whose municipality
        # codes may no longer appear in the current Kartverket dataset.
        LOG.info('Starting Frost station discovery')
        source_result = ingest_sources(codes, root=a.bronze_root / 'frost/sources', refresh=a.refresh_sources)
        stations = sorted({s for result in source_result.values() for s in result['station_ids']})
        LOG.info('Starting Frost series discovery for %d stations', len(stations))
        series_result = ingest_series(stations, a.start_date, a.end_date,
                                      root=a.bronze_root / 'frost/series', refresh=a.refresh_series)
        eligible = sorted(s for s, result in series_result.items() if result['candidates'])
        total_eligible = len(eligible)
        if a.max_stations:
            eligible = eligible[:a.max_stations]
        write_json(a.bronze_root / 'frost/eligible-stations.json', eligible)
        LOG.info('%d eligible stations; downloading %d', total_eligible, len(eligible))
        observations = None
        if not a.skip_observations:
            observations = ingest_observations(eligible, a.start_date, a.end_date,
                root=a.bronze_root / 'frost/observations', refresh=a.refresh_observations)
        summary.update({
            'municipalities': len(codes) if codes is not None else 'all Norway',
            'station_ids': len(stations), 'eligible_stations': total_eligible,
            'selected_stations': len(eligible),
            'source_failures': [k for k, v in source_result.items() if v['status'] == 'failed'],
            'series_failures': [k for k, v in series_result.items() if v['status'] == 'failed'],
            'observations': observations,
        })
        failed = (summary['source_failures'] or summary['series_failures'] or
                  (observations and observations['failed']) or not eligible)
        summary['status'] = 'partial' if failed else 'success'
    except BaseException as exc:
        summary.update(status='failed', error=str(exc), error_type=type(exc).__name__)
        raise
    finally:
        summary['finished_at_utc'] = now()
        write_json(manifest, summary)
        LOG.info('Bronze run %s finished: %s; manifest=%s', run_id, summary['status'], manifest)
    print(json.dumps(summary, indent=2))
    if failed and not a.allow_partial:
        raise SystemExit('Requests failed or no eligible stations found; inspect the run manifest and rerun')


if __name__ == '__main__':
    main()

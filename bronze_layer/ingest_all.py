"""Run the Bronze workflow: Elhub, municipality references, Frost temperature, and spot prices."""

import argparse
import json
import logging
from pathlib import Path
from uuid import uuid4

if __package__ in (None, ''):
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bronze_layer.ingest_elhub import ingest as ingest_elhub
from bronze_layer.bronze_common import write_json
from bronze_layer.ingest_spot_prices import ingest as ingest_prices, PRICE_AREAS
from bronze_layer.discover_municipalities import discover
from bronze_layer.generate_neighbors import generate_neighbors
from bronze_layer.ingest_frost_sources import ingest as ingest_sources
from bronze_layer.ingest_frost_series import ingest as ingest_series
from bronze_layer.ingest_frost_observations import ingest as ingest_observations
from bronze_layer.frost_common import PROJECT_ROOT, frost_session, now, validate_period

LOG = logging.getLogger(__name__)
DEFAULT_GEODATA = Path(__file__).resolve().parent / 'Basisdata_0000_Norge_25833_Kommuner_GeoJSON.zip'


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bronze-root', type=Path, default=PROJECT_ROOT / 'data/bronze')
    parser.add_argument(
        '--municipality-archive',
        type=Path,
        help='Optional municipality-level Elhub ZIP; restrict Frost to its municipalities',
    )
    parser.add_argument(
        '--geodata',
        type=Path,
        default=DEFAULT_GEODATA,
        help='Kartverket ZIP or GeoJSON for municipality neighbors',
    )

    parser.add_argument('--start-date', default='2024-01-01')
    parser.add_argument('--end-date', default='2026-01-01', help='Exclusive end date')

    parser.add_argument('--skip-elhub', action='store_true')
    parser.add_argument('--skip-neighbors', action='store_true')
    parser.add_argument('--skip-observations', action='store_true')
    parser.add_argument('--skip-prices', action='store_true')

    parser.add_argument('--refresh-sources', action='store_true')
    parser.add_argument('--refresh-series', action='store_true')
    parser.add_argument('--refresh-observations', action='store_true')
    parser.add_argument('--refresh-prices', action='store_true')
    parser.add_argument('--price-areas', nargs='+', choices=PRICE_AREAS, default=PRICE_AREAS)

    parser.add_argument(
        '--max-stations',
        type=int,
        default=0,
        help='Pilot limit; 0 means all eligible stations',
    )
    parser.add_argument(
        '--allow-partial',
        action='store_true',
        help='Exit successfully despite failed source requests',
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
    )

    # Validate configuration before starting any downloads.
    try:
        validate_period(args.start_date, args.end_date)
    except ValueError as exc:
        parser.error(str(exc))

    if args.max_stations < 0:
        parser.error('--max-stations must be nonnegative')

    if args.municipality_archive and not args.municipality_archive.is_file():
        parser.error(f'Municipality archive not found: {args.municipality_archive}')

    if not args.skip_neighbors and not args.geodata.is_file():
        parser.error(f'Geodata not found: {args.geodata}; supply --geodata or --skip-neighbors')

    # Fail before a potentially large Elhub download if credentials are missing.
    with frost_session():
        pass

    # Paths must be converted to strings before saving configuration as JSON.
    configuration = {}
    for name, value in vars(args).items():
        if isinstance(value, Path):
            value = str(value)
        configuration[name] = value

    run_id = uuid4().hex
    summary = {
        'run_id': run_id,
        'started_at_utc': now(),
        'status': 'started',
        'period_start': args.start_date,
        'period_end_exclusive': args.end_date,
        'configuration': configuration,
    }
    manifest = args.bronze_root / '_runs' / f'{run_id}.json'
    LOG.info(
        'Bronze run %s: %s to %s; output=%s; manifest=%s',
        run_id,
        args.start_date,
        args.end_date,
        args.bronze_root.resolve(),
        manifest,
    )

    try:
        if not args.skip_elhub:
            LOG.info('Starting Elhub ingestion')
            archive = ingest_elhub(args.bronze_root / 'elhub')
            summary['elhub_archive'] = str(archive)

        if not args.skip_neighbors:
            LOG.info('Generating municipality neighbors from %s', args.geodata)
            generate_neighbors(args.geodata, bronze_root=args.bronze_root)

        # Without a municipality archive, discover stations throughout Norway.
        codes = None
        if args.municipality_archive:
            codes = discover(args.municipality_archive)
            if not codes:
                raise ValueError('Municipality archive contains no municipality codes')
            write_json(args.bronze_root / 'elhub/municipalities.json', codes)

        # Country-wide discovery includes historical stations whose municipality
        # codes may no longer appear in the current Kartverket dataset.
        LOG.info('Starting Frost station discovery')
        source_result = ingest_sources(
            codes,
            root=args.bronze_root / 'frost/sources',
            refresh=args.refresh_sources,
        )

        # A station may appear in several results. Download it only once.
        station_ids = set()
        for result in source_result.values():
            for station_id in result['station_ids']:
                station_ids.add(station_id)
        stations = sorted(station_ids)

        LOG.info('Starting Frost series discovery for %d stations', len(stations))
        series_result = ingest_series(
            stations,
            args.start_date,
            args.end_date,
            root=args.bronze_root / 'frost/series',
            refresh=args.refresh_series,
        )

        # Only request observations from stations with relevant temperature series.
        eligible = []
        for station_id, result in series_result.items():
            if result['candidates']:
                eligible.append(station_id)
        eligible.sort()

        # Keep the full count for reporting, even when running a small pilot.
        total_eligible = len(eligible)
        if args.max_stations:
            eligible = eligible[:args.max_stations]

        write_json(args.bronze_root / 'frost/eligible-stations.json', eligible)
        LOG.info('%d eligible stations; downloading %d', total_eligible, len(eligible))

        observations = None
        if not args.skip_observations:
            observations = ingest_observations(
                eligible,
                args.start_date,
                args.end_date,
                root=args.bronze_root / 'frost/observations',
                refresh=args.refresh_observations,
            )

        # Prices use the same date range, with one request per area and day.
        prices = None
        if not args.skip_prices:
            LOG.info('Starting spot-price ingestion')
            prices = ingest_prices(
                args.start_date,
                args.end_date,
                root=args.bronze_root / 'spot_prices',
                areas=args.price_areas,
                refresh=args.refresh_prices,
            )

        # Collect failed requests so a later run can be inspected and resumed.
        source_failures = []
        for source, result in source_result.items():
            if result['status'] == 'failed':
                source_failures.append(source)

        series_failures = []
        for station_id, result in series_result.items():
            if result['status'] == 'failed':
                series_failures.append(station_id)

        municipalities = 'all Norway'
        if codes is not None:
            municipalities = len(codes)

        summary.update({
            'municipalities': municipalities,
            'station_ids': len(stations),
            'eligible_stations': total_eligible,
            'selected_stations': len(eligible),
            'source_failures': source_failures,
            'series_failures': series_failures,
            'observations': observations,
            'prices': prices,
        })

        failed = bool(source_failures or series_failures or not eligible)
        if observations and observations['failed']:
            failed = True

        if prices and prices['failed']:
            failed = True

        if failed:
            summary['status'] = 'partial'
        else:
            summary['status'] = 'success'

    except BaseException as exc:
        # Record interruptions (such as Ctrl+C) as well as ordinary errors.
        summary.update(status='failed', error=str(exc), error_type=type(exc).__name__)
        raise
    finally:
        # Save a manifest even if a stage fails or the user interrupts the run.
        summary['finished_at_utc'] = now()
        write_json(manifest, summary)
        LOG.info('Bronze run %s finished: %s; manifest=%s', run_id, summary['status'], manifest)

    print(json.dumps(summary, indent=2))

    if failed and not args.allow_partial:
        raise SystemExit(
            'Requests failed or no eligible stations found; inspect the run manifest and rerun'
        )


if __name__ == '__main__':
    main()

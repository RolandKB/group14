"""Preserve daily Hva koster strømmen spot-price responses in Bronze."""

import argparse
import hashlib
import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import requests

if __package__ in (None, ''):
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

LOG = logging.getLogger(__name__)
BASE_URL = 'https://www.hvakosterstrommen.no/api/v1/prices'
PRICE_AREAS = ('NO1', 'NO2', 'NO3', 'NO4', 'NO5')
DEFAULT_ROOT = Path(__file__).resolve().parents[1] / 'data/bronze/spot_prices'


def write_atomic(path, body):
    """Do not leave a half-written raw response or manifest after interruption."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f'.{uuid4().hex}.tmp')
    try:
        temporary.write_bytes(body)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def save_manifest(path, payload):
    body = json.dumps(payload, indent=2, ensure_ascii=False) + '\n'
    write_atomic(path, body.encode('utf-8'))


def cached_response(folder):
    """Reuse only the latest successful retrieval with an intact raw file."""
    manifests = sorted(folder.glob('retrieval_*.metadata.json'))
    if not manifests:
        return None

    try:
        metadata = json.loads(manifests[-1].read_text())
        if metadata['status'] != 'success':
            return None
        raw = folder / metadata['response_file']
        checksum = hashlib.sha256(raw.read_bytes()).hexdigest()
        if checksum == metadata['sha256']:
            return raw
    except (OSError, ValueError, KeyError):
        pass
    return None


def download(session, url):
    """Retry temporary network/server failures, but not permanent HTTP errors."""
    for attempt in range(4):
        try:
            response = session.get(url, timeout=(15, 90))
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            status = None
            if exc.response is not None:
                status = exc.response.status_code
            if attempt == 3 or (status is not None and status not in (429, 500, 502, 503, 504)):
                raise
            delay = 2 ** attempt
            if exc.response is not None:
                retry_after = exc.response.headers.get('Retry-After', '')
                if retry_after.isdigit():
                    delay = max(delay, int(retry_after))
            time.sleep(delay)


def ingest(start, end, root=DEFAULT_ROOT, areas=PRICE_AREAS, refresh=False):
    """Fetch local calendar dates from start through end (exclusive)."""
    first = date.fromisoformat(start)
    limit = date.fromisoformat(end)
    if first >= limit:
        raise ValueError('start must precede exclusive end')
    if first < date(2021, 12, 1):
        raise ValueError('The provider documents history from 2021-12-01')

    areas = sorted(set(areas))
    if not areas:
        raise ValueError('Select at least one price area')
    for area in areas:
        if area not in PRICE_AREAS:
            raise ValueError(f'Unknown price area: {area}')

    root = Path(root)
    run_id = uuid4().hex
    results = {'downloaded': 0, 'cached': 0, 'failed': []}
    summary = {
        'run_id': run_id,
        'started_at_utc': datetime.now(timezone.utc).isoformat(),
        'status': 'started',
        'period_start': start,
        'period_end_exclusive': end,
        'price_areas': areas,
        'refresh': refresh,
        'results': results,
    }
    total = (limit - first).days * len(areas)
    completed = 0

    try:
        with requests.Session() as session:
            session.headers.update({'Accept': 'application/json', 'User-Agent': 'DAT535-group14/1.0'})
            day = first
            while day < limit:
                for area in areas:
                    folder = root / f'area={area}' / f'date={day.isoformat()}'
                    url = f'{BASE_URL}/{day.year}/{day:%m-%d}_{area}.json'
                    completed += 1

                    if not refresh and cached_response(folder):
                        results['cached'] += 1
                        LOG.info('Prices %d/%d: %s %s cached', completed, total, day, area)
                        continue

                    try:
                        response = download(session, url)
                        payload = response.json()
                        if not isinstance(payload, list):
                            raise ValueError('Expected a JSON list of price intervals')

                        # Preserve bytes, negative prices, offsets, and native intervals.
                        body = response.content
                        checksum = hashlib.sha256(body).hexdigest()
                        raw = folder / f'response_{checksum}.json'
                        write_atomic(raw, body)
                        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
                        metadata = {
                            'source': 'Hva koster strømmen',
                            'url': response.url,
                            'retrieved_at_utc': datetime.now(timezone.utc).isoformat(),
                            'run_id': run_id,
                            'price_area': area,
                            'date': day.isoformat(),
                            'date_timezone': 'Europe/Oslo',
                            'http_status': response.status_code,
                            'response_file': raw.name,
                            'sha256': checksum,
                            'size_bytes': len(body),
                            'record_count': len(payload),
                            'status': 'success' if payload else 'empty',
                        }
                        save_manifest(folder / f'retrieval_{stamp}_{uuid4().hex}.metadata.json', metadata)
                        if not payload:
                            raise ValueError('Empty price list; retry this date on the next run')
                        results['downloaded'] += 1
                        LOG.info('Prices %d/%d: %s %s downloaded (%d intervals)',
                                 completed, total, day, area, len(payload))
                    except (requests.RequestException, ValueError) as exc:
                        # A missing date is not complete. Continue other dates and
                        # report a partial run so it can be resumed without refresh.
                        results['failed'].append({'date': day.isoformat(), 'area': area,
                                                  'url': url, 'error': str(exc)})
                        LOG.error('Prices %s %s: %s', day, area, exc)

                day += timedelta(days=1)

        summary['status'] = 'partial' if results['failed'] else 'success'
    except BaseException as exc:
        summary.update(status='failed', error=str(exc), error_type=type(exc).__name__)
        raise
    finally:
        summary['finished_at_utc'] = datetime.now(timezone.utc).isoformat()
        save_manifest(root / '_runs' / f'{run_id}.json', summary)

    return results


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-date', default='2024-01-01')
    parser.add_argument('--end-date', default='2026-01-01', help='Exclusive local calendar date')
    parser.add_argument('--bronze-root', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--areas', nargs='+', choices=PRICE_AREAS, default=PRICE_AREAS)
    parser.add_argument('--refresh', action='store_true', help='Check for revisions, preserving old versions')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    results = ingest(args.start_date, args.end_date, args.bronze_root, args.areas, args.refresh)
    print(json.dumps(results, indent=2))
    if results['failed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()

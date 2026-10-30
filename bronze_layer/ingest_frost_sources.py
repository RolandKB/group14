"""Retrieve raw Frost station metadata for Norway or selected municipalities."""
import argparse
import json
import logging
from pathlib import Path
if __package__ in (None, ''):
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bronze_layer.frost_common import frost_session, get_json, store_response, cached_response, write_run

LOG = logging.getLogger(__name__)


def ingest(codes=None, root=Path('data/bronze/frost/sources'), refresh=False):
    result = {}
    with frost_session() as session:
        for code in ([None] if codes is None else sorted(set(codes))):
            if code == '0000':
                LOG.warning('Skipping Elhub placeholder municipality 0000')
                result[code] = {'status': 'skipped_placeholder', 'station_ids': []}
                continue
            folder = Path(root) / (f'municipality={code}' if code else 'country=NO')
            existing = cached_response(folder)
            if existing and not refresh:
                payload = json.loads(existing.read_text(encoding='utf-8'))
                status = 'cached'
            else:
                try:
                    payload, url = get_json(session, '/sources/v0.jsonld', {'municipality': str(int(code))} if code else {'country': 'NO', 'types': 'SensorSystem'})
                    store_response(folder, payload, url, {'elhub_municipality': code, 'frost_municipality': str(int(code)) if code else None})
                    status = 'downloaded'
                except Exception as exc:
                    LOG.error('Municipality %s: %s', code, exc)
                    result[code or 'NO'] = {'status': 'failed', 'error': str(exc), 'station_ids': []}
                    continue
            ids = sorted({item['id'] for item in payload.get('data', []) if item.get('id')})
            result[code or 'NO'] = {'status': status, 'station_ids': ids}
            LOG.info('%s: %s (%d stations)', code, status, len(ids))
    write_run(root, result)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--municipalities-json', type=Path, help='JSON list of Elhub codes (default: all Norway)')
    p.add_argument('--bronze-root', type=Path, default=Path('data/bronze/frost/sources'))
    p.add_argument('--refresh', action='store_true')
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    result = ingest(json.loads(a.municipalities_json.read_text()) if a.municipalities_json else None, a.bronze_root, a.refresh)
    print(json.dumps({k: v['status'] for k, v in result.items()}, indent=2))
    if any(v['status'] == 'failed' for v in result.values()):
        raise SystemExit(1)

if __name__ == '__main__':
    main()

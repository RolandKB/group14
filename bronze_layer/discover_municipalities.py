"""Read Elhub municipality codes directly from CSV member(s) inside a ZIP."""
import argparse
import csv
import io
import json
import zipfile
from pathlib import Path


def discover(archive_path: Path) -> list[str]:
    codes = set()
    with zipfile.ZipFile(archive_path) as archive:
        members = [m for m in archive.namelist() if m.lower().endswith('.csv') and not m.endswith('/')]
        if not members:
            raise ValueError('ZIP contains no CSV files')
        for member in members:
            with archive.open(member) as raw, io.TextIOWrapper(raw, encoding='utf-8-sig', newline='') as stream:
                sample = stream.read(4096)
                stream.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=',;\t')
                    delimiter = dialect.delimiter
                except csv.Error:
                    delimiter = ','
                reader = csv.DictReader(stream, delimiter=delimiter)
                if not reader.fieldnames or 'MUNICIPALITY_NUMBER' not in [f.strip() for f in reader.fieldnames]:
                    raise ValueError(f'{member}: missing MUNICIPALITY_NUMBER; found {reader.fieldnames}')
                reader.fieldnames = [name.strip() for name in reader.fieldnames]
                for row in reader:
                    value = (row.get('MUNICIPALITY_NUMBER') or '').strip().strip('"')
                    if value:
                        if not value.isdigit() or len(value) > 4:
                            raise ValueError(f'Unexpected municipality code {value!r} in {member}')
                        codes.add(value.zfill(4))
    return sorted(codes)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--archive', type=Path, required=True)
    p.add_argument('--output', type=Path)
    a = p.parse_args()
    codes = discover(a.archive)
    if a.output:
        a.output.parent.mkdir(parents=True, exist_ok=True)
        a.output.write_text(json.dumps(codes, indent=2) + '\n', encoding='utf-8')
    print(f'{len(codes)} codes: {codes[:15]}')

if __name__ == '__main__':
    main()

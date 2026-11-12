"""Read Elhub municipality codes directly from CSV member(s) inside a ZIP."""
import argparse
import json
import zipfile
import pandas as pd
from pathlib import Path

def discover(archive_path: Path) -> list[str]:
    zf = zipfile.ZipFile(archive_path)
    df = pd.concat([pd.read_csv(zf.open(f)) for f in zf.namelist() if f.lower().endswith('.csv') and not f.endswith('/')])
    return sorted(df['MUNICIPALITY_NUMBER'].astype(str).str.zfill(4).unique().tolist())


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

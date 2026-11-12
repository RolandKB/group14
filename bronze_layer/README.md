# Bronze ingestion

From the repository root:

```bash
python -m pip install -e .
python -m bronze_layer.ingest_all
```

`python bronze_layer/ingest_all.py` also works. Configure `FROST_CLIENT_ID`
in the repository `.env` or environment first. Existing `CLIENT_ID` and
`FROST_API_TOKEN` variables are supported. Frost uses the client ID as the Basic
Auth username with an empty password; `CLIENT_SECRET` is not needed.
See the [Frost authentication and query guide](https://frost.met.no/howto.html).

The workflow runs these stages in dependency order:

1. `ingest_elhub`: download and version the existing price-area (MBA) consumption ZIP.
2. `generate_neighbors`: generate the municipality adjacency reference from the
   bundled Kartverket ZIP. Use `--geodata PATH` for another ZIP or GeoJSON.
3. `discover_municipalities`: when `--municipality-archive PATH` is supplied,
   extract and save its municipality codes. This must be a municipality-level
   Elhub ZIP containing `MUNICIPALITY_NUMBER`; the MBA ZIP does not have it.
4. `ingest_frost_sources`: discover all Norwegian stations by default, or restrict
   discovery to the supplied municipality archive. Nationwide discovery includes
   historical stations whose codes may differ from the current Kartverket map.
5. `ingest_frost_series`: identify stations with `air_temperature` coverage in the
   requested period, including all time resolutions and parallel sensors.
6. `ingest_frost_observations`: download station/month temperature responses for
   2024-01-01 through 2026-01-01 (exclusive) by default.

7. `ingest_spot_prices`: download daily spot-price responses for NO1–NO5 using
   the same start date and exclusive end date.

`frost_common.py` is a shared library, not a separate ingestion stage.

All orchestrated output goes under the repository's `data/bronze`, independently
of the working directory. Override it with `--bronze-root PATH` to separate
sample and full datasets. The ingestion stages are importable functions; only
`ingest_all.py` is a command-line script.

For a development run:

```bash
python -m bronze_layer.ingest_all \
  --bronze-root data/dev/bronze --skip-elhub \
  --start-date 2024-01-01 --end-date 2024-02-01 --max-stations 2
```

The station limit applies to observation downloads; source and series discovery
still runs for all stations in scope. `--skip-neighbors` and `--skip-observations`
allow existing steps to be omitted explicitly.

Frost response bytes are preserved unchanged, with SHA-256 filenames and
retrieval metadata containing URLs, timestamps, request periods where applicable,
HTTP status, and run IDs. Per-stage results and the overall workflow summary are
saved in `_runs` directories. Municipality maps and station lists are derived
reference artifacts, separate from raw responses. No quality filtering,
resampling, or geographic enrichment is performed here; those belong in Silver.

Reruns reuse completed Frost requests after checking their checksums. They select
the latest retrieval manifest, not a checksum's alphabetical order. Failed
requests are retried on the next run. `--refresh-sources`, `--refresh-series`, and
`--refresh-observations` check for upstream revisions while retaining old raw
versions. Existing Elhub ingestion downloads and hashes the archive on each run;
`--skip-elhub` avoids that download when it has already been collected.

Frost's documented 404/no-data and observation 412/no-time-series responses
are retained and cached as empty results. These are coverage gaps, not failed
downloads. Observation summaries count `downloaded` nonempty responses, `cached`
requests, and `empty` periods (including cached empty periods); these counters
are not all mutually exclusive. Authentication and other request failures cause a nonzero exit status;
`--allow-partial` permits successful exit while the manifest still reports a
partial run. A run with no eligible stations is also partial. Raw API interval
boundaries are preserved; Silver should enforce the requested exclusive end and
remove any overlapping boundary observations.

Validation:

```bash
python -m unittest discover -s tests -v
```

## Progress and development workflow

Observation logs include station/request positions, completion percentage,
request outcome, returned record count, cumulative counters, and elapsed time.
After ten network requests, the estimated remaining time uses the most recent
100 request durations. It assumes remaining periods require downloads; cached
periods can shorten it. Series discovery logs show progress and cache reuse.
A running Python process must be restarted to pick up logging changes.

`.github/workflows/dat535-dev.yml` runs the complete Bronze pipeline using the
`dev` environment's `FROST_CLIENT_ID` secret and the runner's `~/spark-env` Python
environment. It installs dependencies before ingestion; tests are not run by this workflow. Raw
files persist in `~/dat535-volume/dev/bronze`; logs and run manifests are uploaded
as artifacts even if ingestion fails. Runs are serialized to avoid writing to
that directory concurrently. The job allows 20 hours for the initial backfill.
Production and the separate scheduled workflow still use their existing runners.

## Spot-price ingestion

Source: [Hva koster strømmen API documentation](https://www.hvakosterstrommen.no/strompris-api).
The API requires no credentials and documents historical availability from
2021-12-01. Prices exclude VAT and electricity support; the provider converts
EUR prices using its exchange rate, so NOK values may differ from Nord Pool.
Credit Hva koster strømmen when presenting these data.

To download only prices for the project period, from the repository root:

```bash
.venv/bin/python -m bronze_layer.ingest_spot_prices
```

The defaults cover 2024-01-01 through 2026-01-01 (exclusive), for all five areas:
731 days × 5 areas = 3,655 daily requests before cache reuse. This standalone
command does not read Frost credentials or scan the existing weather data.

For a small development sample:

```bash
.venv/bin/python -m bronze_layer.ingest_spot_prices \
  --start-date 2024-01-01 --end-date 2024-01-03 \
  --areas NO1 --bronze-root data/dev/bronze/spot_prices
```

Raw JSON is stored unchanged at:

```text
data/bronze/spot_prices/area=NO1/date=2024-01-01/
  response_<sha256>.json
  retrieval_<timestamp>_<id>.metadata.json
```

Metadata records the URL, retrieval time, area, requested local date, checksum,
HTTP status, record count, and run ID. Stage summaries are stored in
`spot_prices/_runs/`. Reruns check checksums and reuse successful responses.
`--refresh` checks for revised responses while retaining older raw versions;
rerun with `--refresh` again if a refresh attempt fails.

Missing dates (including HTTP 404), malformed responses, and empty lists make
the run partial and the standalone command exits nonzero. Empty lists are
preserved but not considered a completed day. Failed dates without a successful
cache entry are retried on the next run. Temporary HTTP/network failures receive
bounded retries. Raw record counts are informational: Silver must validate
interval coverage, units, and business keys rather than assuming 24 prices per day.
Dates identify Norwegian local calendar days; timestamps, negative prices, and
native interval durations are preserved for Silver to interpret.

`ingest_all` includes prices by default. It accepts `--skip-prices`,
`--refresh-prices`, and `--price-areas NO1 NO2`. Price failures make the overall
run partial and cause a nonzero exit unless `--allow-partial` is used.
The dev workflow includes the price stage automatically and uploads its run
manifests alongside the other ingestion reports.

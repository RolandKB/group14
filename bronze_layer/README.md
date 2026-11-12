# Bronze ingestion

From the repository root:

```bash
python -m pip install -r requirements.txt
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

`frost_common.py` is a shared library, not a separate ingestion stage. Spot-price
 ingestion is still to be implemented; there is no price ingestion script in this folder.

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
files persist in `~/dat535-project-data/dev/bronze`; logs and run manifests are uploaded
as artifacts even if ingestion fails. Runs are serialized to avoid writing to
that directory concurrently. The job allows 20 hours for the initial backfill.
Production and the separate scheduled workflow still use their existing runners.

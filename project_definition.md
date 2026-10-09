# DAT535 Group 14 Project Definition

## Project statement

We will develop a reproducible data engineering pipeline using Apache Spark, PySpark, and the Medallion Architecture to integrate historical Norwegian electricity consumption, spot prices, and temperature observations.

Our main analytical question is: How do electricity consumption patterns vary with temperature and spot prices across Norwegian price areas and consumer groups?

The purpose is to provide a reusable, validated dataset that enables students and analysts to explore historical electricity market trends in Norway.

The project focuses on historical batch processing and data engineering. Real-time streaming, price prediction, causal modeling, and consumer electricity billing are outside its scope.

## Dataset

We plan to integrate three public data sources:

- **Elhub:** Historical electricity consumption records in CSV format, including measurement intervals, consumption in kWh, grid areas, price areas (NO1–NO5), consumption groups, metering point counts, and update timestamps.
- **MET Norway Frost API:** Historical temperature observations in JSON format, including weather station identifiers, observation timestamps, temperature values, units, and quality metadata.
- **Norwegian spot prices:** Historical electricity prices by price area and time interval, obtained from the public "Hva koster strømmen" API in JSON format.

Our initial target period is 2024–2025, subject to confirming overlapping coverage. We will begin with a smaller development sample and process larger historical extracts for the complete pipeline and performance experiments.

The sources differ in format, geographical granularity, temporal resolution, and update frequency. Expected challenges include missing observations, daylight-saving transitions, historical revisions, inconsistent intervals, and weather-station coverage. Elhub's detailed records provide the primary processing workload, prices and weather provide enrichment.

We will verify actual dataset volumes, access restrictions, licenses, and refresh patterns before implementation. Source URLs, retrieval parameters, and dataset versions will be documented to support reproducibility.

## Bronze layer

The Bronze layer will ingest and preserve original Elhub CSV files and JSON responses from the Frost and electricity price APIs without changing their contents.

Data will be organized by source, dataset, and reporting period, for example `bronze/elhub/year=2024/month=01/`. Each ingestion run will record the source URL, retrieval time, reporting period, checksum, run identifier, and execution status.

The pipeline will use batch ingestion with support for historical backfills and scheduled updates. Checksums and ingestion manifests will prevent unnecessary duplicate downloads. Revised or late-arriving source files will be retained as new raw versions rather than overwriting existing evidence.

Automated tests will validate ingestion behavior, expected file structures, and failure handling. Development and production-style configurations will use separate storage paths and settings. GitHub Actions will validate code changes and support scheduled execution.

## Silver layer

The Silver layer is the main technical focus. PySpark DataFrame transformations and Spark SQL will convert heterogeneous raw records into standardized, validated, reusable Parquet datasets.

**Data model and grain:** We will create separate Silver datasets for consumption, prices, and temperature. Consumption records will represent a measurement interval for a specific grid area and consumer group. Price records will represent a price area and market interval. Temperature records will represent a weather station and observation time. Candidate business keys will be tested for uniqueness before being finalized. Source identifiers, measurement timestamps, and revision metadata will be retained for traceability.

**Cleaning and standardization:** We will define explicit schemas, standardize column names and data types, convert timestamps to UTC, validate interval durations, and normalize units such as kWh, NOK/kWh, and °C. We will identify duplicates, nulls, invalid identifiers, and malformed timestamps. Records will be accepted, flagged, corrected only when justified, or rejected with documented reasons. Legitimate negative spot prices will not be treated as errors.

**Historical revisions:** Repeated measurements with updated source timestamps will be identified using tested business keys. The latest valid revision will be selected for current Silver outputs, while earlier raw versions remain available in Bronze.

**Integration and enrichment:** Consumption will be joined with spot prices using price areas and aligned time intervals. Weather stations will be mapped to price areas using documented reference data. Where several stations represent one area, temperature observations will be aggregated using a defined rule before joining, avoiding unintended row multiplication. Hourly and 15-minute measurements will be aligned through explicit interval-based aggregation, and daylight-saving transitions will be tested.

**Data quality evidence:** We will record source and output row counts, null percentages, duplicate counts, rejected records, missing intervals, and join-match rates. Automated tests will check schemas, business-key uniqueness, transformation rules, interval alignment, and aggregate reconciliation. Quality reports and rejected records will be retained for inspection.

**Spark performance:** We will examine execution plans, scans, joins, shuffles, and partitioning. A baseline transformation will be compared with an optimized implementation using measured execution times and Spark execution metrics. Potential improvements include early filtering, column pruning, appropriate partitioning, and broadcast joins for small reference tables. Optimizations will be justified by evidence.

**Lifecycle and scalability:** Transformations will be modular, version-controlled, tested in CI, and designed for repeatable runs and historical backfills. We will document how the pipeline responds to larger datasets, schema evolution, late records, and revised source data.

The Silver output will consist of trusted datasets that can support multiple analytical uses.

## Gold layer

The Gold layer will produce purpose-specific analytical tables from validated Silver data.

The main Gold dataset will contain daily electricity consumption, average spot price, and representative temperature by Norwegian price area, with additional consumption breakdowns by consumer group. Measures will include total consumption (kWh), consumption per metering point where meaningful, average price (NOK/kWh), average temperature (°C), and peak consumption periods.

Unlike Silver's detailed measurement records, Gold will provide aggregated datasets designed for direct analysis and visualization. Consumption totals and record coverage will be reconciled against Silver.

Gold will refresh after successful upstream processing. Required quality checks must pass before publication. Otherwise, the previous successful output will remain available and the failure will be logged.

## End-to-end delivery

The project will be maintained in a shared GitHub repository containing PySpark source code, configuration files, tests, workflow definitions, documentation, and reproducible setup instructions.

Development and production-style settings will be separated through configuration, with API credentials stored as environment variables or GitHub secrets rather than committed to the repository.

GitHub Actions will run automated checks on pull requests, covering ingestion, schemas, transformations, and data quality. We will also demonstrate an end-to-end Bronze → Silver → Gold execution with scheduling.

Each run will produce logs, execution status, quality metrics, and rejected-record outputs. The repository will document pipeline execution, data dependencies, assumptions, known limitations, and recovery procedures. Team members will share implementation responsibilities and review each other's code.

The final report will provide architecture and data-model diagrams, representative PySpark and Spark SQL code, test and quality results, performance comparisons, and reflection on technical decisions, trade-offs, challenges, and possible improvements.

## Use of AI

AI has been used to help structure and refine this project definition. All proposed solutions will be critically evaluated by the group. The implementation, testing, validation, and understanding of the data pipeline will remain the responsibility of the group members.
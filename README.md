# eurostat-trade-pipeline

Reproducible ETL pipeline that fetches trade data from the Eurostat Comext API to analyze EU car imports (HS 8703), with a focus on imports from China and electric vehicles (EVs). The pipeline produces reproducible datasets and loads curated data into PostgreSQL for querying and analysis.

## Pipeline overview

1. **Fetch** → download data from the Eurostat Comext API
2. **Raw** → store immutable source snapshots in `data/raw/`
3. **Transform** → normalize schema, types, and naming
4. **Processed** → write curated outputs to `data/processed/` (e.g., Parquet)
5. **Load** → load curated tables into PostgreSQL for analysis

## Status
Scaffolding complete (structure, docs, CI). No business logic yet.
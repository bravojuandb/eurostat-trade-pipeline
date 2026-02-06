# eurostat-trade-pipeline

Reproducible ETL pipeline that ingests monthly datasets from **COMEXT** [(Eurostat international trade database)](https://ec.europa.eu/eurostat/comext/newxtweb/), with a concrete analytical focus on **EU imports of passenger cars (HS 8703)**, including imports from China and electric vehicles (EVs).

The project emphasizes reproducibility, explicit data contracts, and clear separation of concerns (bronze / silver / gold).

---

## Contents

- [Pipeline Overview](#pipeline-overview)
- [Ingestion Layer - Bronze](#ingestion-layer-bronze)
- [How to Run](#how-to-run)
- [Continuous Integration](#continuous-integration)
- [Pipeline Status](#pipeline-status)

---

## Pipeline overview

→ **Source:** Eurostat COMEXT [bulk download facility](https://ec.europa.eu/eurostat/databrowser/bulk?lang=en&selectedTab=fileComext)  
→ **Bronze:** raw, immutable interval of monthly datasets (completed)  
→ **Silver:** cleaned, typed, analysis-ready datasets (planned)  
→ **Gold:** PostgreSQL analytical tables (planned)  

---


## Ingestion layer (Bronze)

The ingestion layer is responsible for **downloading** and **extracting** raw COMEXT bulk data files into local storage.

### Architecture & orchestration

The ingestion system is built as a layered, reproducible pipeline with clear separation of concerns, with the following components:

- **Python CLI orchestrator** - `fetch.py`  
  Controls execution flow and dry‑run mode.

- **Download worker (Bash)** — `comext_download.sh`  
  Fetches monthly `.7z` files for selected interval (idempotent).

- **Extract worker (Bash)** — `comext_extract.sh`  
  Expands `.7z → .dat` safely (idempotent).

- **Docker runtime (Compose)**  
  Reproducible environment with persistent raw storage.


### What the ingestion layer does

- Downloads **monthly COMEXT bulk files** (`full_v2_YYYYMM.7z`) from Eurostat.
- Extracts `full_v2_YYYYMM.7z` into `full_v2_YYYYMM.dat`
- Requires an **explicit date interval**  via environment variables (`FROM_MONTH=YYYY-MM` → `TO_MONTH=YYYY-MM`)
- The interval is inclusive: FROM and TO months are both downloaded  
- Stores raw files without modification (no filtering, no schema changes).
- Supports **DRY_RUN mode** to validate behavior without downloading data.
- Is **idempotent**: already‑downloaded months are skipped.

---

## How to run

### 1) Clone the repository
```bash
git clone https://github.com/bravojuandb/eurostat-trade-pipeline
```
The ingestion layer is designed to run **inside Docker** for portability and reproducibility.

### 2) Quick test, dry-run off (default interval)

No FROM/TO are provided and the pipeline defaults to 2002-12 → 2003-01 for quick smoke testing:

```bash
docker compose run --rm ingestion
```

### 3) Dry‑run on (prints paths, skips downloads)

```bash
DRY_RUN=1 FROM_MONTH=2005-11 TO_MONTH=2025-11 docker compose run --rm ingestion
```

- prints what **would** be downloaded (month, file path, URL)
- performs a lightweight availability check (HTTP status)
- does **not** download any data
- does **not** create files or directories

### 4) Normal execution (select an interval, dry-run off by default)

```bash
FROM_MONTH=2013-11 TO_MONTH=2014-01 docker compose run --rm ingestion
```

### Raw data folder structure

Interval: **FROM_MONTH=**2005-11 **TO_MONTH=**2006-01
```
data/raw/
  comext_products/
    2005-11/
      full_v2_200511.7z
      full_v2_200511.dat
    2005-12/
      full_v2_200512.7z
      full_v2_200512.dat
    2006-01/
      full_v2_200601.7z
      full_v2_200601.dat
```

For the full raw data contract and guarantees, see [data/raw/README.md](data/raw/README.md)

### Validation rules

- Both `FROM_MONTH` and `TO_MONTH` are required, unless set to default ([Quick test](#2-quick-test-dry-run-off-default-interval))  
- Period format must be `YYYY-MM`  
- Periods earlier than **2002-01** are rejected  
- `FROM_MONTH` must be less than or equal to `TO_MONTH`  

⚠️ **Important:**
A new COMEXT dataset is published monthly, with the latest available month typically being two months prior to the current month.  
If `TO_MONTH` is set for non-published months then:
- DRY_RUN=1 prints month, file path, and URL even if artificial
- DRY_RUN=0 prints a message: "dataset not published yet for this month".

---

## Continuous Integration

A lightweight CI workflow runs on each push and pull request:

- Python linting with **Ruff**
- Docker image build
- Container smoke test (dry‑run, no downloads)
- Static analysis of Bash scripts with **ShellCheck**

The workflow can also be triggered manually via **GitHub Actions → Run workflow**.

---

## Pipeline Status

- ✅ **Ingestion layer (bronze) - implemented**
- ⏭️ Transformation layer (silver) — planned
- ⏭️ Loading into PostgreSQL (gold) — planned

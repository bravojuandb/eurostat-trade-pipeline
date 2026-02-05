# eurostat-trade-pipeline

Reproducible ETL pipeline that ingests monthly datasets from **COMEXT (Eurostat international trade database)**, with a concrete analytical focus on **EU imports of passenger cars (HS 8703)**, including imports from China and electric vehicles (EVs).

The project emphasizes reproducibility, explicit data contracts, and clear separation of concerns (bronze / silver / gold).

---

## Contents

- [Pipeline Overview](#pipeline-overview)
- [Ingestion Layer](#ingestion-layer-bronze)
- [How to Run](#how-to-run)
- [Validation Rules](#validation-rules)
- [Continuous Integration](#continuous-integration)
- [Pipeline Status](#pipeline-status)

---

## Pipeline overview

```
Eurostat Comext bulk files
→ Bronze: raw, immutable interval of monthly datasets (completed)
→ Silver: cleaned, typed, analysis-ready datasets (planned)
→ Gold: PostgreSQL analytical tables (planned)
```

---


## Ingestion layer (Bronze)

The ingestion layer is responsible for **downloading** and **extracting** raw Eurostat Comext bulk trade data into local storage.

### Architecture & orchestration

The ingestion system is built as a layered, reproducible pipeline with clear separation of concerns.

**Components**

- **Python CLI orchestrator**  
  Controls execution flow and dry‑run mode.

- **Download worker (Bash)** — `comext_download.sh`  
  Fetches monthly `.7z` files for a closed interval (idempotent).

- **Extract worker (Bash)** — `comext_extract.sh`  
  Expands `.7z → .dat` safely (idempotent).

- **Docker runtime (Compose)**  
  Reproducible environment with persistent raw storage.

**Execution flow**

```
Python CLI
   ↓
download.sh
   ↓
extract.sh
   ↓
data/raw/comext_products/
```

### What the ingestion layer does

- Downloads **monthly COMEXT bulk files** (`full_v2_YYYYMM.7z`) from Eurostat.
- Extracts `full_v2_YYYYMM.7z` into `full_v2_YYYYMM.dat`
- Requires an **explicit closed interval** (`--from YYYY-MM` → `--to YYYY-MM`)
- Closed interval is inclusive: FROM and TO months are both downloaded  
- Stores raw files without modification (no filtering, no schema changes).
- Supports **dry‑run mode** to validate behavior without downloading data.
- Is **idempotent**: already‑downloaded months are skipped.


---

## How to run

The ingestion layer is designed to run **inside Docker** for portability and reproducibility.

1) Quick test (default interval, dry-run off)

If no FROM/TO are provided, the pipeline defaults to 2002-12 → 2003-01 for quick smoke testing:

```bash
docker compose run --rm pipeline
```

2) Dry‑run mode on (DRY_RUN=1)

```bash
DRY_RUN=1 FROM_MONTH=2005-11 TO_MONTH=2025-11 docker compose run --rm pipeline
```

- prints what **would** be downloaded (month, file path, URL)
- performs a lightweight availability check (HTTP status)
- does **not** download any data
- does **not** create files or directories

3) Normal execution (select an interval, dry-run off)

```bash
FROM_MONTH=2013-11 TO_MONTH=2014-01 docker compose run --rm pipeline
```

### Raw data folder structure

```
Interval: -from 2005-11 -to 2006-01

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

For the full raw data contract and guarantees, see `data/raw/README.md`.

### Validation rules

- Both `FROM` and `TO` are required, unless set to default (Quick test)
- Period format must be `YYYY-MM`.
- Periods earlier than **2002-01** are rejected.
- `FROM` must be less than or equal to `TO`.

⚠️ Important:
A new COMEXT datasets is published every month, with some delay. If `TO` is set for non-existent months then:
-  DRY_RUN=1 prints month, file path and URL
-  DRY_RUN=0 prints a message "dataset not published yet for this month".

---

## Continuous Integration

A lightweight CI pipeline runs on each push and pull request:

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

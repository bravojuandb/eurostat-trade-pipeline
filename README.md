## Architecture & orchestration design

The ingestion system is built as a layered, reproducible pipeline with clear separation of concerns.

**Components**

- **Python CLI orchestrator** (`python -m src.bronze.fetch --from YYYY-MM --to YYYY-MM`)  
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

## Ingestion layer (Bronze)

The ingestion layer is responsible for **downloading** and **extracting** raw Eurostat Comext bulk trade data into local storage.

### What the ingestion layer does

- Downloads **monthly Comext bulk files** (`full_v2_YYYYMM.7z`) from Eurostat.
- Extracts `full_v2_YYYYMM.7z` into `full_v2_YYYYMM.dat`
- Requires an **explicit closed interval** (`--from YYYY-MM` → `--to YYYY-MM`)  
- Stores raw files without modification (no filtering, no schema changes).
- Supports **dry‑run mode** to validate behavior without downloading data.
- Is **idempotent**: already‑downloaded months are skipped.

### Raw data folder structure

Each ingestion run materializes a directory like this:

Example interval   `-from 2005-11 -to 2006-01`
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

For the full raw data contract and guarantees, see `data/raw/README.md`.

---

## Running the ingestion layer with Docker

The ingestion layer is designed to run **inside Docker** for portability and reproducibility.

### Quick test (default interval)

Downloads data from **2002‑12 to 2003‑01**:

```bash
docker compose run --rm pipeline
```

### Dry‑run mode (planning only)

```bash
DRY_RUN=1 FROM_MONTH=2005-11 TO_MONTH=2025-11 docker compose run --rm pipeline
```

Dry‑run mode:
- prints what **would** be downloaded (month, file path, URL)
- performs a lightweight availability check (HTTP status)
- does **not** download or extract any data
- does **not** create files or directories

### Normal execution

```bash
FROM_MONTH=2013-11 TO_MONTH=2014-01 docker compose run --rm pipeline
```

## Validation rules

- Both `FROM` and `TO` are required, unless set to default
- Period format must be `YYYY-MM`.
- Periods earlier than **2002-01** are rejected.
- `FROM` must be less than or equal to `TO`.

---

## Continuous Integration

A lightweight CI pipeline runs on each push and pull request:

- Python linting with **Ruff**
- Docker image build
- Container smoke test (dry‑run, no downloads)
- Static analysis of Bash scripts with **ShellCheck**

The workflow can also be triggered manually via **GitHub Actions → Run workflow**.

---

## Status

- ✅ Ingestion layer (bronze) implemented and production‑style
- ⏭️ Transformation layer (silver) — planned
- ⏭️ Loading into PostgreSQL (gold) — planned

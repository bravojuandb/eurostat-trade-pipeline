"""
Silver layer transform: raw Comext .dat files → fact_trade_clean.parquet

Input:  data/raw/comext_products/*/*.dat
Output: data/silver/fact_trade_clean.parquet

Transforms applied:
- Select 7 columns: reporter, partner, product_nc, flow, period, value_eur, quantity_kg
- Cast FLOW: BIGINT → INTEGER
- Cast PERIOD: BIGINT (e.g. 200301) → DATE (e.g. 2003-01-01)
- Append all months into a single Parquet file

How to run:

python -m src.silver.transform --from YYYY-MM --to YYYY-MM

    - It assumes the selected interval is already inside data/raw
    - It doesn't fail elegantly if the selected interval doesn't exist
    - If a .dat file doesn't exist, it doesn't fail elegantly
    - It assumes all the files have the same column disposition (source is reliable)
    - Re-runs are idempotent by design (COPY overwrites the output file)
"""

import duckdb
from pathlib import Path
import argparse
import logging

from src.utils.cli_dates import parse_yyyy_mm, validate_range

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def find_input_dat_files(raw_root: Path) -> list[Path]:
    files = list(raw_root.glob("*/*.dat"))
    if not files:
        raise FileNotFoundError(
            f"No input .dat files found under {raw_root}. "
            "Run ingestion first or check --from/--to range."
        )
    return files


def ensure_rows_in_range(input_glob: Path, start: int, end: int) -> None:
    row_count = duckdb.sql(f"""
        SELECT COUNT(*)
        FROM read_csv('{input_glob}')
        WHERE PERIOD >= {start} AND PERIOD <= {end}
    """).fetchone()[0]
    if row_count == 0:
        raise FileNotFoundError(
            f"No rows found for range {start} to {end}. "
            "Check ingestion coverage for the selected interval."
        )


def cast_to_parquet(input_glob: Path, start: int, end: int, output: Path):
    """
    Select the desired columns, cast them to proper dtypes, and write a single parquet
    """
    duckdb.sql(f"""
        COPY(
        SELECT
            REPORTER AS reporter,
            PARTNER AS partner,
            PRODUCT_NC AS product_nc,
            CAST(FLOW AS INTEGER) AS flow,
            CAST(STRPTIME(CAST(PERIOD AS VARCHAR), '%Y%m') AS DATE) AS date,
            VALUE_EUR AS value_eur,
            QUANTITY_KG AS quantity_kg
        FROM read_csv('{input_glob}')
        WHERE PERIOD >= {start} AND PERIOD <= {end})
        TO '{output}' (FORMAT PARQUET)
    """)


def main() -> None:

    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="from_date", required=True, type=parse_yyyy_mm,  help="YYYY-MM")
    p.add_argument("--to", dest="to_date", required=True, type=parse_yyyy_mm, help="YYYY-MM")
    args = p.parse_args()

    BASE_PATH = Path(__file__).parent.parent.parent / "data"
    RAW_ROOT = BASE_PATH / "raw" / "comext_products"
    FILE_PATH = RAW_ROOT / "*" / "*.dat"
    OUT_PATH = BASE_PATH / "silver" / "fact_trade_clean.parquet"

    start = args.from_date
    end = args.to_date

    try:
        validate_range(start, end)
    except ValueError as e:
        p.error(str(e))

    try:
        files = find_input_dat_files(RAW_ROOT)
        logger.info("Found %s .dat files in raw input", len(files))

        ensure_rows_in_range(FILE_PATH, start, end)

        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Starting transform: %s to %s", start, end)
        logger.info("Writing to %s", OUT_PATH)
        logger.info("processing...")
        cast_to_parquet(FILE_PATH, start, end, OUT_PATH)

        row_count = duckdb.sql(f"SELECT COUNT(*) FROM '{OUT_PATH}'").fetchone()[0]
        logger.info("Rows written: %s", f"{row_count:,}")
    except (FileNotFoundError, duckdb.Error) as e:
        logger.error("%s", e)
        raise SystemExit(1)

if __name__ == "__main__":
    main()

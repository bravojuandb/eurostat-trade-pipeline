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
    - Re-runs are idempotent by design (COPY overwrites the output file)
"""

import duckdb
from pathlib import Path
import argparse
import logging

from src.utils.cli_dates import parse_yyyy_mm, validate_range

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "REPORTER", "PARTNER", "PRODUCT_NC", "FLOW", "PERIOD", "VALUE_EUR", "QUANTITY_KG"
}


def find_input_dat_files(raw_root: Path) -> list[Path]:
    files = list(raw_root.glob("*/*.dat"))
    if not files:
        raise FileNotFoundError(
            f"No input .dat files found under {raw_root}. "
            "Run ingestion first or check --from/--to range."
        )
    return files


def validate_period_parsing(input_glob: Path, start: int, end: int) -> None:
    """Fail if any PERIOD values in the requested range cannot be parsed as YYYYMM dates."""
    bad = duckdb.sql(f"""
        SELECT COUNT(*)
        FROM read_csv('{input_glob}')
        WHERE PERIOD >= {start} AND PERIOD <= {end}
          AND TRY_STRPTIME(CAST(PERIOD AS VARCHAR), '%Y%m') IS NULL
    """).fetchone()[0]
    if bad > 0:
        raise ValueError(
            f"{bad} rows have PERIOD values that cannot be parsed as YYYYMM in range {start}–{end}."
        )


def ensure_rows_in_range(input_glob: Path, start: int, end: int) -> None:
    """Validate the input glob as a whole: fail if it yields zero rows within the requested PERIOD range."""
    row_count = duckdb.sql(f"""
        SELECT COUNT(*)
        FROM read_csv('{input_glob}')
        WHERE PERIOD >= {start} AND PERIOD <= {end}
    """).fetchone()[0]
    if row_count == 0:
        raise ValueError(
            f"No rows found for PERIOD range {start}–{end}. "
            "Action: re-run ingestion for this interval (or verify the raw months exist)."
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

    logger.info("Run start")
    logger.info("Input range: %s to %s", start, end)
    logger.info("Input pattern: %s", FILE_PATH)
    logger.info("Output path: %s", OUT_PATH)

    try:
        files = find_input_dat_files(RAW_ROOT)
        logger.info("Found %s .dat files in raw input", len(files))

        ensure_rows_in_range(FILE_PATH, start, end)
        validate_period_parsing(FILE_PATH, start, end)

        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Writing to %s", OUT_PATH)
        cast_to_parquet(FILE_PATH, start, end, OUT_PATH)

        row_count = duckdb.sql(f"SELECT COUNT(*) FROM '{OUT_PATH}'").fetchone()[0]
        logger.info("Rows written: %s", f"{row_count:,}")
        logger.info("Run end — OK")
    except (FileNotFoundError, ValueError, duckdb.Error) as e:
        logger.error("Run failed: %s", e)
        raise SystemExit(1)

if __name__ == "__main__":
    main()

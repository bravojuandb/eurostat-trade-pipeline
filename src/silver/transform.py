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


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def cast_to_parquet(input: Path, start: int, end: int, output: Path):
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
            QUANTITY_KG AS quantity
        FROM read_csv('{input}')
        WHERE PERIOD >= {start} AND PERIOD <= {end})
        TO '{output}' (FORMAT PARQUET)
    """)


def main() -> None:

    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="from_date", required=True, help="YYYY-MM")
    p.add_argument("--to", dest="to_date", required=True, help="YYYY-MM")
    args = p.parse_args()

    BASE_PATH = Path(__file__).parent.parent.parent / "data"
    FILE_PATH = BASE_PATH / "raw" / "comext_products" / "*" / "*.dat"
    OUT_PATH = BASE_PATH / "silver" / "fact_trade_clean.parquet"

    start = int(args.from_date.replace("-", ""))
    end = int(args.to_date.replace("-", ""))

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Starting transform: %s to %s", args.from_date, args.to_date)
    logger.info("Writing to %s", OUT_PATH)

    logger.info("processing...")
    cast_to_parquet(FILE_PATH, start, end, OUT_PATH)

    row_count = duckdb.sql(f"SELECT COUNT(*) FROM '{OUT_PATH}'").fetchone()[0]
    logger.info("Rows written: %s", f"{row_count:,}")

if __name__ == "__main__":
    main()
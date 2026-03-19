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

    - Re-runs are idempotent by design (COPY overwrites the output file)
"""

import duckdb
from pathlib import Path
import argparse
import logging

from src.utils.cli_dates import parse_yyyy_mm, validate_range

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

EXPECTED_COLUMNS = {"REPORTER", "PARTNER", "PRODUCT_NC", "FLOW", "PERIOD", "VALUE_EUR", "QUANTITY_KG"}
CSV_READ_OPTS = "delim=',', header=True"

def validate_input_schema(files: list[Path]) -> None:
    for f in files:
        actual = set(duckdb.sql(f"SELECT * FROM read_csv('{f}', {CSV_READ_OPTS}) LIMIT 0").columns)
        missing = EXPECTED_COLUMNS - actual
        if missing:
            raise ValueError(
                f"{f.name} is missing expected columns: {sorted(missing)}. "
                "Eurostat may have changed the COMEXT schema."
            )


def count_input_dat_files(raw_root: Path, start: str, end: str) -> list[Path]:
    """Check existence of .dat files inside raw_root. Return list of .dat files"""
    files = [
        path
        for path in raw_root.glob("*/*.dat")
        if start <= path.parent.name <= end
    ]
    if not files:
        raise FileNotFoundError(
            f"No input .dat files found for requested interval {start} to {end} under {raw_root}. "
            "Run ingestion first or verify the raw month folders exist."
        )
    return files

def validate_period_parsing(file_pattern: Path, start: int, end: int) -> None:
    """Fail if any PERIOD values in the requested range cannot be parsed as YYYYMM dates."""
    bad = duckdb.sql(f"""
        SELECT COUNT(*)
        FROM read_csv('{file_pattern}', {CSV_READ_OPTS})
        WHERE PERIOD >= {start} AND PERIOD <= {end}
          AND TRY_STRPTIME(CAST(PERIOD AS VARCHAR), '%Y%m') IS NULL
    """).fetchone()[0]
    if bad > 0:
        raise ValueError(
            f"{bad} rows have PERIOD values that cannot be parsed as YYYYMM in range {start}–{end}."
        )

def ensure_rows_in_range(file_pattern: Path, start: int, end: int) -> None:
    """Validate the file pattern as a whole: fail if it yields zero rows within the requested PERIOD range."""
    row_count = duckdb.sql(f"""
        SELECT COUNT(*)
        FROM read_csv('{file_pattern}', {CSV_READ_OPTS})
        WHERE PERIOD >= {start} AND PERIOD <= {end}
    """).fetchone()[0]
    if row_count == 0:
        raise ValueError(
            f"No rows found for PERIOD range {start}–{end}. "
            "Action: re-run ingestion for this interval (or verify the raw months exist)."
        )


def cast_to_parquet(file_pattern: Path, start: int, end: int, output: Path) -> None:
    """Select the desired columns, cast them to proper dtypes, and write a single parquet."""
    duckdb.sql(f"""
        COPY(
        SELECT
            REPORTER AS reporter,
            PARTNER AS partner,
            PRODUCT_NC AS product_nc,
            CAST(FLOW AS INTEGER) AS flow,
            CAST(STRPTIME(CAST(PERIOD AS VARCHAR), '%Y%m') AS DATE) AS period,
            VALUE_EUR AS value_eur,
            QUANTITY_KG AS quantity_kg
        FROM read_csv('{file_pattern}', {CSV_READ_OPTS})
        WHERE PERIOD >= {start} AND PERIOD <= {end})
        TO '{output}' (FORMAT PARQUET)
    """)


def validate_output_quality(output_file: Path) -> None:
    """Warn on nulls in measures; fail on invalid flow, negative measures, or bad period dates."""
    null_value_eur, null_quantity_kg, bad_flow, bad_measures, bad_period = duckdb.sql(f"""
        SELECT
            COUNT(*) FILTER (WHERE value_eur IS NULL)                AS null_value_eur,
            COUNT(*) FILTER (WHERE quantity_kg IS NULL)              AS null_quantity_kg,
            COUNT(*) FILTER (WHERE flow NOT IN (1, 2))               AS bad_flow,
            COUNT(*) FILTER (WHERE value_eur < 0 OR quantity_kg < 0) AS bad_measures,
            COUNT(*) FILTER (WHERE DAY(period) != 1)                 AS bad_period
        FROM '{output_file}'
    """).fetchone()

    if null_value_eur > 0:
        logger.warning("value_eur has %s null rows", f"{null_value_eur:,}")
    if null_quantity_kg > 0:
        logger.warning("quantity_kg has %s null rows", f"{null_quantity_kg:,}")
    if bad_flow > 0:
        raise ValueError(f"{bad_flow:,} rows have flow values outside {{1, 2}}.")
    if bad_measures > 0:
        raise ValueError(f"{bad_measures:,} rows have negative value_eur or quantity_kg.")
    if bad_period > 0:
        raise ValueError(f"{bad_period:,} rows have period not on the first day of the month.")


def main() -> None:

    p = argparse.ArgumentParser()
    p.add_argument("--from", dest="from_date", required=True, type=parse_yyyy_mm,  help="YYYY-MM")
    p.add_argument("--to", dest="to_date", required=True, type=parse_yyyy_mm, help="YYYY-MM")
    args = p.parse_args()

    BASE_PATH = Path(__file__).parent.parent.parent / "data"
    RAW_ROOT = BASE_PATH / "raw" / "comext_products"
    FILE_PATTERN = RAW_ROOT / "*" / "*.dat"
    OUT_PATH = BASE_PATH / "silver" / "fact_trade_clean.parquet"

    start = args.from_date
    end = args.to_date

    try:
        validate_range(start, end)
    except ValueError as e:
        p.error(str(e))

    logger.info("Run start")
    logger.info("Input range: %s to %s", start, end)
    logger.info("Input file pattern: %s", FILE_PATTERN)
    logger.info("Output path: %s", OUT_PATH)

    try:
        files = count_input_dat_files(RAW_ROOT, start, end)
        logger.info("Found %s .dat files in raw input", len(files))

        validate_input_schema(files)
        logger.info("Schema validated for %s files", len(files))

        # CLI date values must be converted to valid PERIOD values
        start_num = int(start.replace("-", ""))
        end_num = int(end.replace("-", ""))

        validate_period_parsing(FILE_PATTERN, start_num, end_num)
        ensure_rows_in_range(FILE_PATTERN, start_num, end_num)

        source_rows = duckdb.sql(f"""
            SELECT COUNT(*) FROM read_csv('{FILE_PATTERN}', {CSV_READ_OPTS})
            WHERE PERIOD >= {start_num} AND PERIOD <= {end_num}
        """).fetchone()[0]

        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Writing to %s", OUT_PATH)
        cast_to_parquet(FILE_PATTERN, start_num, end_num, OUT_PATH)

        # Read back to confirm the output is readable and row count matches source
        rows_in_file = duckdb.sql(f"SELECT COUNT(*) FROM '{OUT_PATH}'").fetchone()[0]
        if source_rows != rows_in_file:
            raise ValueError(
                f"Post-write mismatch: source had {source_rows:,} rows "
                f"but Parquet contains {rows_in_file:,}."
            )
        logger.info("Rows written: %s", f"{rows_in_file:,}")

        validate_output_quality(OUT_PATH)
        logger.info("Output quality checks passed")
        logger.info("Run end — OK")
    except (FileNotFoundError, ValueError, duckdb.Error) as e:
        logger.error("Run failed: %s", e)
        raise SystemExit(1)

if __name__ == "__main__":
    main()

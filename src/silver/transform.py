"""
Silver layer transform: raw Comext .dat files → fact_trade_clean.parquet

Input:  data/raw/comext_products/YYYY-MM/*.dat
Output: data/silver/fact_trade_clean.parquet

Transforms applied:
- Select 7 columns: reporter, partner, product_nc, flow, period, value_eur, quantity_kg
- Cast FLOW: BIGINT → INTEGER
- Cast PERIOD: BIGINT (e.g. 200301) → DATE (e.g. 2003-01-01)
- Append all months into a single Parquet file
"""

import duckdb
from pathlib import Path


BASE_PATH = Path(__file__).parent.parent.parent / "data" / "raw" / "comext_products"
FILE_PATH =  BASE_PATH / "2002-12" / "full_v2_200212.dat"

print(FILE_PATH)

rel = duckdb.read_csv(FILE_PATH)


table =  duckdb.sql("""
        SELECT 
            REPORTER AS reporter,
            PARTNER AS partner,
            PRODUCT_NC AS product_nc,
            CAST(FLOW AS INTEGER) AS flow,
            CAST(STRPTIME(CAST(PERIOD AS VARCHAR), '%Y%m') AS DATE) AS date,
            VALUE_EUR AS value_eur,
            QUANTITY_KG AS quantity 
        FROM rel
        LIMIT 20
    """
    )

print(table)
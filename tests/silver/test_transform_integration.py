from pathlib import Path
import duckdb

from src.silver.transform import (
    cast_to_parquet
)

def create_dat_file(folder: Path, month: str, row: str):
    month_dir = folder / month
    month_dir.mkdir()
    (month_dir / "test.dat").write_text(
        "REPORTER|PARTNER|PRODUCT_NC|FLOW|PERIOD|VALUE_EUR|QUANTITY_KG\n"
        f"{row}\n"
    )

def test_cast_to_parquet_merges_multiple_months_into_one_file(tmp_path):
    """
    Creates two different .dat files in a temporary path. 
    Asserts:
    - Outpus is one uniwue file
    - Output contains two rows
    """

    create_dat_file(tmp_path, "2003-01", "DE|FR|01020304|1|200301|1000|500")
    create_dat_file(tmp_path, "2003-02", "DE|IT|01020304|1|200302|2000|700")

    out = tmp_path / "out.parquet"
    cast_to_parquet(tmp_path / "*" / "*.dat", 200301, 200302, out) 

    row_count = duckdb.sql(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0]
    assert row_count == 2

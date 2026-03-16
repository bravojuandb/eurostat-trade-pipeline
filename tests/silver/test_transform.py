from pathlib import Path
import duckdb

from src.silver.transform import (
    cast_to_parquet
)

# Create a temporary folder with  fake .dat file

def create_dat_file(folder: Path):
    month_dir = folder / "2003-01"
    month_dir.mkdir()
    (month_dir / "test.dat").write_text(
        "REPORTER|PARTNER|PRODUCT_NC|FLOW|PERIOD|VALUE_EUR|QUANTITY_KG\n"
        "DE|FR|01020304|1|200301|1000|500"
    )

def test_cast_to_parquet_row_count(tmp_path):
    """creates a fake .dat file, runs the transform, 
    and asserts the output has exactly 1 row."""
    create_dat_file(tmp_path)
    print((tmp_path / "2003-01" / "test.dat").read_text())
    cast_to_parquet(
        tmp_path / "*" / "*.dat",
        200301,
        200301,
        tmp_path / "out.parquet"
        )
    row_count = duckdb.sql(f"SELECT COUNT(*) FROM '{tmp_path / 'out.parquet'}'").fetchone()[0]
    assert row_count == 1
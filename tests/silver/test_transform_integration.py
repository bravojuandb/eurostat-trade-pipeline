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
    - Output is one unique file
    - Output contains two rows
    """

    create_dat_file(tmp_path, "2003-01", "DE|FR|01020304|1|200301|1000|500")
    create_dat_file(tmp_path, "2003-02", "DE|IT|01020304|1|200302|2000|700")

    out = tmp_path / "out.parquet"
    cast_to_parquet(tmp_path / "*" / "*.dat", 200301, 200302, out) 

    assert out.exists(), "Expected cast_to_parquet() to create out.parquet"

    row_count = duckdb.sql(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0]
    assert row_count == 2


def test_output_parquet_is_readable_and_has_expected_schema(tmp_path):
    """
    Creates two different .dat files in a temporary path. 
    Asserts:
    - Output Parquet file is readable
    - Output contains all seven columns
    """

    create_dat_file(tmp_path, "2003-01", "DE|FR|01020304|1|200301|1000|500")
    create_dat_file(tmp_path, "2003-02", "DE|IT|01020304|1|200302|2000|700")

    out = tmp_path / "out.parquet"
    cast_to_parquet(tmp_path / "*" / "*.dat", 200301, 200302, out) 

    columns = duckdb.sql(f"SELECT * FROM '{out}' LIMIT 0").columns

    assert columns== [
        "reporter",
        "partner",
        "product_nc",
        "flow",
        "period",
        "value_eur",
        "quantity_kg",
    ] , "Expected out.parquet to include all columns "
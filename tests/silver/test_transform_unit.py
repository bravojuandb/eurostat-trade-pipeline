from pathlib import Path
import pytest

from src.silver.transform import (
    count_input_dat_files
)

def create_dat_file(folder: Path, month: str):
    month_dir = folder / month
    month_dir.mkdir()
    (month_dir / "test.dat").write_text(
        "REPORTER|PARTNER|PRODUCT_NC|FLOW|PERIOD|VALUE_EUR|QUANTITY_KG\n"
    )

def test_count_input_dat_files(tmp_path):

    months = ["2004-01", "2004-02", "2004-03", "2004-05", "2004-06"]

    for month in months:
        create_dat_file(tmp_path, month)
    
    files = count_input_dat_files(tmp_path, "2004-01", "2004-06")

    assert  len(files) == len(months)


def test_count_input_dat_files_raises_when_no_files_match_range(tmp_path):

    create_dat_file(tmp_path, "2004-05")
    
    with pytest.raises(FileNotFoundError):
        count_input_dat_files(tmp_path, "2004-01", "2004-03")
from pathlib import Path
import pytest

from src.silver.transform import (
    count_input_dat_files,
    validate_input_schema,
    ensure_rows_in_range
)

def create_dat_file(folder: Path, month: str, content: str):
    month_dir = folder / month
    month_dir.mkdir()
    (month_dir / "test.dat").write_text(content)

content = (
    "REPORTER,PARTNER,PRODUCT_NC,FLOW,PERIOD,VALUE_EUR,QUANTITY_KG\n"
    "DE,FR,01020304,1,200401,1000,500\n"
)

def test_count_input_dat_files(tmp_path):
    """
    Assert that matching .dat files are counted within the requested month range.
    """

    months = ["2004-01", "2004-02", "2004-03", "2004-04", "2004-05", "2004-06"]

    for month in months:
        create_dat_file(tmp_path, month, content)

    files = count_input_dat_files(tmp_path, "2004-01", "2004-06")

    assert  len(files) == len(months)


def test_count_input_dat_files_raises_when_no_files_match_range(tmp_path):
    """
    Assert that FileNotFoundError is raised when no .dat files match the requested range.
    """

    create_dat_file(tmp_path, "2004-05", content)
    
    with pytest.raises(FileNotFoundError):
        count_input_dat_files(tmp_path, "2004-01", "2004-03")


def test_validate_input_schema_passes(tmp_path):
    """
    Assert that valid input .dat files pass schema validation without raising an exception.
    """

    months = ["2004-01", "2004-02", "2004-03", "2004-04", "2004-05", "2004-06"]

    for month in months:
        create_dat_file(tmp_path, month, content)
    
    files = count_input_dat_files(tmp_path, "2004-01", "2004-06")

    validate_input_schema(files)


def test_validate_input_schema_raises(tmp_path):
    """
    Raises an exception if thers a schema mismatch.
    """
    content_corrupt = (
    "REPORTER,PARTNER,PRODUCT_NC,FLOW,PERIOD,VALUE_EUR\n"
    "DE,FR,01020304,1,200401,1000\n"
    )

    months = ["2004-01", "2004-02", "2004-03", "2004-04", "2004-05", "2004-06"]

    for month in months:
        create_dat_file(tmp_path, month, content_corrupt)
    
    files = count_input_dat_files(tmp_path, "2004-01", "2004-06")

    with pytest.raises(ValueError):
        validate_input_schema(files)




def test_ensure_rows_in_range_raises(tmp_path):
    """
    Assert that ValueError is raised when matching input files exist,
    but none of their rows fall within the requested PERIOD range.
    """

    months = ["2004-01", "2004-02", "2004-03", "2004-04", "2004-05", "2004-06"]

    content_out_of_range = (
    "REPORTER,PARTNER,PRODUCT_NC,FLOW,PERIOD,VALUE_EUR,QUANTITY_KG\n"
    "DE,FR,01020304,1,200301,1000,500\n"
    )

    for month in months:
        create_dat_file(tmp_path, month, content_out_of_range)

    file_pattern = tmp_path / "*" / "*.dat"

    with pytest.raises(ValueError):
        ensure_rows_in_range(file_pattern, 200401, 200406)

"""
This is a helper module to parse and validate the CLI entrypoints for fetch.py and transform.py
"""

import argparse
from datetime import datetime


def parse_yyyy_mm(value: str) -> str:
    try:
        dt = datetime.strptime(value, "%Y-%m")
        return dt.strftime("%Y-%m")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected format YYYY-MM (example: 2024-03)."
        )


def validate_range(start: int, end: int) -> None:
    if start > end:
        raise ValueError(f"Invalid range: --from must be <= --to (got {start} > {end}).")
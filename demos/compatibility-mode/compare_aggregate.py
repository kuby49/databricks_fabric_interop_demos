"""Compare exported aggregate CSVs locally; never query or deploy cloud resources."""

from __future__ import annotations

import argparse
import csv
from datetime import date
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import sys

KEY = ("DateKey", "ProductKey", "Region", "Segment")
COLUMNS = (
    "DateKey", "Date", "CalendarYear", "CalendarMonth", "MonthName", "DayOfMonth",
    "IsWeekend", "ProductKey", "ProductName", "Category", "Region", "Segment",
    "TotalSales", "TotalQuantity", "SalesLineCount",
)
INTEGER_COLUMNS = {"DateKey", "CalendarYear", "CalendarMonth", "DayOfMonth", "ProductKey",
                   "TotalQuantity", "SalesLineCount"}


def read_groups(path: str | Path) -> dict[tuple, tuple]:
    groups = {}
    with Path(path).open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None or any(name not in reader.fieldnames for name in COLUMNS):
            raise ValueError(f"{path}: missing required aggregate columns")
        if len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise ValueError(f"{path}: duplicate column names")
        for line, row in enumerate(reader, 2):
            try:
                values = {}
                for name in COLUMNS:
                    value = row[name]
                    if value is None or value == "":
                        raise ValueError(f"{name} is empty")
                    if name in INTEGER_COLUMNS:
                        values[name] = int(value)
                    elif name == "Date":
                        values[name] = date.fromisoformat(value)
                    elif name == "TotalSales":
                        values[name] = Decimal(value)
                        if not values[name].is_finite():
                            raise ValueError("TotalSales is not finite")
                    elif name == "IsWeekend":
                        if value.lower() not in ("true", "false"):
                            raise ValueError("IsWeekend must be true or false")
                        values[name] = value.lower() == "true"
                    else:
                        values[name] = value
                key = tuple(values[name] for name in KEY)
                if key in groups:
                    raise ValueError(f"duplicate aggregate key {key}")
                groups[key] = tuple(values[name] for name in COLUMNS)
            except (TypeError, ValueError, InvalidOperation) as error:
                raise ValueError(f"{path}:{line}: {error}") from error
    return groups


def compare(reference: str | Path, actual: str | Path) -> dict[str, int | str]:
    expected_groups = read_groups(reference)
    actual_groups = read_groups(actual)
    expected_keys, actual_keys = set(expected_groups), set(actual_groups)
    return {
        "status": "MATCH" if expected_groups == actual_groups else "MISMATCH",
        "expected_groups": len(expected_groups),
        "actual_groups": len(actual_groups),
        "missing_groups": len(expected_keys - actual_keys),
        "extra_groups": len(actual_keys - expected_keys),
        "different_groups": sum(
            expected_groups[key] != actual_groups[key] for key in expected_keys & actual_keys
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--actual", required=True)
    args = parser.parse_args(argv)
    try:
        result = compare(args.reference, args.actual)
    except (OSError, ValueError) as error:
        print(f"Comparison blocked: {error}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "MATCH" else 1


if __name__ == "__main__":
    raise SystemExit(main())

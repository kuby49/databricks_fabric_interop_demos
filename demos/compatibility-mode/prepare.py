"""Prepare local baseline and comparison references; never connect or deploy."""

from __future__ import annotations

import argparse
import csv
from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import re
import sys
from typing import Any
from uuid import UUID
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from demos.common.star_schema import (  # noqa: E402
    StarSchemaConfig,
    generate_star_schema,
    json_safe,
    validate_star_schema,
)

OBJECTS = ("dimdate", "dimproduct", "dimcustomer", "factsales", "sales_by_day_product_region_segment")
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def paths(config: dict[str, Any]) -> dict[str, Any]:
    """Validate path configuration only; this does not verify resources or permissions."""
    dbx, fabric = config["databricks"], config["fabric"]
    if "compat_schema" in dbx or "compat_storage_id" in fabric:
        raise ValueError("legacy two-producer config: inventory and migrate explicitly; do not replay loads")
    for key in ("parent_catalog", "normal_schema"):
        if not isinstance(dbx[key], str) or not IDENTIFIER.fullmatch(dbx[key]):
            raise ValueError(f"{key} must be a simple SQL identifier")
    if dbx["parent_catalog_approved"] is not True:
        raise ValueError("parent catalog must be explicitly approved")
    ids = {}
    for key in ("workspace_id", "normal_storage_id"):
        try:
            ids[key] = str(UUID(fabric[key]))
        except (ValueError, TypeError, AttributeError) as error:
            raise ValueError(f"{key} must be a GUID") from error
    base = f"abfss://{ids['workspace_id']}@onelake.dfs.fabric.microsoft.com/"
    normal = f"{base}{ids['normal_storage_id']}/Files"
    output_root = config["deployment"]["output_root"].rstrip("/")
    parsed = urlsplit(output_root)
    if (parsed.scheme != "abfss" or not parsed.username or parsed.password
            or not parsed.hostname or not parsed.hostname.endswith(".dfs.core.windows.net")
            or parsed.query or parsed.fragment or not parsed.path.strip("/")
            or any(p in ("", ".", "..") for p in parsed.path.strip("/").split("/"))):
        raise ValueError("output_root must be an isolated unsigned ADLS abfss directory")
    return {
        "normal_managed": f"{normal}/managed",
        "compatibility_destinations": {
            name: f"{output_root}/{name}" for name in OBJECTS
        },
    }


def aggregate(tables: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    dates = {row["DateKey"]: row for row in tables["DimDate"]}
    products = {row["ProductKey"]: row for row in tables["DimProduct"]}
    customers = {row["CustomerKey"]: row for row in tables["DimCustomer"]}
    groups: dict[tuple[Any, ...], dict[str, Any]] = {}
    for sale in tables["FactSales"]:
        day = dates[sale["DateKey"]]
        product = products[sale["ProductKey"]]
        customer = customers[sale["CustomerKey"]]
        key = (sale["DateKey"], sale["ProductKey"], customer["Region"], customer["Segment"])
        if key not in groups:
            groups[key] = {
                "DateKey": key[0], "Date": day["Date"], "CalendarYear": day["CalendarYear"],
                "CalendarMonth": day["CalendarMonth"], "MonthName": day["MonthName"],
                "DayOfMonth": day["DayOfMonth"], "IsWeekend": day["IsWeekend"],
                "ProductKey": key[1], "ProductName": product["ProductName"],
                "Category": product["Category"], "Region": key[2], "Segment": key[3],
                "TotalSales": Decimal("0.00"), "TotalQuantity": 0, "SalesLineCount": 0,
            }
        group = groups[key]
        group["TotalSales"] += sale["SalesAmount"]
        group["TotalQuantity"] += sale["Quantity"]
        group["SalesLineCount"] += 1
    return [groups[key] for key in sorted(groups)]


def classify(path: str, engine: str, query: str, evidence: dict[str, bool]) -> str:
    """Classify one query observation without treating blocked checks as a negative control."""
    if path not in ("normal", "compatibility") or engine not in ("sql", "spark"):
        raise ValueError("invalid path or engine")
    if query not in ("success", "feature_exclusion", "permission", "timeout", "absent", "other"):
        raise ValueError("invalid query observation")
    required = ("ready", "authorized", "selection_verified", "source_catalog_managed")
    if not all(evidence.get(key) is True for key in required):
        return "BLOCKED"
    if path == "normal":
        if evidence.get("resolved_target_verified") is not True:
            return "BLOCKED"
        if (query == "feature_exclusion" and engine == "spark"
                and evidence.get("resolved_target_catalog_managed") is True):
            return "EXPECTED_LIMITATION_OBSERVED"
        if query == "success":
            return "OBSERVED_SUCCESS" if evidence.get("data_matches") is True else (
                "FAIL" if evidence.get("data_matches") is False else "BLOCKED"
            )
        return "BLOCKED"
    if not all(evidence.get(key) is True for key in (
        "output_location_verified", "output_no_catalog_managed"
    )):
        return "BLOCKED"
    if query != "success" or evidence.get("data_matches") is None:
        return "BLOCKED"
    return "PASS" if evidence["data_matches"] is True else "FAIL"


def prepare(config: dict[str, Any], output_dir: str | Path) -> dict[str, Any]:
    sample = config["deployment"]["sample_data"]
    data_config = StarSchemaConfig(
        date_rows=sample["date_rows"], product_rows=sample["product_rows"],
        customer_rows=sample["customer_rows"], sales_rows=sample["sales_rows"],
        seed=sample["seed"], start_date=date.fromisoformat(sample["start_date"]),
    )
    timeout = config["deployment"]["verification_timeout_seconds"]
    if type(timeout) is not int or timeout <= 0:
        raise ValueError("verification_timeout_seconds must be a positive integer")
    data = generate_star_schema(data_config)
    metrics = validate_star_schema(data, data_config)
    reference = aggregate(data)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=False)
    for name, rows in {**data, "SalesAggregate": reference}.items():
        with (output / f"{name}.csv").open("x", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(json_safe(rows))
    result = {
        "status": "local_reference_only", "deployed": False,
        "sample_data": sample, "verification_timeout_seconds": timeout,
        "row_counts": metrics["row_counts"], "sales_total": metrics["sales_total"],
        "quantity_total": sum(row["Quantity"] for row in data["FactSales"]),
        "aggregate_group_count": len(reference),
        "aggregate_sales_total": sum((row["TotalSales"] for row in reference), Decimal("0.00")),
        "aggregate_quantity_total": sum(row["TotalQuantity"] for row in reference),
        "aggregate_line_count": sum(row["SalesLineCount"] for row in reference),
    }
    with (output / "reference.json").open("x", encoding="utf-8") as file:
        json.dump(json_safe(result), file, indent=2)
        file.write("\n")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--output-dir")
    modes.add_argument("--check-paths", action="store_true")
    args = parser.parse_args(argv)
    try:
        with Path(args.config).open(encoding="utf-8") as file:
            config = json.load(file)
        if args.check_paths:
            print(json.dumps(paths(config), indent=2))
            print("Local syntax check only; no resource, permission or destination verified.")
            return 0
        prepare(config, args.output_dir)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Local preparation failed: {error}", file=sys.stderr)
        return 2
    print(f"Local references written to {args.output_dir}; no cloud deployment performed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

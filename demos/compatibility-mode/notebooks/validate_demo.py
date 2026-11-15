"""Optional, separately cost-approved reads; resolved targets are recorded in the manifest."""

import hashlib
import json
import platform
from datetime import date, datetime, timezone
from decimal import Decimal

from notebookutils import mssparkutils

spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")
observations = {
    "observed_at_utc": datetime.now(timezone.utc).isoformat(),
    "spark_version": spark.version,
    "python_version": platform.python_version(),
    "tables": {},
}
lakehouse_name = "fabricinterop_cc_compat_validation"

objects = {
    "dimdate": (["DateKey", "Date", "CalendarYear", "CalendarMonth", "MonthName",
                 "DayOfMonth", "IsWeekend"], ["DateKey"]),
    "dimproduct": (["ProductKey", "ProductName", "Category", "UnitPrice"], ["ProductKey"]),
    "dimcustomer": (["CustomerKey", "CustomerName", "Region", "Segment"], ["CustomerKey"]),
    "factsales": (["SalesLineKey", "DateKey", "ProductKey", "CustomerKey",
                   "Quantity", "UnitPrice", "SalesAmount"], ["SalesLineKey"]),
    "sales_by_day_product_region_segment": (
        ["DateKey", "Date", "CalendarYear", "CalendarMonth", "MonthName", "DayOfMonth",
         "IsWeekend", "ProductKey", "ProductName", "Category", "Region", "Segment",
         "TotalSales", "TotalQuantity", "SalesLineCount"],
        ["DateKey", "ProductKey", "Region", "Segment"],
    ),
}


def canonical(value):
    if isinstance(value, Decimal):
        return format(value, ".2f")
    if isinstance(value, date):
        return value.isoformat()
    return value


for path in ("normal", "compat"):
    for name, (columns, keys) in objects.items():
        shortcut_name = "compat_salesagg" if path == "compat" and name == "sales_by_day_product_region_segment" else f"{path}_{name}"
        table = f"{lakehouse_name}.{shortcut_name}"
        try:
            if not spark.catalog.tableExists(table):
                observations["tables"][f"{path}.{name}"] = {"status": "not_discovered"}
                continue
            frame = spark.table(table).select(*columns).orderBy(*keys)
            rows = [[canonical(value) for value in row] for row in frame.collect()]
            digest = hashlib.sha256(
                json.dumps(rows, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
            ).hexdigest()
            observations["tables"][f"{path}.{name}"] = {
                "status": "succeeded", "rows": len(rows), "sha256": digest
            }
        except Exception as error:
            observations["tables"][f"{path}.{name}"] = {
                "status": "failed", "error_type": type(error).__name__, "error": str(error)
            }

mssparkutils.notebook.exit(json.dumps(observations))

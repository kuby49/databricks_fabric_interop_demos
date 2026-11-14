"""Deterministic, dependency-free sample star-schema data for both demos."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
import random
from typing import Any


_MONTH_NAMES = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)

TABLE_COLUMN_TYPES = {
    "DimDate": (
        ("DateKey", "INT"),
        ("Date", "DATE"),
        ("CalendarYear", "INT"),
        ("CalendarMonth", "INT"),
        ("MonthName", "STRING"),
        ("DayOfMonth", "INT"),
        ("IsWeekend", "BOOLEAN"),
    ),
    "DimProduct": (
        ("ProductKey", "INT"),
        ("ProductName", "STRING"),
        ("Category", "STRING"),
        ("UnitPrice", "DECIMAL(12,2)"),
    ),
    "DimCustomer": (
        ("CustomerKey", "INT"),
        ("CustomerName", "STRING"),
        ("Region", "STRING"),
        ("Segment", "STRING"),
    ),
    "FactSales": (
        ("SalesLineKey", "BIGINT"),
        ("DateKey", "INT"),
        ("ProductKey", "INT"),
        ("CustomerKey", "INT"),
        ("Quantity", "INT"),
        ("UnitPrice", "DECIMAL(12,2)"),
        ("SalesAmount", "DECIMAL(14,2)"),
    ),
}

PRIMARY_KEYS = {
    "DimDate": ("DateKey",),
    "DimProduct": ("ProductKey",),
    "DimCustomer": ("CustomerKey",),
    "FactSales": ("SalesLineKey",),
}

FOREIGN_KEYS = {
    "FactSales": (
        ("DateKey", "DimDate", "DateKey"),
        ("ProductKey", "DimProduct", "ProductKey"),
        ("CustomerKey", "DimCustomer", "CustomerKey"),
    )
}


@dataclass(frozen=True)
class StarSchemaConfig:
    date_rows: int = 365
    product_rows: int = 20
    customer_rows: int = 100
    sales_rows: int = 10_000
    seed: int = 42
    start_date: date = date(2024, 1, 1)

    def validate(self) -> None:
        for field_name in ("date_rows", "product_rows", "customer_rows", "sales_rows"):
            if getattr(self, field_name) <= 0:
                raise ValueError(f"{field_name} must be greater than zero")
        if self.sales_rows < max(self.date_rows, self.product_rows, self.customer_rows):
            raise ValueError("sales_rows must cover every key in each dimension")


def _date_key(value: date) -> int:
    return value.year * 10_000 + value.month * 100 + value.day


def _unit_price(product_key: int) -> Decimal:
    cents = 250 + (product_key * 137 % 25_000)
    return (Decimal(cents) / Decimal(100)).quantize(Decimal("0.01"))


def _dimensions(config: StarSchemaConfig) -> dict[str, list[dict[str, Any]]]:
    dates = [
        config.start_date + timedelta(days=index)
        for index in range(config.date_rows)
    ]
    return {
        "DimDate": [
            {
                "DateKey": _date_key(value),
                "Date": value,
                "CalendarYear": value.year,
                "CalendarMonth": value.month,
                "MonthName": _MONTH_NAMES[value.month - 1],
                "DayOfMonth": value.day,
                "IsWeekend": value.weekday() >= 5,
            }
            for value in dates
        ],
        "DimProduct": [
            {
                "ProductKey": key,
                "ProductName": f"Product-{key:03d}",
                "Category": ("Accessories", "Apparel", "Equipment", "Outdoor")[
                    (key - 1) % 4
                ],
                "UnitPrice": _unit_price(key),
            }
            for key in range(1, config.product_rows + 1)
        ],
        "DimCustomer": [
            {
                "CustomerKey": key,
                "CustomerName": f"Customer-{key:04d}",
                "Region": ("Central", "East", "South", "West")[(key - 1) % 4],
                "Segment": ("Consumer", "Corporate", "Small Business")[
                    (key - 1) % 3
                ],
            }
            for key in range(1, config.customer_rows + 1)
        ],
    }


def _sales_rows(
    config: StarSchemaConfig,
    dimensions: dict[str, list[dict[str, Any]]],
    first_key: int,
    row_count: int,
    seed: int,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    dates = dimensions["DimDate"]
    products = dimensions["DimProduct"]
    customers = dimensions["DimCustomer"]
    rows = []
    for offset in range(row_count):
        date_row = dates[offset % len(dates)]
        product_row = products[offset % len(products)]
        customer_row = customers[offset % len(customers)]
        if offset >= max(len(dates), len(products), len(customers)):
            date_row = rng.choice(dates)
            product_row = rng.choice(products)
            customer_row = rng.choice(customers)
        quantity = rng.randint(1, 10)
        unit_price = product_row["UnitPrice"]
        rows.append(
            {
                "SalesLineKey": first_key + offset,
                "DateKey": date_row["DateKey"],
                "ProductKey": product_row["ProductKey"],
                "CustomerKey": customer_row["CustomerKey"],
                "Quantity": quantity,
                "UnitPrice": unit_price,
                "SalesAmount": (unit_price * quantity).quantize(Decimal("0.01")),
            }
        )
    return rows


def generate_star_schema(
    config: StarSchemaConfig | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Return all four tables with deterministic keys, values, and Decimal money."""
    config = config or StarSchemaConfig()
    config.validate()
    tables = _dimensions(config)
    tables["FactSales"] = _sales_rows(
        config,
        tables,
        first_key=1,
        row_count=config.sales_rows,
        seed=config.seed,
    )
    return tables


def generate_sales_batch(
    config: StarSchemaConfig,
    first_sales_line_key: int,
    row_count: int,
) -> list[dict[str, Any]]:
    """Generate an independent, reproducible batch that can be appended to FactSales."""
    config.validate()
    if first_sales_line_key <= config.sales_rows:
        raise ValueError("batch keys must start after the configured initial sales rows")
    if row_count <= 0:
        raise ValueError("row_count must be greater than zero")
    dimensions = _dimensions(config)
    return _sales_rows(
        config,
        dimensions,
        first_key=first_sales_line_key,
        row_count=row_count,
        seed=config.seed + first_sales_line_key,
    )


def validate_star_schema(
    tables: dict[str, list[dict[str, Any]]],
    config: StarSchemaConfig | None = None,
    expected_sales_rows: int | None = None,
) -> dict[str, Any]:
    """Raise ValueError on a data-quality failure and return exact reference metrics."""
    config = config or StarSchemaConfig()
    config.validate()
    required = ("DimDate", "DimProduct", "DimCustomer", "FactSales")
    missing = [name for name in required if name not in tables]
    if missing:
        raise ValueError(f"missing tables: {', '.join(missing)}")

    keys = {
        "DimDate": "DateKey",
        "DimProduct": "ProductKey",
        "DimCustomer": "CustomerKey",
        "FactSales": "SalesLineKey",
    }
    expected_dimensions = {
        "DimDate": config.date_rows,
        "DimProduct": config.product_rows,
        "DimCustomer": config.customer_rows,
    }
    key_sets: dict[str, set[int]] = {}
    for table_name, key_name in keys.items():
        rows = tables[table_name]
        if table_name in expected_dimensions and len(rows) != expected_dimensions[table_name]:
            raise ValueError(
                f"{table_name} has {len(rows)} rows; "
                f"expected {expected_dimensions[table_name]}"
            )
        values = [row.get(key_name) for row in rows]
        if any(value is None for value in values):
            raise ValueError(f"{table_name}.{key_name} contains NULL")
        if len(values) != len(set(values)):
            raise ValueError(f"{table_name}.{key_name} contains duplicate keys")
        key_sets[table_name] = set(values)

    fact_rows = tables["FactSales"]
    expected = expected_sales_rows
    if expected is None:
        expected = config.sales_rows
    if len(fact_rows) != expected:
        raise ValueError(f"FactSales has {len(fact_rows)} rows; expected {expected}")

    foreign_keys = (
        ("DateKey", "DimDate"),
        ("ProductKey", "DimProduct"),
        ("CustomerKey", "DimCustomer"),
    )
    for column, dimension in foreign_keys:
        dimension_key = keys[dimension]
        allowed = key_sets[dimension]
        for row in fact_rows:
            if row.get(column) is None:
                raise ValueError(f"FactSales.{column} contains NULL")
            if row[column] not in allowed:
                raise ValueError(
                    f"FactSales.{column} value {row[column]!r} has no "
                    f"{dimension}.{dimension_key} match"
                )
        observed = {row[column] for row in fact_rows}
        if observed != allowed:
            raise ValueError(
                f"FactSales.{column} does not exercise every {dimension} key"
            )

    total = sum((row["SalesAmount"] for row in fact_rows), Decimal("0.00"))
    by_date: dict[str, Decimal] = {}
    by_category: dict[str, Decimal] = {}
    by_region: dict[str, Decimal] = {}
    by_month: dict[str, Decimal] = {}
    product_by_key = {row["ProductKey"]: row for row in tables["DimProduct"]}
    customer_by_key = {row["CustomerKey"]: row for row in tables["DimCustomer"]}
    date_by_key = {row["DateKey"]: row for row in tables["DimDate"]}
    for row in fact_rows:
        if not isinstance(row["UnitPrice"], Decimal) or not isinstance(
            row["SalesAmount"], Decimal
        ):
            raise ValueError("FactSales money columns must use Decimal values")
        expected_amount = (row["UnitPrice"] * row["Quantity"]).quantize(
            Decimal("0.01")
        )
        if row["SalesAmount"] != expected_amount:
            raise ValueError(
                f"FactSales.SalesAmount for line {row['SalesLineKey']} "
                "does not equal Quantity times UnitPrice"
            )
        category = product_by_key[row["ProductKey"]]["Category"]
        region = customer_by_key[row["CustomerKey"]]["Region"]
        sales_date = date_by_key[row["DateKey"]]["Date"]
        sales_date_key = sales_date.isoformat()
        month = sales_date.strftime("%Y-%m")
        by_date[sales_date_key] = by_date.get(
            sales_date_key, Decimal("0.00")
        ) + row["SalesAmount"]
        by_category[category] = by_category.get(category, Decimal("0.00")) + row[
            "SalesAmount"
        ]
        by_region[region] = by_region.get(region, Decimal("0.00")) + row[
            "SalesAmount"
        ]
        by_month[month] = by_month.get(month, Decimal("0.00")) + row["SalesAmount"]

    return {
        "row_counts": {name: len(tables[name]) for name in required},
        "sales_total": total,
        "sales_by_date": dict(sorted(by_date.items())),
        "sales_by_category": dict(sorted(by_category.items())),
        "sales_by_region": dict(sorted(by_region.items())),
        "sales_by_month": dict(sorted(by_month.items())),
    }


def json_safe(value: Any) -> Any:
    """Convert generated values and validation metrics to JSON-safe values."""
    if isinstance(value, Decimal):
        return format(value, ".2f")
    if isinstance(value, (date,)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value

"""Offline configuration validation shared by the demo's local tools/notebooks."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import re
import sys
from typing import Any


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from demos.common.star_schema import PRIMARY_KEYS, StarSchemaConfig  # noqa: E402


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
GUID = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
ONELAKE_URL_TEMPLATE = (
    "abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
    "{storage_item_id}/Files/"
)


def load_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as file:
        config = json.load(file)
    if not isinstance(config, dict):
        raise ValueError("configuration root must be a JSON object")
    return config


def star_schema_config(config: dict[str, Any]) -> StarSchemaConfig:
    sample = config["deployment"]["sample_data"]
    result = StarSchemaConfig(
        date_rows=int(sample["date_rows"]),
        product_rows=int(sample["product_rows"]),
        customer_rows=int(sample["customer_rows"]),
        sales_rows=int(sample["sales_rows"]),
        seed=int(sample["seed"]),
        start_date=date.fromisoformat(sample.get("start_date", "2024-01-01")),
    )
    result.validate()
    return result


def append_rows(config: dict[str, Any]) -> int:
    count = int(config["deployment"]["sample_data"].get("append_rows", 1000))
    if count <= 0:
        raise ValueError("deployment.sample_data.append_rows must be greater than zero")
    return count


def approved_target(config: dict[str, Any]) -> dict[str, str]:
    """Return sanitized SQL target values only after the explicit catalog approval gate."""
    dbx = config.get("databricks", {})
    catalog = dbx.get("parent_catalog")
    if not isinstance(catalog, str) or not catalog.strip():
        raise ValueError("set databricks.parent_catalog explicitly; no default is inferred")
    if dbx.get("parent_catalog_approved") is not True:
        raise ValueError("set databricks.parent_catalog_approved to true after approval")
    schema = dbx.get("schema_name")
    for label, value in (("parent_catalog", catalog), ("schema_name", schema)):
        if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
            raise ValueError(f"databricks.{label} must be a simple SQL identifier")
    prefix = config.get("deployment", {}).get("prefix")
    if not isinstance(prefix, str) or not IDENTIFIER.fullmatch(prefix):
        raise ValueError("deployment.prefix must be a simple SQL identifier")
    if not schema.startswith(prefix + "_"):
        raise ValueError(
            "databricks.schema_name must start with the dedicated deployment prefix"
        )

    fabric = config.get("fabric", {})
    workspace_id = fabric.get("workspace_id")
    storage_item_id = fabric.get("databricks_storage_id")
    for label, value in (
        ("fabric.workspace_id", workspace_id),
        ("fabric.databricks_storage_id", storage_item_id),
    ):
        if not isinstance(value, str) or not GUID.fullmatch(value):
            raise ValueError(f"{label} must be a GUID")
    return {
        "catalog": catalog,
        "schema": schema,
        "workspace_id": workspace_id,
        "storage_item_id": storage_item_id,
        "managed_location": ONELAKE_URL_TEMPLATE.format(
            workspace_id=workspace_id, storage_item_id=storage_item_id
        ),
    }


def append_transaction_identity(config: dict[str, Any]) -> tuple[str, int]:
    """Return one stable Delta transaction identity for the configured append batch."""
    target = approved_target(config)
    prefix = config.get("deployment", {}).get("prefix")
    if not isinstance(prefix, str) or not IDENTIFIER.fullmatch(prefix):
        raise ValueError("deployment.prefix must be a simple SQL identifier")
    return (
        f"{prefix}:{target['catalog']}:{target['schema']}:FactSales:append-v1",
        1,
    )


def classify_append_state(
    observed_state: tuple[int, int, int, int],
    baseline_rows: int,
    append_rows: int,
) -> str:
    baseline = (baseline_rows, baseline_rows, 1, baseline_rows)
    final_rows = baseline_rows + append_rows
    appended = (final_rows, final_rows, 1, final_rows)
    if observed_state == baseline:
        return "baseline"
    if observed_state == appended:
        return "appended"
    raise ValueError(
        "append requires the exact baseline or complete deterministic batch; "
        f"observed row_count/distinct/min/max={observed_state}"
    )


def primary_key_not_null_statements(target: dict[str, str]) -> list[str]:
    """Make every shared primary-key column non-null before adding UC keys."""
    catalog = f"`{target['catalog']}`"
    schema = f"`{target['schema']}`"
    return [
        f"ALTER TABLE {catalog}.{schema}.`{table_name}` "
        f"ALTER COLUMN `{column_name}` SET NOT NULL"
        for table_name, columns in PRIMARY_KEYS.items()
        for column_name in columns
    ]


def constraint_statements(target: dict[str, str]) -> list[str]:
    """Declare informational UC keys after the Delta tables have been created."""
    catalog = f"`{target['catalog']}`"
    schema = f"`{target['schema']}`"

    def table(name: str) -> str:
        return f"{catalog}.{schema}.`{name}`"

    statements = [
        f"ALTER TABLE {table('DimDate')} ADD CONSTRAINT pk_dimdate "
        "PRIMARY KEY (DateKey)",
        f"ALTER TABLE {table('DimProduct')} ADD CONSTRAINT pk_dimproduct "
        "PRIMARY KEY (ProductKey)",
        f"ALTER TABLE {table('DimCustomer')} ADD CONSTRAINT pk_dimcustomer "
        "PRIMARY KEY (CustomerKey)",
        f"ALTER TABLE {table('FactSales')} ADD CONSTRAINT pk_factsales "
        "PRIMARY KEY (SalesLineKey)",
        f"ALTER TABLE {table('FactSales')} ADD CONSTRAINT fk_factsales_dimdate "
        f"FOREIGN KEY (DateKey) REFERENCES {table('DimDate')} (DateKey)",
        f"ALTER TABLE {table('FactSales')} ADD CONSTRAINT fk_factsales_dimproduct "
        f"FOREIGN KEY (ProductKey) REFERENCES {table('DimProduct')} (ProductKey)",
        f"ALTER TABLE {table('FactSales')} ADD CONSTRAINT fk_factsales_dimcustomer "
        f"FOREIGN KEY (CustomerKey) REFERENCES {table('DimCustomer')} (CustomerKey)",
    ]
    return statements

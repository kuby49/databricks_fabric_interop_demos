"""Prepare offline SQL and reference data; never connects to any service."""
from __future__ import annotations

import argparse
from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from demos.common.star_schema import (  # noqa: E402
    FOREIGN_KEYS, PRIMARY_KEYS, TABLE_COLUMN_TYPES, StarSchemaConfig,
    generate_sales_batch, generate_star_schema, json_safe, validate_star_schema,
)


def ident(value):
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError("catalog and schema must be nonempty identifiers")
    return "`" + value.replace("`", "``") + "`"


def literal(value):
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        # Spark SQL string literals treat backslashes as escape characters.
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    raise TypeError(f"unsupported SQL value: {type(value).__name__}")


def read_config(path):
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    if config.get("parent_catalog_approved") is not True:
        raise ValueError("explicit parent_catalog_approved=true is required")
    ident(config.get("parent_catalog"))
    ident(config.get("schema_name"))
    if config.get("storage_mode") not in ("DirectQuery", "Import"):
        raise ValueError("storage_mode must be DirectQuery or Import")
    if config["storage_mode"] == "Import" and config.get("import_explicitly_approved") is not True:
        raise ValueError("Import requires import_explicitly_approved=true")
    if type(config.get("verification_timeout_seconds")) is not int or config["verification_timeout_seconds"] <= 0:
        raise ValueError("verification_timeout_seconds must be a positive integer")
    if type(config.get("change_batch_rows")) is not int or config["change_batch_rows"] <= 0:
        raise ValueError("change_batch_rows must be a positive integer")
    sample = dict(config["sample_data"])
    sample["start_date"] = date.fromisoformat(sample["start_date"])
    star = StarSchemaConfig(**sample)
    star.validate()
    return config, star


def inserts(table, rows):
    columns = list(rows[0])
    target = ", ".join(ident(c) for c in columns)
    statements = []
    for start in range(0, len(rows), 500):
        values = ",\n".join(
            "(" + ", ".join(literal(row[c]) for c in columns) + ")"
            for row in rows[start:start + 500]
        )
        statements.append(f"INSERT INTO {table} ({target}) VALUES\n{values};")
    return "\n\n".join(statements)


def sql_artifacts(config, star):
    catalog, schema = ident(config["parent_catalog"]), ident(config["schema_name"])
    namespace = f"{catalog}.{schema}"
    def table(name):
        return f"{namespace}.{ident(name)}"

    tables = generate_star_schema(star)
    initial = validate_star_schema(tables, star)
    batch = generate_sales_batch(star, star.sales_rows + 1, config["change_batch_rows"])
    updated = dict(tables, FactSales=tables["FactSales"] + batch)
    after = validate_star_schema(updated, star, star.sales_rows + len(batch))
    d, p, c, f = (table(n) for n in ("DimDate", "DimProduct", "DimCustomer", "FactSales"))
    create = f"""-- Run ONCE only in a verified new namespace. No replacement or adoption.
CREATE SCHEMA {namespace};
"""
    for name in ("DimDate", "DimProduct", "DimCustomer", "FactSales"):
        primary = PRIMARY_KEYS[name]
        foreign = FOREIGN_KEYS.get(name, ())
        required = set(primary) | {source for source, _, _ in foreign}
        definitions = [
            f"{ident(column)} {sql_type}" + (" NOT NULL" if column in required else "")
            for column, sql_type in TABLE_COLUMN_TYPES[name]
        ]
        definitions.append(
            f"CONSTRAINT {ident(name.lower() + '_pk')} PRIMARY KEY "
            f"({', '.join(ident(column) for column in primary)})"
        )
        for source, target, target_column in foreign:
            definitions.append(
                f"CONSTRAINT {ident(name.lower() + '_' + source.lower() + '_fk')} "
                f"FOREIGN KEY ({ident(source)}) REFERENCES {table(target)} ({ident(target_column)})"
            )
        create += f"CREATE TABLE {table(name)} (\n  " + ",\n  ".join(definitions) + "\n) USING DELTA;\n"
    for name in ("DimDate", "DimProduct", "DimCustomer", "FactSales"):
        create += "\n" + inserts(table(name), tables[name]) + "\n"

    checks = """-- Expected: configured row counts; zero NULL keys, duplicate_keys and orphans.
-- Run before publishing AND again after the change batch. PK/FK constraints are informational.
"""
    for name, key in (("DimDate", "DateKey"), ("DimProduct", "ProductKey"),
                      ("DimCustomer", "CustomerKey"), ("FactSales", "SalesLineKey")):
        checks += (f"SELECT '{name}' AS table_name, COUNT(*) AS row_count, "
                   f"COUNT(*) - COUNT({ident(key)}) AS null_keys, "
                   f"COUNT({ident(key)}) - COUNT(DISTINCT {ident(key)}) AS duplicate_keys "
                   f"FROM {table(name)};\n")
    for key, dimension in (("DateKey", d), ("ProductKey", p), ("CustomerKey", c)):
        checks += (f"SELECT '{key}' AS foreign_key, COUNT(*) AS orphans "
                   f"FROM {f} fact LEFT JOIN {dimension} dim ON fact.{ident(key)} = dim.{ident(key)} "
                   f"WHERE fact.{ident(key)} IS NULL OR dim.{ident(key)} IS NULL;\n")
    for name in ("DimDate", "DimProduct", "DimCustomer", "FactSales"):
        checks += f"SHOW CREATE TABLE {table(name)};\n"
    checks += f"""
-- Reference queries: compare the full groups and grand total to the report.
SELECT COUNT(*) AS sales_rows, SUM(SalesAmount) AS total_sales FROM {f};
SELECT d.`Date`, SUM(f.SalesAmount) AS total_sales
FROM {f} f JOIN {d} d ON f.DateKey = d.DateKey
GROUP BY d.`Date` ORDER BY 1;
SELECT d.CalendarYear, d.CalendarMonth, SUM(f.SalesAmount) AS total_sales
FROM {f} f JOIN {d} d ON f.DateKey = d.DateKey
GROUP BY d.CalendarYear, d.CalendarMonth ORDER BY 1, 2;
SELECT p.Category, SUM(f.SalesAmount) AS total_sales
FROM {f} f JOIN {p} p ON f.ProductKey = p.ProductKey GROUP BY p.Category ORDER BY 1;
SELECT c.Region, SUM(f.SalesAmount) AS total_sales
FROM {f} f JOIN {c} c ON f.CustomerKey = c.CustomerKey GROUP BY c.Region ORDER BY 1;
-- Combined slicer comparison: replace these three literals with the actual selected values.
SELECT SUM(f.SalesAmount) AS total_sales
FROM {f} f JOIN {d} d ON f.DateKey = d.DateKey
JOIN {p} p ON f.ProductKey = p.ProductKey JOIN {c} c ON f.CustomerKey = c.CustomerKey
WHERE d.`Date` = DATE '{star.start_date.isoformat()}' AND p.Category = 'Accessories' AND c.Region = 'Central';
"""
    first, last = star.sales_rows + 1, star.sales_rows + len(batch)
    change = f"""-- Before executing the INSERT, run this query separately. STOP if occupied_keys is not zero.
SELECT COUNT(*) AS occupied_keys FROM {f} WHERE SalesLineKey BETWEEN {first} AND {last};
-- After independently confirming zero above, run the INSERT once only.
{inserts(f, batch)}
"""
    return create, checks, change, {"before": json_safe(initial), "after": json_safe(after),
                                    "batch_key_range": [first, last]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True, help="new directory inside demos/power-bi")
    args = parser.parse_args()
    config, star = read_config(args.config)
    output = Path(args.output).resolve()
    demo = Path(__file__).resolve().parent
    if output == demo or demo not in output.parents:
        raise ValueError("output must be a new directory inside demos/power-bi")
    create, checks, change, metrics = sql_artifacts(config, star)
    output.mkdir(parents=False, exist_ok=False)
    for name, text in (("01-create-and-load.sql", create), ("02-check-and-reference.sql", checks),
                       ("03-change-batch.sql", change),
                       ("expected-local.json", json.dumps(metrics, indent=2) + "\n")):
        (output / name).write_text(text, encoding="utf-8")
    print(f"Prepared local files in {output}; nothing deployed.")


if __name__ == "__main__":
    main()

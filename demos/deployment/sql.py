"""Executable guarded SQL generated from the shared sample schema."""

from datetime import date
from decimal import Decimal

from demos.common.star_schema import FOREIGN_KEYS, PRIMARY_KEYS, TABLE_COLUMN_TYPES

MV = "sales_by_day_product_region_segment"
TABLES = tuple(TABLE_COLUMN_TYPES)


def ident(value):
    return "`" + value.replace("`", "``") + "`"


def literal(value):
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, (Decimal, int)):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
    raise TypeError("Unsupported SQL value.")


def qualified(catalog, schema, table=None):
    return ".".join(ident(p) for p in (catalog, schema, table) if p is not None)


def create_table(catalog, schema, table, marker, catalog_commits=False):
    key_columns = set(PRIMARY_KEYS[table]) | {c for c, _, _ in FOREIGN_KEYS.get(table, ())}
    columns = [f"{ident(c)} {t}" + (" NOT NULL" if c in key_columns else "")
               for c, t in TABLE_COLUMN_TYPES[table]]
    columns.append(f"CONSTRAINT {ident(table.lower() + '_pk')} PRIMARY KEY ({ident(PRIMARY_KEYS[table][0])})")
    for source, target, column in FOREIGN_KEYS.get(table, ()):
        columns.append(f"CONSTRAINT {ident(table.lower() + '_' + source.lower() + '_fk')} "
                       f"FOREIGN KEY ({ident(source)}) REFERENCES "
                       f"{qualified(catalog, schema, target)} ({ident(column)})")
    properties = " TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported')" if catalog_commits else ""
    return (f"CREATE TABLE {qualified(catalog, schema, table)} (" + ", ".join(columns)
            + f") USING DELTA COMMENT {literal(marker)}" + properties)


def inserts(target, rows, batch_size=250):
    columns = list(rows[0])
    for start in range(0, len(rows), batch_size):
        values = ", ".join("(" + ", ".join(literal(row[c]) for c in columns) + ")"
                            for row in rows[start:start + batch_size])
        yield f"INSERT INTO {target} ({', '.join(ident(c) for c in columns)}) VALUES {values}"


def compatibility_properties(location):
    return ("'delta.universalFormat.enabledFormats' = 'compatibility', "
            f"'delta.universalFormat.compatibility.location' = {literal(location)}")


def aggregate_select(catalog, schema):
    descriptors = ("d.DateKey", "d.`Date`", "d.CalendarYear", "d.CalendarMonth", "d.MonthName",
                   "d.DayOfMonth", "d.IsWeekend", "p.ProductKey", "p.ProductName", "p.Category",
                   "c.Region", "c.Segment")
    grain = ", ".join(descriptors)
    return (f"SELECT {grain}, SUM(f.SalesAmount) AS TotalSales, SUM(f.Quantity) AS TotalQuantity, "
            f"COUNT(*) AS SalesLineCount FROM {qualified(catalog, schema, 'FactSales')} f "
            f"JOIN {qualified(catalog, schema, 'DimDate')} d ON f.DateKey = d.DateKey "
            f"JOIN {qualified(catalog, schema, 'DimProduct')} p ON f.ProductKey = p.ProductKey "
            f"JOIN {qualified(catalog, schema, 'DimCustomer')} c ON f.CustomerKey = c.CustomerKey "
            f"GROUP BY {grain}")


def create_mv(catalog, schema, marker, output=None):
    properties = "'delta.feature.catalogManaged' = 'supported'"
    if output:
        properties += ", " + compatibility_properties(output)
    return (f"CREATE MATERIALIZED VIEW {qualified(catalog, schema, MV)} COMMENT {literal(marker)} "
            f"TBLPROPERTIES ({properties}) AS {aggregate_select(catalog, schema)}")

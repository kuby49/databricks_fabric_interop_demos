"""Read-only Databricks notebook cell, after shared star_schema.py and parameters."""

from datetime import date, datetime, timezone
import json

spark.conf.set("spark.sql.session.timeZone", "UTC")
config_values = dict(sample_config)
if "start_date" in config_values:
    config_values["start_date"] = date.fromisoformat(config_values["start_date"])
sample = StarSchemaConfig(**config_values)
expected = generate_star_schema(sample)
if include_append:
    expected["FactSales"] += generate_sales_batch(sample, sample.sales_rows + 1, 10)

for table, rows in expected.items():
    schema_name, remote_name = discovered_tables[table]
    target = ".".join("`" + value.replace("`", "``") + "`"
                      for value in (foreign_catalog, schema_name, remote_name))
    columns = [column for column, _ in TABLE_COLUMN_TYPES[table]]
    actual = [row.asDict() for row in spark.read.table(target).select(*columns).orderBy(columns[0]).collect()]
    wanted = sorted(rows, key=lambda row: row[columns[0]])
    if actual != wanted:
        raise RuntimeError("Federated Spark exact-row comparison failed: " + table)

evidence = json_safe({"status": "pass", "observed_utc": datetime.now(timezone.utc).isoformat(),
                     "include_append": include_append, "reference": validate_star_schema(
                         expected, sample, len(expected["FactSales"]))})
print(json.dumps(evidence))

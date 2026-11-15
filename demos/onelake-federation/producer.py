"""Fabric notebook cell; parameters and shared generator are injected by the deployer."""

from datetime import datetime, timezone
from decimal import Decimal
import json

from delta.tables import DeltaTable
from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, IntegerType, LongType, StringType,
    StructField, StructType,
)

spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")
spark.conf.set("spark.sql.session.timeZone", "UTC")
config_values = dict(sample_config)
if "start_date" in config_values:
    config_values["start_date"] = date.fromisoformat(config_values["start_date"])
sample = StarSchemaConfig(**config_values)
expected = generate_star_schema(sample)
batch = generate_sales_batch(sample, sample.sales_rows + 1, 10)
types = {"INT": IntegerType(), "BIGINT": LongType(), "STRING": StringType(),
         "DATE": DateType(), "BOOLEAN": BooleanType(),
         "DECIMAL(12,2)": DecimalType(12, 2), "DECIMAL(14,2)": DecimalType(14, 2)}


def target(table):
    return ".".join("`" + part.replace("`", "``") + "`"
                    for part in (workspace_name, lakehouse_name, "dbo", table))


def frame(table, rows):
    schema = StructType([StructField(c, types[t], False) for c, t in TABLE_COLUMN_TYPES[table]])
    return spark.createDataFrame(rows, schema)


def equal(table, rows):
    wanted = frame(table, rows)
    actual = spark.read.table(target(table)).select(*wanted.columns)
    return (actual.exceptAll(wanted).limit(1).count() == 0
            and wanted.exceptAll(actual).limit(1).count() == 0)


def write_rows(table, rows, mode, label):
    key = "spark.databricks.delta.commitInfo.userMetadata"
    previous = spark.conf.get(key, None)
    spark.conf.set(key, deployment_marker + ":" + label)
    try:
        frame(table, rows).write.format("delta").mode(mode).option(
            "userMetadata", deployment_marker + ":" + label
        ).saveAsTable(target(table))
    finally:
        if previous is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, previous)


def source_commit(history, label, marker, sales_rows):
    if label == "baseline":
        # Fabric CTAS can omit writer userMetadata; ownership and exact rows are checked above.
        matches = [r for r in history if r["version"] == 0
                   and r["operation"] == "CREATE TABLE AS SELECT"
                   and int(r.get("operationMetrics", {}).get("numOutputRows", -1)) == sales_rows]
    else:
        matches = [r for r in history if r.get("userMetadata") == marker + ":append"
                   and r["operation"] == "WRITE"
                   and r.get("operationParameters", {}).get("mode", "").lower() == "append"
                   and int(r.get("operationMetrics", {}).get("numOutputRows", -1)) == 10]
    if len(matches) != 1:
        raise RuntimeError("Cannot uniquely identify the verified source data commit")
    return matches[0]


if operation == "baseline":
    for table, rows in expected.items():
        if spark.catalog.tableExists(target(table)):
            detail = spark.sql(f"DESCRIBE DETAIL {target(table)}").first().asDict()
            if detail.get("properties", {}).get("demo.deployment") != deployment_marker:
                raise RuntimeError("Existing table has a different deployment owner")
            if not equal(table, rows):
                raise RuntimeError("Existing baseline changed; never overwrite or append again")
        else:
            write_rows(table, rows, "errorifexists", "baseline")
            spark.sql(f"ALTER TABLE {target(table)} SET TBLPROPERTIES ('demo.deployment' = '{deployment_marker}')")
elif operation == "append":
    for table, rows in expected.items():
        if table != "FactSales" and not equal(table, rows):
            raise RuntimeError("Dimension baseline differs")
    detail = spark.sql(f"DESCRIBE DETAIL {target('FactSales')}").first().asDict()
    if detail.get("properties", {}).get("demo.deployment") != deployment_marker:
        raise RuntimeError("Fact table ownership differs")
    if equal("FactSales", expected["FactSales"]):
        write_rows("FactSales", batch, "append", "append")
    elif not equal("FactSales", expected["FactSales"] + batch):
        raise RuntimeError("Fact baseline/key range differs; append refused")
    expected["FactSales"] += batch
else:
    raise RuntimeError("Unknown operation")

for table, rows in expected.items():
    if not equal(table, rows):
        raise RuntimeError("Exact source row comparison failed: " + table)
history = [r.asDict() for r in DeltaTable.forName(spark, target("FactSales")).history().collect()]
commit = source_commit(history, operation, deployment_marker, sample.sales_rows)
evidence = {
    "operation": operation, "version": commit["version"],
    "source_commit_utc": commit["timestamp"].replace(tzinfo=timezone.utc).isoformat(),
    "commit_operation": commit["operation"],
    "observed_utc": datetime.now(timezone.utc).isoformat(),
    "reference": json_safe(validate_star_schema(expected, sample, len(expected["FactSales"]))),
}
notebookutils.notebook.exit(json.dumps(evidence))

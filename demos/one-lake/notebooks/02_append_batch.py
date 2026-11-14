# Databricks notebook source
# This notebook performs a deterministic, idempotent append.

# COMMAND ----------

import os
import runpy
import sys
from pathlib import Path

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

notebook_working_directory = Path(os.getcwd()).resolve()
path_helper = notebook_working_directory.parent / "scripts" / "notebook_paths.py"
if not path_helper.is_file():
    raise RuntimeError(
        "Expected CWD <repo>/demos/one-lake/notebooks; keep the documented "
        "repository layout and use classic Databricks Runtime 18.1+."
    )
resolve_repository_root = runpy.run_path(str(path_helper))[
    "resolve_repository_root"
]
repository_root = resolve_repository_root(notebook_working_directory)
if str(repository_root) not in sys.path:
    sys.path.insert(0, str(repository_root))

from demos.common.star_schema import (
    TABLE_COLUMN_TYPES,
    generate_sales_batch,
    generate_star_schema,
)

demo_config_path = repository_root / "demos" / "one-lake" / "config.json"
if not demo_config_path.is_file():
    raise FileNotFoundError(
        f"Copy config.example.json to the required local config: {demo_config_path}"
    )
demo_config = runpy.run_path(
    str(repository_root / "demos" / "one-lake" / "scripts" / "demo_config.py")
)
config = demo_config["load_config"](demo_config_path)
target = demo_config["approved_target"](config)
star_config = demo_config["star_schema_config"](config)
batch_size = demo_config["append_rows"](config)
txn_app_id, txn_version = demo_config["append_transaction_identity"](config)

# COMMAND ----------

fact_name = f"{target['catalog']}.{target['schema']}.FactSales"
fact = spark.table(fact_name)
expected_count = star_config.sales_rows
expected_final_count = expected_count + batch_size
current = fact.selectExpr(
    "count(*) AS row_count",
    "count(DISTINCT SalesLineKey) AS distinct_keys",
    "min(SalesLineKey) AS min_key",
    "max(SalesLineKey) AS max_key",
).first()
observed_state = (
    current["row_count"],
    current["distinct_keys"],
    current["min_key"],
    current["max_key"],
)
appended_state = (
    expected_final_count,
    expected_final_count,
    1,
    expected_final_count,
)
append_state = demo_config["classify_append_state"](
    observed_state, expected_count, batch_size
)

# COMMAND ----------

spark_types = {
    "INT": IntegerType,
    "BIGINT": LongType,
    "DATE": DateType,
    "STRING": StringType,
    "BOOLEAN": BooleanType,
}


def spark_type(type_name):
    if type_name.startswith("DECIMAL(") and type_name.endswith(")"):
        precision, scale = (int(value) for value in type_name[8:-1].split(","))
        return DecimalType(precision, scale)
    if type_name not in spark_types:
        raise ValueError(f"Unsupported shared schema type: {type_name}")
    return spark_types[type_name]()


batch_schema = StructType(
    [
        StructField(column_name, spark_type(type_name), False)
        for column_name, type_name in TABLE_COLUMN_TYPES["FactSales"]
    ]
)
batch = generate_sales_batch(
    star_config,
    first_sales_line_key=star_config.sales_rows + 1,
    row_count=batch_size,
)
batch_df = spark.createDataFrame(batch, schema=batch_schema)
baseline_rows = generate_star_schema(star_config)["FactSales"]
baseline_df = spark.createDataFrame(baseline_rows, schema=batch_schema)


def require_exact_rows(actual, expected, description):
    if (
        actual.exceptAll(expected).limit(1).count()
        or expected.exceptAll(actual).limit(1).count()
    ):
        raise RuntimeError(
            f"{description} differs from the deterministic shared generator; "
            "refusing to write."
        )


if append_state == "appended":
    existing_baseline = fact.filter(
        fact["SalesLineKey"] <= expected_count
    ).select(*baseline_df.columns)
    require_exact_rows(existing_baseline, baseline_df, "Configured baseline")
    existing_batch = fact.filter(
        fact["SalesLineKey"] > expected_count
    ).select(*batch_df.columns)
    require_exact_rows(existing_batch, batch_df, "Configured append batch")
    print("The exact deterministic batch is already present; skipping the write.")
else:
    require_exact_rows(fact, baseline_df, "Configured baseline")
    (
        batch_df.write.format("delta")
        .mode("append")
        .option("txnAppId", txn_app_id)
        .option("txnVersion", txn_version)
        .saveAsTable(fact_name)
    )

after = spark.table(fact_name).selectExpr(
    "count(*) AS row_count",
    "count(DISTINCT SalesLineKey) AS distinct_keys",
    "min(SalesLineKey) AS min_key",
    "max(SalesLineKey) AS max_key",
).first()
after_state = (
    after["row_count"],
    after["distinct_keys"],
    after["min_key"],
    after["max_key"],
)
if after_state != appended_state:
    raise RuntimeError(
        f"Post-append check failed: expected {appended_state}, observed {after_state}."
    )
print(
    f"Verified the deterministic batch in {fact_name} with Delta transaction "
    f"identity {txn_app_id!r}/{txn_version}."
)
print("Verify the mirror separately; no Fabric query was performed by this notebook.")

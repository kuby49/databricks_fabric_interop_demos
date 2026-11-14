# Databricks notebook source
# This notebook is prepared for manual execution only.

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
    generate_star_schema,
    validate_star_schema,
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
tables = generate_star_schema(star_config)
validate_star_schema(tables, star_config)

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


schemas = {
    table_name: StructType(
        [
            StructField(column_name, spark_type(type_name), False)
            for column_name, type_name in columns
        ]
    )
    for table_name, columns in TABLE_COLUMN_TYPES.items()
}

# COMMAND ----------

for table_name, rows in tables.items():
    full_name = f"{target['catalog']}.{target['schema']}.{table_name}"
    dataframe = spark.createDataFrame(rows, schema=schemas[table_name])
    (
        dataframe.write.format("delta")
        .mode("error")
        .saveAsTable(full_name)
    )
    print(f"Created {full_name}: {len(rows)} rows")

# COMMAND ----------

for statement in demo_config["primary_key_not_null_statements"](target):
    spark.sql(statement)

for statement in demo_config["constraint_statements"](target):
    spark.sql(statement)

expected_constraint_names = {
    "DimDate": ("pk_dimdate",),
    "DimProduct": ("pk_dimproduct",),
    "DimCustomer": ("pk_dimcustomer",),
    "FactSales": (
        "pk_factsales",
        "fk_factsales_dimdate",
        "fk_factsales_dimproduct",
        "fk_factsales_dimcustomer",
    ),
}
for table_name, names in expected_constraint_names.items():
    full_name = f"{target['catalog']}.{target['schema']}.{table_name}"
    definition = spark.sql(f"SHOW CREATE TABLE {full_name}").first()[0]
    missing = [name for name in names if name.lower() not in definition.lower()]
    if missing:
        raise RuntimeError(
            f"UC constraints were not confirmed in SHOW CREATE TABLE for "
            f"{full_name}: {', '.join(missing)}"
        )
    print(f"Confirmed UC constraints for {full_name}: {', '.join(names)}")

# Do not change to overwrite or IF NOT EXISTS. Existing objects are a stop condition.
print("Initial demo tables created. No Fabric mirror was created or queried by this notebook.")

"""Observe the pilot's modern source through a mirror-based Lakehouse shortcut."""

import json
import platform
from datetime import datetime, timezone

from notebookutils import mssparkutils

spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")
observations = {
    "observed_at_utc": datetime.now(timezone.utc).isoformat(),
    "spark_version": spark.version,
    "python_version": platform.python_version(),
    "table": "fabricinterop_cc_validation.pilot_dimdate",
    "path_role": "compatibility-enabled managed source, not generated output or normal control",
}

try:
    tables = spark.catalog.listTables("fabricinterop_cc_validation")
    observations["listing"] = {
        "status": "succeeded",
        "pilot_dimdate_visible": any(table.name == "pilot_dimdate" for table in tables),
    }
except Exception as error:
    observations["listing"] = {
        "status": "failed",
        "error_type": type(error).__name__,
        "error": str(error),
    }

try:
    count = spark.sql(
        "SELECT COUNT(*) AS row_count FROM fabricinterop_cc_validation.pilot_dimdate"
    ).first()["row_count"]
    observations["query"] = {"status": "succeeded", "row_count": count}
except Exception as error:
    observations["query"] = {
        "status": "failed",
        "error_type": type(error).__name__,
        "error": str(error),
    }

mssparkutils.notebook.exit(json.dumps(observations))

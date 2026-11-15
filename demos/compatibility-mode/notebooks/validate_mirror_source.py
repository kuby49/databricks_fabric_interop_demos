"""Optional, cost-approved mirror-based read; inspect resolved targets separately."""

import json
import platform
from datetime import datetime, timezone

from notebookutils import mssparkutils

spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")
observations = {
    "observed_at_utc": datetime.now(timezone.utc).isoformat(),
    "spark_version": spark.version,
    "python_version": platform.python_version(),
    "table": "fabricinterop_cc_compat_validation.normal_dimdate",
    "path_role": "mirror-based shortcut; target may redirect after Compatibility Mode enablement",
}

try:
    tables = spark.catalog.listTables("fabricinterop_cc_compat_validation")
    observations["listing"] = {
        "status": "succeeded",
        "normal_dimdate_visible": any(table.name == "normal_dimdate" for table in tables),
    }
except Exception as error:
    observations["listing"] = {
        "status": "failed",
        "error_type": type(error).__name__,
        "error": str(error),
    }

try:
    count = spark.sql(
        "SELECT COUNT(*) AS row_count FROM fabricinterop_cc_compat_validation.normal_dimdate"
    ).first()["row_count"]
    observations["query"] = {"status": "succeeded", "row_count": count}
except Exception as error:
    observations["query"] = {
        "status": "failed",
        "error_type": type(error).__name__,
        "error": str(error),
    }

mssparkutils.notebook.exit(json.dumps(observations))

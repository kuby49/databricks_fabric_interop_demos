"""Read the populated compatibility canary through its Lakehouse shortcut."""

import json
import platform
from datetime import datetime, timezone

from notebookutils import mssparkutils

spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")
observations = {
    "observed_at_utc": datetime.now(timezone.utc).isoformat(),
    "spark_version": spark.version,
    "python_version": platform.python_version(),
    "table": "fabricinterop_cc_validation.canary_compat",
    "path_role": "generated compatibility output via ADLS shortcut, not a mirrored catalog",
}

try:
    rows = spark.sql(
        "SELECT id, label FROM fabricinterop_cc_validation.canary_compat ORDER BY id"
    ).collect()
    observations["query"] = {
        "status": "succeeded",
        "rows": [row.asDict() for row in rows],
    }
except Exception as error:
    observations["query"] = {
        "status": "failed",
        "error_type": type(error).__name__,
        "error": str(error),
    }

try:
    rows = spark.sql(
        "SELECT label, row_count FROM fabricinterop_cc_validation.canary_mv_compat "
        "ORDER BY label"
    ).collect()
    observations["mv_query"] = {
        "status": "succeeded",
        "rows": [row.asDict() for row in rows],
    }
except Exception as error:
    observations["mv_query"] = {
        "status": "failed",
        "error_type": type(error).__name__,
        "error": str(error),
    }

mssparkutils.notebook.exit(json.dumps(observations))

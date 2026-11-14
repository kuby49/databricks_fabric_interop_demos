"""Generate, validate, and export the deterministic OneLake demo data locally."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import sys
from typing import Any

from demo_config import append_rows, load_config, star_schema_config
from demos.common.star_schema import (
    generate_sales_batch,
    generate_star_schema,
    json_safe,
    validate_star_schema,
)


def write_table_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"refusing to write an empty table: {path.name}")
    with path.open("x", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json_safe(value) for key, value in row.items()})


def prepare(config: dict[str, Any], output_dir: str | Path) -> dict[str, Any]:
    data_config = star_schema_config(config)
    batch_size = append_rows(config)
    tables = generate_star_schema(data_config)
    base_metrics = validate_star_schema(tables, data_config)

    batch = generate_sales_batch(
        data_config,
        first_sales_line_key=data_config.sales_rows + 1,
        row_count=batch_size,
    )
    combined = {name: list(rows) for name, rows in tables.items()}
    combined["FactSales"].extend(batch)
    combined_metrics = validate_star_schema(
        combined,
        data_config,
        expected_sales_rows=data_config.sales_rows + batch_size,
    )

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=False)
    for name, rows in tables.items():
        write_table_csv(output / f"{name}.csv", rows)
    write_table_csv(output / "FactSales_append.csv", batch)
    metrics = {
        "status": "locally_generated_and_validated",
        "deployed": False,
        "config": {
            "date_rows": data_config.date_rows,
            "product_rows": data_config.product_rows,
            "customer_rows": data_config.customer_rows,
            "sales_rows": data_config.sales_rows,
            "append_rows": batch_size,
            "seed": data_config.seed,
            "start_date": data_config.start_date.isoformat(),
        },
        "base_validation": json_safe(base_metrics),
        "combined_validation": json_safe(combined_metrics),
    }
    with (output / "validation.json").open("x", encoding="utf-8") as file:
        json.dump(metrics, file, indent=2)
        file.write("\n")
    return metrics


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        required=True,
        help="the same local config.json used by both Databricks notebooks",
    )
    parser.add_argument("--output-dir", required=True, help="local output directory")
    args = parser.parse_args(argv)
    try:
        result = prepare(load_config(args.config), args.output_dir)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Data preparation failed: {error}", file=sys.stderr)
        return 2
    print(
        f"Local data prepared and validated; no deployment performed. "
        f"Output: {Path(args.output_dir)}"
    )
    print(json.dumps(result["combined_validation"]["row_counts"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

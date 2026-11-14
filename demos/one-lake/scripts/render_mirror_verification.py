"""Render offline expected-versus-mirrored checks from the deployment config."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
import sys
from typing import Any

from demo_config import (
    append_rows,
    approved_target,
    load_config,
    star_schema_config,
)
from demos.common.star_schema import generate_sales_batch, generate_star_schema


def visibility_outcome(
    event_utc: datetime | None,
    observed_utc: datetime,
    timeout_seconds: int,
    mismatch_count: int,
) -> str:
    if timeout_seconds <= 0 or mismatch_count < 0:
        raise ValueError("timeout must be positive and mismatch_count non-negative")
    if event_utc is None or event_utc > observed_utc:
        return "BLOCKED"
    deadline_utc = event_utc + timedelta(seconds=timeout_seconds)
    if mismatch_count == 0 and observed_utc <= deadline_utc:
        return "PASS"
    if observed_utc >= deadline_utc:
        return "FAIL"
    return "BLOCKED"


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _values(rows: list[tuple[Any, ...]]) -> str:
    return ",\n        ".join("(" + ", ".join(row) + ")" for row in rows)


def _expected_data(config: dict[str, Any], phase: str) -> dict[str, Any]:
    star_config = star_schema_config(config)
    tables = generate_star_schema(star_config)
    if phase == "after":
        tables["FactSales"].extend(
            generate_sales_batch(
                star_config,
                first_sales_line_key=star_config.sales_rows + 1,
                row_count=append_rows(config),
            )
        )

    date_by_key = {
        row["DateKey"]: row["Date"].isoformat() for row in tables["DimDate"]
    }
    product_by_key = {
        row["ProductKey"]: row["Category"] for row in tables["DimProduct"]
    }
    customer_by_key = {
        row["CustomerKey"]: row["Region"] for row in tables["DimCustomer"]
    }
    by_date: dict[int, list[Any]] = {
        key: [value, 0, Decimal("0.00")] for key, value in date_by_key.items()
    }
    by_category: dict[str, list[Any]] = {
        row["Category"]: [0, Decimal("0.00")] for row in tables["DimProduct"]
    }
    by_region: dict[str, list[Any]] = {
        row["Region"]: [0, Decimal("0.00")] for row in tables["DimCustomer"]
    }
    total = Decimal("0.00")
    for row in tables["FactSales"]:
        amount = row["SalesAmount"]
        date_group = by_date[row["DateKey"]]
        date_group[1] += 1
        date_group[2] += amount
        category_group = by_category[product_by_key[row["ProductKey"]]]
        category_group[0] += 1
        category_group[1] += amount
        region_group = by_region[customer_by_key[row["CustomerKey"]]]
        region_group[0] += 1
        region_group[1] += amount
        total += amount

    def decimal_sql(value: Decimal) -> str:
        return f"CAST({format(value, '.2f')} AS DECIMAL(19,2))"

    date_values = [
        (
            f"CAST({key} AS INT)",
            f"CAST({_literal(bucket)} AS NVARCHAR(30))",
            f"CAST({row_count} AS BIGINT)",
            decimal_sql(sales_total),
        )
        for key, (bucket, row_count, sales_total) in sorted(by_date.items())
    ]
    category_values = [
        (
            f"CAST({_literal(category)} AS NVARCHAR(100))",
            f"CAST({row_count} AS BIGINT)",
            decimal_sql(sales_total),
        )
        for category, (row_count, sales_total) in sorted(by_category.items())
    ]
    region_values = [
        (
            f"CAST({_literal(region)} AS NVARCHAR(100))",
            f"CAST({row_count} AS BIGINT)",
            decimal_sql(sales_total),
        )
        for region, (row_count, sales_total) in sorted(by_region.items())
    ]
    return {
        "row_count": len(tables["FactSales"]),
        "sales_total": total,
        "date_values": date_values,
        "category_values": category_values,
        "region_values": region_values,
    }


def render(config: dict[str, Any], phase: str) -> str:
    if phase not in ("before", "after"):
        raise ValueError("phase must be 'before' or 'after'")
    approved_target(config)
    timeout = config.get("deployment", {}).get("verification_timeout_seconds")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError("deployment.verification_timeout_seconds must be a positive integer")
    expected = _expected_data(config, phase)
    expected_count_sql = f"CAST({expected['row_count']} AS BIGINT)"
    expected_total_sql = (
        f"CAST({format(expected['sales_total'], '.2f')} AS DECIMAL(19,2))"
    )

    return f"""-- Offline-generated expected-vs-actual check for phase: {phase}.
-- Run only against the Fabric mirrored catalog SQL analytics endpoint.
-- Replace <mirror_schema> using the schema shown by that endpoint.
-- Set @VisibilityEventUtc to the actual UTC time the source write completed.
-- PASS means exact expected values were observed by the deadline; it does not prove first visibility time.
DECLARE @Phase NVARCHAR(10) = N'{phase}';
DECLARE @VisibilityEventUtc DATETIME2(7) = NULL; -- Set the source event time in UTC.
DECLARE @VisibilityTimeoutSeconds INT = {timeout};
DECLARE @ObservedUtc DATETIME2(7) = SYSUTCDATETIME();
DECLARE @DeadlineUtc DATETIME2(7) =
    CASE WHEN @VisibilityEventUtc IS NULL THEN NULL
         ELSE DATEADD(SECOND, @VisibilityTimeoutSeconds, @VisibilityEventUtc) END;

;WITH ExpectedDate AS (
    SELECT v.DateKey, v.Bucket, v.RowCount, v.SalesTotal
    FROM (VALUES
        {_values(expected["date_values"])}
    ) AS v(DateKey, Bucket, RowCount, SalesTotal)
),
ActualDate AS (
    SELECT
        d.DateKey,
        CONVERT(NVARCHAR(30), d.[Date], 23) AS Bucket,
        COUNT_BIG(f.SalesLineKey) AS RowCount,
        COALESCE(SUM(CAST(f.SalesAmount AS DECIMAL(19,2))),
                 CAST(0 AS DECIMAL(19,2))) AS SalesTotal
    FROM [<mirror_schema>].[DimDate] AS d
    LEFT JOIN [<mirror_schema>].[FactSales] AS f ON f.DateKey = d.DateKey
    GROUP BY d.DateKey, d.[Date]
),
ExpectedCategory AS (
    SELECT v.Bucket, v.RowCount, v.SalesTotal
    FROM (VALUES
        {_values(expected["category_values"])}
    ) AS v(Bucket, RowCount, SalesTotal)
),
ActualCategory AS (
    SELECT
        p.Category AS Bucket,
        COUNT_BIG(f.SalesLineKey) AS RowCount,
        COALESCE(SUM(CAST(f.SalesAmount AS DECIMAL(19,2))),
                 CAST(0 AS DECIMAL(19,2))) AS SalesTotal
    FROM [<mirror_schema>].[DimProduct] AS p
    LEFT JOIN [<mirror_schema>].[FactSales] AS f ON f.ProductKey = p.ProductKey
    GROUP BY p.Category
),
ExpectedRegion AS (
    SELECT v.Bucket, v.RowCount, v.SalesTotal
    FROM (VALUES
        {_values(expected["region_values"])}
    ) AS v(Bucket, RowCount, SalesTotal)
),
ActualRegion AS (
    SELECT
        c.Region AS Bucket,
        COUNT_BIG(f.SalesLineKey) AS RowCount,
        COALESCE(SUM(CAST(f.SalesAmount AS DECIMAL(19,2))),
                 CAST(0 AS DECIMAL(19,2))) AS SalesTotal
    FROM [<mirror_schema>].[DimCustomer] AS c
    LEFT JOIN [<mirror_schema>].[FactSales] AS f ON f.CustomerKey = c.CustomerKey
    GROUP BY c.Region
),
ExpectedTotal AS (
    SELECT {expected_count_sql} AS RowCount, {expected_total_sql} AS SalesTotal
),
ActualTotal AS (
    SELECT COUNT_BIG(*) AS RowCount,
           COALESCE(SUM(CAST(SalesAmount AS DECIMAL(19,2))),
                    CAST(0 AS DECIMAL(19,2))) AS SalesTotal
    FROM [<mirror_schema>].[FactSales]
),
Checks AS (
    SELECT
        CAST('TOTAL' AS NVARCHAR(20)) AS CheckName,
        CAST('all FactSales rows' AS NVARCHAR(100)) AS Bucket,
        e.RowCount AS ExpectedRowCount,
        a.RowCount AS ActualRowCount,
        e.SalesTotal AS ExpectedSalesTotal,
        a.SalesTotal AS ActualSalesTotal,
        CASE WHEN e.RowCount <> a.RowCount OR e.SalesTotal <> a.SalesTotal
             THEN 1 ELSE 0 END AS IsMismatch
    FROM ExpectedTotal AS e CROSS JOIN ActualTotal AS a
    UNION ALL
    SELECT 'DATE', COALESCE(e.Bucket, a.Bucket),
           e.RowCount, a.RowCount, e.SalesTotal, a.SalesTotal,
           CASE WHEN e.DateKey IS NULL OR a.DateKey IS NULL
                      OR e.Bucket <> a.Bucket
                      OR e.RowCount <> a.RowCount OR e.SalesTotal <> a.SalesTotal
                THEN 1 ELSE 0 END
    FROM ExpectedDate AS e
    FULL OUTER JOIN ActualDate AS a ON e.DateKey = a.DateKey
    UNION ALL
    SELECT 'CATEGORY', COALESCE(e.Bucket, a.Bucket),
           e.RowCount, a.RowCount, e.SalesTotal, a.SalesTotal,
           CASE WHEN e.Bucket IS NULL OR a.Bucket IS NULL
                      OR e.RowCount <> a.RowCount OR e.SalesTotal <> a.SalesTotal
                THEN 1 ELSE 0 END
    FROM ExpectedCategory AS e
    FULL OUTER JOIN ActualCategory AS a ON e.Bucket = a.Bucket
    UNION ALL
    SELECT 'REGION', COALESCE(e.Bucket, a.Bucket),
           e.RowCount, a.RowCount, e.SalesTotal, a.SalesTotal,
           CASE WHEN e.Bucket IS NULL OR a.Bucket IS NULL
                      OR e.RowCount <> a.RowCount OR e.SalesTotal <> a.SalesTotal
                THEN 1 ELSE 0 END
    FROM ExpectedRegion AS e
    FULL OUTER JOIN ActualRegion AS a ON e.Bucket = a.Bucket
),
MismatchCount AS (
    SELECT COUNT_BIG(*) AS Value FROM Checks WHERE IsMismatch = 1
)
SELECT
    CAST('MISMATCH' AS NVARCHAR(10)) AS ResultType,
    c.CheckName,
    c.Bucket,
    c.ExpectedRowCount,
    c.ActualRowCount,
    c.ExpectedSalesTotal,
    c.ActualSalesTotal,
    m.Value AS MismatchCount,
    CASE WHEN @VisibilityEventUtc IS NULL
                   OR @VisibilityEventUtc > @ObservedUtc THEN 'BLOCKED'
         WHEN @ObservedUtc >= @DeadlineUtc THEN 'FAIL'
         ELSE 'BLOCKED' END AS Outcome,
    @VisibilityEventUtc AS VisibilityEventUtc,
    @DeadlineUtc AS VisibilityDeadlineUtc,
    @ObservedUtc AS ObservedUtc
FROM Checks AS c
CROSS JOIN MismatchCount AS m
WHERE c.IsMismatch = 1
UNION ALL
SELECT
    'SUMMARY',
    'ALL',
    @Phase,
    c.ExpectedRowCount,
    c.ActualRowCount,
    c.ExpectedSalesTotal,
    c.ActualSalesTotal,
    m.Value,
    CASE WHEN @VisibilityEventUtc IS NULL
                   OR @VisibilityEventUtc > @ObservedUtc THEN 'BLOCKED'
         WHEN m.Value = 0 AND @ObservedUtc <= @DeadlineUtc THEN 'PASS'
         WHEN @ObservedUtc >= @DeadlineUtc THEN 'FAIL'
         ELSE 'BLOCKED' END,
    @VisibilityEventUtc,
    @DeadlineUtc,
    @ObservedUtc
FROM Checks AS c
CROSS JOIN MismatchCount AS m
WHERE c.CheckName = 'TOTAL' AND m.Value = 0
ORDER BY ResultType, CheckName, Bucket;
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="the deployment config.json")
    parser.add_argument("--phase", choices=("before", "after"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        sql = render(load_config(args.config), args.phase)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x", encoding="utf-8") as file:
            file.write(sql)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Refusing to render mirror verification SQL: {error}", file=sys.stderr)
        return 2
    print(f"Offline mirror verification SQL written to new file: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import csv
from contextlib import closing
from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import re
import runpy
import sqlite3
import subprocess
import sys
import tempfile
import unittest


DEMO = Path(__file__).resolve().parent
TOOLS = runpy.run_path(str(DEMO / "prepare.py"))
COMPARISON = runpy.run_path(str(DEMO / "compare_aggregate.py"))
from demos.common.star_schema import FOREIGN_KEYS, PRIMARY_KEYS, TABLE_COLUMN_TYPES, generate_star_schema


class CompatibilityLocalTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((DEMO / "config.example.json").read_text(encoding="utf-8"))

    def test_aggregate_combines_customers_at_requested_grain(self):
        tables = {
            "DimDate": [{"DateKey": 20240101, "Date": date(2024, 1, 1), "CalendarYear": 2024,
                         "CalendarMonth": 1, "MonthName": "January", "DayOfMonth": 1,
                         "IsWeekend": False}],
            "DimProduct": [{"ProductKey": 1, "ProductName": "Widget", "Category": "Tools"}],
            "DimCustomer": [{"CustomerKey": 1, "Region": "East", "Segment": "Consumer"},
                            {"CustomerKey": 2, "Region": "East", "Segment": "Consumer"},
                            {"CustomerKey": 3, "Region": "West", "Segment": "Consumer"}],
            "FactSales": [
                {"DateKey": 20240101, "ProductKey": 1, "CustomerKey": 1,
                 "Quantity": 2, "SalesAmount": Decimal("4.25")},
                {"DateKey": 20240101, "ProductKey": 1, "CustomerKey": 2,
                 "Quantity": 3, "SalesAmount": Decimal("7.50")},
                {"DateKey": 20240101, "ProductKey": 1, "CustomerKey": 3,
                 "Quantity": 1, "SalesAmount": Decimal("2.50")},
            ],
        }
        groups = TOOLS["aggregate"](tables)
        self.assertEqual(len(groups), 2)
        self.assertEqual(groups[0]["TotalSales"], Decimal("11.75"))
        self.assertEqual((groups[0]["TotalQuantity"], groups[0]["SalesLineCount"]), (5, 2))
        self.assertNotIn("CustomerKey", groups[0])
        self.assertEqual(groups[0]["ProductName"], "Widget")

    def test_producer_tables_match_shared_schema_and_keep_destinations_distinct(self):
        sql = (DEMO / "sql" / "producer-template.sql").read_text(encoding="utf-8")
        blocks = re.findall(
            r"CREATE TABLE \{\{catalog\}\}\.\{\{schema\}\}\.(\w+) \(\n"
            r"(.*?)\n\) USING DELTA TBLPROPERTIES \('delta.feature.catalogManaged' = 'supported'\);",
            sql, re.S,
        )
        self.assertEqual(len(blocks), len(TABLE_COLUMN_TYPES))
        for name, body in blocks:
            columns = []
            for line in body.splitlines():
                match = re.fullmatch(r"\s*`?(\w+)`?\s+([A-Z]+(?:\(\d+,\d+\))?)(?: NOT NULL)?,?", line)
                self.assertIsNotNone(match, line)
                columns.append(match.groups())
            self.assertEqual(tuple(columns), TABLE_COLUMN_TYPES[name])
            required = set(PRIMARY_KEYS[name]) | {
                column for column, _, _ in FOREIGN_KEYS.get(name, ())
            }
            not_null = {line.strip().split()[0] for line in body.splitlines()
                        if " NOT NULL" in line}
            self.assertEqual(not_null, required)
            primary = ", ".join(PRIMARY_KEYS[name])
            self.assertRegex(
                sql, rf"ALTER TABLE \{{\{{catalog\}}\}}\.\{{\{{schema\}}\}}\.{name}"
                rf"\s+ADD CONSTRAINT \w+ PRIMARY KEY \({primary}\);"
            )
            for column, target, target_column in FOREIGN_KEYS.get(name, ()):
                self.assertRegex(
                    sql, rf"ALTER TABLE \{{\{{catalog\}}\}}\.\{{\{{schema\}}\}}\.{name}"
                    rf"\s+ADD CONSTRAINT \w+ FOREIGN KEY \({column}\)\s+REFERENCES "
                    rf"\{{\{{catalog\}}\}}\.\{{\{{schema\}}\}}\.{target} \({target_column}\);"
                )

        alterations = re.findall(
            r"ALTER TABLE \{\{catalog\}\}\.\{\{schema\}\}\.(\w+)"
            r" SET TBLPROPERTIES \((.*?)\);", sql, re.S,
        )
        self.assertEqual({name for name, _ in alterations}, set(TABLE_COLUMN_TYPES))
        destinations = []
        for _, properties in alterations:
            self.assertIn("'delta.universalFormat.enabledFormats' = 'compatibility'", properties)
            destinations.extend(re.findall(r"\{\{unique_empty_\w+_destination\}\}", properties))
        self.assertEqual(len(destinations), 4)
        self.assertEqual(len(set(destinations)), 4)

    def test_single_mv_query_matches_complete_baseline_reference(self):
        sql = (DEMO / "sql" / "producer-template.sql").read_text(encoding="utf-8")
        definitions = re.findall(
            r"CREATE MATERIALIZED VIEW .*?TBLPROPERTIES\s*\((.*?)\)\s*AS\s+(SELECT.*?);",
            sql, re.S,
        )
        self.assertEqual(len(definitions), 1)
        self.assertIn("'delta.feature.catalogManaged' = 'supported'", definitions[0][0])
        self.assertIn("'delta.universalFormat.enabledFormats' = 'compatibility'", definitions[0][0])
        self.assertIn("{{unique_empty_mv_destination}}", definitions[0][0])

        data = generate_star_schema()
        reference = TOOLS["aggregate"](data)

        def sqlite_value(value):
            if isinstance(value, Decimal):
                return int(value * 100)
            if isinstance(value, date):
                return value.isoformat()
            if isinstance(value, bool):
                return int(value)
            return value

        columns = list(reference[0])
        expected = sorted(tuple(sqlite_value(group[column]) for column in columns)
                          for group in reference)
        with closing(sqlite3.connect(":memory:")) as connection:
            for name, table_columns in TABLE_COLUMN_TYPES.items():
                names = [column for column, _ in table_columns]
                connection.execute(f"CREATE TABLE {name} ({', '.join(names)})")
                placeholders = ", ".join("?" for _ in names)
                connection.executemany(
                    f"INSERT INTO {name} VALUES ({placeholders})",
                    [tuple(sqlite_value(row[column]) for column in names) for row in data[name]],
                )
            for _, query in definitions:
                query = re.sub(r"\{\{catalog\}\}\.\{\{schema\}\}\.", "", query)
                cursor = connection.execute(query)
                self.assertEqual([column[0] for column in cursor.description], columns)
                self.assertEqual(sorted(cursor.fetchall()), expected)

    def test_baseline_reference_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "prepared"
            result = TOOLS["prepare"](self.config, output)
            self.assertEqual(result["row_counts"], {
                "DimDate": 365, "DimProduct": 20, "DimCustomer": 100, "FactSales": 10000
            })
            self.assertEqual(result["sales_total"], Decimal("930299.36"))
            self.assertEqual(result["sales_total"], result["aggregate_sales_total"])
            self.assertEqual(result["quantity_total"], result["aggregate_quantity_total"])
            self.assertEqual(result["aggregate_line_count"], 10000)
            self.assertFalse(result["deployed"])
            with (output / "SalesAggregate.csv").open(newline="", encoding="utf-8") as file:
                self.assertEqual(sum(1 for _ in csv.DictReader(file)), result["aggregate_group_count"])
            with self.assertRaises(FileExistsError):
                TOOLS["prepare"](self.config, output)

    def test_paths_require_approval_ids_and_adls_outputs(self):
        with self.assertRaisesRegex(ValueError, "approved"):
            TOOLS["paths"](self.config)
        self.config["databricks"]["parent_catalog_approved"] = True
        with self.assertRaisesRegex(ValueError, "GUID"):
            TOOLS["paths"](self.config)
        self.config["fabric"].update(
            workspace_id="00000000-0000-0000-0000-000000000001",
            normal_storage_id="00000000-0000-0000-0000-000000000002",
        )
        result = TOOLS["paths"](self.config)
        destinations = result["compatibility_destinations"]
        self.assertEqual(len(set(destinations.values())), 5)
        self.assertTrue(all(".dfs.core.windows.net/compatibility/" in item for item in destinations.values()))
        self.assertTrue(result["normal_managed"].endswith("/Files/managed"))
        self.config["fabric"]["compat_storage_id"] = "00000000-0000-0000-0000-000000000003"
        with self.assertRaisesRegex(ValueError, "legacy"):
            TOOLS["paths"](self.config)

    def test_result_classification_does_not_invent_negative_controls(self):
        classify = TOOLS["classify"]
        evidence = dict(ready=True, authorized=True, selection_verified=True,
                        source_catalog_managed=True, resolved_target_verified=True,
                        resolved_target_catalog_managed=True)
        self.assertEqual(classify("normal", "spark", "feature_exclusion", evidence),
                         "EXPECTED_LIMITATION_OBSERVED")
        for result in ("permission", "timeout", "absent", "other"):
            self.assertEqual(classify("normal", "spark", result, evidence), "BLOCKED")
        self.assertEqual(classify("normal", "sql", "feature_exclusion", evidence), "BLOCKED")
        evidence["data_matches"] = True
        self.assertEqual(classify("normal", "spark", "success", evidence), "OBSERVED_SUCCESS")
        self.assertEqual(classify("compatibility", "sql", "success", evidence), "BLOCKED")
        evidence.update(output_location_verified=True, output_no_catalog_managed=True)
        self.assertEqual(classify("compatibility", "sql", "success", evidence), "PASS")
        evidence["data_matches"] = False
        self.assertEqual(classify("compatibility", "spark", "success", evidence), "FAIL")

    def test_comparison_detects_mismatches_and_duplicate_keys(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "prepared"
            TOOLS["prepare"](self.config, output)
            reference = output / "SalesAggregate.csv"
            with reference.open(newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                columns, rows = reader.fieldnames, list(reader)
            actual = Path(tmp) / "actual.csv"

            def write_actual():
                with actual.open("w", newline="", encoding="utf-8") as file:
                    writer = csv.DictWriter(file, fieldnames=columns)
                    writer.writeheader()
                    writer.writerows(rows)

            write_actual()
            self.assertEqual(COMPARISON["compare"](reference, actual)["status"], "MATCH")
            rows[0]["TotalSales"] = "0.00"
            rows.pop()
            write_actual()
            result = COMPARISON["compare"](reference, actual)
            self.assertEqual((result["status"], result["different_groups"], result["missing_groups"]),
                             ("MISMATCH", 1, 1))
            rows.append(rows[0])
            write_actual()
            with self.assertRaisesRegex(ValueError, "duplicate aggregate key"):
                COMPARISON["compare"](reference, actual)

    def test_offline_cli_prepares_and_compares_without_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "prepared"
            command = [sys.executable, str(DEMO / "prepare.py"), "--config",
                       str(DEMO / "config.example.json"), "--output-dir", str(output)]
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 0)
            self.assertEqual(subprocess.run(command, capture_output=True).returncode, 2)
            comparison = subprocess.run(
                [sys.executable, str(DEMO / "compare_aggregate.py"), "--reference",
                 str(output / "SalesAggregate.csv"), "--actual", str(output / "SalesAggregate.csv")],
                capture_output=True, text=True,
            )
            self.assertEqual(comparison.returncode, 0)
            self.assertEqual(json.loads(comparison.stdout)["status"], "MATCH")


if __name__ == "__main__":
    unittest.main()

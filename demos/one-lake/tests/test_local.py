from contextlib import redirect_stderr, redirect_stdout
import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import runpy
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from demos.common.star_schema import (  # noqa: E402
    StarSchemaConfig,
    generate_sales_batch,
    generate_star_schema,
    validate_star_schema,
)

DEMO = ROOT / "demos" / "one-lake"
sys.path.insert(0, str(DEMO / "scripts"))
SCRIPT_VARS = runpy.run_path(str(DEMO / "scripts" / "demo_config.py"))
PREPARE_VARS = runpy.run_path(str(DEMO / "scripts" / "prepare_data.py"))
SQL_VARS = runpy.run_path(str(DEMO / "scripts" / "render_schema_sql.py"))
MIRROR_VARS = runpy.run_path(
    str(DEMO / "scripts" / "render_mirror_verification.py")
)
CLEANUP_VARS = runpy.run_path(str(DEMO / "scripts" / "cleanup_plan.py"))
NOTEBOOK_PATHS = runpy.run_path(str(DEMO / "scripts" / "notebook_paths.py"))


class OneLakeLocalChecks(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((DEMO / "config.example.json").read_text(encoding="utf-8"))
        self.config["deployment"]["sample_data"].update(
            date_rows=10, product_rows=4, customer_rows=6, sales_rows=10, append_rows=3
        )
        self.temp_directory = tempfile.TemporaryDirectory()
        self.output_dir = Path(self.temp_directory.name) / "generated"

    def tearDown(self):
        self.temp_directory.cleanup()

    def test_base_and_deterministic_append_validate(self):
        config = StarSchemaConfig(
            date_rows=10, product_rows=4, customer_rows=6, sales_rows=10, seed=42
        )
        base = generate_star_schema(config)
        batch = generate_sales_batch(config, first_sales_line_key=11, row_count=3)
        combined = {name: list(rows) for name, rows in base.items()}
        combined["FactSales"].extend(batch)
        metrics = validate_star_schema(combined, config, expected_sales_rows=13)
        self.assertEqual(metrics["row_counts"]["FactSales"], 13)
        self.assertEqual([row["SalesLineKey"] for row in batch], [11, 12, 13])
        self.assertEqual(batch, generate_sales_batch(config, 11, 3))

    def test_prepare_data_reports_local_validation_only(self):
        result = PREPARE_VARS["prepare"](self.config, self.output_dir)
        self.assertFalse(result["deployed"])
        self.assertEqual(result["combined_validation"]["row_counts"]["FactSales"], 13)
        self.assertTrue((self.output_dir / "FactSales_append.csv").exists())

    def test_prepare_data_refuses_existing_output_without_changing_files(self):
        self.output_dir.mkdir()
        sentinel = self.output_dir / "user-data.txt"
        sentinel.write_text("preserve", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            PREPARE_VARS["prepare"](self.config, self.output_dir)
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "preserve")
        self.assertEqual(list(self.output_dir.iterdir()), [sentinel])

    def test_prepare_data_requires_explicit_deployment_config(self):
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                PREPARE_VARS["main"](["--output-dir", str(self.output_dir)])
        self.assertEqual(error.exception.code, 2)

    def test_catalog_gate_requires_value_and_explicit_approval(self):
        with self.assertRaisesRegex(ValueError, "parent_catalog"):
            SCRIPT_VARS["approved_target"](self.config)
        self.config["databricks"]["parent_catalog"] = "demo_catalog"
        with self.assertRaisesRegex(ValueError, "approved"):
            SCRIPT_VARS["approved_target"](self.config)
        self.config["databricks"]["parent_catalog_approved"] = True
        self.config["fabric"].update(
            workspace_id="00000000-0000-0000-0000-000000000001",
            databricks_storage_id="00000000-0000-0000-0000-000000000002",
        )
        target = SCRIPT_VARS["approved_target"](self.config)
        self.assertEqual(
            target["managed_location"],
            "abfss://00000000-0000-0000-0000-000000000001@"
            "onelake.dfs.fabric.microsoft.com/"
            "00000000-0000-0000-0000-000000000002/Files/",
        )

    def test_onelake_ids_must_be_guid_values(self):
        self.config["databricks"].update(
            parent_catalog="demo_catalog", parent_catalog_approved=True
        )
        self.config["fabric"].update(
            workspace_id="workspace-123", databricks_storage_id="storage-456"
        )
        with self.assertRaisesRegex(ValueError, "must be a GUID"):
            SCRIPT_VARS["approved_target"](self.config)

    def test_schema_sql_is_create_only_and_never_uses_default_catalog(self):
        with self.assertRaisesRegex(ValueError, "parent_catalog"):
            SQL_VARS["render"](self.config)
        self.config["databricks"]["parent_catalog"] = "demo_catalog"
        self.config["databricks"]["parent_catalog_approved"] = True
        self.config["fabric"].update(
            workspace_id="00000000-0000-0000-0000-000000000001",
            databricks_storage_id="00000000-0000-0000-0000-000000000002",
        )
        sql = SQL_VARS["render"](self.config)
        self.assertIn("CREATE SCHEMA `demo_catalog`.`fabricinterop_onelake`", sql)
        self.assertNotIn("CREATE SCHEMA IF NOT EXISTS", sql)
        self.assertIn(
            "abfss://00000000-0000-0000-0000-000000000001@"
            "onelake.dfs.fabric.microsoft.com/",
            sql,
        )

    def test_mirror_verification_renders_complete_before_and_after_checks(self):
        self.config["databricks"].update(
            parent_catalog="demo_catalog", parent_catalog_approved=True
        )
        self.config["fabric"].update(
            workspace_id="00000000-0000-0000-0000-000000000001",
            databricks_storage_id="00000000-0000-0000-0000-000000000002",
        )
        before = MIRROR_VARS["render"](self.config, "before")
        after = MIRROR_VARS["render"](self.config, "after")
        for sql in (before, after):
            self.assertIn("ExpectedDate AS", sql)
            self.assertIn("ExpectedCategory AS", sql)
            self.assertIn("ExpectedRegion AS", sql)
            self.assertIn("FULL OUTER JOIN ActualDate", sql)
            self.assertIn("FULL OUTER JOIN ActualCategory", sql)
            self.assertIn("FULL OUTER JOIN ActualRegion", sql)
            self.assertIn("@VisibilityTimeoutSeconds INT = 900", sql)
            self.assertIn("@VisibilityEventUtc DATETIME2(7) = NULL", sql)
            self.assertIn(
                "PASS means exact expected values were observed by the deadline",
                sql,
            )
            self.assertIn("SYSUTCDATETIME()", sql)
            self.assertIn("THEN 'BLOCKED'", sql)
            self.assertIn("WHEN @ObservedUtc >= @DeadlineUtc THEN 'FAIL'", sql)
            self.assertIn(
                "WHEN m.Value = 0 AND @ObservedUtc <= @DeadlineUtc THEN 'PASS'",
                sql,
            )
            self.assertIn("@VisibilityEventUtc > @ObservedUtc", sql)
        self.assertIn("DECLARE @Phase NVARCHAR(10) = N'before'", before)
        self.assertIn("DECLARE @Phase NVARCHAR(10) = N'after'", after)
        self.assertIn("CAST(10 AS BIGINT) AS SalesRowCount", before)
        self.assertIn("CAST(13 AS BIGINT) AS SalesRowCount", after)
        for sql in (before, after):
            self.assertNotRegex(sql, r"\bRowCount\b")
            for table in ("dimdate", "dimproduct", "dimcustomer", "factsales"):
                self.assertIn(f"[<mirror_schema>].[{table}]", sql)

    def test_mirror_visibility_passes_at_deadline_but_not_after(self):
        event = datetime(2026, 5, 1, tzinfo=timezone.utc)
        deadline = event + timedelta(seconds=900)
        outcome = MIRROR_VARS["visibility_outcome"]

        self.assertEqual(outcome(event, deadline, 900, 0), "PASS")
        self.assertEqual(
            outcome(event, deadline + timedelta(seconds=1), 900, 0),
            "FAIL",
        )
        self.assertEqual(outcome(None, event, 900, 0), "BLOCKED")
        self.assertEqual(outcome(event, event, 900, 1), "BLOCKED")
        self.assertEqual(outcome(event, deadline, 900, 1), "FAIL")

    def test_mirror_verification_cli_does_not_overwrite_existing_sql(self):
        self.config["databricks"].update(
            parent_catalog="demo_catalog", parent_catalog_approved=True
        )
        self.config["fabric"].update(
            workspace_id="00000000-0000-0000-0000-000000000001",
            databricks_storage_id="00000000-0000-0000-0000-000000000002",
        )
        config_path = Path(self.temp_directory.name) / "config.json"
        config_path.write_text(json.dumps(self.config), encoding="utf-8")
        output_path = Path(self.temp_directory.name) / "verify_before.sql"
        with redirect_stdout(io.StringIO()):
            self.assertEqual(
                MIRROR_VARS["main"](
                    [
                        "--config",
                        str(config_path),
                        "--phase",
                        "before",
                        "--output",
                        str(output_path),
                    ]
                ),
                0,
            )
        original_sql = output_path.read_text(encoding="utf-8")
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(
                MIRROR_VARS["main"](
                    [
                        "--config",
                        str(config_path),
                        "--phase",
                        "after",
                        "--output",
                        str(output_path),
                    ]
                ),
                2,
            )
        self.assertEqual(output_path.read_text(encoding="utf-8"), original_sql)

    def test_notebook_paths_resolve_only_from_documented_layout(self):
        self.assertEqual(
            NOTEBOOK_PATHS["resolve_repository_root"](
                DEMO / "notebooks"
            ),
            ROOT,
        )
        with self.assertRaisesRegex(ValueError, "repository layout"):
            NOTEBOOK_PATHS["resolve_repository_root"](DEMO)

        for notebook_name in ("01_create_initial_tables.py", "02_append_batch.py"):
            source = (DEMO / "notebooks" / notebook_name).read_text(encoding="utf-8")
            self.assertIn('Path(os.getcwd()).resolve()', source)
            self.assertIn('repository_root / "demos" / "one-lake" / "config.json"', source)

    def test_append_transaction_identity_and_state_gate_are_stable(self):
        self.config["databricks"].update(
            parent_catalog="demo_catalog", parent_catalog_approved=True
        )
        self.config["fabric"].update(
            workspace_id="00000000-0000-0000-0000-000000000001",
            databricks_storage_id="00000000-0000-0000-0000-000000000002",
        )
        identity = SCRIPT_VARS["append_transaction_identity"](self.config)
        self.assertEqual(identity[1], 1)
        self.assertEqual(
            SCRIPT_VARS["append_transaction_identity"](self.config), identity
        )
        self.assertEqual(
            SCRIPT_VARS["classify_append_state"]((10, 10, 1, 10), 10, 3),
            "baseline",
        )
        self.assertEqual(
            SCRIPT_VARS["classify_append_state"]((13, 13, 1, 13), 10, 3),
            "appended",
        )
        with self.assertRaisesRegex(ValueError, "exact baseline"):
            SCRIPT_VARS["classify_append_state"]((11, 11, 1, 11), 10, 3)

        append_notebook = (
            DEMO / "notebooks" / "02_append_batch.py"
        ).read_text(encoding="utf-8")
        self.assertIn('.option("txnAppId", txn_app_id)', append_notebook)
        self.assertIn('.option("txnVersion", txn_version)', append_notebook)
        self.assertIn("actual.exceptAll(expected)", append_notebook)
        self.assertIn("expected.exceptAll(actual)", append_notebook)
        self.assertIn('fact["SalesLineKey"] <= expected_count', append_notebook)

    def test_notebook_declares_and_verifies_uc_keys(self):
        self.config["databricks"].update(
            parent_catalog="demo_catalog", parent_catalog_approved=True
        )
        self.config["fabric"].update(
            workspace_id="00000000-0000-0000-0000-000000000001",
            databricks_storage_id="00000000-0000-0000-0000-000000000002",
        )
        target = SCRIPT_VARS["approved_target"](self.config)
        not_null_statements = SCRIPT_VARS["primary_key_not_null_statements"](
            target
        )
        statements = SCRIPT_VARS["constraint_statements"](target)
        self.assertEqual(len(not_null_statements), 4)
        self.assertIn(
            "ALTER TABLE `demo_catalog`.`fabricinterop_onelake`.`DimDate` "
            "ALTER COLUMN `DateKey` SET NOT NULL",
            not_null_statements,
        )
        self.assertEqual(len(statements), 7)
        self.assertEqual(
            sum("PRIMARY KEY" in statement for statement in statements), 4
        )
        self.assertEqual(
            sum("FOREIGN KEY" in statement for statement in statements), 3
        )
        notebook = (
            DEMO / "notebooks" / "01_create_initial_tables.py"
        ).read_text(encoding="utf-8")
        self.assertIn('.mode("error")', notebook)
        self.assertIn('spark.sql(f"SHOW CREATE TABLE {full_name}")', notebook)
        self.assertIn("UC constraints were not confirmed", notebook)
        self.assertLess(
            notebook.index(".saveAsTable(full_name)"),
            notebook.index('demo_config["primary_key_not_null_statements"]'),
        )
        self.assertLess(
            notebook.index('demo_config["primary_key_not_null_statements"]'),
            notebook.index('demo_config["constraint_statements"]'),
        )

    def test_cleanup_plan_is_manifest_scoped_and_confirmation_is_required(self):
        manifest = json.loads((DEMO / "deployment-manifest.json").read_text(encoding="utf-8"))
        manifest["resource_creation_performed"] = False
        manifest["created_resources"] = []
        self.assertEqual(CLEANUP_VARS["eligible_resources"](manifest), [])
        self.assertEqual(
            CLEANUP_VARS["CONFIRMATION"], "CLEANUP ONLY RESOURCES IN MANIFEST"
        )
        manifest["resource_creation_performed"] = True
        manifest["created_resources"] = [
            {
                "kind": "fabric_connection",
                "name": "fabricinterop_connection",
                "created_by_demo": True,
            },
            {
                "kind": "fabric_workspace",
                "name": "fabricinterop_onelake",
                "created_by_demo": True,
            },
            {
                "kind": "fabric_workspace",
                "name": "unrelated_workspace",
                "created_by_demo": True,
            },
            {
                "kind": "fabric_workspace",
                "name": "fabricinterop_not_proven",
                "created_by_demo": False,
            },
        ]
        self.assertEqual(
            CLEANUP_VARS["eligible_resources"](manifest),
            [
                {"kind": "fabric_connection", "name": "fabricinterop_connection"},
                {"kind": "fabric_workspace", "name": "fabricinterop_onelake"},
            ],
        )


if __name__ == "__main__":
    unittest.main()

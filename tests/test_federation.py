"""Focused offline federation contracts; no cloud credentials or writes."""

import ast
import base64
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from demos.common.star_schema import generate_star_schema, json_safe, StarSchemaConfig
from demos.deployment.config import DEMOS, validate
from demos.deployment.federation import (
    catalog_sql, compare_sql, connection_sql, create, deploy, discover, metadata, notebook, run_notebook,
)
from demos.deployment.safety import DeploymentError, State, Waiting, nonsecret
from demos.deployment.services import Azure, notebook_content
from demos.deployment.workflow import Workflow
from tests.test_deploy import config, SUB, TENANT

WS = "00000000-0000-0000-0000-000000000021"
LH = "00000000-0000-0000-0000-000000000022"


def federation_config():
    cfg = config()
    spec = cfg["demos"].pop("onelake")
    spec.pop("file_events")
    spec.update(catalog="independent_foreign", connection={"mode": "create", "name": "demo_connection"})
    cfg["demos"]["federation"] = spec
    validate(cfg)
    return cfg


class FederationTests(unittest.TestCase):
    def test_arm_auth_selects_subscription_not_mutually_exclusive_cli_flags(self):
        cli = Mock(return_value={"accessToken": "offline"})
        Azure(federation_config(), cli=cli).authenticate()
        arguments = cli.call_args.args[0]
        self.assertIn("--subscription", arguments)
        self.assertNotIn("--tenant", arguments)

    def test_config_fourth_demo_and_reject_unrelated_storage_options(self):
        cfg = federation_config()
        self.assertIn("federation", DEMOS)
        cfg["demos"]["federation"]["file_events"] = "disabled"
        with self.assertRaises(DeploymentError):
            validate(cfg)

    def test_foreign_catalog_must_not_reuse_parent(self):
        cfg = federation_config()
        cfg["shared_foundation"] = True
        original = config()["demos"]["onelake"]
        original["workspace"]["name"] = "other_workspace"
        cfg["demos"]["onelake"] = original
        cfg["demos"]["federation"]["catalog"] = original["catalog"]
        with self.assertRaises(DeploymentError):
            validate(cfg)

    def test_supported_sql_exact_workspace_and_item_no_managed_copy(self):
        sql = connection_sql("connection", WS, "credential", "marker")
        self.assertIn("TYPE onelake", sql)
        self.assertIn(f"workspace '{WS}'", sql)
        sql = catalog_sql("independent", "connection", LH, "marker")
        self.assertIn("CREATE FOREIGN CATALOG `independent`", sql)
        self.assertIn(f"data_item '{LH}'", sql)
        self.assertIn("item_type 'Lakehouse'", sql)
        self.assertNotIn("MANAGED LOCATION", sql)
        with self.assertRaises(DeploymentError):
            catalog_sql("catalog", "connection", "invalid", "marker")

    def test_notebook_binding_and_embedded_shared_generator_compile(self):
        definition = notebook(WS, LH, "workspace", "lakehouse", {"sales_rows": 10000}, "marker", "baseline")
        body = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        binding = body["metadata"]["trident"]["lakehouse"]
        self.assertEqual(binding["default_lakehouse"], LH)
        self.assertEqual(binding["default_lakehouse_workspace_id"], WS)
        for cell in body["cells"]:
            source = "".join(cell["source"])
            compile(source, "<notebook>", "exec")
            self.assertTrue(all(line.endswith("\n") for line in cell["source"]))
        self.assertIn("def generate_sales_batch", "".join(body["cells"][1]["source"]))
        producer = "".join(body["cells"][2]["source"])
        self.assertNotIn('mode("overwrite")', producer)
        self.assertIn('write_rows(table, rows, "errorifexists"', producer)
        self.assertIn('elif not equal("FactSales"', producer)

    def test_actual_source_commit_not_property_commit_or_unverified_append(self):
        path = Path(__file__).resolve().parents[1] / "demos" / "onelake-federation" / "producer.py"
        tree = ast.parse(path.read_text())
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                        and node.name == "source_commit")
        scope = {}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), "exec"), scope)
        select = scope["source_commit"]
        baseline = {"version": 0, "operation": "CREATE TABLE AS SELECT",
                    "operationMetrics": {"numOutputRows": "10000"}}
        properties = {"version": 1, "operation": "SET TBLPROPERTIES"}
        self.assertEqual(select([properties, baseline], "baseline", "owner", 10000), baseline)
        append = {"version": 2, "operation": "WRITE", "userMetadata": "owner:append",
                  "operationParameters": {"mode": "Append"}, "operationMetrics": {"numOutputRows": "10"}}
        self.assertEqual(select([append, properties, baseline], "append", "owner", 10000), append)
        with self.assertRaises(RuntimeError):
            select([dict(append, userMetadata="other:append")], "append", "owner", 10000)
        with self.assertRaises(RuntimeError):
            select([append, dict(append, version=3)], "append", "owner", 10000)
        with self.assertRaises(RuntimeError):
            select([baseline], "baseline", "owner", 9999)

    def test_ui_notebook_normalization_preserves_code_and_binding_checks(self):
        definition = notebook(WS, LH, "workspace", "lakehouse", {}, "marker", "baseline")
        expected = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        actual = deepcopy(expected)
        actual["metadata"]["dependencies"] = actual["metadata"].pop("trident")
        actual["metadata"]["nteract"] = {"version": "generated"}
        actual["cells"][0].update(id="generated", execution_count=1, outputs=[{"text": "output"}])
        actual["cells"][0]["metadata"]["microsoft"] = {"language": "python"}
        self.assertEqual(notebook_content(actual), notebook_content(expected))
        actual["cells"][0]["source"] = ["changed code"]
        self.assertNotEqual(notebook_content(actual), notebook_content(expected))
        actual = deepcopy(expected)
        actual["metadata"]["dependencies"] = {"lakehouse": {"default_lakehouse": WS}}
        with self.assertRaises(DeploymentError):
            notebook_content(actual)

    def test_connection_reuse_checks_type_workspace_credential_and_id(self):
        db = Mock()
        options = {"workspace": WS, "credential": "cred"}
        db.get.return_value = {"name": "demo", "connection_type": "ONELAKE",
                               "connection_id": "immutable", "options": options}
        self.assertEqual(metadata(db, "connections", "demo", options)["id"], "immutable")
        db.get.return_value["options"] = dict(options, workspace=LH)
        with self.assertRaises(DeploymentError):
            metadata(db, "connections", "demo", options)

    def test_exact_rows_detect_equal_count_total_but_changed_dimension(self):
        tables = generate_star_schema(StarSchemaConfig(date_rows=2, product_rows=2, customer_rows=2, sales_rows=5))
        discovered = {table: ("dbo", table.lower()) for table in tables}
        db = Mock()
        responses = [{"statement_id": table, "rows": [[json_safe(value) for value in row.values()] for row in rows]}
                     for table, rows in tables.items()]
        db.query.side_effect = deepcopy(responses)
        self.assertEqual(compare_sql(db, "warehouse", "catalog", discovered, tables)["status"], "pass")
        responses[1]["rows"][0][1] = "changed product"
        db.query.side_effect = responses
        with self.assertRaises(DeploymentError):
            compare_sql(db, "warehouse", "catalog", discovered, tables)

    def test_discovery_not_assumed_and_ambiguous_rejected(self):
        db = Mock()
        db.query.side_effect = [
            {"rows": [["actual_schema"]]},
            {"columns": ["namespace", "tableName", "isTemporary"],
             "rows": [["actual_schema", t.lower(), False] for t in generate_star_schema()]},
        ]
        result = discover(db, "warehouse", "catalog")
        self.assertEqual(result["FactSales"], ["actual_schema", "factsales"])
        nonsecret(result)
        db.query.side_effect = [{"rows": [["one"], ["two"]]}] + [
            {"columns": ["tableName"], "rows": [["factsales"]]} for _ in range(2)]
        with self.assertRaises(DeploymentError):
            discover(db, "warehouse", "catalog")

    def test_create_resume_does_not_replay_sql_and_detects_id_change(self):
        cfg = federation_config()
        with tempfile.TemporaryDirectory() as directory:
            flow = Mock()
            flow.state = State(Path(directory) / "state.json", cfg)
            flow.marker = "marker"
            db = Mock()
            remote = {"name": "connection", "connection_type": "ONELAKE", "connection_id": "one",
                      "comment": "marker", "options": {"workspace": WS, "credential": "cred"}}
            db.get.side_effect = [None, remote, remote]
            options = remote["options"]
            db.submit_sql.return_value = {"statement_id": "statement"}
            create(flow, db, "warehouse", "connections", "connection", "SQL", options)
            create(flow, db, "warehouse", "connections", "connection", "SQL", options)
            self.assertEqual(db.submit_sql.call_count, 1)
            db.get.side_effect = None
            db.get.return_value = dict(remote, connection_id="changed")
            with self.assertRaises(DeploymentError):
                create(flow, db, "warehouse", "connections", "connection", "SQL", options)

    def test_notebook_job_resume_does_not_resubmit(self):
        cfg = federation_config()
        with tempfile.TemporaryDirectory() as directory:
            flow = Mock()
            flow.state = State(Path(directory) / "state.json", cfg)
            http = flow.fabric.http
            http.list.return_value = []
            http.request.side_effect = [
                Mock(headers={"location": "https://api.fabric.microsoft.com/v1/job"}),
                Mock(data={"status": "Completed", "id": "job"}),
                Mock(data={"status": "Completed", "id": "job"}),
            ]
            http.clock.return_value = 0
            http.timeout = 10
            run_notebook(flow, "run", WS, LH)
            run_notebook(flow, "run", WS, LH)
            self.assertEqual([c.args[0] for c in http.request.call_args_list].count("POST"), 1)

    def test_recent_fabric_utc_time_without_offset_still_blocks_duplicate(self):
        cfg = federation_config()
        with tempfile.TemporaryDirectory() as directory:
            flow = Mock()
            flow.state = State(Path(directory) / "state.json", cfg)
            flow.fabric.http.list.return_value = [{
                "startTimeUtc": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                "status": "Completed",
            }]
            with self.assertRaises(Waiting):
                run_notebook(flow, "run", WS, LH)
            flow.fabric.http.request.assert_not_called()

    def test_four_demo_all_path_preserves_order_and_setup_only_default(self):
        cfg = config(("onelake", "power-bi", "compatibility"))
        fourth = federation_config()["demos"]["federation"]
        fourth["workspace"]["name"] = "fourth_workspace"
        cfg["demos"]["federation"] = fourth
        validate(cfg)
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "state.json", cfg)
            console = Mock()
            console.confirm.return_value = True
            flow = Workflow(cfg, state, console)
            flow.preview = Mock()
            flow.gate = Mock()
            flow.run_demo = Mock(side_effect=lambda demo: ({"status": "setup-checked"}, Mock(), "warehouse"))
            flow.optional_checks = Mock()
            self.assertEqual(list(flow.run()), list(DEMOS))
            flow.optional_checks.assert_not_called()


if __name__ == "__main__":
    unittest.main()

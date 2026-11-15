"""Offline tests of the real CLI, workflow and service adapters."""

import base64
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path
import re
import sqlite3
import tempfile
import unittest
import weakref
from unittest.mock import Mock, patch
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from demos.common.star_schema import TABLE_COLUMN_TYPES, generate_star_schema, StarSchemaConfig
from demos.deployment.cli import main
from demos.deployment.config import Console, collect, validate, workspace_display_name
from demos.deployment.safety import DeploymentError, State, Waiting, digest, nonsecret, write_private
from demos.deployment.services import ADLS, ARM, FABRIC, Azure, Databricks, Fabric, HTTP, Response
from demos.deployment.sql import MV, TABLES, aggregate_select, create_table, inserts, literal
from demos.deployment.workflow import Workflow, mirror_definition, notebook_definition

TENANT = "00000000-0000-0000-0000-000000000001"
SUB = "00000000-0000-0000-0000-000000000002"
CAP = "00000000-0000-0000-0000-000000000003"
WAREHOUSE = "1234567890abcdef"
CONNECTOR = f"/subscriptions/{SUB}/resourceGroups/demo/providers/Microsoft.Databricks/accessConnectors/connector"


def config(demos=("onelake",), create=False):
    foundation = {
        "capacity": {"mode": "reuse", "resource_id": f"/subscriptions/{SUB}/resourceGroups/demo/providers/Microsoft.Fabric/capacities/democap"},
        "databricks": {"mode": "reuse", "resource_id": f"/subscriptions/{SUB}/resourceGroups/demo/providers/Microsoft.Databricks/workspaces/demodb"},
    }
    if create:
        foundation = {"capacity": {"mode": "create", "name": "democap", "sku": "F2", "administrators": ["operator@example.test"]},
                      "databricks": {"mode": "create", "name": "demodb", "sku": "premium", "managed_group": "managed",
                                     "networking": "public"}}
    result = {"version": 1, "tenant_id": TENANT, "subscription_id": SUB, "region": "westeurope",
              "resource_group": "demo", "timeout": 10, "shared_foundation": len(demos) > 1,
              "sample": {"date_rows": 2, "product_rows": 2, "customer_rows": 2, "sales_rows": 5, "seed": 42},
              "demos": {}}
    for demo in demos:
        prefix = demo.replace("-", "")
        spec = {"foundation": deepcopy(foundation), "prefix": prefix, "catalog": "parent",
                "profile": "OFFLINE", "warehouse": {"mode": "reuse", "id": WAREHOUSE},
                "workspace": {"mode": "create", "name": prefix + "_workspace"}}
        if demo == "power-bi":
            spec["storage_mode"] = "DirectQuery"
        else:
            spec.update(identity_resource_id=CONNECTOR, identity_object_id=TENANT,
                        credential="onelake_credential", file_events="disabled")
        if demo == "compatibility":
            spec.update(output_root="abfss://outputs@testaccount.dfs.core.windows.net/demo",
                        output_external_location="output_location")
        result["demos"][demo] = spec
    validate(result)
    return result


class SimulatedCloud:
    """A separate fake service state. Production adapters still build every request."""
    def __init__(self, cfg, lro=False):
        self.cfg = cfg
        self.calls = []
        self.resources = {}
        self.items = {}
        self.schemas = {}
        self.tables = {}
        self.locations = {}
        self.credentials = {}
        self.shortcuts = {}
        self.operations = {}
        self.statements = {}
        self.sql = []
        self.models = {}
        self.reports = {}
        self.lro = lro
        self.backing_id = str(uuid4())
        self.clock_value = 0
        self.sql_database = sqlite3.connect(":memory:")
        weakref.finalize(self, self.sql_database.close)
        for spec in cfg["demos"].values():
            for kind, target in spec["foundation"].items():
                if target["mode"] == "reuse":
                    self.resources[target["resource_id"]] = self.azure_resource(kind, target["resource_id"], {})
        self.capacities = [{"id": CAP, "displayName": "democap", "state": "Active"}]

    def clock(self):
        return self.clock_value

    def sleep(self, seconds):
        self.clock_value += seconds

    def client(self, root):
        return HTTP(root, lambda: {"Authorization": "Bearer OFFLINE-ONLY"}, self.cfg["timeout"],
                    self.send, self.clock, self.sleep)

    def cli(self, args):
        if args[:2] == ["account", "show"]:
            return {"id": SUB, "tenantId": TENANT}
        if args[:3] == ["deployment", "group", "what-if"]:
            template = json.loads(Path(args[args.index("--template-file") + 1]).read_text())
            self.calls.append(("WHATIF", template))
            return {"changes": [{"changeType": "Create"}]}
        raise AssertionError("Unexpected CLI operation")

    def workflow(self, cfg, state, console):
        return Workflow(cfg, state, console,
                        azure=Azure(cfg, self.client(ARM), self.cli),
                        fabric=Fabric(cfg, self.client(FABRIC)),
                        databricks_factory=lambda host, profile: Databricks(host, profile, cfg["timeout"], self.client(host)),
                        adls_factory=lambda root: ADLS(root, TENANT, cfg["timeout"], http=self.client("https://testaccount.dfs.core.windows.net")),
                        powerbi=self.client("https://api.powerbi.com/v1.0/myorg"))

    def azure_resource(self, kind, resource_id, body):
        result = dict(body, id=resource_id, type=("Microsoft.Fabric/capacities" if kind == "capacity" else "Microsoft.Databricks/workspaces"),
                      location="westeurope", properties={"provisioningState": "Succeeded"})
        if kind == "databricks":
            result["properties"].update(workspaceUrl="adb-123.azuredatabricks.net", workspaceId=123)
        return result

    def send(self, method, endpoint, headers, body, timeout):
        self.calls.append((method, endpoint, body))
        parsed = urlsplit(endpoint)
        path = unquote(parsed.path)
        if parsed.hostname == "management.azure.com":
            return self.arm(method, path, body)
        if parsed.hostname == "api.fabric.microsoft.com":
            return self.fabric(method, path.removeprefix("/v1"), body)
        if parsed.hostname == "adb-123.azuredatabricks.net":
            return self.db(method, path, body, parsed.query)
        if parsed.hostname == "api.powerbi.com":
            if "/datasets/" in path:
                return Response(200, {"id": path.rsplit("/", 1)[-1]}, {})
            return Response(200, {"datasetId": self.model_id}, {})
        if parsed.hostname == "testaccount.dfs.core.windows.net":
            if method == "HEAD":
                return Response(200, {}, {})
            if "resource=filesystem" in parsed.query:
                if "_delta_log" in parsed.query:
                    return Response(200, {"paths": [{"name": "demo/_delta_log/00000000000000000000.json", "isDirectory": "false"}]}, {})
                return Response(200, {"paths": []}, {})
            return Response(200, {}, {}, json.dumps({"protocol": {"minReaderVersion": 1, "minWriterVersion": 7}}) + "\n"
                            + json.dumps({"metaData": {"id": self.backing_id}}))
        raise AssertionError(endpoint)

    def arm(self, method, path, body):
        if "/providers/" in path and path.endswith(("Microsoft.Fabric", "Microsoft.Databricks")):
            return Response(200, {"registrationState": "Registered"}, {})
        if path.endswith("/resourceGroups/demo"):
            return Response(200, {"id": path}, {})
        if method == "PUT":
            kind = "capacity" if "/capacities/" in path else "databricks"
            self.resources[path] = self.azure_resource(kind, path, body)
            return Response(201, self.resources[path], {})
        return Response(200, self.resources[path], {}) if path in self.resources else Response(404, {}, {})

    def fabric(self, method, path, body):
        if path == "/capacities":
            return Response(200, {"value": self.capacities}, {})
        if path.startswith("/connections/"):
            return Response(200, {"id": path.rsplit("/", 1)[-1]}, {})
        if path.startswith("/operations/"):
            op = path.split("/")[2]
            if path.endswith("/result"):
                return Response(200, self.operations[op], {})
            return Response(200, {"status": "Succeeded"}, {})
        if path.endswith("/getDefinition"):
            item = path.split("/")[-2]
            return Response(200, {"definition": self.items[item]["definition"]}, {})
        if path.endswith("/shortcuts") and method == "GET":
            item = path.split("/")[-2]
            return Response(200, {"value": self.shortcuts.get(item, [])}, {})
        if "/shortcuts/" in path and method == "GET":
            item = path.split("/")[4]
            value = next((s for s in self.shortcuts.get(item, [])
                          if path.endswith("/" + s["path"] + "/" + s["name"])), None)
            return Response(200, value, {}) if value else Response(404, {}, {})
        if path.endswith("/shortcuts") and method == "POST":
            self.shortcuts.setdefault(path.split("/")[-2], []).append(body)
            return Response(201, body, {})
        if method == "GET":
            if path == "/workspaces":
                return Response(200, {"value": [i for i in self.items.values() if i["type"] == "Workspace"]}, {})
            if path.endswith("/items"):
                workspace = path.split("/")[2]
                return Response(200, {"value": [i for i in self.items.values() if i.get("workspaceId") == workspace]}, {})
            item = path.rsplit("/", 1)[-1]
            return Response(200, self.items[item], {}) if item in self.items else Response(404, {}, {})
        kinds = {"workspaces": "Workspace", "azureDatabricksStorages": "AzureDatabricksStorage",
                 "mirroredAzureDatabricksCatalogs": "MirroredAzureDatabricksCatalog", "lakehouses": "Lakehouse", "notebooks": "Notebook"}
        item_id = str(uuid4())
        item = dict(body, id=item_id, type=kinds[path.rsplit("/", 1)[-1]])
        if item["type"] != "Workspace":
            item["workspaceId"] = path.split("/")[2]
        self.items[item_id] = item
        if item["type"] == "MirroredAzureDatabricksCatalog":
            definition = json.loads(base64.b64decode(body["definition"]["parts"][0]["payload"]))
            schema = definition["mirrorConfiguration"]["schemas"][0]
            self.shortcuts[item_id] = [{"path": "Tables/" + schema["name"], "name": t["name"],
                                       "target": {"oneLake": {"itemId": item_id}}} for t in schema["tables"]]
        if self.lro and item["type"] != "Workspace":
            op = str(uuid4())
            self.operations[op] = {"id": item_id}
            return Response(202, {}, {"x-ms-operation-id": op, "retry-after": "1"})
        return Response(201, {"id": item_id}, {})

    def db(self, method, path, body, query):
        if path.endswith("/sql/warehouses") and method == "GET":
            return Response(200, {"warehouses": []}, {})
        if path.endswith("/sql/warehouses") and method == "POST":
            return Response(200, {"id": WAREHOUSE}, {})
        if "/sql/warehouses/" in path:
            return Response(200, {"id": WAREHOUSE, "state": "STOPPED", "name": "newwarehouse"}, {})
        if path.endswith("/sql/statements") and method == "POST":
            statement_id = str(uuid4())
            self.sql.append(body["statement"])
            rows = self.execute(body["statement"])
            self.statements[statement_id] = rows
            return Response(200, {"statement_id": statement_id}, {})
        if "/sql/statements/" in path:
            return Response(200, {"status": {"state": "SUCCEEDED"}, "result": {"data_array": self.statements[path.rsplit("/", 1)[-1]]}}, {})
        kind, full_name = path.split("/")[-2:]
        if kind == "catalogs":
            return Response(200, {"name": full_name}, {})
        objects = {"schemas": self.schemas, "tables": self.tables, "external-locations": self.locations, "storage-credentials": self.credentials}
        if full_name not in objects[kind]:
            return Response(404, {}, {})
        result = deepcopy(objects[kind][full_name])
        if query and kind == "tables":
            result["storage_location"] = result["properties"]["delta.universalFormat.compatibility.location"]
        return Response(200, result, {})

    def execute(self, sql):
        parts = re.findall(r"`([^`]+)`", sql)
        marker = re.search(r"COMMENT '([^']+)'", sql)
        if sql.startswith("CREATE SCHEMA"):
            full = ".".join(parts[:2])
            location = re.search(r"MANAGED LOCATION '([^']+)'", sql)
            self.schemas[full] = {"schema_id": str(uuid4()), "comment": marker[1],
                                 "storage_root": location[1] if location else "abfss://default/managed/" + full}
        elif sql.startswith(("CREATE TABLE", "CREATE MATERIALIZED VIEW")):
            full = ".".join(parts[:3])
            properties = {"delta.feature.catalogManaged": "supported"} if "delta.feature.catalogManaged" in sql else {}
            location = re.search(r"'delta.universalFormat.compatibility.location' = '([^']+)'", sql)
            if location:
                properties["delta.universalFormat.compatibility.location"] = location[1]
            self.tables[full] = {"table_id": str(uuid4()), "name": parts[2].lower(), "comment": marker[1],
                                 "storage_location": self.schemas[".".join(parts[:2])]["storage_root"] + "/tables/" + parts[2],
                                 "table_type": "MATERIALIZED_VIEW" if "MATERIALIZED VIEW" in sql else "MANAGED", "properties": properties}
            if sql.startswith("CREATE TABLE"):
                columns = [(c, "TEXT" if t in ("DATE", "STRING") else "NUMERIC") for c, t in TABLE_COLUMN_TYPES[parts[2]]]
                self.sql_database.execute(f'CREATE TABLE "{full}" (' + ", ".join(f'"{c}" {t}' for c, t in columns) + ")")
        elif sql.startswith("ALTER TABLE"):
            full = ".".join(parts[:3])
            location = re.search(r"'delta.universalFormat.compatibility.location' = '([^']+)'", sql)[1]
            self.tables[full]["properties"]["delta.universalFormat.compatibility.location"] = location
        elif sql.startswith("INSERT"):
            converted = self.sqlite_sql(sql)
            self.sql_database.execute(converted)
        elif sql.startswith("SELECT"):
            rows = self.sql_database.execute(self.sqlite_sql(sql)).fetchall()
            return [[str(Decimal(str(v)).quantize(Decimal(".01"))) if isinstance(v, float) else str(v) for v in row] for row in rows]
        elif sql.startswith("DESCRIBE TABLE EXTENDED"):
            return [["Last Refreshed Version", "2"], ["Last Refreshed", "2024-01-01T00:00:00+00:00"]]
        elif sql.startswith("DESCRIBE HISTORY"):
            return [["2"]]
        return []

    @staticmethod
    def sqlite_sql(sql):
        sql = re.sub(r"`([^`]+)`\.`([^`]+)`\.`([^`]+)`", lambda m: '"' + ".".join(m.groups()) + '"', sql)
        return re.sub(r"DATE '([^']+)'", r"'\1'", sql)


class Operator(Console):
    def __init__(self, cloud, deny=None):
        self.cloud = cloud
        self.lines = []
        self.target = None
        self.key = None
        self.deny = deny
        super().__init__(self.read_input, self.print_line)

    def print_line(self, line):
        self.lines.append(line)
        if line.startswith("\nMANUAL CHECKPOINT"):
            self.key = line.splitlines()[1].split(" ", 2)[2]
            self.target = json.loads(line.split("Target: ", 1)[1])

    def confirm(self, label):
        if self.deny is not None and (self.key == self.deny or self.deny == "apply" and label.startswith("Apply")):
            return False
        if self.key and self.key.endswith("/access"):
            target = self.target
            self.cloud.locations[target["location"]] = {"url": target["url"], "credential_name": target["credential"],
                                                        "read_only": False, "enable_file_events": False}
            self.cloud.credentials[target["credential"]] = {"read_only": False,
                "azure_managed_identity": {"access_connector_id": target["identity_resource_id"]}}
        if self.key and self.key.endswith("/output-eligibility"):
            self.cloud.locations["output_location"] = {"url": self.cloud.cfg["demos"]["compatibility"]["output_root"], "read_only": False}
        return True

    def read_input(self, label):
        if "source/backing version" in label or "Last Refreshed Version" in label:
            return "2"
        if "Last Refreshed UTC" in label:
            return "2024-01-01T00:00:00+00:00"
        if "Fabric capacity GUID" in label:
            return CAP
        if "normal mirror folder" in label:
            schema = self.cloud.cfg["demos"]["compatibility"]["prefix"] + "_normal"
            return "Tables/" + schema
        if "MV backing-table GUID" in label:
            return self.cloud.backing_id
        if "clone source/backing GUID" in label:
            table = re.search(r"generated (\S+) clone", label)[1]
            if table == MV:
                return self.cloud.backing_id
            return self.cloud.tables["parent.compatibility_compat." + table]["table_id"]
        value = str(uuid4())
        if "semantic-model GUID" in label:
            self.cloud.model_id = value
        return value


class DeploymentTests(unittest.TestCase):
    def test_each_demo_and_all_real_orchestration_no_reload_on_resume(self):
        for selected in (("onelake",), ("power-bi",), ("compatibility",), ("onelake", "power-bi", "compatibility")):
            with self.subTest(selected=selected), tempfile.TemporaryDirectory() as directory:
                cfg = config(selected)
                cloud = SimulatedCloud(cfg, lro=True)
                operator = Operator(cloud)
                state = State(Path(directory) / "state.json", cfg)
                workflow = cloud.workflow(cfg, state, operator)
                results = workflow.run()
                self.assertEqual({r["status"] for r in results.values()}, {"setup-checked"}, results)
                before = len(cloud.sql)
                before_writes = len([sql for sql in cloud.sql if not sql.startswith("DESCRIBE")])
                writes = sum(c[0] in ("POST", "PUT") and not c[1].endswith("/getDefinition") for c in cloud.calls if len(c) == 3)
                results = cloud.workflow(cfg, State(state.path, cfg), operator).run()
                self.assertEqual(len([sql for sql in cloud.sql if not sql.startswith("DESCRIBE")]), before_writes)
                new_calls = cloud.calls
                # Compatibility resource setup legitimately rereads SQL version metadata on resume.
                metadata_queries = len([sql for sql in cloud.sql[before:] if sql.startswith("DESCRIBE")])
                self.assertEqual(sum(c[0] in ("POST", "PUT") and not c[1].endswith("/getDefinition") for c in new_calls if len(c) == 3),
                                 writes + metadata_queries)
                for demo in selected:
                    schemas = results[demo].get("schemas", [results[demo].get("schema")])
                    for schema in schemas:
                        for table in TABLES:
                            count = cloud.sql_database.execute(f'SELECT COUNT(*) FROM "parent.{schema}.{table}"').fetchone()[0]
                            self.assertEqual(count, len(generate_star_schema(StarSchemaConfig(**cfg["sample"]))[table]))
                text = state.path.read_text()
                self.assertNotIn("OFFLINE-ONLY", text)

    def test_create_resources_warehouse_and_preview(self):
        cfg = config(("onelake",), create=True)
        cfg["demos"]["onelake"]["warehouse"] = {"mode": "create", "name": "newwarehouse", "size": "2X-Small"}
        cloud = SimulatedCloud(cfg)
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "state.json", cfg)
            result = cloud.workflow(cfg, state, Operator(cloud)).run()
            self.assertEqual(result["onelake"]["status"], "setup-checked", result)
            self.assertEqual(len([c for c in cloud.calls if c[0] == "WHATIF"]), 2)
            self.assertEqual(len([c for c in cloud.calls if c[0] == "PUT"]), 2)

    def test_checkpoint_stops_then_resumes_without_recreating_workspace(self):
        cfg = config()
        cloud = SimulatedCloud(cfg)
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "state.json", cfg)
            first = cloud.workflow(cfg, state, Operator(cloud, deny="onelake/onelake/access")).run()
            self.assertEqual(first["onelake"]["status"], "waiting")
            self.assertFalse(cloud.sql)
            workspaces = [i["id"] for i in cloud.items.values() if i["type"] == "Workspace"]
            second = cloud.workflow(cfg, State(state.path, cfg), Operator(cloud)).run()
            self.assertEqual(second["onelake"]["status"], "setup-checked", second)
            self.assertEqual([i["id"] for i in cloud.items.values() if i["type"] == "Workspace"], workspaces)

    def test_no_apply_consent_no_cloud_write(self):
        cfg = config(create=True)
        cloud = SimulatedCloud(cfg)
        with tempfile.TemporaryDirectory() as directory:
            workflow = cloud.workflow(cfg, State(Path(directory) / "state.json", cfg), Operator(cloud, deny="apply"))
            with self.assertRaises(Waiting):
                workflow.run()
            self.assertFalse(any(c[0] in ("PUT", "POST") for c in cloud.calls))

    def test_real_cli_apply_resume_status(self):
        cfg = config(("power-bi",))
        cloud = SimulatedCloud(cfg)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            write_private(path / "config.json", cfg)
            operator = Operator(cloud)
            self.assertEqual(main(["--directory", directory, "--action", "apply"], operator, cloud.workflow), 0)
            self.assertEqual(main(["--directory", directory, "--action", "resume"], operator, cloud.workflow), 0)
            before = len(cloud.calls)
            self.assertEqual(main(["--directory", directory, "--action", "status"], operator, cloud.workflow), 0)
            self.assertEqual(len(cloud.calls), before)

    def test_optional_sql_requires_separate_consent(self):
        cfg = config()
        for allow in (False, True):
            cloud = SimulatedCloud(cfg)
            operator = Operator(cloud)
            original = operator.confirm
            operator.confirm = lambda label: allow if label.startswith("Run optional") else original(label)
            with tempfile.TemporaryDirectory() as directory:
                result = cloud.workflow(cfg, State(Path(directory) / "state.json", cfg), operator).run(optional=True)
                self.assertEqual(result["onelake"]["status"], "setup-checked", result)
                self.assertEqual(any(sql.startswith("SELECT") for sql in cloud.sql), allow)

    def test_config_shared_and_secret_boundaries(self):
        cfg = config(("onelake", "power-bi"))
        cfg["shared_foundation"] = False
        with self.assertRaises(DeploymentError):
            validate(cfg)
        for secret in ({"accessToken": "anything"}, {"url": "https://example.test?sig=xyz"},
                       {"url": "https://user:password@example.test"}, {"x": "Bearer opaque"}):
            with self.subTest(secret=secret), self.assertRaises(DeploymentError):
                nonsecret(secret)
        cfg = config()
        cfg["demos"]["onelake"]["identity_resource_id"] = CONNECTOR.replace(SUB, TENANT)
        with self.assertRaises(DeploymentError):
            validate(cfg)

    def test_conditional_prompt_powerbi_omits_storage_inputs(self):
        answers = iter(["power-bi", TENANT, SUB, "westeurope", "demo", "42", "10000", "900",
                        "reuse", f"/subscriptions/{SUB}/resourceGroups/demo/providers/Microsoft.Fabric/capacities/democap",
                        "reuse", f"/subscriptions/{SUB}/resourceGroups/demo/providers/Microsoft.Databricks/workspaces/demodb",
                        "pbi", "parent", "OFFLINE", "reuse", WAREHOUSE, "create", "New workspace", "DirectQuery"])
        prompts = []
        def read(label):
            prompts.append(label)
            return next(answers)
        result = collect(Console(read, lambda s: None))
        self.assertEqual(set(result["demos"]), {"power-bi"})
        self.assertEqual(result["demos"]["power-bi"]["workspace"]["name"], "New workspace")
        self.assertFalse(any("credential" in p or "ADLS" in p or "connector" in p for p in prompts))

    def test_unconfigured_profile_gate_precedes_client_and_safe_resume(self):
        cfg = config(create=True)
        cloud = SimulatedCloud(cfg)
        configured = [False]
        constructed = []
        def factory(cfg, state, console):
            workflow = cloud.workflow(cfg, state, console)
            original = workflow.db_factory
            def initialize(host, profile):
                constructed.append(profile)
                if not configured[0]:
                    raise DeploymentError("Profile is not configured yet.")
                self.assertIn("onelake/uc", state.data["gates"])
                return original(host, profile)
            workflow.db_factory = initialize
            return workflow
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            operator = Operator(cloud, deny="onelake/uc")
            result = factory(cfg, State(path, cfg), operator).run()
            self.assertEqual(result["onelake"]["status"], "waiting", result)
            self.assertEqual(constructed, [])
            self.assertFalse(cloud.sql)
            self.assertTrue(any("profile OFFLINE for https://adb-123.azuredatabricks.net" in line for line in operator.lines))
            creations = len([c for c in cloud.calls if c[0] == "PUT"])
            self.assertEqual(creations, 2)
            operator = Operator(cloud)
            original_confirm = operator.confirm
            def complete_profile(label):
                approved = original_confirm(label)
                if operator.key == "onelake/uc" and approved:
                    configured[0] = True
                return approved
            operator.confirm = complete_profile
            result = factory(cfg, State(path, cfg), operator).run()
            self.assertEqual(result["onelake"]["status"], "setup-checked", result)
            self.assertEqual(constructed, ["OFFLINE"])
            self.assertEqual(len([c for c in cloud.calls if c[0] == "PUT"]), creations)
            self.assertTrue(any("/unity-catalog/catalogs/parent" in c[1] for c in cloud.calls if len(c) == 3))

    def test_unavailable_mv_backing_location_pauses_without_recreating(self):
        cfg = config(("compatibility",))
        cloud = SimulatedCloud(cfg)
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "state.json", cfg)
            workflow = cloud.workflow(cfg, state, Operator(cloud, deny="mv/backing-location"))
            metadata = {"table_id": TENANT, "table_type": "MATERIALIZED_VIEW", "comment": workflow.marker}
            db = Mock()
            db.get.side_effect = lambda kind, full_name, missing=False: None if missing else metadata
            db.submit_sql.return_value = {"statement_id": SUB}
            root = f"abfss://{TENANT}@onelake.dfs.fabric.microsoft.com/{SUB}/Files/managed"
            for _ in range(2):
                with self.assertRaises(Waiting):
                    workflow.uc_create("mv", db, WAREHOUSE, "tables", "parent.compat.mv",
                                       "CREATE MATERIALIZED VIEW `parent`.`compat`.`mv`", root)
            self.assertEqual(db.submit_sql.call_count, 1)
            self.assertEqual(state.data["steps"]["mv"]["status"], "submitted")
            self.assertNotIn("uc-storage/parent.compat.mv", state.data["inputs"])

    def test_workspace_display_names_create_and_reuse_without_relaxing_identifiers(self):
        long_name = "New workspace " + "x" * (256 - len("New workspace "))
        for mode, display_name in (("create", "New workspace"), ("reuse", long_name)):
            cfg = config(("power-bi",))
            cloud = SimulatedCloud(cfg)
            spec = cfg["demos"]["power-bi"]
            spec["workspace"].update(mode=mode, name=display_name)
            if mode == "reuse":
                workspace = str(uuid4())
                spec["workspace"]["id"] = workspace
                cloud.items[workspace] = {"id": workspace, "type": "Workspace", "displayName": display_name, "capacityId": CAP}
            validate(cfg)
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                result = cloud.workflow(cfg, State(Path(directory) / "state.json", cfg), Operator(cloud)).run()
                self.assertEqual(result["power-bi"]["status"], "setup-checked", result)
                self.assertEqual(cloud.items[result["power-bi"]["workspace"]]["displayName"], display_name)
        for value in ("", " ", "x" * 257, "Admin monitoring"):
            with self.subTest(value=value), self.assertRaises(DeploymentError):
                workspace_display_name(value)
        for field in ("catalog", "prefix"):
            cfg["demos"]["power-bi"][field] = "Invalid identifier"
            with self.assertRaises(DeploymentError):
                validate(cfg)
            cfg["demos"]["power-bi"][field] = "valid"
        cfg["resource_group"] = "Invalid group"
        with self.assertRaises(DeploymentError):
            validate(cfg)

    def test_offline_cli_configure_persists_no_cloud_calls(self):
        cfg = config(("power-bi",))
        with tempfile.TemporaryDirectory() as directory, patch("demos.deployment.cli.collect", return_value=cfg):
            constructor = lambda *a: self.fail("configure must not construct cloud clients")
            console = Console(output=lambda s: None)
            self.assertEqual(main(["--directory", directory], console, constructor), 0)
            self.assertTrue((Path(directory) / "state.json").exists())
            self.assertEqual(main(["--directory", directory], console, constructor), 2)

    def test_reused_fabric_workspace_is_not_reassigned(self):
        cfg = config(("power-bi",))
        cloud = SimulatedCloud(cfg)
        workspace = str(uuid4())
        cfg["demos"]["power-bi"]["workspace"]["mode"] = "reuse"
        cfg["demos"]["power-bi"]["workspace"]["id"] = workspace
        cloud.items[workspace] = {"id": workspace, "type": "Workspace", "displayName": "powerbi_workspace", "capacityId": CAP}
        with tempfile.TemporaryDirectory() as directory:
            result = cloud.workflow(cfg, State(Path(directory) / "state.json", cfg), Operator(cloud)).run()
            self.assertEqual(result["power-bi"]["status"], "setup-checked", result)
            self.assertFalse(any(c[0] == "POST" and c[1].endswith("/workspaces") for c in cloud.calls if len(c) == 3))

    def test_reused_workspace_wrong_capacity_blocks_without_assignment_write(self):
        cfg = config()
        cloud = SimulatedCloud(cfg)
        spec = cfg["demos"]["onelake"]
        workspace = str(uuid4())
        spec["workspace"].update(mode="reuse", id=workspace)
        cloud.items[workspace] = {"id": workspace, "type": "Workspace", "displayName": spec["workspace"]["name"], "capacityId": TENANT}
        with tempfile.TemporaryDirectory() as directory:
            result = cloud.workflow(cfg, State(Path(directory) / "state.json", cfg), Operator(cloud)).run()
            self.assertEqual(result["onelake"]["status"], "blocked")
            self.assertFalse(cloud.sql)

    def test_one_blocked_demo_does_not_block_an_independent_demo(self):
        cfg = config(("onelake", "power-bi"))
        cloud = SimulatedCloud(cfg)
        with tempfile.TemporaryDirectory() as directory:
            result = cloud.workflow(cfg, State(Path(directory) / "state.json", cfg),
                                    Operator(cloud, deny="onelake/onelake/access")).run()
            self.assertEqual(result["onelake"]["status"], "waiting")
            self.assertEqual(result["power-bi"]["status"], "setup-checked", result)

    def test_stale_output_metadata_never_claims_setup_success(self):
        cfg = config(("compatibility",))
        cloud = SimulatedCloud(cfg)
        with tempfile.TemporaryDirectory() as directory:
            workflow = cloud.workflow(cfg, State(Path(directory) / "state.json", cfg), Operator(cloud))
            db = cloud.workflow(cfg, workflow.state, workflow.console).db_factory("https://adb-123.azuredatabricks.net", "OFFLINE")
            original = cloud.execute
            cloud.execute = lambda sql: [["1"]] if sql.startswith("DESCRIBE HISTORY") else original(sql)
            with self.assertRaisesRegex(Waiting, "stale"):
                workflow.freshness("compatibility", db, WAREHOUSE, "parent", "compatibility_compat", "DimDate")

    def test_unknown_config_fields_and_signed_output_urls_are_rejected(self):
        cfg = config()
        cfg["demos"]["onelake"]["unexpected_auth"] = "opaque-value"
        with self.assertRaises(DeploymentError):
            validate(cfg)
        cfg = config(("compatibility",))
        cfg["demos"]["compatibility"]["output_root"] += "?sv=version&sig=opaque"
        with self.assertRaises(DeploymentError):
            validate(cfg)


class SafetyTests(unittest.TestCase):
    def test_uncertain_intent_never_retries_and_changed_config_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            state = State(path, {"demo": "one"})
            calls = []
            def submit():
                calls.append("write")
                raise DeploymentError("connection dropped")
            with self.assertRaises(DeploymentError):
                state.step("create", {"name": "thing"}, submit, lambda h: h)
            with self.assertRaisesRegex(DeploymentError, "Uncertain partial write"):
                State(path, {"demo": "one"}).step("create", {"name": "thing"}, submit, lambda h: h)
            self.assertEqual(calls, ["write"])
            with self.assertRaises(DeploymentError):
                State(path, {"demo": "different"})

    def test_submitted_operation_resumes_poll_not_create(self):
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "state.json", {"demo": "one"})
            calls = []
            def submit():
                calls.append("submit")
                return {"operation_id": TENANT}
            with self.assertRaises(Waiting):
                state.step("create", {}, submit, lambda h: (_ for _ in ()).throw(Waiting("pending")))
            result = State(state.path, {"demo": "one"}).step("create", {}, submit, lambda h: {"id": h["operation_id"]})
            self.assertEqual(calls, ["submit"])
            self.assertEqual(result["id"], TENANT)

    def test_lock_and_private_repo_path(self):
        with tempfile.TemporaryDirectory() as directory:
            state = State(Path(directory) / "state.json", {})
            with state.lock(), self.assertRaises(DeploymentError):
                with state.lock():
                    pass
        with self.assertRaises(DeploymentError):
            write_private(Path(__file__).parent / "unsafe.json", {})

    def test_safe_429_lro_and_no_retry_permission_or_ambiguous_write(self):
        clock = [0]
        requests = []
        responses = iter([Response(429, {}, {"retry-after": "1"}), Response(202, {}, {"x-ms-operation-id": TENANT})])
        def send(*args):
            requests.append(args[0])
            return next(responses)
        http = HTTP(FABRIC, lambda: {}, 10, send, lambda: clock[0], lambda t: clock.__setitem__(0, clock[0] + t))
        self.assertEqual(http.request("POST", "/workspaces", {}) .status, 202)
        self.assertEqual(requests, ["POST", "POST"])
        for code in (403, 409, 500):
            requests.clear()
            http.send = lambda *args, code=code: (requests.append(args[0]) or Response(code, {}, {}))
            with self.assertRaises(DeploymentError):
                http.request("POST", "/workspaces", {})
            self.assertEqual(len(requests), 1)
        with self.assertRaises(DeploymentError):
            http.request("GET", "https://evil.example.test/operation")

    def test_azure_lro_bounded_and_recorded_target_identity(self):
        cfg = config()
        cloud = SimulatedCloud(cfg)
        azure = Azure(cfg, cloud.client(ARM), cloud.cli)
        resource_id = cfg["demos"]["onelake"]["foundation"]["capacity"]["resource_id"]
        result = azure.finish("capacity", {"id": resource_id})
        azure.reconcile("capacity", result)
        cloud.resources[resource_id]["tags"] = {"demo-deployment": "someone-else"}
        with self.assertRaises(DeploymentError):
            azure.reconcile("capacity", result)

    def test_empty_location_permission_and_nonempty_are_not_success(self):
        for status, data in ((403, {}), (404, {}), (200, {"paths": [{"name": "occupied"}]})):
            http = HTTP("https://testaccount.dfs.core.windows.net", lambda: {}, send=lambda *a: Response(
                status if a[0] == "HEAD" else 200, data, {}))
            adls = ADLS("abfss://outputs@testaccount.dfs.core.windows.net/demo", TENANT, 10, http=http)
            with self.subTest(status=status), self.assertRaises(DeploymentError):
                adls.empty("abfss://outputs@testaccount.dfs.core.windows.net/demo/table")

    def test_fabric_missing_lro_handle_and_failed_operation_are_explicit(self):
        cfg = config()
        responses = iter([Response(200, {"value": []}, {}), Response(202, {}, {})])
        fabric = Fabric(cfg, HTTP(FABRIC, lambda: {}, send=lambda *a: next(responses)))
        with self.assertRaisesRegex(DeploymentError, "without operation ID"):
            fabric.submit("/workspaces", {"displayName": "new"})
        fabric.http.send = lambda *a: Response(200, {"status": "Failed"}, {})
        with self.assertRaisesRegex(DeploymentError, "operation failed"):
            fabric.finish({"operation_id": TENANT})

    def test_pagination_and_bounded_throttling(self):
        calls = []
        pages = iter([Response(200, {"value": [{"id": 1}], "continuationUri": FABRIC + "/workspaces?continuationToken=page2"}, {}),
                      Response(200, {"value": [{"id": 2}]}, {})])
        http = HTTP(FABRIC, lambda: {}, send=lambda *a: (calls.append(a[1]) or next(pages)))
        self.assertEqual(http.list("/workspaces"), [{"id": 1}, {"id": 2}])
        self.assertEqual(len(calls), 2)
        http.send = lambda *a: Response(429, {}, {"retry-after": "100"})
        http.timeout = 1
        with self.assertRaises(Waiting):
            http.request("POST", "/workspaces", {})

    def test_shortcut_collision_keeps_original_target(self):
        cfg = config()
        calls = []
        fabric = Fabric(cfg, HTTP(FABRIC, lambda: {}, send=lambda *a: (calls.append(a[0]) or Response(200, {"name": "existing"}, {}))))
        with self.assertRaisesRegex(DeploymentError, "Shortcut collision"):
            fabric.shortcut(TENANT, SUB, {"name": "existing", "path": "Tables", "target": {"oneLake": {"itemId": CAP}}})
        self.assertEqual(calls, ["GET"])

    def test_sql_failed_statement_is_not_a_success_shaped_result(self):
        http = HTTP("https://adb-123.azuredatabricks.net", lambda: {},
                    send=lambda *a: Response(200, {"status": {"state": "FAILED"}}, {}))
        db = Databricks(http.root, "OFFLINE", 10, http)
        with self.assertRaisesRegex(DeploymentError, "FAILED"):
            db.finish_sql({"statement_id": TENANT})


class PayloadTests(unittest.TestCase):
    def test_partial_mirror_and_notebook_real_binding(self):
        definition = mirror_definition("parent", "isolated", CAP, (*TABLES, MV))
        content = json.loads(base64.b64decode(definition["parts"][0]["payload"]))
        self.assertEqual(content["mirroringMode"], "Partial")
        self.assertEqual(content["mirrorConfiguration"]["schemas"][0]["mirroringMode"], "Partial")
        self.assertEqual([t["name"] for t in content["mirrorConfiguration"]["schemas"][0]["tables"]], [*TABLES, MV])
        notebook = notebook_definition(TENANT, SUB, "validation")
        content = json.loads(base64.b64decode(notebook["parts"][0]["payload"]))
        binding = content["metadata"]["trident"]["lakehouse"]
        self.assertEqual(binding["default_lakehouse"], SUB)
        self.assertEqual(binding["default_lakehouse_workspace_id"], TENANT)
        self.assertEqual(binding["known_lakehouses"], [{"id": SUB}])
        compile("".join(content["cells"][0]["source"]), "notebook", "exec")

    def test_sql_types_constraints_literals_and_actual_insert_shape(self):
        create = create_table("parent", "isolated", "FactSales", "owner", True)
        self.assertIn("SalesLineKey` BIGINT NOT NULL", create)
        self.assertIn("DECIMAL(14,2)", create)
        self.assertEqual(create.count("FOREIGN KEY"), 3)
        self.assertNotIn("OR REPLACE", create)
        self.assertEqual(literal("O'Brien\\path"), "'O\\'Brien\\\\path'")
        select = aggregate_select("parent", "isolated")
        self.assertIn("c.Region, c.Segment", select)
        self.assertNotIn("GROUP BY c.CustomerKey", select)
        tables = generate_star_schema(StarSchemaConfig(date_rows=2, product_rows=2, customer_rows=2, sales_rows=5))
        statements = list(inserts("target", tables["FactSales"], 2))
        self.assertEqual(len(statements), 3)
        self.assertTrue(all("INSERT INTO target" in s for s in statements))


if __name__ == "__main__":
    unittest.main()

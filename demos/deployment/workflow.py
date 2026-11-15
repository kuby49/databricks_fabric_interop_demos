"""Dependency-ordered independent demo orchestration."""

from __future__ import annotations

import base64
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from urllib.parse import urlsplit

from demos.common.star_schema import (
    StarSchemaConfig, generate_star_schema, json_safe, validate_star_schema,
)
from .config import DEMOS
from .safety import DeploymentError, Waiting, digest, guid, url
from .services import ADLS, Azure, Databricks, Fabric, HTTP, azure_cli
from .sql import (
    MV, TABLES, aggregate_select, compatibility_properties, create_mv, create_table,
    ident, inserts, literal, qualified,
)

ONELAKE_DOC = "https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-onelake"
MIRROR_DOC = "https://learn.microsoft.com/en-us/fabric/mirroring/azure-databricks-tutorial"
COMPAT_DOC = "https://learn.microsoft.com/en-us/azure/databricks/external-access/compatibility-mode"
PBI_DOC = "https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi/service"
UC_DOC = "https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/get-started"
AZURE_DOC = "https://learn.microsoft.com/en-us/azure/azure-resource-manager/templates/deploy-what-if"


def encoded_part(path, body):
    return {"path": path, "payload": base64.b64encode(json.dumps(body).encode()).decode(),
            "payloadType": "InlineBase64"}


def mirror_definition(catalog, schema, connection, objects):
    return {"parts": [encoded_part("definition.json", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/mirroredAzureDatabricksCatalog/definition/mirroredAzureDatabricksCatalogDefinition/1.0.0/schema.json",
        "catalogName": catalog, "databricksWorkspaceConnectionId": connection,
        "autoSync": "Enabled", "mirroringMode": "Partial",
        "mirrorConfiguration": {"schemas": [{"name": schema, "mirroringMode": "Partial",
                                            "tables": [{"name": n, "mirroringMode": "Full"} for n in objects]}]},
    })]}


def notebook_definition(workspace, lakehouse, lakehouse_name):
    source = (
        'spark.conf.set("spark.fabric.session.highConcurrency.enabled", "true")\n'
        f'catalog = {lakehouse_name!r}\n'
        f'for table in {["compat_" + n.lower() for n in (*TABLES, MV)]!r}:\n'
        '    print(table, spark.read.table(f"{catalog}.{table}").count())\n'
        '# Normal mirror reads may fail with a commit-coordinator error; do not disable catalog commits.\n'
    )
    notebook = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {"kernelspec": {"display_name": "Synapse PySpark", "language": "python", "name": "synapse_pyspark"},
                     "language_info": {"name": "python"},
                     "trident": {"lakehouse": {"default_lakehouse": lakehouse,
                                              "default_lakehouse_name": lakehouse_name,
                                              "default_lakehouse_workspace_id": workspace,
                                              "known_lakehouses": [{"id": lakehouse}]}}},
        "cells": [{"cell_type": "code", "metadata": {}, "source": source.splitlines(keepends=True),
                   "outputs": [], "execution_count": None}],
    }
    return {"format": "ipynb", "parts": [encoded_part("notebook-content.ipynb", notebook)]}


class Workflow:
    def __init__(self, config, state, console, azure=None, fabric=None, databricks_factory=None, adls_factory=None, powerbi=None):
        self.config, self.state, self.console = config, state, console
        self.azure = azure or Azure(config)
        self.fabric = fabric or Fabric(config)
        self.db_factory = databricks_factory or (lambda host, profile: Databricks(host, profile, config["timeout"]))
        self.adls_factory = adls_factory or (lambda root: ADLS(root, config["tenant_id"], config["timeout"]))
        self.powerbi = powerbi
        self.marker = "guided-demo:" + state.data["deployment_id"]
        sample = dict(config["sample"])
        if "start_date" in sample:
            sample["start_date"] = date.fromisoformat(sample["start_date"])
        self.sample = StarSchemaConfig(**sample)
        self.tables = generate_star_schema(self.sample)
        self.reference = json_safe(validate_star_schema(self.tables, self.sample))

    def gate(self, key, target, instructions, link, recheck=None, always=False):
        fingerprint = digest(target)
        previous = self.state.data["gates"].get(key)
        if previous and previous["hash"] != fingerprint:
            raise DeploymentError("Manual checkpoint target changed. Do not reuse its previous attestation.")
        if not previous or always:
            self.console.output(f"\nMANUAL CHECKPOINT {key}\nTarget: {json.dumps(target, sort_keys=True)}")
            for i, instruction in enumerate(instructions, 1):
                self.console.output(f"{i}. {instruction}")
            self.console.output(link)
            if not self.console.confirm("Completed the exact actions and approved only the stated scope?"):
                raise Waiting(f"Waiting at {key}. Resume with the same config and state; no subsequent write was made.")
            self.state.data["gates"][key] = {"hash": fingerprint, "attestation": True,
                                             "observed_utc": datetime.now(timezone.utc).isoformat()}
            self.state.save()
        if recheck:
            recheck()

    def input(self, key, label, validator=guid):
        if key not in self.state.data["inputs"]:
            self.state.data["inputs"][key] = validator(self.console.ask(label))
            self.state.save()
        return self.state.data["inputs"][key]

    def preview(self):
        self.console.output("READ-ONLY PREVIEW. No writes, data loading or optional queries.")
        context = self.azure.preflight()
        self.console.output(json.dumps(context, sort_keys=True))
        seen = set()
        for demo, spec in self.config["demos"].items():
            self.console.output(f"{demo}: isolated schema(s) {spec['prefix']}; Fabric workspace {spec['workspace']['name']}.")
            for kind, target in spec["foundation"].items():
                key = digest({"kind": kind, "target": target})
                if key not in seen:
                    seen.add(key)
                    remote = self.azure.get(kind, self.azure.resource_id(kind, target), missing=True)
                    existing_step = self.state.data["steps"].get("azure/" + key)
                    if target["mode"] == "create" and remote and not existing_step:
                        raise DeploymentError("Azure creation name is already occupied. Select explicit reuse in a new config.")
                    if not existing_step:
                        preview = self.azure.preview(kind, target, self.state.data["deployment_id"])
                        self.console.output(json.dumps(preview, sort_keys=True))
                    else:
                        self.console.output(f"{kind}: recorded operation; resume only, not a new creation.")
            self.console.output("Required writes: workspace/items, isolated UC schema/tables/keys and deterministic initial data.")
            if demo != "power-bi":
                self.console.output("OneLake external-location/identity and mirror-connection manual checkpoints will follow item creation.")
            if demo == "compatibility":
                self.console.output("Two catalog-commit producer sets, two native MVs, SYNC UNIFORM output refreshes, five real ADLS output shortcuts and a bound notebook.")
            if demo == "power-bi":
                self.console.output("Native Catalog Explorer publication, consent and report creation remain manual (no connector substitution).")
        self.console.output(
            "COST WARNING: initial loading, SQL/serverless compute, MV creation/refresh, compatibility generation and "
            "Fabric capacity are deployment work and can incur charges. Capacity and predictive optimization/background "
            "refresh can continue billing after this process exits. No automatic suspension or deletion.\n"
            "https://azure.microsoft.com/pricing/details/microsoft-fabric/\n"
            "https://azure.microsoft.com/pricing/details/databricks/"
        )

    def foundation(self, demo, spec):
        resolved = {}
        for kind, target in spec["foundation"].items():
            resource_id = self.azure.resource_id(kind, target)
            key = "azure/" + digest({"kind": kind, "target": target})
            if target["mode"] == "reuse":
                remote = self.azure.get(kind, resource_id)
                if remote.get("properties", {}).get("provisioningState") != "Succeeded":
                    raise Waiting("Reused Azure resource is not ready.")
                resolved[kind] = self.azure.summary(kind, remote)
                selected = dict(resolved[kind], owned=False)
                previous = self.state.data["inputs"].get(key)
                if previous and previous != selected:
                    raise DeploymentError("Reused Azure resource identity changed. Inspect the exact immutable workspace/capacity IDs.")
                self.state.data["inputs"][key] = selected
                self.state.save()
            else:
                body = self.azure.body(kind, target, self.state.data["deployment_id"])
                resolved[kind] = self.state.step(
                    key, {"id": resource_id, "body": body},
                    lambda: self.azure.submit(kind, resource_id, body),
                    lambda handle: self.azure.finish(kind, handle),
                    lambda result: self.azure.reconcile(kind, result),
                )
        key = "capacity/" + digest(spec["foundation"]["capacity"])
        capacity_id = self.input(key, f"Fabric capacity GUID corresponding to Azure resource {resolved['capacity']['id']}")
        capacities = self.fabric.http.list("/capacities")
        capacity = next((c for c in capacities if c["id"] == capacity_id), None)
        if not capacity or capacity.get("state", "").lower() != "active":
            raise Waiting("Selected Fabric capacity is not visible/active. Wait for provisioning or obtain capacity access.")
        arm_name = resolved["capacity"]["id"].rsplit("/", 1)[-1]
        if capacity.get("displayName", "").casefold() != arm_name.casefold():
            raise DeploymentError("Fabric capacity name does not match the explicitly selected ARM resource.")
        self.gate(key + "/mapping", {"arm_id": resolved["capacity"]["id"], "fabric_id": capacity_id}, [
            "In Azure/Fabric capacity administration, verify this immutable Fabric capacity GUID belongs to the exact ARM resource and tenant.",
            "Verify matching regions, F SKU availability/quotas, license, capacity-assignment permission and tenant policies. What-If is not a quota or license guarantee.",
        ], "https://learn.microsoft.com/en-us/fabric/admin/capacity-settings")
        db = self.db_factory(resolved["databricks"]["host"], spec["profile"])
        self.gate(demo + "/uc", {"workspace_id": resolved["databricks"]["workspace_id"], "catalog": spec["catalog"]}, [
            "For a new workspace, have an account/metastore administrator attach a Unity Catalog metastore and create or explicitly approve the named parent catalog with suitable managed storage.",
            "Configure the selected Databricks unified-auth profile for this exact workspace. Verify UC, serverless SQL availability and USE CATALOG/CREATE SCHEMA; do not change other workloads.",
            "For OneLake, enable its workspace preview and use serverless or DBR 18.1+. For native MVs verify the required serverless entitlement. Obtain approval separately for any shared policy or permission change.",
        ], UC_DOC, lambda: db.get("catalogs", spec["catalog"]))
        warehouse = spec["warehouse"]
        if warehouse["mode"] == "reuse":
            warehouse_id = warehouse["id"]
        else:
            warehouse_id = self.state.step(
                demo + "/warehouse", warehouse, lambda: db.create_warehouse(warehouse),
                lambda handle: handle,
                lambda result: self.check_warehouse(db, result["id"], warehouse.get("name")),
            )["id"]
        self.check_warehouse(db, warehouse_id, warehouse.get("name"))
        return db, warehouse_id, capacity_id

    def check_warehouse(self, db, warehouse, expected_name=None):
        remote = db.warehouse(warehouse)
        if remote.get("id") != warehouse or (expected_name and remote.get("name") != expected_name):
            raise DeploymentError("SQL warehouse identity changed.")
        if remote.get("state") in ("DELETED", "DELETING"):
            raise Waiting("SQL warehouse is unavailable.")

    def workspace(self, demo, spec, capacity):
        target = spec["workspace"]
        if target["mode"] == "reuse":
            remote = self.fabric.get("/workspaces/" + target["id"])
            if remote.get("displayName") != target["name"] or remote.get("capacityId") != capacity:
                raise DeploymentError("Reused workspace name/capacity does not match. Shared assignments are never changed.")
            self.gate(demo + "/workspace-reuse", {"id": target["id"], "name": target["name"]}, [
                "Verify this workspace is dedicated to this demo and approve adding only the named new artifacts.",
                "Confirm workspace Contributor or higher access and capacity assignment. Existing artifacts will not be overwritten.",
            ], "https://learn.microsoft.com/en-us/fabric/fundamentals/roles-workspaces")
            return target["id"]
        body = {"displayName": target["name"], "description": self.marker, "capacityId": capacity}
        result = self.state.step(
            demo + "/workspace", body, lambda: self.fabric.submit("/workspaces", body), self.fabric.finish,
            lambda result: self.fabric.reconcile("/workspaces/" + result["id"], result, body),
        )
        self.fabric.reconcile("/workspaces/" + result["id"], result, body)
        return result["id"]

    def item(self, key, workspace, collection, item_type, display_name, definition=None, extra=None):
        body = {"displayName": display_name, "description": self.marker}
        if definition:
            body["definition"] = definition
        if extra:
            body.update(extra)
        path = f"/workspaces/{workspace}/{collection}"
        result = self.state.step(
            key, {"workspace": workspace, "body": body},
            lambda: self.fabric.submit(path, body), self.fabric.finish,
            lambda result: self.fabric.reconcile(f"/workspaces/{workspace}/items/{result['id']}", result,
                                                  {"displayName": display_name, "type": item_type, "description": self.marker}),
        )
        self.fabric.reconcile(f"/workspaces/{workspace}/items/{result['id']}", result,
                              {"displayName": display_name, "type": item_type, "description": self.marker})
        if definition:
            actual = self.fabric.definition(workspace, result["id"])
            expected_parts = {p["path"]: json.loads(base64.b64decode(p["payload"])) for p in definition["parts"]}
            actual_parts = {p["path"]: json.loads(base64.b64decode(p["payload"])) for p in actual["parts"]}
            for part, expected in expected_parts.items():
                if actual_parts.get(part) != expected:
                    raise DeploymentError("Fabric definition differs from the approved deployment payload.")
        return result["id"]

    def onelake_location(self, demo, suffix, spec, workspace, db):
        key = demo + "/" + suffix
        storage = self.item(key + "/storage", workspace, "azureDatabricksStorages", "AzureDatabricksStorage",
                            spec["prefix"] + "_" + suffix + "_storage")
        root = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{storage}/Files"
        external_name = spec["prefix"] + "_" + suffix + "_location"
        target = {"workspace": workspace, "item": storage, "identity": spec["identity_object_id"],
                  "identity_resource_id": spec["identity_resource_id"], "credential": spec["credential"],
                  "location": external_name, "url": root, "file_events": spec["file_events"]}
        self.gate(key + "/access", target, [
            "In Fabric Admin portal, verify Users can create Azure Databricks Storage items and Use short-lived user-delegated SAS tokens are allowed. Ask the tenant admin if blocked; this script does not change tenant settings.",
            f"In this workspace's Delegated settings > OneLake Settings, enable/verify Authenticate with OneLake user-delegated SAS tokens for this workspace only, with administrator approval.",
            "In Azure verify the access connector resource's managed identity object ID matches the target. In Fabric Manage access add ONLY that identity as Viewer; in the new Storage item Manage OneLake security add ONLY it to a ReadWrite role.",
            f"Use the explicitly chosen {spec['file_events']} file-events mode. Automatic needs Contributor and can create a chargeable eventstream; provided needs Member and a separately created eventstream. Approve those separately. For disabled mode confirm it is supported, not merely omitted.",
            "In Databricks Catalog Explorer, create/reuse the named writable, workspace-isolated Azure Managed Identity storage credential referencing exactly this access connector. Do not edit a shared credential.",
            f"Create the named external location with Storage type OneLake, this credential, the exact ID-based URL {root}/ and the selected file-events mode. Test connection and grant CREATE MANAGED STORAGE to the deploying identity narrowly on this location.",
        ], ONELAKE_DOC, lambda: self.check_location(db, external_name, root, spec["credential"], spec["file_events"]))
        self.state.data["inputs"][key + "/location"] = {"name": external_name, "url": root, "owned": False}
        self.state.save()
        return root + "/managed"

    def check_location(self, db, location, root, credential, file_events):
        remote = db.get("external-locations", location)
        if (remote.get("url", "").rstrip("/") != root or remote.get("credential_name") != credential
                or remote.get("read_only") is not False):
            raise DeploymentError("OneLake external location is not the selected writable location/credential.")
        if "enable_file_events" in remote and remote["enable_file_events"] != (file_events != "disabled"):
            raise DeploymentError("External location file-events setting differs from the selection.")
        metadata = db.get("storage-credentials", credential)
        if metadata.get("read_only") is not False:
            raise DeploymentError("Selected storage credential is not writable.")
        matching_specs = [s for s in self.config["demos"].values() if s.get("credential") == credential]
        if not matching_specs or metadata.get("azure_managed_identity", {}).get("access_connector_id") not in {
            s["identity_resource_id"] for s in matching_specs
        }:
            raise DeploymentError("Storage credential does not reference the explicitly selected access connector.")
        # Type-specific file-event configuration and role membership remain attestations.

    def uc_summary(self, db, kind, full_name):
        remote = db.get(kind, full_name)
        id_field = "schema_id" if kind == "schemas" else "table_id"
        if not remote.get(id_field):
            raise DeploymentError("UC metadata did not expose an immutable object ID.")
        if remote.get("comment") != self.marker:
            raise DeploymentError("UC object ownership marker differs from this deployment.")
        storage = remote.get("storage_location", remote.get("storage_root", ""))
        recorded = self.state.data["inputs"].get("uc-storage/" + full_name)
        return {"id": remote[id_field], "name": full_name, "type": remote.get("table_type", "SCHEMA"),
                "storage": storage or recorded or "", "ownership": self.marker,
                "storage_evidence": "API metadata" if storage else "operator-entered location" if recorded else "not exposed"}

    def uc_check(self, db, kind, expected):
        if self.uc_summary(db, kind, expected["name"]) != expected:
            raise DeploymentError("UC immutable identity, storage or ownership changed.")

    def sql_write(self, key, db, warehouse, statement, check=None):
        return self.state.step(key, {"warehouse": warehouse, "sql": statement},
                               lambda: db.submit_sql(warehouse, statement), db.finish_sql,
                               (lambda result: check()) if check else None)

    def uc_create(self, key, db, warehouse, kind, full_name, statement, storage_prefix=None):
        def submit():
            if db.get(kind, full_name, missing=True):
                raise DeploymentError("UC name collision. Never replace or adopt an existing object.")
            return db.submit_sql(warehouse, statement)

        def finish(handle):
            db.finish_sql(handle)
            result = self.uc_summary(db, kind, full_name)
            expected_type = "MATERIALIZED_VIEW" if statement.startswith("CREATE MATERIALIZED VIEW") else "MANAGED"
            if kind == "tables" and result["type"] != expected_type:
                raise DeploymentError("Created UC object is not the required managed table or native materialized view.")
            if storage_prefix and not result["storage"] and expected_type == "MATERIALIZED_VIEW":
                self.gate(key + "/backing-location", {"id": result["id"], "name": full_name, "expected_root": storage_prefix}, [
                    "The native MV API did not expose its backing storage location. Inspect Catalog Explorer/DESCRIBE EXTENDED or the actual pipeline/backing metadata for this exact MV.",
                    "Verify and provide the unsigned physical OneLake backing path below the selected managed root. Do not substitute the generated ADLS compatibility output or infer a path from a different table.",
                    "If this location cannot be established, answer no and preserve the pending create statement. A source backing-protocol gap remains separate even when the location is known.",
                ], ONELAKE_DOC)
                self.input("uc-storage/" + full_name, "Actual unsigned OneLake MV backing storage URL",
                           lambda value: url(value, ("onelake.dfs.fabric.microsoft.com",), "abfss"))
                result = self.uc_summary(db, kind, full_name)
            if storage_prefix and not result["storage"].rstrip("/").startswith(storage_prefix.rstrip("/") + "/"):
                # Schema storage_root may be exactly the supplied managed root.
                if kind != "schemas" or result["storage"].rstrip("/") != storage_prefix.rstrip("/"):
                    raise DeploymentError("UC source storage does not match the selected OneLake managed location; inspect the actual backing location.")
            return result

        return self.state.step(key, {"sql": statement, "warehouse": warehouse, "storage_prefix": storage_prefix}, submit, finish,
                               lambda result: self.uc_check(db, kind, result))

    def producer(self, demo, suffix, spec, db, warehouse, managed=None, commits=False):
        schema = spec["prefix"] + "_" + suffix
        namespace = spec["catalog"] + "." + schema
        key = demo + "/" + suffix
        statement = f"CREATE SCHEMA {qualified(spec['catalog'], schema)}"
        if managed:
            statement += f" MANAGED LOCATION {literal(managed)}"
        statement += f" COMMENT {literal(self.marker)}"
        self.uc_create(key + "/schema", db, warehouse, "schemas", namespace, statement, managed)
        for table in TABLES:
            full_name = namespace + "." + table
            result = self.uc_create(key + "/" + table, db, warehouse, "tables", full_name,
                                    create_table(spec["catalog"], schema, table, self.marker, commits), managed)
            if commits:
                remote = db.get("tables", full_name)
                if remote.get("properties", {}).get("delta.feature.catalogManaged") != "supported":
                    raise DeploymentError("Catalog commits were not reported as supported. No down-level producer substitution.")
            for index, statement in enumerate(inserts(qualified(spec["catalog"], schema, table), self.tables[table])):
                self.uc_check(db, "tables", result)
                self.sql_write(f"{key}/{table}/load/{index}", db, warehouse, statement,
                               lambda result=result: self.uc_check(db, "tables", result))
        return schema

    def mirror(self, demo, spec, workspace, db, schema, objects):
        objects = tuple(db.get("tables", spec["catalog"] + "." + schema + "." + table)["name"] for table in objects)
        key = demo + "/mirror-access"
        self.gate(key, {"workspace": workspace, "catalog": spec["catalog"], "schema": schema, "objects": list(objects)}, [
            "In Databricks have the metastore administrator verify external data access is enabled. Do not silently change the shared metastore.",
            "Grant only the chosen Fabric connection identity USE CATALOG, USE SCHEMA, EXTERNAL USE SCHEMA and SELECT on the selected objects, with explicit administrator approval.",
            "In Fabric Settings > Manage connections and gateways create/test an Azure Databricks cloud connection for this exact workspace with supported organizational/OAuth authentication. Supply credentials only in that UI.",
            "Record the connection GUID, not a token or credential-bearing URL. This script will create a Partial mirror selecting exactly these objects; it does not mirror the entire parent catalog.",
        ], MIRROR_DOC)
        connection = self.input(demo + "/mirror-connection", "Tested Fabric Databricks connection GUID")
        self.fabric.get("/connections/" + connection)
        definition = mirror_definition(spec["catalog"], schema, connection, objects)
        return self.item(demo + "/mirror", workspace, "mirroredAzureDatabricksCatalogs", "MirroredAzureDatabricksCatalog",
                         spec["prefix"] + "_mirror", definition)

    def powerbi_http(self):
        if self.powerbi is None:
            self.powerbi = HTTP("https://api.powerbi.com/v1.0/myorg",
                                lambda: {"Authorization": "Bearer " + azure_cli([
                                    "account", "get-access-token", "--resource", "https://analysis.windows.net/powerbi/api",
                                    "--tenant", self.config["tenant_id"]])["accessToken"]}, self.config["timeout"])
        return self.powerbi

    def native_powerbi(self, demo, spec, workspace, db, warehouse):
        schema = self.producer(demo, "powerbi", spec, db, warehouse)
        self.gate(demo + "/publish", {"workspace": workspace, "catalog": spec["catalog"], "schema": schema,
                                     "warehouse": warehouse, "mode": spec["storage_mode"]}, [
            "Verify home-tenant workspace access, Power BI license/Premium/PPU/Fabric entitlement and XMLA endpoint Read Write. Have an administrator approve Entra Databricks Dataset Publishing Integration consent if needed; do not change tenant settings silently.",
            "In Databricks Catalog Explorer select this independent schema's four tables and the selected UC SQL warehouse. Choose Use with BI tools > Publish to Power BI workspace.",
            f"Sign in with the home-tenant account; choose the exact target workspace, a NEW semantic model and {spec['storage_mode']} with OAuth. Approve Import/refresh charges if Import was explicitly selected.",
            "Before editing the model, capture all generated relationships (source/target columns, cardinality, active flag, filter direction). Record any inactive relationships or missing keys as observed, not repaired propagation.",
            "Finish native publication and record its semantic-model GUID. Do not substitute a manually connected model or overwrite an existing model.",
        ], PBI_DOC)
        model = self.input(demo + "/model", "Natively published semantic-model GUID")
        dataset = self.powerbi_http().request("GET", f"/groups/{workspace}/datasets/{model}").data
        if dataset.get("id") != model:
            raise DeploymentError("Published model immutable ID did not match the selected model.")
        self.gate(demo + "/report", {"workspace": workspace, "model": model}, [
            "Create a NEW report from this natively published model in the Power BI service.",
            "Add total sales and date, product-category and customer-region slicers. Save it in this workspace and record its report GUID.",
            "Do not change relationships to manufacture a propagation pass. Default setup verification checks report/model identity only; slicer behavior requires the separately confirmed optional checks.",
        ], "https://learn.microsoft.com/en-us/power-bi/create-reports/service-report-create-new")
        report = self.input(demo + "/report-id", "New report GUID")
        actual = self.powerbi_http().request("GET", f"/groups/{workspace}/reports/{report}").data
        if actual.get("datasetId") != model:
            raise DeploymentError("Report is not bound to the natively published model.")
        return {"workspace": workspace, "schema": schema, "model": model, "report": report,
                "publishing_origin": "operator-attested native Catalog Explorer", "relationships": "observed manually; not asserted"}

    def compatibility(self, demo, spec, workspace, db, warehouse):
        normal_root = self.onelake_location(demo, "normal", spec, workspace, db)
        compat_root = self.onelake_location(demo, "compat", spec, workspace, db)
        normal = self.producer(demo, "normal", spec, db, warehouse, normal_root, True)
        compat = self.producer(demo, "compat", spec, db, warehouse, compat_root, True)
        output_root = spec["output_root"].rstrip("/") + "/" + self.state.data["deployment_id"]
        outputs = {n: output_root + "/" + n.lower() for n in (*TABLES, MV)}
        adls = self.adls_factory(spec["output_root"])
        self.gate(demo + "/output-eligibility", {"paths": outputs, "external_location": spec["output_external_location"]}, [
            "In the named ADLS UC external location verify the deploying identity has CREATE EXTERNAL TABLE and the managed identity can read/write only this isolated output scope.",
            "In Azure Storage Explorer/portal create each exact listed output directory EMPTY, in the existing HNS-enabled account/container. Do not write data or Delta files there.",
            "Verify each destination and EVERY ancestor/descendant has not been used as another table's Compatibility Mode location within seven days. The script's unique UUID avoids collisions but does not prove this history; inspect deployment inventories/audits.",
            "Grant the Fabric ADLS connection identity read access to these generated outputs. Create/test a Fabric ADLS Gen2 connection in Manage connections and gateways with credentials only in the UI.",
        ], COMPAT_DOC, lambda: self.check_output_location(db, spec, outputs))
        connection = self.input(demo + "/adls-connection", "Tested Fabric ADLS Gen2 connection GUID")
        self.fabric.get("/connections/" + connection)
        for table in TABLES:
            key = demo + "/enable/" + table
            if key not in self.state.data["steps"]:
                adls.empty(outputs[table])
            statement = (f"ALTER TABLE {qualified(spec['catalog'], compat, table)} SET TBLPROPERTIES ("
                         + compatibility_properties(outputs[table]) + ")")
            self.sql_write(key, db, warehouse, statement)
        for schema in (normal, compat):
            output = outputs[MV] if schema == compat else None
            key = demo + "/" + schema + "/mv"
            if output and key not in self.state.data["steps"]:
                adls.empty(output)
            self.uc_create(key, db, warehouse, "tables", spec["catalog"] + "." + schema + "." + MV,
                           create_mv(spec["catalog"], schema, self.marker, output),
                           compat_root if schema == compat else normal_root)
            self.sql_write(key + "/refresh", db, warehouse,
                           f"REFRESH MATERIALIZED VIEW {qualified(spec['catalog'], schema, MV)}")
        for table in (*TABLES, MV):
            kind = "MATERIALIZED VIEW" if table == MV else "TABLE"
            self.sql_write(demo + "/uniform-refresh/" + table, db, warehouse,
                           f"REFRESH {kind} {qualified(spec['catalog'], compat, table)} SYNC UNIFORM")
        self.gate(demo + "/output-freshness", {"paths": outputs, "schema": spec["catalog"] + "." + compat}, [
            "In Databricks run DESCRIBE HISTORY for each managed producer and DESCRIBE EXTENDED for all five objects. Record Uniform Compatibility Information Last Refreshed Version and Last Refreshed timestamp; compare to the current source version, or the MV's actual backing version.",
            "Inspect the actual generated output Delta histories/logs in ADLS for all five paths. Record output ID, clone source/backing ID, source version, timestamp, reader/writer protocol and feature lists. Verify source ID/version equality, non-stale outputs and absence of catalogManaged in the generated copies; NEVER write to these paths.",
            "If an MV's source protocol cannot be inspected (including DESCRIBE DETAIL returning EXPECT_TABLE_NOT_VIEW), record that specific backing-protocol gap. Do not infer a protocol from the UC MV type or a table's protocol.",
            "If output generation/freshness is still pending, answer no and resume later. Initial generation can take an hour; the script does not extend a measured propagation deadline or regenerate/reload on resume.",
        ], COMPAT_DOC, lambda: self.check_generated_paths(db, spec["catalog"], compat, outputs))
        output_protocols = {table: adls.protocol(path) for table, path in outputs.items()}
        freshness = {table: self.freshness(demo, db, warehouse, spec["catalog"], compat, table)
                     for table in (*TABLES, MV)}
        previous = self.state.data["results"].get(demo, {}).get("output_protocols", {})
        for table, protocol in output_protocols.items():
            if table in previous and previous[table]["id"] != protocol["id"]:
                raise DeploymentError("Generated output immutable ID changed. Inspect its provenance before resuming.")
        provenance = {}
        for table in (*TABLES, MV):
            source = db.get("tables", spec["catalog"] + "." + compat + "." + table)
            source_id = (self.input(demo + "/backing-id", "Actual compatibility MV backing-table GUID from its metadata/log evidence")
                         if table == MV else source["table_id"])
            observed_clone_id = self.input(demo + "/clone-source/" + table,
                                           f"Observed generated {table} clone source/backing GUID from its Delta log")
            if observed_clone_id != source_id:
                raise DeploymentError("Output clone source ID does not match the actual producer/backing ID.")
            provenance[table] = {"source_id": source_id, "clone_source_id": observed_clone_id,
                                 "comparison": "matches; clone metadata entered by operator"}
        mirror = self.mirror(demo, spec, workspace, db, normal, (*TABLES, MV))
        lakehouse_name = spec["prefix"] + "_validation"
        lakehouse = self.item(demo + "/lakehouse", workspace, "lakehouses", "Lakehouse", lakehouse_name,
                              extra={"creationPayload": {"enableSchemas": False}})
        for table, path in outputs.items():
            parsed = urlsplit(path)
            body = {"path": "Tables", "name": "compat_" + table.lower(),
                    "target": {"adlsGen2": {"location": "https://" + parsed.hostname,
                                           "subpath": "/" + parsed.username + parsed.path,
                                           "connectionId": connection}}}
            self.shortcut(demo + "/shortcut/compat/" + table, workspace, lakehouse, body)
        self.gate(demo + "/normal-discovery", {"mirror": mirror, "schema": normal, "tables": list(TABLES)}, [
            "Open the normal mirror and inspect its actual discovered objects and shortcut paths. Wait for the four baseline tables; record normal MV discovery separately.",
            "The mirror intentionally selects the native normal MV too. If it is not exposed, record that discovery gap; do not route it to a compatibility output or invent a successful negative control.",
            "Supply the actual mirror Tables/<schema> folder path at the next prompt. Normal shortcut creation needs real discovered paths, not guessed source locations.",
        ], MIRROR_DOC)
        folder = self.input(demo + "/normal-folder", "Actual normal mirror folder (Tables/<schema>)", self.mirror_folder)
        shortcuts = self.fabric.http.list(f"/workspaces/{workspace}/items/{mirror}/shortcuts")
        for table in TABLES:
            found = [s for s in shortcuts if s.get("path") == folder and s.get("name", "").lower() == table.lower()]
            if len(found) != 1:
                raise Waiting("Normal table shortcut is not discoverable at the attested folder. No fabricated shortcut was created.")
            body = {"path": "Tables", "name": "normal_" + table.lower(),
                    "target": {"oneLake": {"workspaceId": workspace, "itemId": mirror,
                                          "path": folder + "/" + found[0]["name"]}}}
            self.shortcut(demo + "/shortcut/normal/" + table, workspace, lakehouse, body)
        mv_shortcuts = [s for s in shortcuts if s.get("path") == folder and s.get("name", "").lower() == MV]
        normal_mv = len(mv_shortcuts) == 1
        if normal_mv:
            body = {"path": "Tables", "name": "normal_" + MV,
                    "target": {"oneLake": {"workspaceId": workspace, "itemId": mirror, "path": folder + "/" + mv_shortcuts[0]["name"]}}}
            self.shortcut(demo + "/shortcut/normal/" + MV, workspace, lakehouse, body)
        notebook = self.item(demo + "/notebook", workspace, "notebooks", "Notebook", spec["prefix"] + "_inspect",
                             notebook_definition(workspace, lakehouse, lakehouse_name))
        return {"workspace": workspace, "schemas": [normal, compat], "mirror": mirror, "lakehouse": lakehouse,
                "notebook": notebook, "normal_mv_discovered": normal_mv, "outputs": outputs,
                "output_protocols": output_protocols,
                "output_provenance": provenance,
                "freshness_observations": freshness,
                "output_freshness": "version metadata checked; MV backing version/provenance remain operator-attested",
                "mv_backing_protocol": "not independently verified"}

    @staticmethod
    def mirror_folder(value):
        if not value.startswith("Tables/") or any(p in ("", ".", "..") for p in value.split("/")) or "%" in value:
            raise DeploymentError("Enter an actual Tables/<schema> folder without encoded/relative segments.")
        return value.rstrip("/")

    def check_output_location(self, db, spec, outputs):
        remote = db.get("external-locations", spec["output_external_location"])
        root = remote.get("url", "").rstrip("/") + "/"
        if not root.startswith("abfss://") or not all(p.startswith(root) for p in outputs.values()):
            raise DeploymentError("ADLS destinations are not beneath the explicitly selected UC external location.")
        if remote.get("read_only") is not False:
            raise DeploymentError("Output external location is not writable.")

    def check_generated_paths(self, db, catalog, schema, outputs):
        for table, expected in outputs.items():
            remote = db.get("tables", catalog + "." + schema + "." + table)
            if remote.get("properties", {}).get("delta.universalFormat.compatibility.location", "").rstrip("/") != expected:
                raise Waiting("UC compatibility destination does not match the selected actual output path. Inspect generation; no shortcut was created.")
            if table == MV:
                exposed = db.get("tables", catalog + "." + schema + "." + table, compatibility=True)
                if exposed.get("storage_location", "").rstrip("/") != expected:
                    raise Waiting("Native MV compatibility routing does not expose the expected generated output. No source-location substitute was used.")

    def freshness(self, demo, db, warehouse, catalog, schema, table):
        target = qualified(catalog, schema, table)
        extended = db.query(warehouse, f"DESCRIBE TABLE EXTENDED {target}")
        metadata = {str(row[0]).strip().lower().lstrip("# ").strip(): str(row[1]).strip()
                    for row in extended["rows"] if len(row) >= 2}
        refreshed = metadata.get("last refreshed version")
        timestamp = metadata.get("last refreshed")
        if table != MV:
            history = db.query(warehouse, f"DESCRIBE HISTORY {target} LIMIT 1")
            source_version = int(history["rows"][0][0]) if history["rows"] else None
        else:
            source_version = None
        mode = "SQL metadata"
        if source_version is None or refreshed is None or timestamp is None:
            self.gate(demo + "/freshness-metadata/" + table, {"object": catalog + "." + schema + "." + table}, [
                "The returned metadata did not expose all required version fields in the documented row shape, or this is a native MV whose backing version requires inspection.",
                "Inspect Catalog Explorer Uniform Compatibility Information and actual producer/backing history; record latest source/backing version, last-refreshed version and UTC timestamp at the following prompts.",
                "Record the backing-protocol gap separately. Matching numeric versions alone is not evidence of the backing Delta protocol. If generation is pending, answer no.",
            ], COMPAT_DOC, always=True)
            source_version = self.version(self.console.ask(f"{table} actual latest source/backing version"))
            refreshed = self.console.ask(f"{table} observed compatibility Last Refreshed Version")
            timestamp = self.console.ask(f"{table} observed Last Refreshed UTC (ISO 8601)")
            mode = "operator-entered metadata"
        refreshed_version = self.version(refreshed)
        try:
            moment = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            raise Waiting("Compatibility freshness timestamp is not an ISO UTC observation.") from None
        if moment.tzinfo is None or moment > datetime.now(timezone.utc):
            raise Waiting("Compatibility freshness timestamp is missing a timezone or is in the future.")
        if source_version != refreshed_version:
            raise Waiting("Compatibility output is stale relative to the current source/backing version. Wait or explicitly approve another refresh; resume never blindly refreshes.")
        return {"source_version": source_version, "refreshed_version": refreshed_version,
                "refreshed_utc": moment.isoformat(), "evidence": mode}

    @staticmethod
    def version(value):
        try:
            version = int(value)
        except (ValueError, TypeError):
            raise DeploymentError("A nonnegative actual Delta version is required.") from None
        if version < 0:
            raise DeploymentError("A nonnegative actual Delta version is required.")
        return version

    def shortcut(self, key, workspace, lakehouse, body):
        return self.state.step(key, body,
                               lambda: self.fabric.shortcut(workspace, lakehouse, body),
                               lambda handle: self.fabric.check_shortcut(handle, body),
                               lambda handle: self.fabric.check_shortcut(handle, body))

    def run_demo(self, demo):
        spec = self.config["demos"][demo]
        db, warehouse, capacity = self.foundation(demo, spec)
        workspace = self.workspace(demo, spec, capacity)
        if demo == "power-bi":
            result = self.native_powerbi(demo, spec, workspace, db, warehouse)
        elif demo == "onelake":
            managed = self.onelake_location(demo, "onelake", spec, workspace, db)
            schema = self.producer(demo, "onelake", spec, db, warehouse, managed)
            mirror = self.mirror(demo, spec, workspace, db, schema, TABLES)
            result = {"workspace": workspace, "schema": schema, "mirror": mirror}
        else:
            result = self.compatibility(demo, spec, workspace, db, warehouse)
        result.update(status="setup-checked", verification="resource identities, definitions and recorded deployment statements; manual gates are attestations; no end-to-end pass",
                      reference=self.reference)
        return result, db, warehouse

    def optional_checks(self, demo, result, db, warehouse):
        self.console.output("OPTIONAL END-TO-END COST WARNING: SQL scans, Spark, Power BI/report tests and any append/refresh can incur extra charges.")
        if not self.console.confirm(f"Run optional end-to-end checks for {demo} now?"):
            result["optional_checks"] = "not run; no affirmative consent"
            return
        schema = result.get("schema") or result["schemas"][0]
        fact = qualified(self.config["demos"][demo]["catalog"], schema, "FactSales")
        observed = db.query(warehouse, f"SELECT COUNT(*), SUM(SalesAmount) FROM {fact}")["rows"]
        expected = [[str(self.sample.sales_rows), str(self.reference["sales_total"])]]
        if len(observed) != 1 or int(observed[0][0]) != int(expected[0][0]) or Decimal(str(observed[0][1])) != Decimal(expected[0][1]):
            raise DeploymentError("Optional Databricks baseline count/total check failed.")
        result["optional_checks"] = {"databricks_baseline": "pass", "fabric_sql_spark_powerbi_append_propagation": "not independently automated"}
        self.gate(demo + "/optional-reader-checks", {"workspace": result["workspace"], "reference": self.reference}, [
            "Query the intended Fabric SQL path and compare all four baseline tables/keys/full aggregates with the shared deterministic reference (not just a count). For compatibility, compare the full native MV grain and inspect resolved generated paths/protocol/freshness separately.",
            "For compatibility, run the bound inspection notebook only now that compute was approved; counts alone are not full data equality. Perform full row/hash comparisons, record normal commit-coordinator errors separately and do not disable catalog commits.",
            "For Power BI, verify the unedited relationships and combined date/category/region slicer results against the independent reference. Record actual filter selections and any inactive relationship or missing propagation as a limitation.",
            "If append/propagation is desired, obtain separate approval for source mutation, use the existing guarded append helper with this exact sample config and new key range, record source commit time and the original deadline; observe exact before/after equality in the intended reader. Import requires an explicitly approved refresh. Do not blindly rerun an append or reset a late deadline.",
            "Record pass/fail/blocked values and timestamps in your private evidence. This manual checkpoint records completion only, not an independently verified end-to-end pass.",
        ], "https://learn.microsoft.com/en-us/fabric/mirroring/azure-databricks", always=True)
        result["optional_checks"]["reader_evidence"] = "operator-attested; inspect private evidence for individual outcomes"

    def run(self, optional=False):
        self.preview()
        self.gate("preflight-policy", {"tenant": self.config["tenant_id"], "subscription": self.config["subscription_id"]}, [
            "Review the What-If output and exact proposed scope. Verify regional feature/SKU availability, quotas, policy/network requirements and Azure/Fabric/UC permissions with administrators; unreadable prerequisites are blockers, not passes.",
            "Approve only the selected infrastructure creations/reuse and isolated demo writes. No shared tenant policy, permission, identity or connection is silently modified.",
        ], AZURE_DOC)
        if not self.console.confirm("Apply the displayed cloud writes, required data loading/refresh and continuing cost risks?"):
            raise Waiting("Apply not approved. No provisioning or data write was made.")
        results = {}
        for demo in DEMOS:
            if demo not in self.config["demos"]:
                continue
            try:
                result, db, warehouse = self.run_demo(demo)
                if optional:
                    self.optional_checks(demo, result, db, warehouse)
                else:
                    result["optional_checks"] = "not requested"
            except Waiting as error:
                result = {"status": "waiting", "reason": str(error)}
            except DeploymentError as error:
                result = {"status": "blocked", "reason": str(error)}
            results[demo] = result
            self.state.data["results"][demo] = result
            self.state.save()
            summary = {k: v for k, v in result.items() if k not in ("reference", "output_protocols", "output_provenance", "freshness_observations")}
            self.console.output(f"{demo}: {json.dumps(summary, sort_keys=True)}")
        return results

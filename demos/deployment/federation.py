"""Read-only OneLake catalog federation, distinct from writable OneLake storage."""

import base64
from datetime import datetime, timezone
import json
from pathlib import Path
from urllib.parse import quote

from demos.common import star_schema
from .safety import DeploymentError, Waiting, guid
from .sql import ident, literal, qualified

DOC = "https://learn.microsoft.com/en-us/azure/databricks/query-federation/onelake"
ASSETS = Path(__file__).resolve().parents[1] / "onelake-federation"


def connection_sql(name, workspace, credential, marker):
    guid(workspace)
    return (f"CREATE CONNECTION {ident(name)} TYPE onelake OPTIONS "
            f"(workspace {literal(workspace)}, credential {literal(credential)}) COMMENT {literal(marker)}")


def catalog_sql(catalog, connection, item, marker):
    guid(item)
    return (f"CREATE FOREIGN CATALOG {ident(catalog)} USING CONNECTION {ident(connection)} "
            f"COMMENT {literal(marker)} "
            f"OPTIONS (data_item {literal(item)}, item_type 'Lakehouse', "
            f"create_volume_for_lakehouse_files 'false')")


def notebook(workspace, lakehouse, workspace_name, lakehouse_name, sample, marker, operation):
    parameters = "\n".join(f"{key} = {value!r}" for key, value in {
        "workspace_name": workspace_name, "lakehouse_name": lakehouse_name,
        "sample_config": sample, "deployment_marker": marker, "operation": operation,
    }.items()) + "\n"
    source = Path(star_schema.__file__).read_text(encoding="utf-8")
    body = {
        "nbformat": 4, "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "Synapse PySpark", "language": "python", "name": "synapse_pyspark"},
            "language_info": {"name": "python"},
            "trident": {"lakehouse": {
                "default_lakehouse": lakehouse, "default_lakehouse_name": lakehouse_name,
                "default_lakehouse_workspace_id": workspace, "known_lakehouses": [{"id": lakehouse}],
            }},
        },
        "cells": [{"cell_type": "code", "metadata": {}, "source": text.splitlines(keepends=True),
                   "outputs": [], "execution_count": None}
                  for text in (parameters, source, (ASSETS / "producer.py").read_text(encoding="utf-8"))],
    }
    return {"format": "ipynb", "parts": [{"path": "notebook-content.ipynb",
            "payload": base64.b64encode(json.dumps(body).encode()).decode(), "payloadType": "InlineBase64"}]}


def run_notebook(flow, key, workspace, item):
    http = flow.fabric.http
    root = f"/workspaces/{workspace}/items/{item}/jobs/instances"

    def submit():
        recent = http.list(root)
        now = datetime.now(timezone.utc)
        for job in recent:
            started = datetime.fromisoformat(job["startTimeUtc"].replace("Z", "+00:00"))
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            if (now - started).total_seconds() < 300 or job.get("status") in ("InProgress", "NotStarted"):
                raise Waiting("Recent notebook run exists. Reconcile it rather than submitting a duplicate.")
        response = http.request("POST", root + "?jobType=RunNotebook", {})
        location = response.headers.get("location")
        if not location:
            raise DeploymentError("Notebook accepted without job Location. Inspect job history; do not retry.")
        http.endpoint(location)
        return {"location": location}

    def finish(handle):
        deadline = http.clock() + http.timeout
        while True:
            result = http.request("GET", handle["location"], deadline=deadline).data
            if result.get("status") == "Completed":
                return {"job_id": result["id"], "status": "Completed",
                        "start_utc": result.get("startTimeUtc"), "end_utc": result.get("endTimeUtc")}
            if result.get("status") in ("Failed", "Cancelled", "Deduped"):
                raise DeploymentError("Producer notebook failed; inspect job " + result["id"])
            http.delay({"retry-after": "20"}, deadline)

    def reconcile(result):
        live = http.request("GET", root + "/" + result["job_id"]).data
        if live.get("status") != "Completed":
            raise DeploymentError("Recorded producer job no longer reports Completed.")
    return flow.state.step(key, {"workspace": workspace, "item": item}, submit, finish, reconcile)


def metadata(db, kind, name, options, marker=None):
    remote = db.get(kind, name)
    if remote.get("name") != name:
        raise DeploymentError("Federation object name mismatch.")
    if kind == "connections" and remote.get("connection_type") != "ONELAKE":
        raise DeploymentError("Selected connection is not OneLake.")
    if kind == "catalogs" and remote.get("catalog_type") != "FOREIGN_CATALOG":
        raise DeploymentError("Selected catalog is not a FOREIGN CATALOG.")
    actual = remote.get("options", {})
    if any(str(actual.get(k)).lower() != str(v).lower() for k, v in options.items()):
        raise DeploymentError("Federation object targets a different workspace, credential or data item.")
    if marker is not None and remote.get("comment") != marker:
        raise DeploymentError("Federation ownership differs.")
    object_id = remote.get("connection_id") if kind == "connections" else remote.get("id")
    if kind == "catalogs" and not object_id and remote.get("metastore_id") and remote.get("created_at"):
        # Catalogs API does not expose a UUID; retain its API creation identity.
        object_id = f"{remote['metastore_id']}:{name}:{remote['created_at']}"
    if not object_id:
        raise DeploymentError("Federation immutable ID is not exposed.")
    return {"id": object_id, "name": name, "options": options, "ownership": marker}


def create(flow, db, warehouse, kind, name, statement, options):
    def submit():
        if db.get(kind, name, missing=True):
            raise DeploymentError("Federation name collision; never adopt or overwrite.")
        return db.submit_sql(warehouse, statement)
    def finish(handle):
        db.finish_sql(handle)
        return metadata(db, kind, name, options, flow.marker)
    def reconcile(result):
        if metadata(db, kind, name, options, flow.marker) != result:
            raise DeploymentError("Federation immutable identity changed.")
    return flow.state.step("federation/" + kind, {"sql": statement, "warehouse": warehouse},
                           submit, finish, reconcile)


def deploy(flow, spec, workspace, db, warehouse):
    connector = flow.azure.http.request("GET", spec["identity_resource_id"] + "?api-version=2023-05-01").data
    identity = connector.get("identity", {})
    if (identity.get("principalId") != spec["identity_object_id"]
            or identity.get("tenantId", "").lower() != flow.config["tenant_id"].lower()):
        raise DeploymentError("Access connector identity/tenant differs.")
    credential = db.get("storage-credentials", spec["credential"])
    if (credential.get("azure_managed_identity", {}).get("access_connector_id", "").lower()
            != spec["identity_resource_id"].lower() or credential.get("read_only") is not True):
        raise DeploymentError("Select a verified read-only managed-identity credential. Shared credentials are never changed.")
    lakehouse_name = spec["prefix"] + "_lakehouse"
    lakehouse = flow.item("federation/lakehouse", workspace, "lakehouses", "Lakehouse", lakehouse_name,
                         extra={"creationPayload": {"enableSchemas": True}})
    flow.gate("federation/access", {"workspace": workspace, "lakehouse": lakehouse,
              "principal": spec["identity_object_id"], "credential_id": credential["id"]}, [
        "An administrator must verify Service principals can call Fabric public APIs; Allow apps running outside of Fabric to access data via OneLake; and Use short-lived user-delegated SAS tokens. Do not silently change tenant policies.",
        "Enable Authenticate with OneLake user-delegated SAS tokens in this workspace's Delegated settings > OneLake settings only.",
        "Assign only the selected managed identity workspace Viewer, then grant Read all OneLake data (Read all with Apache Spark) on this Lakehouse. SQL endpoint read-only permissions are insufficient. Do not grant Contributor unless item-scoped access is unsupported and separately approved.",
        "Verify network access; for restricted same-tenant workspaces add only this connector's resource instance rule with admin approval, otherwise arrange the documented private endpoint.",
    ], DOC)
    notebooks = {}
    for operation in ("baseline", "append"):
        notebooks[operation] = flow.item(
            "federation/notebook/" + operation, workspace, "items", "Notebook",
            spec["prefix"] + "_" + operation,
            notebook(workspace, lakehouse, spec["workspace"]["name"], lakehouse_name,
                     flow.config["sample"], flow.marker, operation), extra={"type": "Notebook"})
    source = run_notebook(flow, "federation/baseline-run", workspace, notebooks["baseline"])
    name = spec["connection"]["name"]
    options = {"workspace": workspace, "credential": spec["credential"]}
    if spec["connection"]["mode"] == "reuse":
        connection = metadata(db, "connections", name, options)
        key = "federation/reused-connection"
        previous = flow.state.data["inputs"].get(key)
        if previous and previous != connection:
            raise DeploymentError("Reused connection immutable identity changed.")
        flow.state.data["inputs"][key] = connection
        flow.state.save()
    else:
        connection = create(flow, db, warehouse, "connections", name,
                            connection_sql(name, workspace, spec["credential"], flow.marker), options)
    catalog = create(flow, db, warehouse, "catalogs", spec["catalog"],
                     catalog_sql(spec["catalog"], name, lakehouse, flow.marker),
                     {"data_item": lakehouse})
    remote = db.get("catalogs", spec["catalog"])
    if remote.get("connection_name") != name:
        raise DeploymentError("Foreign catalog connection differs.")
    return {"workspace": workspace, "lakehouse": lakehouse, "notebooks": notebooks,
            "connection": connection, "foreign_catalog": catalog, "baseline_job": source}


def discover(db, warehouse, catalog):
    schemas = db.query(warehouse, f"SHOW SCHEMAS IN {ident(catalog)}")["rows"]
    found = {}
    for row in schemas:
        schema = row[0]
        if schema.lower() == "information_schema":
            continue
        result = db.query(warehouse, f"SHOW TABLES IN {qualified(catalog, schema)}")
        columns = [c.lower() for c in result["columns"]]
        index = columns.index("tablename")
        for entry in result["rows"]:
            table = entry[index]
            canonical = next((t for t in star_schema.TABLE_COLUMN_TYPES if t.lower() == table.lower()), None)
            if canonical:
                if canonical in found:
                    raise DeploymentError("Federated table discovery is ambiguous.")
                found[canonical] = [schema, table]
    if set(found) != set(star_schema.TABLE_COLUMN_TYPES):
        raise Waiting("Not all four federated tables are visible yet.")
    return found


def compare_sql(db, warehouse, catalog, discovered, expected):
    statements = {}
    for table, rows in expected.items():
        columns = [c for c, _ in star_schema.TABLE_COLUMN_TYPES[table]]
        target = qualified(catalog, *discovered[table])
        result = db.query(warehouse, f"SELECT {', '.join(ident(c) for c in columns)} FROM {target} ORDER BY {ident(columns[0])}")
        statements[table] = result["statement_id"]
        if len(result["rows"]) != len(rows):
            raise DeploymentError("Federated row count differs: " + table)
        for actual, wanted in zip(result["rows"], sorted(rows, key=lambda r: r[columns[0]])):
            if len(actual) != len(columns):
                raise DeploymentError("Federated row shape differs: " + table)
            for column, value in zip(columns, actual):
                reference = star_schema.json_safe(wanted[column])
                if isinstance(reference, bool):
                    matches = str(value).lower() == str(reference).lower()
                else:
                    matches = str(value) == str(reference)
                if not matches:
                    raise DeploymentError("Federated exact row comparison differs: " + table + "." + column)
    return {"status": "pass", "observed_utc": datetime.now(timezone.utc).isoformat(),
            "statement_ids": statements,
            "comparison": "all columns and rows in four tables; independent deterministic reference"}


def optional_checks(flow, result, db, warehouse):
    catalog = result["foreign_catalog"]["name"]
    discovered = discover(db, warehouse, catalog)
    result["optional_checks"] = {"sql_baseline": compare_sql(db, warehouse, catalog, discovered, flow.tables)}
    flow.state.data["results"]["federation"] = result
    flow.state.save()
    flow.gate("federation/spark-evidence", {"catalog": catalog, "tables": discovered}, [
        "Run the read-only Spark example with DBR 18.0+ standard access mode. Compare every row/column against the same independent reference, not just counts. Record runtime/access mode and actual observations in private evidence.",
        "This checkpoint is operator-attested, not an automated Spark pass. Default deployment never triggers an append.",
    ], DOC, always=True)
    result["optional_checks"]["spark_baseline"] = "operator-attested; no automated pass"
    if not flow.console.confirm("Perform ONE guarded 10-row Fabric append, incurring compute, and verify visibility?"):
        result["optional_checks"]["append"] = "not approved"
        return
    job = run_notebook(flow, "federation/append-run", result["workspace"], result["notebooks"]["append"])
    result["optional_checks"]["append_job"] = job
    result["optional_checks"]["append"] = "source job completed; commit timestamp and SQL/Spark visibility not yet verified"
    flow.state.data["results"]["federation"] = result
    flow.state.save()
    flow.gate("federation/append-evidence", {"job": job, "catalog": catalog}, [
        "Retrieve the append notebook exit value containing the actual Delta source_commit_utc. Preserve that timestamp and a single fixed observation deadline even on resume.",
        "Compare all rows in both SQL and Spark against baseline plus generate_sales_batch(sample, sample.sales_rows + 1, 10). Record actual observation times and pass/fail/blocked individually. A completed job alone does not prove visibility.",
    ], DOC, always=True)

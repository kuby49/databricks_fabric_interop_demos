"""Supported REST/SQL operations with bounded waits and no automatic write retry."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .safety import DeploymentError, Waiting, guid, url

ARM = "https://management.azure.com"
FABRIC = "https://api.fabric.microsoft.com/v1"
VERSIONS = {"capacity": "2023-11-01", "databricks": "2024-05-01"}
TYPES = {"capacity": "Microsoft.Fabric/capacities", "databricks": "Microsoft.Databricks/workspaces"}


def notebook_content(body):
    """Compare executable content while allowing Fabric UI-generated metadata."""
    metadata = body["metadata"]
    bindings = [metadata[key]["lakehouse"] for key in ("trident", "dependencies")
                if "lakehouse" in metadata.get(key, {})]
    if bindings and any(binding != bindings[0] for binding in bindings):
        raise DeploymentError("Notebook contains conflicting Lakehouse bindings.")
    return {
        "kernelspec": metadata["kernelspec"],
        "lakehouse": bindings[0] if bindings else None,
        "cells": [{"type": cell["cell_type"], "source": "".join(cell["source"]),
                   "metadata": {key: value for key, value in cell.get("metadata", {}).items()
                                if key != "microsoft"}}
                  for cell in body["cells"]],
    }


def azure_cli(arguments):
    executable = shutil.which("az")
    if not executable:
        raise DeploymentError("Install Azure CLI and sign in separately to the selected tenant.")
    try:
        result = subprocess.run([executable, *arguments, "--only-show-errors", "-o", "json"],
                                capture_output=True, text=True, timeout=180, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise DeploymentError("Azure CLI failed or timed out. Inspect your separate CLI session.") from None
    if result.returncode:
        raise DeploymentError("Azure CLI rejected the read/preview. Check sign-in, permissions and policy in your separate CLI session.")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        raise DeploymentError("Azure CLI returned an unexpected response.") from None


@dataclass
class Response:
    status: int
    data: dict
    headers: dict
    text: str = ""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def wire(method, endpoint, headers, body, timeout):
    request = Request(endpoint, method=method, headers=headers,
                      data=json.dumps(body).encode() if body is not None else None)
    try:
        with build_opener(NoRedirect()).open(request, timeout=timeout) as result:
            payload = result.read()
            headers = {k.lower(): v for k, v in result.headers.items()}
            if not payload:
                return Response(result.status, {}, headers)
            text = payload.decode("utf-8")
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                return Response(result.status, {}, headers, text)
            return Response(result.status, data, headers)
    except HTTPError as error:
        # Service messages can echo credentials or signed storage locations.
        headers = {k.lower(): v for k, v in error.headers.items()}
        status = error.code
        error.close()
        return Response(status, {}, headers)
    except (URLError, TimeoutError, OSError):
        raise DeploymentError("Network failure; a write may have been accepted. Resume only with a recorded operation ID.") from None
    except json.JSONDecodeError:
        raise DeploymentError("Unexpected service response; inspect the recorded operation before retrying.") from None


class HTTP:
    def __init__(self, root, authenticate, timeout=900, send=wire, clock=time.monotonic, sleep=time.sleep):
        self.root, self.authenticate, self.timeout = root.rstrip("/"), authenticate, timeout
        self.send, self.clock, self.sleep = send, clock, sleep

    def endpoint(self, path):
        endpoint = path if path.startswith("https://") else self.root + "/" + path.lstrip("/")
        parsed = urlsplit(endpoint)
        root = urlsplit(self.root)
        if (parsed.scheme != "https" or parsed.hostname != root.hostname or parsed.username
                or parsed.password or parsed.port or parsed.fragment):
            raise DeploymentError("Service attempted to redirect authentication outside its origin.")
        if any(key.lower() in ("sig", "token", "password", "key", "access_token")
               for key, _ in parse_qsl(parsed.query)):
            raise DeploymentError("Signed operation URLs are not accepted.")
        return endpoint

    def delay(self, headers, deadline):
        retry = headers.get("retry-after", "2")
        try:
            seconds = float(retry)
        except ValueError:
            try:
                seconds = (parsedate_to_datetime(retry) - datetime.now(timezone.utc)).total_seconds()
            except (ValueError, TypeError):
                raise DeploymentError("Invalid Retry-After response.") from None
        seconds = max(0.1, seconds)
        if self.clock() + seconds >= deadline:
            raise Waiting("Operation is pending or throttled; bounded wait expired. Resume using recorded state.")
        self.sleep(seconds)

    def request(self, method, path, body=None, missing=False, deadline=None):
        deadline = deadline if deadline is not None else self.clock() + self.timeout
        endpoint = self.endpoint(path)
        while self.clock() < deadline:
            headers = dict(self.authenticate())
            headers["Content-Type"] = "application/json"
            result = self.send(method, endpoint, headers, body, min(60, max(1, deadline - self.clock())))
            if result.status == 429 or (method == "GET" and result.status in (500, 502, 503, 504)):
                self.delay(result.headers, deadline)
                continue
            if missing and result.status == 404:
                return None
            if not 200 <= result.status < 300:
                explanation = {401: "authentication expired", 403: "permission or policy denied",
                               404: "resource not found", 409: "resource collision; do not adopt or overwrite"}.get(
                                   result.status, "request rejected or uncertain; inspect service operation records")
                raise DeploymentError(f"HTTP {result.status}: {explanation}. No write was automatically retried.")
            return result
        raise Waiting("Bounded service wait expired.")

    def list(self, path):
        deadline = self.clock() + self.timeout
        seen = set()
        values = []
        while path:
            endpoint = self.endpoint(path)
            if endpoint in seen:
                raise DeploymentError("Service returned a pagination loop.")
            seen.add(endpoint)
            body = self.request("GET", path, deadline=deadline).data
            values.extend(body.get("value", []))
            path = body.get("continuationUri") or body.get("nextLink")
        return values


class Azure:
    def __init__(self, config, http=None, cli=azure_cli):
        self.config, self.cli = config, cli
        self.http = http or HTTP(ARM, self.authenticate, config["timeout"])

    def authenticate(self):
        token = self.cli(["account", "get-access-token", "--resource", ARM,
                          "--subscription", self.config["subscription_id"]])
        return {"Authorization": "Bearer " + token["accessToken"]}

    def preflight(self):
        account = self.cli(["account", "show", "--subscription", self.config["subscription_id"]])
        if (account["tenantId"].lower() != self.config["tenant_id"].lower()
                or account["id"].lower() != self.config["subscription_id"].lower()):
            raise DeploymentError("Azure context does not match the explicitly selected tenant/subscription.")
        group = self.http.request("GET", self.group_id + "?api-version=2022-09-01").data
        if group["id"].lower() != self.group_id.lower():
            raise DeploymentError("Resource group identity mismatch.")
        availability = {}
        for namespace in ("Microsoft.Fabric", "Microsoft.Databricks"):
            provider = self.http.request("GET", f"/subscriptions/{self.config['subscription_id']}/providers/"
                                         f"{namespace}?api-version=2021-04-01").data
            if provider.get("registrationState") != "Registered":
                raise DeploymentError(f"{namespace} is not registered. Ask an administrator; registration is not automatic.")
            expected_type = "capacities" if namespace == "Microsoft.Fabric" else "workspaces"
            locations = [location for t in provider.get("resourceTypes", []) if t.get("resourceType") == expected_type
                         for location in t.get("locations", [])]
            if locations and self.config["region"] not in {v.lower().replace(" ", "") for v in locations}:
                raise DeploymentError(f"{namespace} does not list the selected region. Verify availability before provisioning.")
            availability[namespace] = "region listed by provider" if locations else "region availability requires operator verification"
        return {"tenant_id": account["tenantId"], "subscription_id": account["id"], "resource_group_id": group["id"],
                "availability": availability, "quotas_licenses_policies": "requires administrator verification and What-If"}

    @property
    def group_id(self):
        return f"/subscriptions/{self.config['subscription_id']}/resourceGroups/{self.config['resource_group']}"

    def resource_id(self, kind, target):
        return target["resource_id"] if target["mode"] == "reuse" else f"{self.group_id}/providers/{TYPES[kind]}/{target['name']}"

    def get(self, kind, resource_id, missing=False, deadline=None):
        response = self.http.request("GET", resource_id + "?api-version=" + VERSIONS[kind], missing=missing, deadline=deadline)
        return response.data if response else None

    def body(self, kind, target, marker):
        body = {"location": self.config["region"], "sku": {"name": target["sku"]},
                "tags": {"demo-deployment": marker}}
        if kind == "capacity":
            body["sku"]["tier"] = "Fabric"
            body["properties"] = {"administration": {"members": [p.strip() for p in target["administrators"]]}}
        else:
            if target["networking"] != "public":
                raise Waiting("Custom/private networking: provision the workspace with the approved network design, then use a new reuse configuration. No speculative networking was deployed.")
            body["properties"] = {
                "managedResourceGroupId": f"/subscriptions/{self.config['subscription_id']}/resourceGroups/{target['managed_group']}",
                "publicNetworkAccess": "Enabled", "parameters": {"enableNoPublicIp": {"value": True}},
            }
        return body

    def preview(self, kind, target, marker):
        if target["mode"] == "reuse":
            self.get(kind, self.resource_id(kind, target))
            return {"action": "reuse", "id": self.resource_id(kind, target)}
        resource = dict(self.body(kind, target, marker), type=TYPES[kind], apiVersion=VERSIONS[kind], name=target["name"])
        template = {"$schema": "https://schema.management.azure.com/schemas/2019-04-01/deploymentTemplate.json#",
                    "contentVersion": "1.0.0.0", "resources": [resource]}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "template.json"
            path.write_text(json.dumps(template), encoding="utf-8")
            return self.cli(["deployment", "group", "what-if", "--subscription", self.config["subscription_id"],
                             "--resource-group", self.config["resource_group"], "--name", marker,
                             "--template-file", str(path), "--no-pretty-print"])

    def submit(self, kind, resource_id, body):
        if self.get(kind, resource_id, missing=True):
            raise DeploymentError("Azure resource already exists. Select explicit reuse; no resource was adopted.")
        response = self.http.request("PUT", resource_id + "?api-version=" + VERSIONS[kind], body)
        operation = response.headers.get("azure-asyncoperation") or response.headers.get("location")
        if operation:
            self.http.endpoint(operation)
        return {"id": resource_id, "operation": operation, "ownership": body["tags"]["demo-deployment"]}

    def finish(self, kind, handle):
        deadline = self.http.clock() + self.config["timeout"]
        while handle.get("operation"):
            response = self.http.request("GET", handle["operation"], deadline=deadline)
            status = response.data.get("status") or response.data.get("properties", {}).get("provisioningState")
            if status == "Succeeded":
                break
            if status in ("Failed", "Canceled", "Cancelled"):
                raise DeploymentError("Azure provisioning failed; inspect the saved operation in Azure.")
            self.http.delay(response.headers, deadline)
        while True:
            remote = self.get(kind, handle["id"], deadline=deadline)
            status = remote.get("properties", {}).get("provisioningState")
            if status == "Succeeded":
                summary = self.summary(kind, remote)
                if handle.get("ownership") and summary["ownership"] != handle["ownership"]:
                    raise DeploymentError("Azure provisioning ownership differs from the recorded create intent.")
                return summary
            if status in ("Failed", "Canceled", "Cancelled"):
                raise DeploymentError("Azure resource provisioning failed.")
            self.http.delay({}, deadline)

    def summary(self, kind, remote):
        result = {"id": remote["id"], "location": remote["location"], "type": remote["type"],
                  "ownership": remote.get("tags", {}).get("demo-deployment")}
        if kind == "databricks":
            result["host"] = url("https://" + remote["properties"]["workspaceUrl"], ("azuredatabricks.net",))
            result["workspace_id"] = str(remote["properties"]["workspaceId"])
        return result

    def reconcile(self, kind, expected):
        actual = self.summary(kind, self.get(kind, expected["id"]))
        if actual != expected:
            raise DeploymentError("Azure resource identity or ownership changed; stop and inspect.")


class Fabric:
    def __init__(self, config, http=None, cli=azure_cli):
        self.config, self.cli = config, cli
        self.http = http or HTTP(FABRIC, self.authenticate, config["timeout"])

    def authenticate(self):
        token = self.cli(["account", "get-access-token", "--resource", "https://api.fabric.microsoft.com",
                          "--tenant", self.config["tenant_id"]])
        return {"Authorization": "Bearer " + token["accessToken"]}

    def get(self, path, missing=False):
        response = self.http.request("GET", path, missing=missing)
        return response.data if response else None

    def submit(self, collection, body):
        candidates = self.http.list(collection if collection == "/workspaces" else collection.rsplit("/", 1)[0] + "/items")
        if any(item.get("displayName", "").casefold() == body["displayName"].casefold() for item in candidates):
            raise DeploymentError("Fabric name collision. Use a new name; existing items are never adopted.")
        response = self.http.request("POST", collection, body)
        if response.status == 202:
            operation_id = response.headers.get("x-ms-operation-id")
            if not operation_id:
                raise DeploymentError("Accepted create without operation ID. Do not retry; inspect Fabric operations.")
            return {"operation_id": guid(operation_id), "collection": collection}
        if not response.data.get("id"):
            raise DeploymentError("Create returned no resource ID. Inspect the write rather than adopting by name.")
        return {"id": guid(response.data["id"]), "collection": collection}

    def finish(self, handle):
        if handle.get("operation_id"):
            deadline = self.http.clock() + self.config["timeout"]
            path = "/operations/" + handle["operation_id"]
            while True:
                response = self.http.request("GET", path, deadline=deadline)
                status = response.data.get("status")
                if status == "Succeeded":
                    result = self.get(path + "/result")
                    if not result.get("id"):
                        raise DeploymentError("Operation result did not identify the created resource.")
                    return {"id": guid(result["id"])}
                if status in ("Failed", "Canceled", "Cancelled"):
                    raise DeploymentError("Fabric operation failed. Inspect its recorded ID.")
                self.http.delay(response.headers, deadline)
        return {"id": handle["id"]}

    def reconcile(self, path, expected, fields):
        actual = self.get(path)
        for key, value in fields.items():
            if actual.get(key) != value:
                raise DeploymentError(f"Fabric resource changed: {key}. Stop rather than overwriting.")
        if actual.get("id") != expected["id"]:
            raise DeploymentError("Fabric immutable resource ID changed.")

    def definition(self, workspace, item, format=None):
        path = f"/workspaces/{workspace}/items/{item}/getDefinition"
        if format:
            path += "?format=" + quote(format, safe="")
        response = self.http.request("POST", path, {})
        if response.status == 202:
            operation = response.headers.get("x-ms-operation-id")
            if not operation:
                raise DeploymentError("Definition retrieval returned no operation ID.")
            deadline = self.http.clock() + self.config["timeout"]
            while True:
                path = "/operations/" + guid(operation)
                status = self.http.request("GET", path, deadline=deadline)
                if status.data.get("status") == "Succeeded":
                    return self.get(path + "/result")["definition"]
                if status.data.get("status") in ("Failed", "Canceled", "Cancelled"):
                    raise DeploymentError("Definition retrieval failed.")
                self.http.delay(status.headers, deadline)
        return response.data["definition"]

    def shortcut(self, workspace, lakehouse, body):
        root = f"/workspaces/{workspace}/items/{lakehouse}/shortcuts"
        path = f"{root}/{quote(body['path'], safe='/')}/{quote(body['name'], safe='')}"
        if self.get(path, missing=True):
            raise DeploymentError("Shortcut collision; existing targets are never overwritten.")
        response = self.http.request("POST", root + "?shortcutConflictPolicy=Abort", body)
        if response.status != 201:
            raise DeploymentError("Expected a new shortcut, not an update.")
        return {"path": path}

    def check_shortcut(self, handle, body):
        actual = self.get(handle["path"])
        for key in ("name", "path"):
            if actual.get(key) != body[key]:
                raise DeploymentError("Shortcut identity changed.")
        for kind, target in body["target"].items():
            if actual.get("target", {}).get(kind) != target:
                raise DeploymentError("Shortcut target changed. Refusing to overwrite.")
        return handle


class Databricks:
    def __init__(self, host, profile, timeout, http=None):
        if http is None:
            try:
                from databricks.sdk import WorkspaceClient
            except ImportError:
                raise DeploymentError("Install requirements-deploy.txt before live use.") from None
            try:
                client = WorkspaceClient(profile=profile)
                if client.config.host.rstrip("/") != host.rstrip("/"):
                    raise DeploymentError("Databricks auth profile targets a different workspace.")
                http = HTTP(host, client.config.authenticate, timeout)
            except (ValueError, OSError):
                raise DeploymentError("Databricks unified authentication failed; configure the selected profile separately.") from None
        self.http = http

    def get(self, kind, full_name, missing=False, compatibility=False):
        path = f"/api/2.1/unity-catalog/{kind}/{quote(full_name, safe='')}"
        if compatibility:
            path += "?read_materialized_view_as_managed=true&read_streaming_table_as_managed=true"
        response = self.http.request("GET", path, missing=missing)
        return response.data if response else None

    def submit_sql(self, warehouse, statement):
        response = self.http.request("POST", "/api/2.0/sql/statements",
                                     {"warehouse_id": warehouse, "statement": statement,
                                      "wait_timeout": "0s", "disposition": "INLINE", "format": "JSON_ARRAY"})
        if not response.data.get("statement_id"):
            raise DeploymentError("SQL write returned no statement ID. Inspect SQL query history; do not retry.")
        return {"statement_id": response.data["statement_id"]}

    def finish_sql(self, handle):
        deadline = self.http.clock() + self.http.timeout
        while True:
            response = self.http.request("GET", "/api/2.0/sql/statements/" + quote(handle["statement_id"], safe=""),
                                         deadline=deadline)
            status = response.data.get("status", {}).get("state")
            if status == "SUCCEEDED":
                if response.data.get("manifest", {}).get("truncated"):
                    raise DeploymentError("SQL result was truncated; no comparison can be asserted.")
                result = response.data.get("result", {})
                if result.get("next_chunk_index") is not None:
                    raise DeploymentError("SQL result has additional chunks; narrow the metadata query.")
                return {"statement_id": handle["statement_id"],
                        "rows": result.get("data_array", []),
                        "columns": [c["name"] for c in response.data.get("manifest", {}).get("schema", {}).get("columns", [])]}
            if status in ("FAILED", "CANCELED", "CLOSED"):
                raise DeploymentError(f"SQL statement {handle['statement_id']} {status}. Inspect query history; no write was retried.")
            self.http.delay(response.headers, deadline)

    def query(self, warehouse, statement):
        return self.finish_sql(self.submit_sql(warehouse, statement))

    def warehouse(self, warehouse_id):
        return self.http.request("GET", "/api/2.0/sql/warehouses/" + quote(warehouse_id, safe="")).data

    def create_warehouse(self, specification):
        existing = self.http.request("GET", "/api/2.0/sql/warehouses").data.get("warehouses", [])
        if any(w["name"] == specification["name"] for w in existing):
            raise DeploymentError("SQL warehouse name collision.")
        result = self.http.request("POST", "/api/2.0/sql/warehouses",
                                   {"name": specification["name"], "cluster_size": specification["size"],
                                    "min_num_clusters": 1, "max_num_clusters": 1,
                                    "auto_stop_mins": 10, "enable_serverless_compute": True,
                                    "warehouse_type": "PRO"}).data
        if not result.get("id"):
            raise DeploymentError("Warehouse create returned no ID. Inspect workspace before any retry.")
        return {"id": result["id"]}


class ADLS:
    """Read-only directory/protocol checks. Never writes compatibility outputs."""
    def __init__(self, root, tenant, timeout, cli=azure_cli, http=None):
        parsed = urlsplit(root)
        self.container = parsed.username
        self.folder = parsed.path.strip("/")
        self.http = http or HTTP("https://" + parsed.hostname,
                                lambda: {"Authorization": "Bearer " + cli(
                                    ["account", "get-access-token", "--resource", "https://storage.azure.com",
                                     "--tenant", tenant])["accessToken"], "x-ms-version": "2023-11-03"}, timeout)

    def empty(self, root):
        parsed = urlsplit(root)
        directory = parsed.path.strip("/")
        head = self.http.request("HEAD", f"/{self.container}/{quote(directory, safe='/')}", missing=True)
        if not head:
            raise Waiting("Output directory is missing. Create the exact empty folder manually before enabling Compatibility Mode.")
        response = self.http.request("GET", f"/{self.container}?" + urlencode(
            {"resource": "filesystem", "directory": directory, "recursive": "true", "maxResults": 1}))
        if response.data.get("paths") or response.headers.get("x-ms-continuation"):
            raise DeploymentError("Compatibility destination is not empty. Never reuse or erase an output path.")
        return {"path": root, "empty_observed_utc": datetime.now(timezone.utc).isoformat()}

    def protocol(self, root):
        directory = urlsplit(root).path.strip("/") + "/_delta_log"
        response = self.http.request("GET", f"/{self.container}?" + urlencode(
            {"resource": "filesystem", "directory": directory, "recursive": "false", "maxResults": 5000}))
        if response.headers.get("x-ms-continuation"):
            raise Waiting("Output log requires paginated inspection; record its actual current protocol manually.")
        logs = sorted(p["name"] for p in response.data.get("paths", [])
                      if str(p.get("isDirectory", False)).lower() != "true" and p["name"].rsplit("/", 1)[-1].endswith(".json")
                      and p["name"].rsplit("/", 1)[-1][:-5].isdigit())
        if not logs:
            raise Waiting("Generated output Delta log is not present yet.")
        protocol = None
        output_id = None
        # Protocol changes can occur after version zero; inspect every retained JSON commit.
        deadline = self.http.clock() + self.http.timeout
        for path in logs:
            result = self.http.request("GET", f"/{self.container}/{quote(path, safe='/')}", deadline=deadline)
            actions = [result.data] if result.data else [json.loads(line) for line in result.text.splitlines() if line]
            for action in actions:
                if "protocol" in action:
                    protocol = action["protocol"]
                if "metaData" in action:
                    output_id = action["metaData"].get("id")
        if not protocol or not output_id:
            raise Waiting("Output protocol/ID cannot be established from retained logs; inspect checkpoint metadata manually.")
        features = set(protocol.get("readerFeatures", [])) | set(protocol.get("writerFeatures", []))
        if "catalogManaged" in features:
            raise DeploymentError("The alleged compatibility output still reports catalogManaged.")
        return {"id": guid(output_id), "reader": protocol["minReaderVersion"],
                "writer": protocol["minWriterVersion"], "features": sorted(features),
                "log_version": int(logs[-1].rsplit("/", 1)[-1][:-5])}

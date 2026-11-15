"""Conditional interactive configuration. No cloud calls and no credentials."""

from __future__ import annotations

from datetime import date
import re

from demos.common.star_schema import StarSchemaConfig
from .safety import DeploymentError, guid, name, nonsecret, url

DEMOS = ("onelake", "power-bi", "compatibility")


def workspace_display_name(value):
    nonsecret(value)
    if not isinstance(value, str) or not value.strip() or not 1 <= len(value) <= 256:
        raise DeploymentError("Fabric workspace display names must contain 1-256 characters and cannot be blank.")
    if value.casefold() == "admin monitoring":
        raise DeploymentError("Admin monitoring is a reserved Fabric workspace name.")
    return value


class Console:
    def __init__(self, read=input, output=print):
        self.read = read
        self.output = output

    def ask(self, label, default=None):
        value = self.read(label + (f" [{default}]" if default is not None else "") + ": ").strip()
        if not value:
            if default is None:
                raise DeploymentError("A required input was not supplied.")
            value = str(default)
        nonsecret(value)
        return value

    def choose(self, label, choices, default=None):
        self.output(f"{label}: {', '.join(choices)}")
        value = self.ask("Choose", default)
        if value not in choices:
            raise DeploymentError("Choose one of the displayed values.")
        return value

    def confirm(self, label):
        return self.ask(label + " (type yes to confirm)", "no") == "yes"


def resource(console, kind):
    mode = console.choose(f"{kind} infrastructure", ("create", "reuse"))
    if mode == "reuse":
        return {"mode": mode, "resource_id": console.ask(f"Exact {kind} Azure resource ID")}
    result = {"mode": mode, "name": name(console.ask(f"New {kind} name"))}
    if kind == "capacity":
        result.update(sku=console.ask("Fabric F SKU", "F2"),
                      administrators=console.ask("Capacity administrator UPNs (comma separated)").split(","))
    else:
        result.update(sku="premium", managed_group=name(console.ask("New Databricks managed resource group name")),
                      networking=console.choose("Networking (custom networking requires manual setup)",
                                                ("public", "custom")))
    return result


def collect(console: Console, selection=None):
    selection = selection or console.choose("Demo", (*DEMOS, "all"))
    selected = list(DEMOS) if selection == "all" else [selection]
    config = {"version": 1, "tenant_id": guid(console.ask("Tenant ID")),
              "subscription_id": guid(console.ask("Subscription ID")),
              "region": console.ask("Azure region (for example westeurope)"),
              "resource_group": name(console.ask("Existing deployment resource group")),
              "sample": {"seed": int(console.ask("Data seed", 42)),
                         "sales_rows": int(console.ask("Initial sales rows", 10000))},
              "timeout": int(console.ask("Bounded operation timeout seconds", 900)),
              "demos": {}}
    shared = len(selected) > 1 and console.confirm("Explicitly share capacity and Databricks workspace across selected demos?")
    config["shared_foundation"] = shared
    common = None
    for demo in selected:
        console.output(f"Inputs for {demo}")
        if common is None or not shared:
            common = {"capacity": resource(console, "capacity"), "databricks": resource(console, "databricks")}
        spec = {"foundation": common, "prefix": name(console.ask("Unique demo prefix")),
                "catalog": name(console.ask("Explicit parent Unity Catalog name")),
                "profile": console.ask("Databricks unified-auth profile (no tokens)"),
                "warehouse": {"mode": console.choose("SQL warehouse", ("reuse", "create"))}}
        if spec["warehouse"]["mode"] == "reuse":
            spec["warehouse"]["id"] = console.ask("SQL warehouse ID")
        else:
            spec["warehouse"].update(name=name(console.ask("New SQL warehouse name")),
                                     size=console.choose("Serverless warehouse size", ("2X-Small", "X-Small", "Small"), "2X-Small"))
        spec["workspace"] = {"mode": console.choose("Dedicated Fabric workspace", ("create", "reuse"))}
        if spec["workspace"]["mode"] == "reuse":
            spec["workspace"]["id"] = guid(console.ask("Exact dedicated Fabric workspace ID"))
        spec["workspace"]["name"] = workspace_display_name(console.ask("Fabric workspace display name"))
        if demo == "power-bi":
            spec["storage_mode"] = console.choose("Native Power BI publication mode", ("DirectQuery", "Import"), "DirectQuery")
        else:
            spec["identity_resource_id"] = console.ask("Access connector Azure resource ID (create manually if absent)")
            spec["identity_object_id"] = guid(console.ask("Access connector managed identity object ID"))
            spec["credential"] = name(console.ask("Existing writable, workspace-isolated UC storage credential"))
            spec["file_events"] = console.choose("Explicit file-events mode", ("disabled", "automatic", "provided"))
        if demo == "compatibility":
            spec["output_root"] = console.ask("Dedicated ADLS output root ABFSS URL (no SAS)")
            spec["output_external_location"] = name(console.ask("Existing ADLS UC external location covering that root"))
        config["demos"][demo] = spec
    validate(config)
    return config


def arm_id(value, subscription, kind):
    if not re.fullmatch(
        rf"/subscriptions/{re.escape(subscription)}/resourceGroups/[A-Za-z0-9_.()-]+/"
        rf"providers/Microsoft\.{kind}/[A-Za-z0-9_-]+", value, re.I
    ):
        raise DeploymentError("Resource ID has the wrong subscription or type.")
    return value


def validate(config):
    nonsecret(config)
    def fields(value, allowed):
        if not isinstance(value, dict) or set(value) - set(allowed):
            raise DeploymentError("Unknown configuration fields are rejected; use the guided-deployer schema.")
    fields(config, ("version", "demos", "tenant_id", "subscription_id", "region", "resource_group",
                    "timeout", "sample", "shared_foundation"))
    if config.get("version") != 1 or not config.get("demos") or set(config["demos"]) - set(DEMOS):
        raise DeploymentError("Unsupported configuration version or demo selection.")
    guid(config["tenant_id"])
    guid(config["subscription_id"])
    name(config["resource_group"])
    if not re.fullmatch(r"[a-z0-9]+", config["region"]):
        raise DeploymentError("Use an Azure region identifier, not a display name.")
    if type(config["timeout"]) is not int or not 1 <= config["timeout"] <= 7200:
        raise DeploymentError("Timeout must be 1-7200 seconds.")
    sample = dict(config["sample"])
    fields(sample, ("date_rows", "product_rows", "customer_rows", "sales_rows", "seed", "start_date"))
    if any(type(v) is not int for k, v in sample.items() if k != "start_date"):
        raise DeploymentError("Sample sizes and seed must be integers.")
    if type(config.get("shared_foundation")) is not bool:
        raise DeploymentError("Explicit shared_foundation boolean is required.")
    if "start_date" in sample:
        sample["start_date"] = date.fromisoformat(sample["start_date"])
    StarSchemaConfig(**sample).validate()
    identities = set()
    foundations = []
    for demo, spec in config["demos"].items():
        fields(spec, ("prefix", "catalog", "profile", "warehouse", "workspace", "foundation", "storage_mode",
                      "identity_resource_id", "identity_object_id", "credential", "file_events",
                      "output_root", "output_external_location"))
        if demo == "power-bi" and any(k in spec for k in ("credential", "file_events", "output_root", "identity_resource_id")):
            raise DeploymentError("Power BI configuration must not contain unrelated storage inputs.")
        fields(spec["workspace"], ("mode", "name", "id"))
        fields(spec["warehouse"], ("mode", "name", "id", "size"))
        for field in ("prefix", "catalog"):
            name(spec[field])
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{1,39}", spec["prefix"]):
            raise DeploymentError("Demo prefixes must be 2-40 letters/digits/underscores for Fabric item names.")
        workspace_display_name(spec["workspace"]["name"])
        if spec["workspace"]["mode"] == "reuse":
            guid(spec["workspace"]["id"])
        elif spec["workspace"]["mode"] != "create":
            raise DeploymentError("Workspace mode must be create or reuse.")
        identity = spec["workspace"].get("id", spec["workspace"]["name"])
        if identity in identities:
            raise DeploymentError("Each demo requires a distinct Fabric workspace.")
        identities.add(identity)
        if spec["warehouse"]["mode"] == "create":
            name(spec["warehouse"]["name"])
            if spec["warehouse"]["size"] not in ("2X-Small", "X-Small", "Small"):
                raise DeploymentError("Unsupported warehouse size.")
        elif spec["warehouse"]["mode"] != "reuse" or not re.fullmatch(r"[a-fA-F0-9]{16}", spec["warehouse"]["id"]):
            raise DeploymentError("Select an exact warehouse ID or create a warehouse.")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", spec["profile"]):
            raise DeploymentError("Use a named Databricks auth profile.")
        foundation = spec["foundation"]
        fields(foundation, ("capacity", "databricks"))
        foundations.append(foundation)
        for kind, namespace in (("capacity", "Fabric/capacities"), ("databricks", "Databricks/workspaces")):
            target = foundation[kind]
            fields(target, ("mode", "name", "resource_id", "sku", "administrators", "managed_group", "networking"))
            if target["mode"] == "reuse":
                arm_id(target["resource_id"], config["subscription_id"], namespace)
            elif target["mode"] == "create":
                name(target["name"])
                if kind == "capacity":
                    if not re.fullmatch(r"[a-z][a-z0-9]{2,62}", target["name"]):
                        raise DeploymentError("Capacity names must be 3-63 lowercase alphanumeric characters.")
                    if not re.fullmatch(r"F(2|4|8|16|32|64|128|256|512|1024|2048)", target["sku"]):
                        raise DeploymentError("Select a supported F SKU; availability is checked during preflight.")
                    if not target["administrators"] or any(not re.fullmatch(r"[^@\s]+@[^@\s]+", p.strip()) for p in target["administrators"]):
                        raise DeploymentError("Supply capacity administrator UPNs.")
                else:
                    name(target["managed_group"])
                    if target["sku"] != "premium" or target["networking"] not in ("public", "custom"):
                        raise DeploymentError("UC requires premium; networking must be explicitly selected.")
            else:
                raise DeploymentError("Infrastructure mode must be create or reuse.")
        if demo == "power-bi":
            if spec["storage_mode"] not in ("DirectQuery", "Import"):
                raise DeploymentError("Publication mode must be DirectQuery or Import.")
        else:
            arm_id(spec["identity_resource_id"], config["subscription_id"], "Databricks/accessConnectors")
            guid(spec["identity_object_id"])
            name(spec["credential"])
            if spec["file_events"] not in ("disabled", "automatic", "provided"):
                raise DeploymentError("Explicitly select file-events mode.")
        if demo == "compatibility":
            url(spec["output_root"], ("dfs.core.windows.net",), "abfss")
            if not re.fullmatch(r"abfss://[a-z0-9-]+@[a-z0-9]+\.dfs\.core\.windows\.net/[A-Za-z0-9_/-]+",
                                spec["output_root"]):
                raise DeploymentError("Use a dedicated ADLS folder with an unsigned ABFSS URL.")
            name(spec["output_external_location"])
    namespaces = set()
    for demo, spec in config["demos"].items():
        target = spec["foundation"]["databricks"]
        workspace_identity = target.get("resource_id", target.get("name")).lower()
        suffixes = ("normal", "compat") if demo == "compatibility" else ("powerbi",) if demo == "power-bi" else ("onelake",)
        for suffix in suffixes:
            namespace = (workspace_identity, spec["catalog"].lower(), (spec["prefix"] + "_" + suffix).lower())
            if namespace in namespaces:
                raise DeploymentError("Selected demos would collide in a Unity Catalog namespace.")
            namespaces.add(namespace)
    if not config.get("shared_foundation"):
        for i, left in enumerate(foundations):
            for right in foundations[i + 1:]:
                for kind in ("capacity", "databricks"):
                    left_id = left[kind].get("resource_id", left[kind].get("name")).lower()
                    right_id = right[kind].get("resource_id", right[kind].get("name")).lower()
                    if left_id == right_id:
                        raise DeploymentError("Shared infrastructure requires explicit shared_foundation=true.")

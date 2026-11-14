"""Local-only configuration checks; this module never contacts cloud services."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path
import re
import sys
from typing import Any


_NAME = re.compile(r"^[a-z][a-z0-9-]{1,39}$")
_GUID = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_SENSITIVE_KEYS = {"token", "password", "client_secret", "pat", "secret_value"}


def _get(config: dict[str, Any], *path: str) -> Any:
    value: Any = config
    for part in path:
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _sensitive_paths(value: Any, path: str = "") -> list[str]:
    found = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else key
            if key.lower() in _SENSITIVE_KEYS:
                found.append(child_path)
            found.extend(_sensitive_paths(child, child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(_sensitive_paths(child, f"{path}[{index}]"))
    return found


def validate_local_config(config: dict[str, Any]) -> list[str]:
    errors = []
    prefix = _get(config, "deployment", "prefix")
    if not isinstance(prefix, str) or not _NAME.fullmatch(prefix):
        errors.append("deployment.prefix must be a lowercase safe resource prefix")

    timeout = _get(config, "deployment", "verification_timeout_seconds")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        errors.append("deployment.verification_timeout_seconds must be a positive integer")

    sample_data = _get(config, "deployment", "sample_data")
    if not isinstance(sample_data, dict):
        errors.append("deployment.sample_data must be an object")
    else:
        row_fields = ("date_rows", "product_rows", "customer_rows", "sales_rows")
        for field in row_fields:
            count = sample_data.get(field)
            if not isinstance(count, int) or isinstance(count, bool) or count <= 0:
                errors.append(f"deployment.sample_data.{field} must be a positive integer")
        counts = [sample_data.get(field) for field in row_fields]
        if all(isinstance(value, int) and not isinstance(value, bool) for value in counts):
            if counts[3] < max(counts[:3]):
                errors.append("sample_data.sales_rows must cover every dimension key")
        if not isinstance(sample_data.get("seed"), int):
            errors.append("deployment.sample_data.seed must be an integer")
        try:
            date.fromisoformat(sample_data.get("start_date", ""))
        except (TypeError, ValueError):
            errors.append("deployment.sample_data.start_date must be an ISO calendar date")

    storage_mode = _get(config, "power_bi", "semantic_model_storage_mode")
    if storage_mode not in ("DirectQuery", "Import"):
        errors.append("power_bi.semantic_model_storage_mode must be DirectQuery or Import")
    if _get(config, "power_bi", "authentication_method") not in ("OAuth", "PAT"):
        errors.append("power_bi.authentication_method must be OAuth or PAT")
    if _get(config, "onelake", "file_events_mode") not in (
        "unverified",
        "disabled",
        "automatic",
        "provided",
    ):
        errors.append(
            "onelake.file_events_mode must be unverified, disabled, automatic, or provided"
        )

    for path in (
        ("environment", "tenant_id"),
        ("environment", "subscription_id"),
        ("fabric", "capacity_id"),
        ("fabric", "workspaces", "onelake", "id"),
        ("fabric", "workspaces", "power_bi", "id"),
        ("onelake", "storage_item_id"),
        ("onelake", "fabric_mirror_id"),
        ("power_bi", "workspace_id"),
    ):
        value = _get(config, *path)
        if value and (not isinstance(value, str) or not _GUID.fullmatch(value)):
            errors.append(f"{'.'.join(path)} must be a GUID when provided")

    onelake_workspace_id = _get(config, "fabric", "workspaces", "onelake", "id")
    storage_item_id = _get(config, "onelake", "storage_item_id")
    abfss_url = _get(config, "onelake", "abfss_url")
    if abfss_url:
        match = re.fullmatch(
            r"abfss://([0-9a-fA-F-]{36})@onelake\.dfs\.fabric\.microsoft\.com/"
            r"([0-9a-fA-F-]{36})/Files/",
            abfss_url,
            flags=re.IGNORECASE,
        )
        if not match or not _GUID.fullmatch(match.group(1)) or not _GUID.fullmatch(
            match.group(2)
        ):
            errors.append("onelake.abfss_url must use the documented ID-based OneLake path")
        elif onelake_workspace_id and match.group(1).lower() != onelake_workspace_id.lower():
            errors.append("OneLake ABFSS workspace ID must match the configured workspace")
        elif storage_item_id and match.group(2).lower() != storage_item_id.lower():
            errors.append("OneLake ABFSS storage item ID must match the configured item")

    secret_paths = _sensitive_paths(config)
    if secret_paths:
        errors.append(
            "configuration must contain secret references, not secret values "
            f"(sensitive keys: {', '.join(secret_paths)})"
        )
    return errors


def deployment_blockers(config: dict[str, Any], demo: str) -> list[str]:
    blockers = []
    catalog = _get(config, "databricks", "parent_catalog")
    approved = _get(config, "databricks", "parent_catalog_approved")
    if not isinstance(catalog, str) or not catalog.strip():
        blockers.append("Set an explicitly selected Databricks parent catalog.")
    if approved is not True:
        blockers.append("Explicitly approve use of that parent catalog in configuration.")
    if not _get(config, "databricks", "workspace_url"):
        blockers.append("Resolve the Databricks workspace URL.")
    if not _get(config, "databricks", "workspace_id"):
        blockers.append("Resolve the Databricks workspace ID.")
    if _get(config, "databricks", "workspace_confirmed") is not True:
        blockers.append("Confirm the selected Databricks workspace in configuration.")
    if demo in ("power-bi", "all") and not _get(
        config, "databricks", "sql_warehouse_id"
    ):
        blockers.append("Resolve the existing Databricks SQL warehouse ID.")
    if not _get(config, "environment", "subscription_id"):
        blockers.append("Resolve the MCAP subscription ID.")

    if demo in ("onelake", "all"):
        if _get(config, "fabric", "workspaces", "onelake", "confirmed") is not True:
            blockers.append("Confirm the selected OneLake Fabric workspace in configuration.")
        if (
            _get(
                config,
                "deployment",
                "approvals",
                "onelake_chargeable_resource_use",
            )
            is not True
        ):
            blockers.append(
                "Obtain explicit approval for OneLake demo chargeable-resource use."
            )
        for path, label in (
            (("fabric", "capacity_id"), "Fabric capacity ID"),
            (("fabric", "workspaces", "onelake", "id"), "OneLake demo Fabric workspace ID"),
            (("onelake", "storage_item_id"), "Azure Databricks Storage item ID"),
            (("onelake", "storage_credential_name"), "approved OneLake storage credential"),
            (("onelake", "external_location_name"), "OneLake external location name"),
            (("onelake", "fabric_mirror_id"), "Fabric mirrored catalog ID"),
        ):
            if not _get(config, *path):
                blockers.append(f"Resolve the {label}.")
        if not _get(config, "onelake", "abfss_url"):
            blockers.append("Set the verified ID-based OneLake ABFSS path.")
        if not _get(config, "onelake", "identity_reference"):
            blockers.append("Record the approved Databricks access identity reference.")
        if _get(config, "onelake", "tenant_short_lived_sas_key_generation") != "enabled":
            blockers.append(
                "Verify tenant-level short-lived user-delegated SAS key generation."
            )
        if (
            _get(config, "onelake", "workspace_user_delegated_sas_authentication")
            != "enabled"
        ):
            blockers.append(
                "Verify the target workspace's user-delegated SAS authentication setting."
            )
        if (
            _get(config, "onelake", "tenant_short_lived_sas_key_generation") == "disabled"
            and _get(config, "onelake", "tenant_setting_change_approved") is not True
        ):
            blockers.append(
                "A disabled tenant-level SAS setting needs separate approval before any change."
            )
        file_events_mode = _get(config, "onelake", "file_events_mode")
        if file_events_mode == "unverified":
            blockers.append(
                "Verify and record the OneLake external-location file-events mode."
            )
        elif file_events_mode in ("automatic", "provided"):
            if (
                _get(
                    config,
                    "onelake",
                    "file_event_role_escalation_approved",
                )
                is not True
            ):
                blockers.append(
                    "Obtain separate approval for any required Fabric file-event identity role."
                )
            if (
                _get(
                    config,
                    "onelake",
                    "file_eventstream_provisioning_approved",
                )
                is not True
            ):
                blockers.append(
                    "Obtain separate approval for any required Fabric file eventstream provisioning."
                )

    if demo in ("power-bi", "all"):
        if _get(config, "fabric", "workspaces", "power_bi", "confirmed") is not True:
            blockers.append("Confirm the selected Power BI Fabric workspace in configuration.")
        if (
            _get(
                config,
                "deployment",
                "approvals",
                "power_bi_chargeable_resource_use",
            )
            is not True
        ):
            blockers.append(
                "Obtain explicit approval for Power BI demo chargeable-resource use."
            )
        if not _get(config, "fabric", "capacity_id"):
            blockers.append("Resolve the Fabric capacity ID for the Power BI demo.")
        if not _get(config, "fabric", "workspaces", "power_bi", "id"):
            blockers.append("Resolve the Power BI demo Fabric workspace ID.")
        if not _get(config, "power_bi", "workspace_id"):
            blockers.append("Resolve the home-tenant Power BI workspace ID.")
        fabric_workspace_id = _get(config, "fabric", "workspaces", "power_bi", "id")
        power_bi_workspace_id = _get(config, "power_bi", "workspace_id")
        if (
            fabric_workspace_id
            and power_bi_workspace_id
            and isinstance(fabric_workspace_id, str)
            and isinstance(power_bi_workspace_id, str)
            and fabric_workspace_id.lower() != power_bi_workspace_id.lower()
        ):
            blockers.append(
                "The Fabric and Power BI workspace IDs must match for this demo."
            )
    return blockers


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate local demo configuration without making cloud calls."
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument(
        "--demo", choices=("onelake", "power-bi", "all"), default="all"
    )
    parser.add_argument(
        "--require-deployment",
        action="store_true",
        help="return exit code 2 when the selected demo still has live prerequisites",
    )
    args = parser.parse_args(argv)

    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(f"Configuration could not be read: {error}", file=sys.stderr)
        return 2
    if not isinstance(config, dict):
        print("Configuration root must be a JSON object.", file=sys.stderr)
        return 2

    errors = validate_local_config(config)
    if errors:
        print("Local configuration: FAIL")
        for error in errors:
            print(f"- {error}")
        return 2
    print("Local configuration: PASS")
    blockers = deployment_blockers(config, args.demo)
    if blockers:
        print(f"{args.demo} deployment readiness: BLOCKED (no cloud calls made)")
        for blocker in blockers:
            print(f"- {blocker}")
        return 2 if args.require_deployment else 0
    print(
        f"{args.demo} deployment readiness: CONFIGURED "
        "(manual confirmations supplied; no cloud calls made)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

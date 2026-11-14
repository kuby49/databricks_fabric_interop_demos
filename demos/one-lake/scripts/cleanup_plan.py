"""Print an offline manual cleanup review list; this tool never deletes resources."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
from typing import Any


CONFIRMATION = "CLEANUP ONLY RESOURCES IN MANIFEST"
ALLOWED_KINDS = {
    "databricks_schema",
    "uc_external_location",
    "uc_storage_credential",
    "fabric_mirror",
    "fabric_storage_item",
    "fabric_workspace",
}
SAFE_NAME = re.compile(r"^[A-Za-z0-9_-]+$")


def eligible_resources(manifest: dict[str, Any]) -> list[dict[str, str]]:
    if manifest.get("demo") != "one-lake":
        raise ValueError("manifest is not for the OneLake demo")
    prefix = manifest.get("prefix", "fabricinterop")
    if not isinstance(prefix, str) or not SAFE_NAME.fullmatch(prefix):
        raise ValueError("manifest prefix is invalid")
    resources = manifest.get("created_resources", [])
    if not isinstance(resources, list):
        raise ValueError("created_resources must be a list")
    if resources and manifest.get("resource_creation_performed") is not True:
        raise ValueError("non-empty resource list is not marked as demo-created")
    eligible = []
    for resource in resources:
        if not isinstance(resource, dict):
            raise ValueError("each created resource must be an object")
        kind = resource.get("kind")
        name = resource.get("name")
        if (
            resource.get("created_by_demo") is True
            and kind in ALLOWED_KINDS
            and isinstance(name, str)
            and SAFE_NAME.fullmatch(name)
            and name.startswith(prefix)
        ):
            eligible.append({"kind": kind, "name": name})
    return eligible


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, help="sanitized manifest JSON")
    parser.add_argument(
        "--confirm",
        help=f"must exactly equal: {CONFIRMATION!r}",
    )
    args = parser.parse_args(argv)
    if args.confirm != CONFIRMATION:
        print("Refusing: exact explicit confirmation is required.", file=sys.stderr)
        return 2
    try:
        with Path(args.manifest).open(encoding="utf-8") as file:
            manifest = json.load(file)
        resources = eligible_resources(manifest)
    except (OSError, ValueError, TypeError) as error:
        print(f"Refusing to produce cleanup plan: {error}", file=sys.stderr)
        return 2
    if not resources:
        print("No eligible demo-created resources in the manifest; nothing to review.")
        return 0
    print("Manual review only. No resources will be deleted by this tool:")
    for resource in resources:
        print(f"- {resource['kind']}: {resource['name']}")
    print("Verify each item was created by this demo before manually removing it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

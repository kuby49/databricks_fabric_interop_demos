"""Render (but never execute) the schema DDL for the explicitly approved target."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from demo_config import approved_target, load_config


def render(config: dict) -> str:
    target = approved_target(config)
    return f"""-- Offline-generated SQL. Review the target and managed location before manual execution.
-- This statement intentionally has no IF NOT EXISTS: an existing schema is a stop condition.
-- Required scope: USE CATALOG and CREATE SCHEMA on the catalog;
-- CREATE MANAGED STORAGE on the external location backing this managed location.
CREATE SCHEMA `{target["catalog"]}`.`{target["schema"]}`
MANAGED LOCATION '{target["managed_location"]}';
"""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="path to a local demo config JSON")
    args = parser.parse_args(argv)
    try:
        print(render(load_config(args.config)), end="")
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Refusing to render SQL: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

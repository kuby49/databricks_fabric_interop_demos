"""Interactive preview/apply/status/resume CLI."""

import argparse
import json
from pathlib import Path

from .config import Console, collect, validate
from .safety import DeploymentError, State, Waiting, nonsecret, write_private
from .workflow import Workflow


def main(argv=None, console=None, factory=Workflow):
    parser = argparse.ArgumentParser(description="Guided Databricks/Fabric demo deployment. Default: offline configuration only.")
    parser.add_argument("--demo", choices=("onelake", "power-bi", "compatibility", "all"))
    parser.add_argument("--directory", type=Path, default=Path(".demo-deploy"),
                        help="Private config/state directory; keep it out of version control")
    parser.add_argument("--config", type=Path, help="Read an existing non-secret guided-deployer JSON config")
    parser.add_argument("--action", choices=("configure", "preview", "apply", "resume", "status"), default="configure")
    parser.add_argument("--optional-checks", action="store_true", help="Offer separately confirmed compute-cost checks after setup")
    args = parser.parse_args(argv)
    console = console or Console()
    config_path = args.config or args.directory / "config.json"
    state_path = args.directory / "state.json"
    try:
        if args.action == "configure":
            if config_path.exists() or state_path.exists():
                raise DeploymentError("Private deployment directory is already in use. Select a new directory or resume.")
            config = collect(console, args.demo)
            write_private(config_path, config)
            state = State(state_path, config)
            state.save()
            console.output(f"Configured offline. Private config: {config_path}; state: {state_path}. Nothing deployed.")
            return 0
        if not config_path.exists():
            raise DeploymentError("Configuration is missing. Run --action configure first.")
        config = json.loads(config_path.read_text(encoding="utf-8"))
        validate(config)
        if args.demo and set(config["demos"]) != ({"onelake", "power-bi", "compatibility"} if args.demo == "all" else {args.demo}):
            raise DeploymentError("Demo selection differs from the saved config.")
        if args.action in ("resume", "status") and not state_path.exists():
            raise DeploymentError("Resume/status requires the original state; never reconstruct it from names.")
        state = State(state_path, config)
        if args.action == "status":
            nonsecret(state.data)
            console.output(json.dumps(state.data, indent=2, sort_keys=True))
            return 0
        with state.lock():
            workflow = factory(config, state, console)
            if args.action == "preview":
                workflow.preview()
                return 0
            results = workflow.run(optional=args.optional_checks)
            return 0 if all(r["status"] == "setup-checked" for r in results.values()) else 3
    except Waiting as error:
        console.output(f"WAITING: {error}")
        return 3
    except DeploymentError as error:
        console.output(f"BLOCKED: {error}")
        return 2
    except (KeyError, TypeError, ValueError, OSError):
        # Never print arbitrary parser/SDK/OS exceptions: they may include credentials.
        console.output("BLOCKED: invalid input, private-state failure or service error. Check the selected configuration/permissions; uncertain writes must be reconciled using the original state.")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

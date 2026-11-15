"""Cross-platform entry point; importing it never authenticates or deploys."""

from demos.deployment.cli import main

if __name__ == "__main__":
    raise SystemExit(main())

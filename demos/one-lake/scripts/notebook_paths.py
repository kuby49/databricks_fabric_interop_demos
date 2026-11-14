"""Resolve the checked-out demo repository from its documented notebook directory."""

from pathlib import Path


def resolve_repository_root(notebook_working_directory: str | Path) -> Path:
    working_directory = Path(notebook_working_directory).resolve()
    if (
        working_directory.name != "notebooks"
        or working_directory.parent.name != "one-lake"
        or working_directory.parent.parent.name != "demos"
    ):
        raise ValueError(
            "run the notebook from <repo>/demos/one-lake/notebooks with the "
            "repository layout preserved"
        )

    repository_root = working_directory.parents[2]
    required_files = (
        repository_root / "demos" / "common" / "star_schema.py",
        repository_root / "demos" / "one-lake" / "scripts" / "demo_config.py",
    )
    missing = [path for path in required_files if not path.is_file()]
    if missing:
        raise ValueError(
            "the notebook working directory is not inside the complete demo "
            f"repository; missing {missing[0]}"
        )
    return repository_root

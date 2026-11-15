"""Private state, input validation and conservative write recovery."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
from urllib.parse import urlsplit
from uuid import UUID, uuid4


class DeploymentError(RuntimeError):
    pass


class Waiting(DeploymentError):
    pass


def guid(value: str) -> str:
    try:
        return str(UUID(value))
    except (ValueError, TypeError, AttributeError):
        raise DeploymentError("Expected a GUID, not a display name.") from None


def name(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{1,62}", value):
        raise DeploymentError("Names must start with a letter and contain 2-63 letters, digits, _ or -.")
    return value


def url(value: str, hosts: tuple[str, ...], scheme: str = "https") -> str:
    parsed = urlsplit(value)
    if (parsed.scheme != scheme or parsed.query or parsed.fragment or parsed.password
            or parsed.port or not parsed.hostname
            or not any(parsed.hostname == h or parsed.hostname.endswith("." + h) for h in hosts)):
        raise DeploymentError("URL must be an unsigned supported service URL without credentials.")
    if scheme == "https" and parsed.username:
        raise DeploymentError("URLs cannot contain credentials.")
    if any(p in (".", "..") for p in parsed.path.split("/")) or "%" in parsed.path:
        raise DeploymentError("Encoded or relative URL paths are not accepted.")
    return value.rstrip("/")


def nonsecret(value):
    """Reject secrets rather than trying to redact an arbitrary configuration."""
    if isinstance(value, dict):
        for key, child in value.items():
            if re.search(r"token|password|secret|authorization|^pat$|credential_value", key, re.I):
                raise DeploymentError("Secret-bearing fields cannot be saved.")
            nonsecret(child)
    elif isinstance(value, list):
        for child in value:
            nonsecret(child)
    elif isinstance(value, str):
        if (re.search(r"(?i)(bearer\s|dapi[a-z0-9]{20,}|[?&](sig|token|key|password)=)", value)
                or re.search(r"https?://[^/\s]*@", value)
                or re.search(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.", value)):
            raise DeploymentError("Credentials and signed URLs cannot be saved.")
        if any(ord(c) < 32 for c in value):
            raise DeploymentError("Control characters cannot be saved.")
    elif value is not None and not isinstance(value, (bool, int, float)):
        raise DeploymentError("State must contain only non-secret JSON values.")


def digest(value) -> str:
    nonsecret(value)
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def private_path(path: Path):
    path = path.absolute()
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise DeploymentError("Private state cannot use symlink paths.")
    repo = Path(__file__).resolve().parents[2]
    if path == repo or (repo in path.parents and repo / ".demo-deploy" not in path.parents):
        raise DeploymentError("Inside this repository, private files must be under .demo-deploy.")


def restrict(path: Path):
    if os.name == "nt":
        who = subprocess.run(["whoami"], capture_output=True, text=True, check=True).stdout.strip()
        result = subprocess.run(
            ["icacls", str(path), "/inheritance:r", "/grant:r", f"{who}:(F)"],
            capture_output=True, check=False,
        )
        if result.returncode:
            raise DeploymentError("Could not restrict private file ACL; no cloud write is safe.")
    else:
        path.chmod(0o600)


def write_private(path: Path, value):
    nonsecret(value)
    private_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        path.parent.chmod(0o700)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".private-")
    temp = Path(temporary)
    try:
        os.close(fd)
        restrict(temp)
        with temp.open("w", encoding="utf-8") as stream:
            json.dump(value, stream, indent=2, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


class State:
    def __init__(self, path: Path, config: dict):
        self.path = path
        fingerprint = digest(config)
        private_path(path)
        if path.exists():
            self.data = json.loads(path.read_text(encoding="utf-8"))
            nonsecret(self.data)
            if self.data.get("version") != 1 or self.data.get("config_hash") != fingerprint:
                raise DeploymentError("State/config mismatch. Resume with the original configuration.")
        else:
            self.data = {"version": 1, "deployment_id": str(uuid4()), "config_hash": fingerprint,
                         "steps": {}, "gates": {}, "inputs": {}, "results": {}}

    def save(self):
        write_private(self.path, self.data)

    @contextmanager
    def lock(self):
        private_path(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lock = self.path.with_suffix(".lock")
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            raise DeploymentError("State is locked. Inspect the owning process before removing a stale lock.") from None
        try:
            os.close(fd)
            restrict(lock)
            self.save()
            yield
        finally:
            lock.unlink(missing_ok=True)

    def step(self, key, specification, submit, finish, reconcile=None):
        fingerprint = digest(specification)
        record = self.data["steps"].get(key)
        if record and record["hash"] != fingerprint:
            raise DeploymentError(f"Step specification changed: {key}. Refusing to overwrite.")
        if record and record["status"] == "done":
            if reconcile:
                reconcile(record["result"])
            return record["result"]
        if record and "handle" not in record:
            raise DeploymentError(f"Uncertain partial write: {key}. Inspect service audit/operation records; do not retry or recreate.")
        if not record:
            record = {"hash": fingerprint, "status": "intent", "owned": True}
            self.data["steps"][key] = record
            self.save()
            handle = submit()
            nonsecret(handle)
            record["handle"] = handle
            record["status"] = "submitted"
            self.save()
        result = finish(record["handle"])
        nonsecret(result)
        record.update(status="done", result=result)
        self.save()
        return result

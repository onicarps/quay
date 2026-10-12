"""Workspace layout, owned-file safety, process identity, and the global registry.

All durable Quay state for a workspace lives in ``<workspace>/.quay/``:

===================  ==========================================
``ledger.db``        SQLite event ledger (plus ``-wal``/``-shm``)
``quay.sock``        control socket (owner-only, ``0o600``)
``quay.pid``         daemon identity record
``bootstrap.lock``   flock used to serialize daemon startup
===================  ==========================================

A small cross-workspace registry lives at
``${XDG_CONFIG_HOME:-~/.config}/quay/workspaces.json`` so ``quay daemons`` can
show every workspace this user has a daemon in. Quay never deletes a ledger
implicitly. The only Git file Quay ever touches is the enclosing worktree's
``.gitignore``, and only to append the single ``.quay/`` line during
onboarding (see :func:`ensure_git_ignore`).
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import stat
import subprocess
import sys
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

STATE_DIRECTORY = ".quay"
DATABASE_NAME = "ledger.db"
SOCKET_NAME = "quay.sock"
PID_NAME = "quay.pid"
LOCK_NAME = "bootstrap.lock"

#: Directory that contains the ``quay`` package. Exported so a spawned daemon
#: can import Quay even when the caller is running from an uninstalled checkout.
PACKAGE_PARENT = Path(__file__).resolve().parents[1]

__all__ = [
    "STATE_DIRECTORY",
    "DATABASE_NAME",
    "SOCKET_NAME",
    "PID_NAME",
    "LOCK_NAME",
    "PACKAGE_PARENT",
    "WorkspaceError",
    "LedgerPaths",
    "canonical_workspace",
    "paths_for",
    "utc_now",
    "assert_owned_file",
    "write_owner_file",
    "unlink_owned",
    "bootstrap_lock",
    "process_start_identity",
    "process_alive",
    "read_json_file",
    "live_daemon_record",
    "config_directory",
    "registry_path",
    "load_registry",
    "write_registry",
    "register_workspace",
    "unregister_workspace",
    "git_advisory",
    "git_worktree_root",
    "ensure_git_ignore",
]


class WorkspaceError(RuntimeError):
    """An actionable, user-facing workspace or lifecycle error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "detail": {}}


@dataclass(frozen=True)
class LedgerPaths:
    """Absolute paths for one workspace's runtime state."""

    workspace: Path
    root: Path
    database: Path
    socket: Path
    pid: Path
    lock: Path


def utc_now() -> str:
    return (
        datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    )


def canonical_workspace(value: str | os.PathLike[str]) -> Path:
    """Resolve and validate a workspace directory."""
    try:
        root = Path(value).expanduser().resolve(strict=True)
    except OSError as exc:
        raise WorkspaceError("workspace_unavailable", f"workspace is unavailable: {value}") from exc
    if not root.is_dir():
        raise WorkspaceError("workspace_not_directory", f"workspace is not a directory: {root}")
    return root


def paths_for(workspace: str | os.PathLike[str], *, create: bool = True) -> LedgerPaths:
    """Return the runtime paths for ``workspace``, creating ``.quay`` if asked."""
    root = canonical_workspace(workspace)
    if root.parent == root:
        raise WorkspaceError(
            "unsafe_workspace",
            f"refusing to use the filesystem root as a workspace: {root}",
        )
    state_root = root / STATE_DIRECTORY
    if state_root.exists() or state_root.is_symlink():
        info = state_root.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise WorkspaceError(
                "unsafe_state_path", f"refusing non-directory runtime path: {state_root}"
            )
    elif create:
        try:
            state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError as exc:
            raise WorkspaceError(
                "workspace_not_writable", f"cannot create {state_root}: {exc}"
            ) from exc
    else:
        raise WorkspaceError("no_state", f"no Quay state in {root}")
    if create:
        os.chmod(state_root, 0o700)
    return LedgerPaths(
        workspace=root,
        root=state_root,
        database=state_root / DATABASE_NAME,
        socket=state_root / SOCKET_NAME,
        pid=state_root / PID_NAME,
        lock=state_root / LOCK_NAME,
    )


# ----------------------------------------------------------------- owned files


def assert_owned_file(path: Path) -> None:
    """Refuse to write through a symlink at an owned runtime path."""
    if path.exists() or path.is_symlink():
        if stat.S_ISLNK(path.lstat().st_mode):
            raise WorkspaceError("symlinked_runtime_file", f"refusing symlinked runtime file: {path}")


def write_owner_file(path: Path, contents: str) -> None:
    """Write an owner-only file atomically, refusing symlinks and using ``O_NOFOLLOW``."""
    assert_owned_file(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", closefd=False) as handle:
            handle.write(contents)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(fd)


def unlink_owned(path: Path, *, missing_ok: bool = True) -> bool:
    """Remove an owned runtime file. Returns True if something was removed."""
    if path.exists() or path.is_symlink():
        try:
            path.unlink()
            return True
        except FileNotFoundError:
            return missing_ok
    return False


@contextlib.contextmanager
def bootstrap_lock(paths: LedgerPaths) -> Iterator[None]:
    """Serialize daemon startup and stale-state reclaim within one workspace."""
    assert_owned_file(paths.lock)
    fd = os.open(paths.lock, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


# ------------------------------------------------------------ process identity


def process_start_identity(pid: int) -> str | None:
    """Return an immutable identity for a live process, or None.

    Uses the Linux process start tick when available. Falls back to the
    ``ps`` reporting time on platforms without ``/proc``, which is stable for
    the lifetime of a process but coarser.
    """
    if pid <= 0:
        return None
    proc_stat = Path(f"/proc/{pid}/stat")
    try:
        fields = proc_stat.read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        return f"linux-ticks:{fields[19]}"
    except (FileNotFoundError, IndexError, OSError):
        pass
    try:
        result = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        stamp = result.stdout.strip()
        return f"ps-lstart:{stamp}" if result.returncode == 0 and stamp else None
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


def process_alive(pid: int, expected_identity: str | None) -> bool:
    """Return True only if ``pid`` is alive *and* still the expected process."""
    if not isinstance(pid, int) or pid <= 0 or not expected_identity:
        return False
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    return process_start_identity(pid) == str(expected_identity)


def read_json_file(path: Path) -> dict[str, Any] | None:
    """Read a JSON object from an owned file, returning None on any problem."""
    try:
        if path.is_symlink():
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None


def live_daemon_record(paths: LedgerPaths) -> dict[str, Any] | None:
    """Return the PID record only if it names a process that is still alive."""
    record = read_json_file(paths.pid)
    if not record:
        return None
    if process_alive(record.get("pid", 0), record.get("identity")):
        return record
    return None


# ------------------------------------------------------------------- registry


def config_directory() -> Path:
    base = Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))).expanduser()
    root = (base / "quay").resolve()
    if root.exists() or root.is_symlink():
        mode = root.lstat().st_mode
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise WorkspaceError("unsafe_config_path", f"refusing invalid config path: {root}")
    else:
        root.mkdir(parents=True, mode=0o700)
    os.chmod(root, 0o700)
    return root


def registry_path() -> Path:
    return config_directory() / "workspaces.json"


@contextlib.contextmanager
def _registry_lock() -> Iterator[None]:
    path = config_directory() / "registry.lock"
    assert_owned_file(path)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def load_registry() -> list[dict[str, Any]]:
    path = registry_path()
    try:
        if path.is_symlink():
            raise WorkspaceError("unsafe_registry", f"refusing symlinked registry: {path}")
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    except OSError as exc:
        raise WorkspaceError("registry_unreadable", f"cannot read registry: {exc}") from exc
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise WorkspaceError("registry_invalid", f"invalid workspace registry: {exc}") from exc
    if not isinstance(value, list) or not all(isinstance(row, dict) for row in value):
        raise WorkspaceError("registry_invalid", "invalid workspace registry shape")
    return value


def write_registry(rows: list[dict[str, Any]]) -> None:
    path = registry_path()
    assert_owned_file(path)
    fd, temporary = tempfile.mkstemp(prefix="registry-", suffix=".tmp", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(rows, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def register_workspace(paths: LedgerPaths, instance_id: str, pid: int, identity: str | None) -> None:
    with _registry_lock():
        rows = [row for row in load_registry() if row.get("workspace") != str(paths.workspace)]
        rows.append(
            {
                "workspace": str(paths.workspace),
                "instance_id": instance_id,
                "pid": pid,
                "identity": identity,
                "socket": str(paths.socket),
                "last_seen": utc_now(),
            }
        )
        write_registry(rows)


def unregister_workspace(paths: LedgerPaths) -> None:
    with _registry_lock():
        rows = load_registry()
        retained = [row for row in rows if row.get("workspace") != str(paths.workspace)]
        if retained != rows:
            write_registry(retained)


def git_advisory(workspace: Path) -> str | None:
    """Return an advisory string when ``.quay/`` is visible to Git.

    ``quay init`` never edits ``.gitignore``; it only tells the user.
    Onboarding (:func:`ensure_git_ignore`) is the one place that appends the
    ignore line, and only for the auto-initialized cockpit and MCP bridge.
    """
    try:
        inside = subprocess.run(
            ["git", "-C", str(workspace), "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        if inside.returncode != 0 or inside.stdout.strip() != "true":
            return None
        ignored = subprocess.run(
            ["git", "-C", str(workspace), "check-ignore", "-q", f"{STATE_DIRECTORY}/"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        if ignored.returncode != 0:
            return (
                f"notice: '{STATE_DIRECTORY}/' is not git-ignored; "
                f"add it to .gitignore to keep ledgers out of commits."
            )
        return None
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


def git_worktree_root(workspace: Path) -> Path | None:
    """Return the enclosing Git worktree root for ``workspace``, or None."""
    try:
        inside = subprocess.run(
            ["git", "-C", str(workspace), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    if inside.returncode != 0:
        return None
    top = inside.stdout.strip()
    return Path(top) if top else None


def ensure_git_ignore(workspace: Path) -> str | None:
    """Append exactly one ``.quay/`` line to the enclosing worktree's ``.gitignore``.

    Returns the path that changed, or ``None`` when there is nothing to do:
    the target is outside Git, the line is already present, or the
    ``.gitignore`` is a symlink (never write through a link). The append is
    idempotent and preserves the existing contents and newline handling.
    """
    top = git_worktree_root(workspace)
    if top is None:
        return None
    target = top / ".gitignore"
    if target.is_symlink():
        return None
    try:
        before = target.read_text(encoding="utf-8") if target.exists() else ""
    except OSError:
        return None
    if any(line.strip() == f"{STATE_DIRECTORY}/" for line in before.splitlines()):
        return None
    suffix = "" if not before or before.endswith("\n") else "\n"
    try:
        target.write_text(before + suffix + f"{STATE_DIRECTORY}/\n", encoding="utf-8")
    except OSError:
        return None
    return str(target)


def daemon_script() -> Path:
    """Path to the module used to spawn a detached daemon."""
    return Path(__file__).with_name("daemon.py")


def default_python() -> str:
    return os.environ.get("QUAY_PYTHON") or sys.executable


def new_instance_id() -> str:
    return uuid.uuid4().hex

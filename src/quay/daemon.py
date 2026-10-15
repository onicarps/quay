"""Daemon lifecycle: start, health-check, reclaim, stop, and the registry.

``quay`` commands auto-start a daemon when one is not already healthy. The
lifecycle is deliberately conservative:

* startup is serialized by an flock so two concurrent clients cannot race into
  two daemons;
* a health check requires both a live PID record *and* a responsive socket
  reporting the same process identity, so a recycled PID is never mistaken for
  our daemon;
* a PID that is alive but unresponsive is never signalled: Quay reports it
  instead of killing an unrelated process;
* stale socket and PID files left by a crash are reclaimed.

The daemon process itself is this module: ``python -m quay.daemon
--workspace <dir>``.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from . import __version__
from .client import ClientError, QuayClient
from .server import QuayServer
from .store import IDLE_SECONDS_DEFAULT
from .workspace import (
    PACKAGE_PARENT,
    WorkspaceError,
    bootstrap_lock,
    default_python,
    git_advisory,
    live_daemon_record,
    paths_for,
    process_alive,
    read_json_file,
    unlink_owned,
)

#: How long to wait for a freshly spawned daemon to answer a health check.
STARTUP_TIMEOUT_SECONDS = 8.0

#: How long to wait for a daemon to release its socket after a stop request.
STOP_TIMEOUT_SECONDS = 8.0

__all__ = [
    "daemon_main",
    "ensure_daemon",
    "stop_daemon",
    "daemon_health",
    "spawn_daemon",
    "reclaim_stale_state",
    "all_daemon_status",
    "STARTUP_TIMEOUT_SECONDS",
]


def daemon_health(workspace: str | os.PathLike[str]) -> dict[str, Any] | None:
    """Return the daemon health for ``workspace``, or None if not healthy."""
    try:
        paths = paths_for(workspace, create=False)
    except WorkspaceError:
        return None
    return health_at(paths)


def health_at(paths) -> dict[str, Any] | None:
    record = live_daemon_record(paths)
    if record is None:
        return None
    try:
        with QuayClient(paths.socket, timeout=2.0) as client:
            health = client.health()
    except (ClientError, OSError):
        return None
    if (
        health.get("pid") != record.get("pid")
        or health.get("instance_id") != record.get("instance_id")
        or health.get("protocol_version") is None
    ):
        return None
    return health


def reclaim_stale_state(paths) -> None:
    """Remove socket/PID files left behind by a daemon that is no longer alive."""
    record = read_json_file(paths.pid)
    if record is not None and process_alive(record.get("pid", 0), record.get("identity")):
        raise WorkspaceError(
            "daemon_unresponsive",
            "a live process holds this workspace's PID file but is not answering on the "
            "control socket; refusing to reclaim or signal it. Investigate with "
            f"'ps -p {record.get('pid')}' or remove {paths.pid} manually.",
        )
    unlink_owned(paths.socket)
    unlink_owned(paths.pid)


def spawn_daemon(paths, *, idle_seconds: float | None = None) -> int:
    """Start a detached daemon process and return its PID."""
    command = [default_python(), "-m", "quay.daemon", "--workspace", str(paths.workspace)]
    if idle_seconds is not None:
        command += ["--idle-seconds", str(idle_seconds)]
    environment = dict(os.environ)
    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = (
        f"{PACKAGE_PARENT}{os.pathsep}{existing}" if existing else str(PACKAGE_PARENT)
    )
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
        start_new_session=True,
        env=environment,
    )
    return int(process.pid)


def ensure_daemon(
    workspace: str | os.PathLike[str],
    *,
    idle_seconds: float | None = None,
    timeout: float = STARTUP_TIMEOUT_SECONDS,
) -> tuple[Any, dict[str, Any]]:
    """Return ``(paths, health)`` for a running daemon, starting one if needed."""
    paths = paths_for(workspace)
    with bootstrap_lock(paths):
        health = health_at(paths)
        if health is not None:
            return paths, health
        reclaim_stale_state(paths)
        spawn_daemon(paths, idle_seconds=idle_seconds)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            health = health_at(paths)
            if health is not None:
                return paths, health
            time.sleep(0.05)
        reclaim_stale_state(paths)
        raise WorkspaceError(
            "daemon_start_failed",
            f"the daemon did not become healthy within {timeout:.0f}s",
        )


def stop_daemon(workspace: str | os.PathLike[str], *, timeout: float = STOP_TIMEOUT_SECONDS) -> bool:
    """Ask the daemon for ``workspace`` to stop. Returns True if one was running."""
    try:
        paths = paths_for(workspace, create=False)
    except WorkspaceError:
        return False
    health = health_at(paths)
    if health is None:
        record = read_json_file(paths.pid)
        if record is not None and process_alive(record.get("pid", 0), record.get("identity")):
            raise WorkspaceError(
                "daemon_unresponsive",
                "a live process holds this workspace's PID file but is not answering; "
                "refusing to signal it.",
            )
        reclaim_stale_state(paths)
        return False
    try:
        with QuayClient(paths.socket, timeout=3.0) as client:
            client.request("stop", {"reason": "client"})
    except ClientError as exc:
        if exc.code not in ("transport_error", "daemon_closed", "timeout"):
            raise
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not paths.socket.exists():
            return True
        time.sleep(0.05)
    raise WorkspaceError("daemon_stop_failed", "the daemon did not release its socket in time")


def all_daemon_status() -> list[dict[str, Any]]:
    """Return the registry joined with live health for every known workspace."""
    from .workspace import load_registry

    rows: list[dict[str, Any]] = []
    for entry in load_registry():
        workspace = entry.get("workspace")
        if not isinstance(workspace, str):
            continue
        health = daemon_health(workspace)
        rows.append(
            {
                "workspace": workspace,
                "pid": entry.get("pid"),
                "registered_at": entry.get("last_seen"),
                "running": health is not None,
                "health": health,
            }
        )
    return rows


def idle_from_env(default: float = IDLE_SECONDS_DEFAULT) -> float:
    raw = os.environ.get("QUAY_IDLE_SECONDS")
    if raw is None or not raw.strip():
        return float(default)
    try:
        value = float(raw)
    except ValueError as exc:
        raise WorkspaceError("invalid_idle", "QUAY_IDLE_SECONDS must be a number") from exc
    return value


def daemon_main(argv: list[str] | None = None) -> int:
    """Entry point for ``python -m quay.daemon`` / the ``quayd`` script."""
    # Establish the process-wide creation mask before SQLite can create the
    # database, WAL, or shared-memory sidecars. Explicit chmod remains defense
    # in depth for already-existing paths.
    os.umask(0o077)
    parser = argparse.ArgumentParser(prog="quayd", description="Quay workspace daemon")
    parser.add_argument("--workspace", default=os.getcwd(), help="workspace root")
    parser.add_argument("--socket", default=None, help="override the control socket path")
    parser.add_argument(
        "--idle-seconds",
        type=float,
        default=None,
        help="shut down after this many idle seconds (0 disables idle shutdown)",
    )
    parser.add_argument("--heartbeat-seconds", type=float, default=10.0)
    parser.add_argument("--subscriber-queue", type=int, default=4096)
    parser.add_argument("--synchronous", default=os.environ.get("QUAY_SQLITE_SYNCHRONOUS", "NORMAL"))
    parser.add_argument("--version", action="version", version=f"quayd (quay {__version__})")
    args = parser.parse_args(argv)

    idle = args.idle_seconds if args.idle_seconds is not None else idle_from_env()
    advisory = git_advisory(Path(args.workspace).expanduser())
    if advisory:
        print(f"quayd: {advisory}", file=sys.stderr)

    try:
        server = QuayServer(
            args.workspace,
            socket_path=args.socket,
            idle_seconds=idle,
            heartbeat_seconds=args.heartbeat_seconds,
            subscriber_queue_size=args.subscriber_queue,
            synchronous=args.synchronous,
        )
    except (WorkspaceError, OSError) as exc:
        print(f"quayd: error: {exc}", file=sys.stderr)
        return 2

    def _handle(signum: int, _frame: Any) -> None:
        server.request_stop(f"signal:{signum}")

    previous_term = signal.signal(signal.SIGTERM, _handle)
    previous_int = signal.signal(signal.SIGINT, _handle)
    try:
        server.serve_forever()
    except (WorkspaceError, OSError) as exc:
        print(f"quayd: error: {exc}", file=sys.stderr)
        return 2
    finally:
        with contextlib.suppress(ValueError):
            signal.signal(signal.SIGTERM, previous_term)
        with contextlib.suppress(ValueError):
            signal.signal(signal.SIGINT, previous_int)
    return 0


def _describe(paths) -> str:
    record = read_json_file(paths.pid)
    return json.dumps(record, sort_keys=True) if record else "{}"


if __name__ == "__main__":  # pragma: no cover - exercised via subprocess
    raise SystemExit(daemon_main())

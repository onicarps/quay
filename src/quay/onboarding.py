"""Zero-friction workspace onboarding, shared by the cockpit and MCP bridge.

One call turns a plain directory into a ready workspace: it creates ``.quay/``
and starts (or reuses) the daemon through :func:`~quay.daemon.ensure_daemon`,
appends ``.quay/`` to the enclosing worktree's ``.gitignore`` when needed, and
writes the single genesis note for a fresh ledger.

Both entry points use the same path so the CLI and the MCP bridge cannot drift
in how they create, ignore, or seed a workspace.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .client import QuayClient
from .daemon import ensure_daemon
from .workspace import LedgerPaths, canonical_workspace, ensure_git_ignore

#: Client timeout for the onboarding request.
CLIENT_TIMEOUT_SECONDS = 15.0

__all__ = ["Onboarding", "onboard", "CLIENT_TIMEOUT_SECONDS"]


@dataclass(frozen=True)
class Onboarding:
    """What onboarding produced, for callers that want to report it."""

    root: Path
    paths: LedgerPaths
    health: dict[str, Any]
    git_ignore: str | None
    genesis: dict[str, Any] | None


def onboard(workspace: str | Path, *, idle_seconds: float | None = None) -> Onboarding:
    """Prepare ``workspace`` and return the resulting paths and side effects."""
    root = canonical_workspace(workspace)
    paths, health = ensure_daemon(root, idle_seconds=idle_seconds)
    git_ignore = ensure_git_ignore(root)
    with QuayClient(paths.socket, timeout=CLIENT_TIMEOUT_SECONDS) as client:
        result = client.request("genesis")
    genesis = result.get("event") if result.get("created") else None
    return Onboarding(root=root, paths=paths, health=health, git_ignore=git_ignore, genesis=genesis)

"""Conflict-safe, standard-library scaffolding for Quay knowledge planes."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from importlib import resources
from pathlib import Path
from string import Template
from typing import Any

from .store import utc_now
from .workspace import STATE_DIRECTORY, WorkspaceError, canonical_workspace, git_common_root, git_worktree_root

SCAFFOLD_VERSION = "1"
MANIFEST_PATH = Path("okf/.scaffold-manifest.json")
TEMPLATE_PATHS = (
    Path("AGENTS.md"),
    Path("okf/index.md"),
    Path("okf/status.md"),
    Path("okf/intent.yaml"),
    Path("okf/log.md"),
    Path("okf/harness/runners.toml"),
    Path("okf/harness/droid.md"),
)

__all__ = [
    "MANIFEST_PATH",
    "SCAFFOLD_VERSION",
    "TEMPLATE_PATHS",
    "ensure_runtime_ignored",
    "scaffold_workspace",
]


def _template_root():
    node = resources.files("quay")
    for part in ("templates", "okf"):
        node = node.joinpath(part)
    return node


def _template_text(relative: Path) -> str:
    node = _template_root()
    for part in relative.parts:
        node = node.joinpath(part)
    try:
        return node.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError) as exc:
        raise WorkspaceError(
            "scaffold_template_missing", f"packaged scaffold template is unavailable: {relative}"
        ) from exc


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists() and not path.is_symlink():
        return None
    if path.is_symlink() or not path.is_file():
        raise WorkspaceError("unsafe_scaffold_path", f"refusing scaffold manifest path: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkspaceError("invalid_scaffold_manifest", f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise WorkspaceError("invalid_scaffold_manifest", f"manifest must contain an object: {path}")
    if value.get("template_version") != SCAFFOLD_VERSION:
        raise WorkspaceError(
            "scaffold_upgrade_required",
            f"scaffold version {value.get('template_version')!r} requires an explicit upgrade",
        )
    if not isinstance(value.get("generated_at"), str) or not value["generated_at"].strip():
        raise WorkspaceError("invalid_scaffold_manifest", "manifest generated_at must be a string")
    if not isinstance(value.get("project_name"), str) or not value["project_name"].strip():
        raise WorkspaceError("invalid_scaffold_manifest", "manifest project_name must be a string")
    return value


def _render_templates(*, project_name: str, generated_at: str) -> dict[Path, str]:
    context = {
        "PROJECT_NAME": project_name,
        "PROJECT_NAME_YAML": json.dumps(project_name, ensure_ascii=False),
        "AGENT_TITLE_YAML": json.dumps(
            f"Agent instructions for {project_name}", ensure_ascii=False
        ),
        "INDEX_TITLE_YAML": json.dumps(f"{project_name} knowledge index", ensure_ascii=False),
        "STATUS_TITLE_YAML": json.dumps(f"{project_name} status", ensure_ascii=False),
        "STATUS_DESCRIPTION_YAML": json.dumps(
            f"Canonical repository-local status for {project_name}.", ensure_ascii=False
        ),
        "LOG_TITLE_YAML": json.dumps(f"{project_name} recent handoffs", ensure_ascii=False),
        "DROID_TITLE_YAML": json.dumps(
            f"Droid mission dispatch for {project_name}", ensure_ascii=False
        ),
        "GENERATED_AT": generated_at,
    }
    rendered: dict[Path, str] = {}
    for relative in TEMPLATE_PATHS:
        try:
            text = Template(_template_text(relative)).substitute(context)
        except (KeyError, ValueError) as exc:
            raise WorkspaceError(
                "invalid_scaffold_template", f"invalid placeholder in template {relative}: {exc}"
            ) from exc
        rendered[relative] = text if text.endswith("\n") else text + "\n"
    return rendered


def _manifest_text(*, project_name: str, generated_at: str, rendered: dict[Path, str]) -> str:
    value = {
        "schema_version": 1,
        "template_version": SCAFFOLD_VERSION,
        "project_name": project_name,
        "generated_at": generated_at,
        "files": {
            path.as_posix(): {"sha256": _sha256(text)}
            for path, text in sorted(rendered.items(), key=lambda item: item[0].as_posix())
        },
    }
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _assert_safe_target(root: Path, target: Path) -> None:
    try:
        relative = target.relative_to(root)
    except ValueError as exc:
        raise WorkspaceError("unsafe_scaffold_path", f"scaffold target escapes workspace: {target}") from exc
    current = root
    for part in relative.parts[:-1]:
        current = current / part
        if current.is_symlink():
            raise WorkspaceError(
                "unsafe_scaffold_path", f"refusing symlinked scaffold directory: {current}"
            )
        if current.exists() and not current.is_dir():
            raise WorkspaceError(
                "scaffold_conflict", f"scaffold parent is not a directory: {current}"
            )
    if target.is_symlink():
        raise WorkspaceError("unsafe_scaffold_path", f"refusing symlinked scaffold file: {target}")


def _atomic_write(target: Path, text: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.parent.is_symlink() or target.is_symlink():
        raise WorkspaceError("unsafe_scaffold_path", f"refusing symlinked scaffold path: {target}")
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    owned_fd: int | None = fd
    try:
        os.fchmod(fd, 0o644)
        stream = os.fdopen(fd, "w", encoding="utf-8", closefd=True)
        owned_fd = None
        with stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        if target.exists() or target.is_symlink():
            raise WorkspaceError("scaffold_race", f"scaffold target appeared while writing: {target}")
        os.replace(temporary, target)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        if owned_fd is not None:
            try:
                os.close(owned_fd)
            except OSError:
                pass
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def scaffold_workspace(
    workspace: str | os.PathLike[str],
    *,
    generated_at: str | None = None,
    check_only: bool = False,
) -> dict[str, Any]:
    """Materialize the packaged OKF scaffold without overwriting user content."""
    root = canonical_workspace(workspace)
    manifest_path = root / MANIFEST_PATH
    manifest = _load_manifest(manifest_path)
    project_name = manifest["project_name"] if manifest is not None else root.name
    creation_time = manifest["generated_at"] if manifest is not None else (generated_at or utc_now())
    if not isinstance(creation_time, str) or not creation_time.strip():
        raise WorkspaceError("invalid_scaffold_time", "scaffold generated_at must be non-empty")
    rendered = _render_templates(project_name=project_name, generated_at=creation_time)
    expected = dict(rendered)
    expected[MANIFEST_PATH] = _manifest_text(
        project_name=project_name, generated_at=creation_time, rendered=rendered
    )

    created: list[str] = []
    unchanged: list[str] = []
    conflicts: list[str] = []
    for relative, text in expected.items():
        target = root / relative
        _assert_safe_target(root, target)
        if target.exists():
            if not target.is_file():
                conflicts.append(relative.as_posix())
                continue
            try:
                existing = target.read_text(encoding="utf-8")
            except OSError as exc:
                raise WorkspaceError("scaffold_unreadable", f"cannot read scaffold target {target}: {exc}") from exc
            if existing == text:
                unchanged.append(relative.as_posix())
            else:
                conflicts.append(relative.as_posix())
        else:
            created.append(relative.as_posix())
    if conflicts:
        raise WorkspaceError(
            "scaffold_conflict",
            "refusing to overwrite non-identical scaffold files: " + ", ".join(conflicts),
        )

    if not check_only:
        # The manifest is the commit marker for the per-file atomic writes.
        ordered = [path for path in TEMPLATE_PATHS if path.as_posix() in created]
        if MANIFEST_PATH.as_posix() in created:
            ordered.append(MANIFEST_PATH)
        for relative in ordered:
            _atomic_write(root / relative, expected[relative])
    return {
        "template_version": SCAFFOLD_VERSION,
        "manifest": str(manifest_path),
        "created": created,
        "unchanged": unchanged,
        "conflicts": [],
        "check_only": check_only,
    }


def _append_ignore_line(target: Path) -> bool:
    if target.is_symlink():
        raise WorkspaceError("unsafe_ignore_path", f"refusing symlinked ignore file: {target}")
    try:
        before = target.read_text(encoding="utf-8") if target.exists() else ""
    except OSError as exc:
        raise WorkspaceError("ignore_unreadable", f"cannot read ignore file {target}: {exc}") from exc
    if any(line.strip() == f"{STATE_DIRECTORY}/" for line in before.splitlines()):
        return False
    suffix = "" if not before or before.endswith("\n") else "\n"
    updated = before + suffix + f"{STATE_DIRECTORY}/\n"
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        if target.exists():
            os.fchmod(fd, target.stat().st_mode & 0o777)
        else:
            os.fchmod(fd, 0o644)
        with os.fdopen(fd, "w", encoding="utf-8", closefd=True) as stream:
            fd = -1
            stream.write(updated)
            stream.flush()
            os.fsync(stream.fileno())
        if target.is_symlink():
            raise WorkspaceError("unsafe_ignore_path", f"refusing symlinked ignore file: {target}")
        os.replace(temporary, target)
    except Exception:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
    return True


def _git_exclude_path(workspace: Path) -> Path | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(workspace), "rev-parse", "--git-path", "info/exclude"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None
    candidate = Path(result.stdout.strip())
    if candidate.is_absolute():
        return candidate
    # Normalize ``..`` without resolving the final path: a symlinked exclude
    # file must remain visible to the refusal in ``_append_ignore_line``.
    return Path(os.path.abspath(workspace / candidate))


def ensure_runtime_ignored(
    workspace: str | os.PathLike[str], *, bare: bool
) -> dict[str, Any]:
    """Ignore ``.quay/`` in tracked ``.gitignore`` or bare-mode local exclude."""
    root = canonical_workspace(workspace)
    worktree = git_worktree_root(root)
    if worktree is None:
        return {"applied": False, "path": None, "scope": None}
    if bare:
        target = _git_exclude_path(root)
        scope = "git-info-exclude"
    else:
        target = git_common_root(root) / ".gitignore"
        scope = "gitignore"
    if target is None:
        return {"applied": False, "path": None, "scope": scope}
    applied = _append_ignore_line(target)
    return {"applied": applied, "path": str(target), "scope": scope}

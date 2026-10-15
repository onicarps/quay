"""Workspace topology tests, including linked Git worktrees."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import quay.workspace as workspace_module
from quay.workspace import WorkspaceError, git_common_root, paths_for


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


def test_non_git_workspace_keeps_its_own_runtime_root(tmp_path: Path):
    workspace = tmp_path / "plain"
    workspace.mkdir()
    assert git_common_root(workspace) == workspace
    assert paths_for(workspace).root == workspace / ".quay"


def test_linked_worktrees_share_primary_ledger_and_socket(tmp_path: Path):
    primary = tmp_path / "repo"
    linked = tmp_path / "linked"
    primary.mkdir()
    _git("init", "-q", cwd=primary)
    _git("config", "user.email", "quay@example.invalid", cwd=primary)
    _git("config", "user.name", "Quay Test", cwd=primary)
    (primary / "README.md").write_text("quay\n", encoding="utf-8")
    _git("add", "README.md", cwd=primary)
    _git("commit", "-qm", "initial", cwd=primary)
    _git("worktree", "add", "-qb", "linked-test", str(linked), cwd=primary)

    assert git_common_root(primary) == primary
    assert git_common_root(linked) == primary
    primary_paths = paths_for(primary)
    linked_paths = paths_for(linked)
    assert linked_paths == primary_paths
    assert linked_paths.database == primary / ".quay" / "ledger.db"
    assert linked_paths.socket == primary / ".quay" / "quay.sock"


def test_bare_repository_keeps_runtime_state_inside_the_repository(tmp_path: Path):
    bare = tmp_path / "bare.git"
    subprocess.run(
        ["git", "init", "--bare", "-q", str(bare)],
        check=True,
        capture_output=True,
        text=True,
    )

    assert git_common_root(bare) == bare
    paths = paths_for(bare)
    assert paths.root == bare / ".quay"
    assert paths.database == bare / ".quay" / "ledger.db"


def test_separate_git_dir_keeps_runtime_state_in_the_worktree(tmp_path: Path):
    worktree = tmp_path / "checkout"
    git_dir = tmp_path / "private-git-storage"
    subprocess.run(
        ["git", "init", "-q", "--separate-git-dir", str(git_dir), str(worktree)],
        check=True,
        capture_output=True,
        text=True,
    )

    assert git_common_root(worktree) == worktree
    assert paths_for(worktree).root == worktree / ".quay"
    assert not (git_dir.parent / ".quay").exists()


def test_submodule_keeps_runtime_state_in_the_submodule_worktree(tmp_path: Path):
    child_source = tmp_path / "child-source"
    child_source.mkdir()
    _git("init", "-q", cwd=child_source)
    _git("config", "user.email", "quay@example.invalid", cwd=child_source)
    _git("config", "user.name", "Quay Test", cwd=child_source)
    (child_source / "README.md").write_text("child\n", encoding="utf-8")
    _git("add", "README.md", cwd=child_source)
    _git("commit", "-qm", "initial", cwd=child_source)

    parent = tmp_path / "parent"
    parent.mkdir()
    _git("init", "-q", cwd=parent)
    _git("config", "user.email", "quay@example.invalid", cwd=parent)
    _git("config", "user.name", "Quay Test", cwd=parent)
    subprocess.run(
        [
            "git",
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "-q",
            str(child_source),
            "vendor/child",
        ],
        cwd=parent,
        check=True,
        capture_output=True,
        text=True,
    )
    submodule = parent / "vendor" / "child"

    assert git_common_root(submodule) == submodule
    assert paths_for(submodule).root == submodule / ".quay"
    assert not (parent / ".git" / "modules" / ".quay").exists()


def test_transient_git_failure_is_not_cached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    repository = tmp_path / "repo"
    repository.mkdir()
    _git("init", "-q", cwd=repository)
    workspace_module._git_common_root_cached.cache_clear()  # noqa: SLF001
    real_run = workspace_module.subprocess.run
    failed = False

    def transient_timeout(args, **kwargs):
        nonlocal failed
        if "--git-common-dir" in args and not failed:
            failed = True
            raise subprocess.TimeoutExpired(args, kwargs.get("timeout", 2))
        return real_run(args, **kwargs)

    monkeypatch.setattr(workspace_module.subprocess, "run", transient_timeout)
    with pytest.raises(WorkspaceError) as excinfo:
        git_common_root(repository)
    assert excinfo.value.code == "git_unavailable"
    assert git_common_root(repository) == repository


def test_git_common_root_uses_pre_231_compatible_rev_parse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    repository = tmp_path / "repo"
    repository.mkdir()
    _git("init", "-q", cwd=repository)
    workspace_module._git_common_root_cached.cache_clear()  # noqa: SLF001
    real_run = workspace_module.subprocess.run
    calls: list[list[str]] = []

    def record_run(args, **kwargs):
        calls.append(list(args))
        return real_run(args, **kwargs)

    monkeypatch.setattr(workspace_module.subprocess, "run", record_run)

    assert git_common_root(repository) == repository
    common_dir_calls = [args for args in calls if "--git-common-dir" in args]
    assert len(common_dir_calls) == 1
    assert "--path-format=absolute" not in common_dir_calls[0]


def test_dubious_ownership_falls_back_to_the_requested_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    repository = tmp_path / "repo"
    repository.mkdir()
    workspace_module._git_common_root_cached.cache_clear()  # noqa: SLF001

    def dubious(args, **kwargs):
        return subprocess.CompletedProcess(
            args,
            128,
            stdout="",
            stderr="fatal: detected dubious ownership in repository",
        )

    monkeypatch.setattr(workspace_module.subprocess, "run", dubious)

    assert git_common_root(repository) == repository


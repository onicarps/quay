from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from importlib import resources
from pathlib import Path

import pytest

from quay.scaffold import MANIFEST_PATH, TEMPLATE_PATHS, scaffold_workspace
from quay.workspace import WorkspaceError


def test_scaffold_creates_manifested_packaged_templates(tmp_path: Path):
    root = tmp_path / "project"
    root.mkdir()

    result = scaffold_workspace(root)

    assert result["created"] == [path.as_posix() for path in (*TEMPLATE_PATHS, MANIFEST_PATH)]
    manifest = json.loads((root / MANIFEST_PATH).read_text(encoding="utf-8"))
    assert manifest["template_version"] == "1"
    assert manifest["project_name"] == "project"
    for relative in TEMPLATE_PATHS:
        text = (root / relative).read_text(encoding="utf-8")
        assert "$PROJECT_NAME" not in text
        assert manifest["files"][relative.as_posix()]["sha256"] == hashlib.sha256(
            text.encode("utf-8")
        ).hexdigest()


def test_scaffold_second_run_is_byte_identical(tmp_path: Path):
    root = tmp_path / "project"
    root.mkdir()
    scaffold_workspace(root)
    before = {path: (root / path).read_bytes() for path in (*TEMPLATE_PATHS, MANIFEST_PATH)}

    result = scaffold_workspace(root)

    assert result["created"] == []
    assert result["unchanged"] == [path.as_posix() for path in (*TEMPLATE_PATHS, MANIFEST_PATH)]
    assert before == {path: (root / path).read_bytes() for path in before}


def test_scaffold_conflict_aborts_before_creating_any_template(tmp_path: Path):
    root = tmp_path / "project"
    root.mkdir()
    (root / "AGENTS.md").write_text("team-owned\n", encoding="utf-8")

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "scaffold_conflict"
    assert not (root / "okf").exists()
    assert (root / "AGENTS.md").read_text(encoding="utf-8") == "team-owned\n"


def test_scaffold_refuses_symlink_target(tmp_path: Path):
    root = tmp_path / "project"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("untouched\n", encoding="utf-8")
    (root / "AGENTS.md").symlink_to(outside)

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "unsafe_scaffold_path"
    assert outside.read_text(encoding="utf-8") == "untouched\n"


def test_scaffold_reports_a_non_directory_parent_as_a_conflict(tmp_path: Path):
    root = tmp_path / "project"
    root.mkdir()
    (root / "okf").write_text("not a directory\n", encoding="utf-8")

    with pytest.raises(WorkspaceError) as excinfo:
        scaffold_workspace(root)

    assert excinfo.value.code == "scaffold_conflict"
    assert not (root / "AGENTS.md").exists()


def test_template_assets_are_importable_package_resources():
    root = resources.files("quay").joinpath("templates").joinpath("okf")
    for relative in TEMPLATE_PATHS:
        node = root
        for part in relative.parts:
            node = node.joinpath(part)
        assert node.is_file(), relative


@pytest.mark.skipif(shutil.which("npm") is None, reason="npm is not installed")
def test_npm_dry_run_includes_template_assets():
    repository = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        ["npm", "pack", "--dry-run", "--json"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout)
    files = {row["path"] for row in payload[0]["files"]}
    assert "src/quay/templates/okf/AGENTS.md" in files
    assert "src/quay/templates/okf/okf/harness/runners.toml" in files

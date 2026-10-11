"""Regression checks for the public fresh-machine installer."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_universal_installer_bootstraps_an_isolated_managed_runtime():
    script = (ROOT / "install.sh").read_text(encoding="utf-8")

    assert "uv/install.sh" in script
    assert "tool install --upgrade" in script
    assert "--managed-python" in script
    assert "UV_TOOL_BIN_DIR" in script
    assert "pip install --user" not in script


def test_readme_makes_the_universal_installer_the_primary_path():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "One command (recommended)" in readme
    assert "raw.githubusercontent.com/onicarps/quay/main/install.sh" in readme
    assert "python3 -m venv .venv" in readme

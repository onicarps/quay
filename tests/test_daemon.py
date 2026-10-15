"""Daemon lifecycle tests using real detached processes (requirements F10, F17).

These tests spawn the actual daemon entry point, so they cover process
identity, startup serialization, stale-state reclaim, idle shutdown, and the
registry exactly as a user would hit them.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from conftest import SRC
from quay.client import ClientError, QuayClient
from quay.daemon import all_daemon_status, daemon_health, ensure_daemon, stop_daemon
from quay.workspace import (
    WorkspaceError,
    load_registry,
    paths_for,
    process_start_identity,
)


@pytest.fixture()
def daemon(workspace: Path):
    """A real detached daemon for the workspace, stopped on teardown."""
    paths, health = ensure_daemon(workspace, idle_seconds=0)
    try:
        yield paths, health
    finally:
        try:
            stop_daemon(workspace)
        except WorkspaceError:
            pass


class TestStartAndReuse:
    def test_ensure_daemon_starts_a_real_daemon(self, workspace: Path, daemon):
        paths, health = daemon
        assert health["pid"] != os.getpid()
        assert health["workspace"] == str(workspace)
        assert paths.socket.exists()
        assert paths.pid.exists()
        assert paths.socket.stat().st_mode & 0o777 == 0o600

        with QuayClient(paths.socket) as client:
            assert client.emit(topic="daemon", producer="p", payload={"hello": True})["seq"] == 1

        assert paths.database.stat().st_mode & 0o777 == 0o600
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(paths.database) + suffix)
            assert sidecar.exists(), f"SQLite sidecar was not created: {sidecar}"
            assert sidecar.stat().st_mode & 0o777 == 0o600

    def test_second_call_reuses_the_same_daemon(self, workspace: Path, daemon):
        _, first = daemon
        _, second = ensure_daemon(workspace, idle_seconds=0)
        assert second["instance_id"] == first["instance_id"]
        assert second["pid"] == first["pid"]

    def test_concurrent_startups_converge_on_one_daemon(self, workspace: Path):
        results: list[dict] = []
        errors: list[Exception] = []
        barrier = threading.Barrier(4)

        def start() -> None:
            try:
                barrier.wait(timeout=10)
                _, health = ensure_daemon(workspace, idle_seconds=0)
                results.append(health)
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=start) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        try:
            assert errors == []
            assert len({health["instance_id"] for health in results}) == 1
            # Exactly one daemon process is answering.
            assert len({health["pid"] for health in results}) == 1
        finally:
            stop_daemon(workspace)

    def test_health_is_none_when_no_daemon_runs(self, workspace: Path):
        assert daemon_health(workspace) is None

    def test_health_is_none_for_an_unknown_workspace(self, tmp_path: Path):
        assert daemon_health(tmp_path / "does-not-exist") is None


class TestStop:
    def test_stop_removes_socket_and_pid(self, workspace: Path, daemon):
        paths, _ = daemon
        assert stop_daemon(workspace) is True
        assert not paths.socket.exists()
        assert not paths.pid.exists()
        assert daemon_health(workspace) is None

    def test_stop_is_idempotent(self, workspace: Path, daemon):
        assert stop_daemon(workspace) is True
        assert stop_daemon(workspace) is False

    def test_stop_on_a_workspace_without_state_is_false(self, workspace: Path):
        assert stop_daemon(workspace) is False

    def test_restart_produces_a_new_instance(self, workspace: Path, daemon):
        _, first = daemon
        stop_daemon(workspace)
        _, second = ensure_daemon(workspace, idle_seconds=0)
        try:
            assert second["instance_id"] != first["instance_id"]
            assert second["pid"] != first["pid"]
        finally:
            stop_daemon(workspace)


class TestStaleState:
    def test_dead_pid_file_and_socket_are_reclaimed(self, workspace: Path):
        paths = paths_for(workspace)
        # A PID that is certainly not running, plus leftover runtime files.
        paths.pid.write_text(
            json.dumps({"instance_id": "dead", "pid": 999_999_999, "identity": "linux-ticks:1"}),
            encoding="utf-8",
        )
        paths.socket.write_text("", encoding="utf-8")

        _, health = ensure_daemon(workspace, idle_seconds=0)
        try:
            assert health["instance_id"] != "dead"
            with QuayClient(paths.socket) as client:
                assert client.request("ping")["pong"] is True
        finally:
            stop_daemon(workspace)

    def test_live_but_unresponsive_process_is_reported_not_killed(self, workspace: Path):
        paths = paths_for(workspace)
        paths.pid.write_text(
            json.dumps(
                {
                    "instance_id": "squatter",
                    "pid": os.getpid(),
                    "identity": process_start_identity(os.getpid()),
                }
            ),
            encoding="utf-8",
        )
        with pytest.raises(WorkspaceError) as excinfo:
            ensure_daemon(workspace, idle_seconds=0)
        assert excinfo.value.code == "daemon_unresponsive"
        # The squatter's PID file must survive: Quay never guesses.
        assert paths.pid.exists()

    def test_stop_refuses_to_signal_a_live_unresponsive_process(self, workspace: Path):
        paths = paths_for(workspace)
        paths.pid.write_text(
            json.dumps(
                {
                    "instance_id": "squatter",
                    "pid": os.getpid(),
                    "identity": process_start_identity(os.getpid()),
                }
            ),
            encoding="utf-8",
        )
        with pytest.raises(WorkspaceError) as excinfo:
            stop_daemon(workspace)
        assert excinfo.value.code == "daemon_unresponsive"


class TestIdleShutdown:
    def test_idle_daemon_exits_by_itself(self, workspace: Path):
        paths, _ = ensure_daemon(workspace, idle_seconds=0.5)
        deadline = time.monotonic() + 20.0
        while time.monotonic() < deadline and paths.socket.exists():
            time.sleep(0.1)
        assert not paths.socket.exists(), "an idle daemon must release its socket"
        assert not paths.pid.exists()
        assert daemon_health(workspace) is None

    def test_idle_zero_disables_shutdown(self, workspace: Path, daemon):
        paths, _ = daemon
        time.sleep(1.2)
        assert paths.socket.exists()
        assert daemon_health(workspace) is not None


class TestRegistry:
    def test_registry_records_the_workspace(self, workspace: Path, daemon):
        rows = load_registry()
        assert [row["workspace"] for row in rows] == [str(workspace)]
        assert rows[0]["pid"] == daemon[1]["pid"]

    def test_all_daemon_status_joins_registry_with_health(self, workspace: Path, daemon):
        rows = all_daemon_status()
        assert len(rows) == 1
        assert rows[0]["workspace"] == str(workspace)
        assert rows[0]["running"] is True
        assert rows[0]["health"]["instance_id"] == daemon[1]["instance_id"]

    def test_registry_cleared_on_stop(self, workspace: Path, daemon):
        stop_daemon(workspace)
        assert load_registry() == []

    def test_all_daemon_status_reports_stopped_entries(self, workspace: Path, daemon):
        paths, _ = daemon
        stop_daemon(workspace)
        # Nothing in the registry afterwards, so the list is empty and honest.
        assert all_daemon_status() == []
        assert not paths.socket.exists()


class TestDaemonEntryPoint:
    def test_module_entry_point_reports_version(self):
        result = subprocess.run(
            [sys.executable, "-m", "quay.daemon", "--version"],
            capture_output=True,
            text=True,
            timeout=30,
            env={**os.environ, "PYTHONPATH": str(SRC)},
        )
        assert result.returncode == 0
        assert "quayd" in result.stdout

    def test_module_entry_point_rejects_a_second_bind(self, workspace: Path, daemon):
        paths, _ = daemon
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "quay.daemon",
                "--workspace",
                str(workspace),
                "--idle-seconds",
                "0",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env={**os.environ, "PYTHONPATH": str(SRC)},
        )
        assert result.returncode == 2
        assert "socket_in_use" in result.stderr or "already exists" in result.stderr
        # The original daemon is untouched.
        with QuayClient(paths.socket) as client:
            assert client.request("ping")["pong"] is True

    def test_client_of_a_stopped_daemon_gets_a_clear_error(self, workspace: Path, daemon):
        paths, _ = daemon
        stop_daemon(workspace)
        with pytest.raises(ClientError) as excinfo:
            with QuayClient(paths.socket, timeout=2.0) as client:
                client.request("ping")
        assert excinfo.value.code == "connection_failed"

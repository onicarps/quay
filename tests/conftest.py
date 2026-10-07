"""Shared fixtures for the Quay test suite.

Tests exercise the real storage engine and real sockets. Nothing here mocks
SQLite or the transport; see COMPETITIVE_ANALYSIS.md requirement N7.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from quay.client import QuayClient, SubscriptionStream  # noqa: E402
from quay.server import QuayServer  # noqa: E402
from quay.store import DATABASE_NAME, STATE_DIRECTORY, LedgerStore  # noqa: E402


@pytest.fixture(autouse=True)
def isolated_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Keep the user's real registry untouched: XDG_CONFIG_HOME per test."""
    config = tmp_path / "config"
    config.mkdir()
    monkeypatch.setenv("XDG_CONFIG_HOME", str(config))
    return config


@pytest.fixture()
def workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    root.mkdir()
    return root


@pytest.fixture()
def ledger_path(workspace: Path) -> Path:
    return workspace / STATE_DIRECTORY / DATABASE_NAME


@pytest.fixture()
def store(ledger_path: Path):
    instance = LedgerStore(ledger_path, synchronous="NORMAL")
    try:
        yield instance
    finally:
        instance.close()


@pytest.fixture()
def server(workspace: Path):
    """An in-process daemon on a real Unix socket, with fast timings."""
    instance = QuayServer(
        workspace,
        idle_seconds=0,
        heartbeat_seconds=0.1,
        subscriber_queue_size=64,
        synchronous="NORMAL",
        reap_interval_seconds=0.2,
    )
    instance.start()
    try:
        yield instance
    finally:
        instance.request_stop("test")
        instance.close()


@pytest.fixture()
def client(server: QuayServer):
    instance = QuayClient(server.socket_path, timeout=10.0)
    instance.connect()
    try:
        yield instance
    finally:
        instance.close()


@pytest.fixture()
def stream(server: QuayServer):
    created: list[SubscriptionStream] = []

    def _make(filters=None, *, replay: int = 0) -> SubscriptionStream:
        item = SubscriptionStream(server.socket_path, filters=filters, replay=replay, timeout=5.0)
        item.open()
        created.append(item)
        return item

    try:
        yield _make
    finally:
        for item in created:
            item.close()


def child_env() -> dict[str, str]:
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{SRC}{os.pathsep}{existing}" if existing else str(SRC)
    return env

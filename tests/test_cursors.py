"""Durable named-consumer cursor and prune-guard tests."""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

import quay.cursors as cursors_module
from quay.client import ClientError, QuayClient
from quay.cursors import CursorStore
from quay.store import StoreError


def test_register_is_durable_and_idempotent(tmp_path: Path):
    cursors = CursorStore(tmp_path / ".quay")

    assert cursors.register("quay.distill") == {"last_acked_seq": 0, "in_flight": None}
    assert cursors.register("quay.distill") == {"last_acked_seq": 0, "in_flight": None}
    assert cursors.minimum_acked_seq() == 0


def test_one_outstanding_delivery_is_atomic_and_idempotent(tmp_path: Path):
    cursors = CursorStore(tmp_path / ".quay")
    first = cursors.begin_delivery("codex", 7, "quay:ledger:7")
    assert first["last_acked_seq"] == 0
    assert first["in_flight"]["seq"] == 7
    assert cursors.begin_delivery("codex", 7, "quay:ledger:7") == first

    with pytest.raises(StoreError) as excinfo:
        cursors.begin_delivery("codex", 8, "quay:ledger:8")
    assert excinfo.value.code == "cursor_in_flight"

    acknowledged = cursors.acknowledge("codex", 7, idempotency_key="quay:ledger:7")
    assert acknowledged == {"last_acked_seq": 7, "in_flight": None}
    assert cursors.acknowledge("codex", 7) == acknowledged
    assert json.loads(cursors.path.read_text(encoding="utf-8"))["codex"] == acknowledged
    assert cursors.path.stat().st_mode & 0o777 == 0o600


def test_ack_must_match_in_flight_sequence_and_key(tmp_path: Path):
    cursors = CursorStore(tmp_path / ".quay")
    cursors.begin_delivery("factory", 4, "key-4")
    with pytest.raises(StoreError) as seq_error:
        cursors.acknowledge("factory", 5)
    assert seq_error.value.code == "cursor_ack_mismatch"
    with pytest.raises(StoreError) as key_error:
        cursors.acknowledge("factory", 4, idempotency_key="wrong")
    assert key_error.value.code == "cursor_ack_mismatch"
    assert cursors.get("factory")["in_flight"]["seq"] == 4


def test_cursor_file_symlink_is_refused(tmp_path: Path):
    root = tmp_path / ".quay"
    root.mkdir()
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    (root / "cursors.json").symlink_to(target)
    with pytest.raises(StoreError) as excinfo:
        CursorStore(root).snapshot()
    assert excinfo.value.code == "unsafe_cursor_path"


def test_atomic_write_does_not_double_close_a_transferred_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    root = tmp_path / ".quay"
    root.mkdir()
    cursors = CursorStore(root)
    close_calls: list[int] = []
    real_close = cursors_module.os.close

    def fail_replace(source, destination):
        raise OSError("replace failed")

    def tracked_close(fd: int):
        close_calls.append(fd)
        return real_close(fd)

    monkeypatch.setattr(cursors_module.os, "replace", fail_replace)
    monkeypatch.setattr(cursors_module.os, "close", tracked_close)

    with pytest.raises(OSError, match="replace failed"):
        cursors._write_unlocked({})  # noqa: SLF001 - deterministic ownership seam

    assert close_calls == []
    assert list(root.glob(".cursors.*.tmp")) == []


@pytest.mark.parametrize(
    "row",
    [
        {"last_acked_seq": "7", "in_flight": None},
        {"last_acked_seq": True, "in_flight": None},
        {"last_acked_seq": -1, "in_flight": None},
        {"last_acked_seq": 0, "in_flight": "broken"},
        {
            "last_acked_seq": 2,
            "in_flight": {"seq": 2, "idempotency_key": "k", "delivered_at": "now"},
        },
        {
            "last_acked_seq": 0,
            "in_flight": {"seq": 1, "idempotency_key": "", "delivered_at": "now"},
        },
    ],
)
def test_malformed_cursor_rows_raise_a_stable_error(tmp_path: Path, row: dict):
    cursors = CursorStore(tmp_path / ".quay")
    cursors.root.mkdir()
    cursors.path.write_text(json.dumps({"codex": row}), encoding="utf-8")

    with pytest.raises(StoreError) as excinfo:
        cursors.snapshot()
    assert excinfo.value.code == "invalid_cursor_file"


def test_server_cursor_protocol_guards_pruning(client: QuayClient):
    events = [client.emit(topic="t", producer="p", payload={"i": index}) for index in range(3)]
    delivered = client.request(
        "cursor_deliver",
        {"consumer": "codex", "seq": events[0]["seq"], "idempotency_key": "ledger:1"},
    )
    assert delivered["cursor"]["in_flight"]["seq"] == 1
    acknowledged = client.request(
        "cursor_ack",
        {"consumer": "codex", "seq": 1, "idempotency_key": "ledger:1"},
    )
    assert acknowledged["cursor"]["last_acked_seq"] == 1
    assert client.request("cursor_get", {"consumer": "codex"})["cursor"] == {
        "last_acked_seq": 1,
        "in_flight": None,
    }

    preview = client.request("prune_preview", {"keep_last": 0})
    assert preview["minimum_acked_seq"] == 1
    assert preview["cursor_blocked"] is True
    with pytest.raises(ClientError) as excinfo:
        client.request("prune", {"keep_last": 0})
    assert excinfo.value.code == "cursor_prune_conflict"
    assert client.status()["ledger"]["records"] == 3


def test_cursor_delivery_waits_for_the_prune_write_lock(server, client: QuayClient):
    event = client.emit(topic="t", producer="p", payload={})
    finished = False
    outcome: list[dict] = []

    def deliver() -> None:
        nonlocal finished
        outcome.append(
            server._op_cursor_deliver(  # noqa: SLF001 - deterministic concurrency seam
                {
                    "consumer": "codex",
                    "seq": event["seq"],
                    "idempotency_key": "ledger:1",
                },
                None,
            )[0]
        )
        finished = True

    server._write_lock.acquire()  # noqa: SLF001 - hold the same lock prune uses
    thread = threading.Thread(target=deliver)
    try:
        thread.start()
        thread.join(timeout=0.1)
        assert finished is False
    finally:
        server._write_lock.release()  # noqa: SLF001
    thread.join(timeout=2.0)

    assert finished is True
    assert outcome[0]["cursor"]["in_flight"]["seq"] == event["seq"]


from __future__ import annotations

import json
import sqlite3
import subprocess
import threading
from pathlib import Path

import pytest

from quay.cursors import CursorStore
from quay.distill import STATE_NAME, distill_workspace, event_uri
from quay.scaffold import scaffold_workspace
from quay.store import LedgerStore
from quay.workspace import WorkspaceError, paths_for


def _git(*args: str, cwd: Path) -> None:
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


def _repository(tmp_path: Path) -> tuple[Path, LedgerStore, CursorStore]:
    root = tmp_path / "repo"
    root.mkdir()
    _git("init", "-q", cwd=root)
    scaffold_workspace(root)
    paths = paths_for(root)
    store = LedgerStore(paths.database, synchronous="NORMAL")
    return root, store, CursorStore(paths.root)


def _handoff(store: LedgerStore, *, sender: str, recipient: str, summary: str) -> dict:
    return store.append(
        topic=f"swarm/{recipient}/handoff",
        producer=sender,
        kind="handoff",
        payload={"from": sender, "to": recipient, "summary": summary, "initiative": "quay"},
    )


def _ack(cursors: CursorStore, event: dict) -> None:
    recipient = event["payload"]["to"]
    key = f"delivery-{event['seq']}"
    cursors.begin_delivery(recipient, event["seq"], key)
    cursors.acknowledge(recipient, event["seq"], idempotency_key=key)


def test_distill_writes_deterministic_partition_and_bounded_index(tmp_path: Path):
    root, store, cursors = _repository(tmp_path)
    try:
        event = _handoff(store, sender="agy", recipient="codex", summary="Implement Phase 2")
        _ack(cursors, event)
        ledger_uuid = store.meta("ledger_uuid")

        first = distill_workspace(root)
        second = distill_workspace(root)
    finally:
        store.close()

    assert first["count"] == 1
    assert first["high_water_seq"] == event["seq"]
    assert second["count"] == 0
    assert second["high_water_seq"] == event["seq"]
    relative = first["created"][0]
    handoff = (root / "okf" / relative).read_text(encoding="utf-8")
    assert event_uri(ledger_uuid, event["seq"]) in handoff
    assert "Implement Phase 2" in handoff
    log = (root / "okf/log.md").read_text(encoding="utf-8")
    assert f"[#{event['seq']}]({relative})" in log
    state = json.loads((root / "okf" / STATE_NAME).read_text(encoding="utf-8"))
    assert state["ledger_uuid"] == ledger_uuid
    assert state["high_water_seq"] == event["seq"]
    assert cursors.get("quay.distill")["last_acked_seq"] == event["seq"]


def test_unacknowledged_handoff_is_an_ordered_barrier(tmp_path: Path):
    root, store, cursors = _repository(tmp_path)
    try:
        first = _handoff(store, sender="agy", recipient="codex", summary="first")
        second = _handoff(store, sender="codex", recipient="agy", summary="second")
        _ack(cursors, second)

        blocked = distill_workspace(root)
        assert blocked["count"] == 0
        assert blocked["blocked_seq"] == first["seq"]
        assert blocked["high_water_seq"] == 0

        _ack(cursors, first)
        completed = distill_workspace(root)
    finally:
        store.close()

    assert completed["count"] == 2
    assert completed["high_water_seq"] == second["seq"]


def test_stdout_preview_does_not_advance_state_or_write_handoffs(tmp_path: Path):
    root, store, cursors = _repository(tmp_path)
    try:
        event = _handoff(store, sender="agy", recipient="codex", summary="preview")
        _ack(cursors, event)
        result = distill_workspace(root, stdout=True)
    finally:
        store.close()

    assert result["count"] == 1
    assert "preview" in result["documents"][0]
    assert not (root / "okf" / STATE_NAME).exists()
    assert not (root / "okf/handoffs").exists()
    assert "quay.distill" not in cursors.snapshot()


def test_explicit_default_output_reuses_the_default_cursor(tmp_path: Path):
    root, store, cursors = _repository(tmp_path)
    try:
        event = _handoff(store, sender="agy", recipient="codex", summary="same root")
        _ack(cursors, event)
        result = distill_workspace(root, output_root=root / "okf")
    finally:
        store.close()

    assert result["count"] == 1
    snapshot = cursors.snapshot()
    assert snapshot["quay.distill"]["last_acked_seq"] == event["seq"]
    assert set(snapshot) == {"codex", "quay.distill"}


def test_concurrent_writers_are_serialized(tmp_path: Path):
    root, store, cursors = _repository(tmp_path)
    try:
        event = _handoff(store, sender="agy", recipient="codex", summary="once")
        _ack(cursors, event)
    finally:
        store.close()
    barrier = threading.Barrier(3)
    results: list[dict] = []
    errors: list[Exception] = []

    def run() -> None:
        barrier.wait()
        try:
            results.append(distill_workspace(root))
        except Exception as exc:  # pragma: no cover - asserted empty below
            errors.append(exc)

    threads = [threading.Thread(target=run) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=10)

    assert errors == []
    assert all(not thread.is_alive() for thread in threads)
    assert sorted(result["count"] for result in results) == [0, 1]
    assert cursors.get("quay.distill")["last_acked_seq"] == event["seq"]


def test_distill_refuses_nonidentical_existing_handoff(tmp_path: Path):
    root, store, cursors = _repository(tmp_path)
    try:
        event = _handoff(store, sender="agy", recipient="codex", summary="conflict")
        _ack(cursors, event)
        preview = distill_workspace(root, stdout=True)
        document = preview["documents"][0]
        year, month = event["ts"][:4], event["ts"][5:7]
        target_dir = root / "okf/handoffs" / year / month
        target_dir.mkdir(parents=True)
        target = target_dir / f"{event['seq']:020d}-{event['hash'][:12]}.md"
        target.write_text(document + "edited\n", encoding="utf-8")

        with pytest.raises(WorkspaceError) as excinfo:
            distill_workspace(root)
    finally:
        store.close()

    assert excinfo.value.code == "distill_conflict"
    assert not (root / "okf" / STATE_NAME).exists()


def test_distill_refuses_a_tampered_ledger(tmp_path: Path):
    root, store, cursors = _repository(tmp_path)
    event = _handoff(store, sender="agy", recipient="codex", summary="authentic")
    _ack(cursors, event)
    database = store.path
    store.close()
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "UPDATE events SET payload = ? WHERE seq = ?",
            ('{"from":"agy","summary":"tampered","to":"codex"}', event["seq"]),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(root)

    assert excinfo.value.code == "ledger_integrity_failed"
    assert not (root / "okf" / STATE_NAME).exists()


def test_distill_refuses_linked_worktree_writes_without_override(tmp_path: Path):
    root, store, _cursors = _repository(tmp_path)
    store.close()
    _git("config", "user.name", "Quay Test", cwd=root)
    _git("config", "user.email", "quay@example.invalid", cwd=root)
    _git("add", "AGENTS.md", "okf", cwd=root)
    _git("commit", "-qm", "initial scaffold", cwd=root)
    linked = tmp_path / "linked"
    _git("worktree", "add", "-q", "-b", "linked-test", str(linked), cwd=root)

    with pytest.raises(WorkspaceError) as excinfo:
        distill_workspace(linked)

    assert excinfo.value.code == "distill_linked_worktree"


def test_ledger_uuid_persists_across_reopen(tmp_path: Path):
    root, store, _cursors = _repository(tmp_path)
    path = paths_for(root).database
    first = store.meta("ledger_uuid")
    store.close()

    reopened = LedgerStore(path, create=False)
    try:
        assert reopened.meta("ledger_uuid") == first
        assert event_uri(first, 42).endswith("/events/42")
    finally:
        reopened.close()

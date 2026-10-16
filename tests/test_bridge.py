"""One-outstanding wake bridge and AgentBus shadow-mirror tests."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import threading
import time
from pathlib import Path

from quay.bridge import WakeBridge
from quay.cursors import CursorStore
from quay.workspace import paths_for


def _handoff(client, *, recipient: str = "factory", sender: str = "agy", summary: str = "go"):
    return client.emit(
        topic=f"swarm/{recipient}/handoff",
        producer=sender,
        kind="handoff",
        payload={"from": sender, "to": recipient, "summary": summary},
    )


def _ack(path: Path, seq: int, summary: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / f"{seq}.json").write_text(
        json.dumps({"seq": seq, "summary": summary}) + "\n",
        encoding="utf-8",
    )


def test_single_outstanding_delivery_and_ack_order(workspace: Path, server, client):
    first = _handoff(client, summary="first")
    second = _handoff(client, summary="second")

    with WakeBridge(workspace, runner="factory") as bridge:
        delivered = bridge.poll_once()
        assert delivered["action"] == "delivered"
        assert delivered["seq"] == first["seq"]
        assert [path.name for path in bridge.wake_dir.glob("*.json")] == [
            f"{first['seq']}.json"
        ]

        waiting = bridge.poll_once()
        assert waiting["action"] == "waiting"
        assert not bridge._wake_path(second["seq"]).exists()  # noqa: SLF001
        assert CursorStore(paths_for(workspace).root).get("factory")["last_acked_seq"] == 0

        _ack(bridge.ack_dir, first["seq"], "RUNNER_ACK: complete")
        acknowledged = bridge.poll_once()
        assert acknowledged["action"] == "acknowledged"
        assert CursorStore(paths_for(workspace).root).get("factory") == {
            "last_acked_seq": first["seq"],
            "in_flight": None,
        }

        following = bridge.poll_once()
        assert following["action"] == "delivered"
        assert following["seq"] == second["seq"]


def test_concurrent_bridge_instances_serialize_delivery(
    workspace: Path, server, client
):
    event = _handoff(client)
    first = WakeBridge(workspace, runner="factory")
    second = WakeBridge(workspace, runner="factory")
    entered = threading.Event()
    release = threading.Event()
    results: list[dict] = []
    original = first._write_wake  # noqa: SLF001 - deterministic lock seam

    def blocked_write(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=2.0)
        return original(*args, **kwargs)

    first._write_wake = blocked_write  # type: ignore[method-assign]  # noqa: SLF001
    one = threading.Thread(target=lambda: results.append(first.poll_once()))
    two = threading.Thread(target=lambda: results.append(second.poll_once()))
    try:
        one.start()
        assert entered.wait(timeout=2.0)
        two.start()
        time.sleep(0.05)
        assert two.is_alive()
        release.set()
        one.join(timeout=2.0)
        two.join(timeout=2.0)
    finally:
        release.set()
        first.close()
        second.close()

    assert not one.is_alive()
    assert not two.is_alive()
    assert sorted(result["action"] for result in results) == ["delivered", "waiting"]
    wakes = list((paths_for(workspace).root / "wake" / "factory").glob("*.json"))
    assert [path.name for path in wakes] == [f"{event['seq']}.json"]
    assert CursorStore(paths_for(workspace).root).get("factory")["in_flight"][
        "seq"
    ] == event["seq"]


def test_missing_wake_is_redelivered_with_same_idempotency_key(
    workspace: Path, server, client
):
    event = _handoff(client)
    with WakeBridge(workspace, runner="factory") as bridge:
        bridge.poll_once()
        original = json.loads(bridge._wake_path(event["seq"]).read_text())  # noqa: SLF001
        bridge._wake_path(event["seq"]).unlink()  # noqa: SLF001 - simulate runner crash

        result = bridge.poll_once()
        redelivered = json.loads(
            bridge._wake_path(event["seq"]).read_text()  # noqa: SLF001
        )

    assert result["action"] == "redelivered"
    assert redelivered["event_id"] == event["seq"]
    assert redelivered["attempt"] == 2
    assert redelivered["idempotency_key"] == original["idempotency_key"]


def test_crash_before_first_wake_restores_without_consuming_retry(
    workspace: Path, server, client
):
    event = _handoff(client)
    state_root = paths_for(workspace).root
    key = f"quay:{server.store.meta('ledger_uuid')}:{event['seq']}"
    CursorStore(state_root).begin_delivery("factory", event["seq"], key)

    with WakeBridge(workspace, runner="factory", max_retries=1) as bridge:
        result = bridge.poll_once()
        retry = bridge.retries.get(event["seq"])

    assert result["action"] == "restored"
    assert retry["attempts"] == 1
    assert retry["failures"] == 0
    assert CursorStore(state_root).get("factory")["in_flight"]["seq"] == event["seq"]


def test_crash_after_retry_init_restores_without_consuming_retry(
    workspace: Path, server, client
):
    event = _handoff(client)
    state_root = paths_for(workspace).root
    key = f"quay:{server.store.meta('ledger_uuid')}:{event['seq']}"
    CursorStore(state_root).begin_delivery("factory", event["seq"], key)

    with WakeBridge(workspace, runner="factory", max_retries=1) as bridge:
        bridge.retries.initialize(event["seq"])
        result = bridge.poll_once()
        retry = bridge.retries.get(event["seq"])

    assert result["action"] == "restored"
    assert retry["written"] is True
    assert retry["attempts"] == 1
    assert retry["failures"] == 0


def test_mirrored_companion_ack_advances_cursor(workspace: Path, server, client):
    event = _handoff(client)
    with WakeBridge(workspace, runner="factory") as bridge:
        bridge.poll_once()
        client.emit(
            topic="swarm/agy/handoff",
            producer="codex",
            kind="handoff",
            causation_id=event["seq"],
            root_causation_id=event["seq"],
            payload={
                "from": "codex",
                "to": "agy",
                "summary": "RUNNER_ACK: not factory's acknowledgement",
                "action": {"type": "runner_ack"},
            },
        )
        assert bridge.poll_once()["action"] == "waiting"
        assert CursorStore(paths_for(workspace).root).get("factory")[
            "last_acked_seq"
        ] == 0
        assert not bridge._ack_path(event["seq"]).exists()  # noqa: SLF001

        companion = client.emit(
            topic="swarm/agy/handoff",
            producer="factory",
            kind="handoff",
            causation_id=event["seq"],
            root_causation_id=event["seq"],
            payload={
                "from": "factory",
                "to": "agy",
                "summary": "RUNNER_ACK: completed shadow turn",
                "action": {"type": "runner_ack"},
            },
        )
        result = bridge.poll_once()

        persisted = json.loads(bridge._ack_path(event["seq"]).read_text())  # noqa: SLF001
        assert persisted["event"]["seq"] == companion["seq"]

    assert result["action"] == "acknowledged"
    assert CursorStore(paths_for(workspace).root).get("factory")["last_acked_seq"] == event["seq"]


def test_retry_exhaustion_dead_letters_and_unblocks_cursor(
    workspace: Path, server, client
):
    event = _handoff(client)
    with WakeBridge(workspace, runner="factory", max_retries=3) as bridge:
        bridge.poll_once()
        for failure in range(1, 4):
            _ack(bridge.ack_dir, event["seq"], "RUNNER_ERROR: crashed")
            result = bridge.poll_once()
            if failure < 3:
                assert result["action"] == "redelivered"
            else:
                assert result["action"] == "dead_lettered"

    cursor = CursorStore(paths_for(workspace).root).get("factory")
    assert cursor == {"last_acked_seq": event["seq"], "in_flight": None}
    dead_letters = client.query(topic="okf/dead-letter", order="asc")
    assert len(dead_letters) == 1
    assert dead_letters[0]["payload"]["reason"] == "RETRY_EXHAUSTED"
    errors = client.query(
        kind="handoff", correlation_id=f"bridge-factory-{event['seq']}", order="asc"
    )
    assert len(errors) == 1
    assert errors[0]["payload"]["summary"].startswith("RUNNER_ERROR")


def test_ledger_runner_errors_are_consumed_once_and_later_acks_are_visible(
    workspace: Path, server, client
):
    event = _handoff(client)
    with WakeBridge(workspace, runner="factory", max_retries=3) as bridge:
        bridge.poll_once()
        for failure in range(1, 4):
            companion = client.emit(
                topic="swarm/agy/handoff",
                producer="factory",
                kind="handoff",
                causation_id=event["seq"],
                root_causation_id=event["seq"],
                payload={
                    "from": "factory",
                    "to": "agy",
                    "summary": f"RUNNER_ERROR: crash {failure}",
                    "action": {"type": "runner_error"},
                },
            )
            result = bridge.poll_once()
            if failure < 3:
                assert result["action"] == "redelivered"
                assert bridge.poll_once()["action"] == "waiting"
                assert bridge.retries.get(event["seq"])["ledger_ack_seq"] == companion["seq"]
            else:
                assert result["action"] == "dead_lettered"

    dead_letters = client.query(topic="okf/dead-letter", order="asc")
    assert len(dead_letters) == 1
    assert dead_letters[0]["payload"]["source_seq"] == event["seq"]


def test_stale_ledger_error_does_not_override_later_attempt_ack(
    workspace: Path, server, client
):
    event = _handoff(client)
    with WakeBridge(workspace, runner="factory", max_retries=3) as bridge:
        bridge.poll_once()
        for label in ("first", "duplicate"):
            client.emit(
                topic="swarm/agy/handoff",
                producer="factory",
                kind="handoff",
                causation_id=event["seq"],
                root_causation_id=event["seq"],
                payload={
                    "from": "factory",
                    "to": "agy",
                    "summary": f"RUNNER_ERROR: {label}",
                    "action": {"type": "runner_error"},
                },
            )
        assert bridge.poll_once()["action"] == "redelivered"
        assert bridge.poll_once()["action"] == "waiting"
        success = client.emit(
            topic="swarm/agy/handoff",
            producer="factory",
            kind="handoff",
            causation_id=event["seq"],
            root_causation_id=event["seq"],
            payload={
                "from": "factory",
                "to": "agy",
                "summary": "RUNNER_ACK: second attempt complete",
                "action": {"type": "runner_ack", "attempt": 2},
            },
        )
        assert bridge.poll_once()["action"] == "acknowledged"
        assert success["seq"] > event["seq"]

    assert CursorStore(paths_for(workspace).root).get("factory")[
        "last_acked_seq"
    ] == event["seq"]


def _load_shadow_module(repo_root: Path):
    path = repo_root / "scripts" / "agentbus_to_quay_shadow.py"
    spec = importlib.util.spec_from_file_location("agentbus_to_quay_shadow", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_shadow_mirror_translates_causation_and_records_legacy_map(
    tmp_path: Path, workspace: Path, server, monkeypatch
):
    source_path = tmp_path / "events.db"
    source = sqlite3.connect(source_path)
    source.executescript(
        """
        CREATE TABLE events (
            event_id INTEGER PRIMARY KEY,
            topic TEXT NOT NULL,
            producer_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            payload TEXT NOT NULL,
            causation_id INTEGER,
            status TEXT NOT NULL
        );
        """
    )
    source.executemany(
        "INSERT INTO events VALUES(?, 'okf/handoff', ?, ?, ?, ?, 'PUBLISHED')",
        [
            (
                10,
                "agy",
                "2026-10-16T00:00:00Z",
                json.dumps({"from": "agy", "to": "factory", "summary": "build"}),
                None,
            ),
            (
                11,
                "factory",
                "2026-10-16T00:01:00Z",
                json.dumps(
                    {
                        "from": "factory",
                        "to": "agy",
                        "summary": "RUNNER_ACK: done",
                        "action": {"type": "runner_ack"},
                    }
                ),
                10,
            ),
        ],
    )
    source.commit()
    source.close()

    module = _load_shadow_module(Path(__file__).resolve().parents[1])
    monkeypatch.setattr(
        module,
        "ensure_daemon",
        lambda root, idle_seconds=0: (paths_for(root, create=False), {"ok": True}),
    )
    with module.ShadowMirror(
        agentbus_db=source_path, quay_workspace=workspace, since_id=0
    ) as mirror:
        results = mirror.poll_once()
        rows = mirror.mapping.execute(
            "SELECT agentbus_id, ledger_uuid, quay_seq FROM legacy_event_map "
            "ORDER BY agentbus_id"
        ).fetchall()
        mapping_path = Path(mirror.mapping.execute("PRAGMA database_list").fetchone()[2])

    assert [result["agentbus_id"] for result in results] == [10, 11]
    assert [row["agentbus_id"] for row in rows] == [10, 11]
    assert rows[0]["ledger_uuid"] == rows[1]["ledger_uuid"]
    assert mapping_path == paths_for(workspace).root / "legacy_event_map.db"
    with sqlite3.connect(paths_for(workspace).database) as ledger:
        ledger.row_factory = sqlite3.Row
        mirrored = ledger.execute(
            "SELECT seq, causation_id, root_causation_id, correlation_id "
            "FROM events ORDER BY seq"
        ).fetchall()
    assert mirrored[0]["correlation_id"] == "agentbus-10"
    assert mirrored[1]["causation_id"] == mirrored[0]["seq"]
    assert mirrored[1]["root_causation_id"] == mirrored[0]["seq"]


def test_shadow_mirror_records_malformed_row_and_continues(
    tmp_path: Path, workspace: Path, server, monkeypatch
):
    source_path = tmp_path / "events.db"
    source = sqlite3.connect(source_path)
    source.executescript(
        """
        CREATE TABLE events (
            event_id INTEGER PRIMARY KEY,
            topic TEXT NOT NULL,
            producer_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            payload TEXT NOT NULL,
            causation_id INTEGER,
            status TEXT NOT NULL
        );
        """
    )
    source.executemany(
        "INSERT INTO events VALUES(?, 'okf/handoff', ?, ?, ?, NULL, 'PUBLISHED')",
        [
            (
                20,
                "agy",
                "2026-10-16T00:00:00Z",
                json.dumps({"from": "agy", "to": "../bad", "summary": "bad"}),
            ),
            (
                21,
                "agy",
                "2026-10-16T00:01:00Z",
                json.dumps({"from": "agy", "to": "factory", "summary": "good"}),
            ),
        ],
    )
    source.commit()
    source.close()

    module = _load_shadow_module(Path(__file__).resolve().parents[1])
    monkeypatch.setattr(
        module,
        "ensure_daemon",
        lambda root, idle_seconds=0: (paths_for(root, create=False), {"ok": True}),
    )
    with module.ShadowMirror(
        agentbus_db=source_path, quay_workspace=workspace, since_id=0
    ) as mirror:
        results = mirror.poll_once()
        rejects = mirror.mapping.execute(
            "SELECT agentbus_id, reason FROM legacy_rejects"
        ).fetchall()
        cursor = mirror.cursor()

    assert results[0]["rejected"] is True
    assert results[1]["agentbus_id"] == 21
    assert [row["agentbus_id"] for row in rejects] == [20]
    assert cursor == 21


def test_ack_spool_pruning(workspace: Path, server, client):
    _handoff(client)
    with WakeBridge(workspace, runner="factory") as bridge:
        bridge.poll_once()
        # Seed 5 old ack files
        for i in range(1, 6):
            _ack(bridge.ack_dir, i, f"ack {i}")
        assert len(list(bridge.ack_dir.glob("*.json"))) == 5

        bridge._prune_acks(keep_last=2)  # noqa: SLF001
        remaining = sorted(int(p.stem) for p in bridge.ack_dir.glob("*.json"))
        assert remaining == [4, 5]


def test_run_once_preserves_delivered_at_when_wake_present(
    workspace: Path, server, client
):
    event = _handoff(client)
    with WakeBridge(workspace, runner="factory") as bridge:
        first = bridge.run(once=True)
        assert first["action"] == "delivered"
        initial_delivered_at = bridge.retries.get(event["seq"])["delivered_at"]

        # Calling run(once=True) again while wake exists must report waiting, not restored
        second = bridge.run(once=True)
        assert second["action"] == "waiting"
        second_delivered_at = bridge.retries.get(event["seq"])["delivered_at"]
        assert initial_delivered_at == second_delivered_at


def test_shadow_mirror_adapts_hermes_relay_and_wildcard_recipient(
    workspace: Path, server, tmp_path: Path, monkeypatch
):
    source_path = tmp_path / "events.db"
    source = sqlite3.connect(source_path)
    source.executescript(
        """
        CREATE TABLE events (
            event_id INTEGER PRIMARY KEY,
            topic TEXT NOT NULL,
            producer_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            payload TEXT NOT NULL,
            causation_id INTEGER,
            status TEXT NOT NULL
        );
        """
    )
    source.executemany(
        "INSERT INTO events VALUES(?, 'okf/handoff', ?, ?, ?, NULL, 'PUBLISHED')",
        [
            (
                30,
                "hermes",
                "2026-10-16T00:00:00Z",
                json.dumps({"from": "codex", "to": "hermes", "summary": "relayed codex handoff"}),
            ),
            (
                31,
                "hermes",
                "2026-10-16T00:01:00Z",
                json.dumps({"from": "hermes", "to": "*", "summary": "broadcast handoff"}),
            ),
        ],
    )
    source.commit()
    source.close()

    module = _load_shadow_module(Path(__file__).resolve().parents[1])
    monkeypatch.setattr(
        module,
        "ensure_daemon",
        lambda root, idle_seconds=0: (paths_for(root, create=False), {"ok": True}),
    )
    with module.ShadowMirror(
        agentbus_db=source_path, quay_workspace=workspace, since_id=0
    ) as mirror:
        results = mirror.poll_once()
        rejects = mirror.mapping.execute(
            "SELECT agentbus_id, reason FROM legacy_rejects"
        ).fetchall()
        cursor = mirror.cursor()

    assert len(results) == 2
    assert results[0]["agentbus_id"] == 30
    assert results[0]["recovered"] is False
    first = server.store.query(
        since_seq=results[0]["quay_seq"], until_seq=results[0]["quay_seq"]
    )[0]
    assert first["producer"] == "codex"
    assert first["topic"] == "swarm/hermes/handoff"

    assert results[1]["agentbus_id"] == 31
    assert results[1]["recovered"] is False
    second = server.store.query(
        since_seq=results[1]["quay_seq"], until_seq=results[1]["quay_seq"]
    )[0]
    assert second["producer"] == "hermes"
    assert second["topic"] == "swarm/swarm/handoff"
    assert rejects == []
    assert cursor == 31


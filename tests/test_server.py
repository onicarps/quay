"""Transport integration tests over a real Unix socket (requirements F9-F13).

Nothing here mocks the transport or the store: every test talks to a live
daemon thread through a real ``AF_UNIX`` socket and the real SQLite ledger.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from pathlib import Path

import pytest

from quay.client import ClientError, QuayClient, SubscriptionStream
from quay.protocol import HEADER, PROTOCOL_VERSION, make_request, read_frame, send_frame
from quay.server import OP_NAMES, QuayServer, event_matches


class TestHandshakeAndBasics:
    def test_health_reports_daemon_identity(self, client: QuayClient, server: QuayServer):
        health = client.health()
        assert health["pid"] == os.getpid()
        assert health["instance_id"] == server.instance_id
        assert health["protocol_version"] == PROTOCOL_VERSION
        assert health["workspace"] == str(server.paths.workspace)

    def test_ping(self, client: QuayClient):
        assert client.request("ping")["pong"] is True

    def test_status_includes_ledger_and_planes(self, client: QuayClient):
        status = client.status()
        assert status["ledger"]["records"] == 0
        assert status["planes"]["work"]["active"] == 0
        assert status["daemon"]["pid"] == os.getpid()

    def test_socket_permissions_are_owner_only(self, server: QuayServer):
        mode = server.socket_path.stat().st_mode & 0o777
        assert mode == 0o600, f"socket must be owner-only, got {oct(mode)}"

    def test_pid_file_is_owner_only_and_names_this_process(self, server: QuayServer):
        record_path = server.paths.pid
        assert record_path.stat().st_mode & 0o777 == 0o600
        record = json.loads(record_path.read_text())
        assert record["pid"] == os.getpid()
        assert record["instance_id"] == server.instance_id

    def test_concurrent_clients_are_served_independently(self, server: QuayServer):
        errors: list[Exception] = []

        def worker(index: int) -> None:
            try:
                with QuayClient(server.socket_path, timeout=10.0) as worker_client:
                    for step in range(10):
                        worker_client.emit(topic="concurrent", producer=f"w{index}", payload={"step": step})
            except Exception as exc:  # pragma: no cover - failure path
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        assert errors == []
        with QuayClient(server.socket_path) as reader:
            assert reader.status()["ledger"]["records"] == 60
            assert reader.request("verify")["ok"] is True


class TestErrorBoundaries:
    def test_unknown_operation_returns_structured_error_and_keeps_connection(
        self, client: QuayClient
    ):
        with pytest.raises(ClientError) as excinfo:
            client.request("not_a_real_op")
        assert excinfo.value.code == "unknown_operation"
        assert "known" in excinfo.value.detail
        # The connection must still work afterwards.
        assert client.request("ping")["pong"] is True

    def test_schema_rejection_is_structured(self, client: QuayClient):
        with pytest.raises(ClientError) as excinfo:
            client.emit(topic="t", producer="p", payload={"state": "nonsense"}, kind="signal")
        assert excinfo.value.code == "invalid_state"
        assert excinfo.value.detail["field"] == "state"

    def test_missing_causation_is_structured(self, client: QuayClient):
        with pytest.raises(ClientError) as excinfo:
            client.emit(topic="t", producer="p", payload={}, causation_id=4242)
        assert excinfo.value.code == "unknown_causation"

    def test_unknown_query_field_is_rejected(self, client: QuayClient):
        with pytest.raises(ClientError) as excinfo:
            client.request("query", {"nonsense": 1})
        assert excinfo.value.code == "unknown_query_field"

    def test_unknown_filter_is_rejected(self, client: QuayClient, stream):
        with pytest.raises(ClientError) as excinfo:
            stream({"nonsense": "x"})
        assert excinfo.value.code == "unknown_filter"

    def test_malformed_frame_yields_an_error_response_then_close(self, server: QuayServer):
        raw = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        raw.settimeout(5.0)
        raw.connect(str(server.socket_path))
        try:
            body = b"{ not json }"
            raw.sendall(HEADER.pack(len(body)) + body)
            frame = read_frame(raw)
            assert frame is not None
            assert frame["ok"] is False
            assert frame["error"]["code"] == "invalid_frame_json"
        finally:
            raw.close()

    def test_unsupported_protocol_version_is_refused(self, server: QuayServer):
        raw = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        raw.settimeout(5.0)
        raw.connect(str(server.socket_path))
        try:
            request = make_request("health")
            request["v"] = "99"
            send_frame(raw, request)
            frame = read_frame(raw)
            assert frame is not None and frame["ok"] is False
            assert frame["error"]["code"] == "unsupported_protocol"
        finally:
            raw.close()

    def test_get_missing_event_is_an_error(self, client: QuayClient):
        with pytest.raises(ClientError) as excinfo:
            client.request("get", {"seq": 999})
        assert excinfo.value.code == "not_found"

    def test_request_timeout_is_enforced(self, tmp_path: Path):
        """A daemon that accepts but never answers must not hang the client."""
        socket_path = tmp_path / "silent.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        listener.listen(1)
        held: list[socket.socket] = []

        def accept_and_stall() -> None:
            conn, _ = listener.accept()
            held.append(conn)  # keep it open, never reply

        thread = threading.Thread(target=accept_and_stall, daemon=True)
        thread.start()
        try:
            with QuayClient(socket_path, timeout=0.4) as silent:
                started = time.monotonic()
                with pytest.raises(ClientError) as excinfo:
                    silent.request("health")
                elapsed = time.monotonic() - started
                assert excinfo.value.code == "timeout"
                assert elapsed < 3.0
        finally:
            for conn in held:
                conn.close()
            listener.close()
            thread.join(timeout=2)

    def test_connection_refused_is_reported_clearly(self, tmp_path: Path):
        with pytest.raises(ClientError) as excinfo:
            QuayClient(tmp_path / "missing.sock").connect().request("health")
        assert excinfo.value.code == "connection_failed"


class TestStreaming:
    def test_maintenance_reaper_waits_for_the_subscription_write_lock(
        self, server: QuayServer, monkeypatch: pytest.MonkeyPatch
    ):
        called = threading.Event()

        def reap():
            called.set()
            return []

        server._write_lock.acquire()  # noqa: SLF001 - deterministic boundary seam
        monkeypatch.setattr(server.store, "reap_expired_leases", reap)
        thread = threading.Thread(target=server._reap_expired)  # noqa: SLF001
        try:
            thread.start()
            assert called.wait(0.1) is False
        finally:
            server._write_lock.release()  # noqa: SLF001
        thread.join(timeout=2.0)

        assert called.is_set()

    def test_subscriber_receives_pushed_events_without_polling(self, client: QuayClient, stream):
        live = stream()
        emitted = client.emit(topic="okf/handoff", producer="factory", payload={"summary": "hi"})
        received = live.wait_for(1, timeout=5.0)
        assert received[0]["seq"] == emitted["seq"]
        assert received[0]["topic"] == "okf/handoff"
        assert received[0]["hash"] == emitted["hash"]

    def test_events_arrive_in_order(self, client: QuayClient, stream):
        live = stream()
        for index in range(25):
            client.emit(topic="burst", producer="p", payload={"i": index})
        received = live.wait_for(25, timeout=10.0)
        assert [event["seq"] for event in received] == list(range(1, 26))
        assert [event["payload"]["i"] for event in received] == list(range(25))

    def test_kind_filter(self, client: QuayClient, stream):
        live = stream({"kind": "note"})
        client.emit(topic="t", producer="p", payload={"text": "ignored"}, kind="note")
        client.emit(topic="t", producer="p", payload={"i": 1}, kind="generic")
        client.emit(topic="t", producer="p", payload={"text": "kept"}, kind="note")
        received = live.wait_for(1, timeout=5.0)
        assert received[0]["payload"]["text"] == "ignored"
        assert live.wait_for(1, timeout=5.0)[0]["payload"]["text"] == "kept"

    def test_topic_glob_filter(self, client: QuayClient, stream):
        live = stream({"topic": "okf/*"})
        client.emit(topic="other/thing", producer="p", payload={})
        client.emit(topic="okf/handoff", producer="p", payload={"n": 1})
        received = live.wait_for(1, timeout=5.0)
        assert received[0]["topic"] == "okf/handoff"

    def test_state_and_producer_filters(self, client: QuayClient, stream):
        live = stream({"kind": "work", "state": "done", "producer": "codex"})
        first = client.emit(topic="w", producer="codex", payload={"key": "a", "executor": "codex", "state": "claimed"}, kind="work")
        token = first["payload"]["fencing_token"]
        second = client.emit(
            topic="w",
            producer="codex",
            payload={
                "key": "a", "executor": "codex", "state": "in_flight",
                "fencing_token": token,
            },
            kind="work",
            causation_id=first["seq"],
        )
        third = client.emit(
            topic="w",
            producer="codex",
            payload={
                "key": "a", "executor": "codex", "state": "done",
                "evidence": {"commit": "c"}, "fencing_token": token,
            },
            kind="work",
            causation_id=second["seq"],
        )
        client.emit(topic="w", producer="factory", payload={"key": "b", "executor": "factory", "state": "claimed"}, kind="work")
        received = live.wait_for(1, timeout=5.0)
        assert received[0]["seq"] == third["seq"]

    def test_replay_delivers_history_on_subscribe(self, client: QuayClient, stream):
        for index in range(5):
            client.emit(topic="hist", producer="p", payload={"i": index})
        live = stream({"topic": "hist"}, replay=3)
        assert live.replayed == 3
        received = live.wait_for(3, timeout=5.0)
        assert [event["seq"] for event in received] == [3, 4, 5]
        # Live delivery still works after the replay backlog.
        client.emit(topic="hist", producer="p", payload={"i": 99})
        assert live.wait_for(1, timeout=5.0)[0]["payload"]["i"] == 99

    def test_replay_zero_starts_recovery_at_the_subscription_boundary(
        self, server: QuayServer
    ):
        with QuayClient(server.socket_path) as writer:
            for index in range(3):
                writer.emit(topic="boundary", producer="p", payload={"i": index})
            live = SubscriptionStream(
                server.socket_path,
                filters={"topic": "boundary"},
                replay=0,
                timeout=5.0,
            ).open()
            try:
                assert live._last_received_seq == 3  # noqa: SLF001
                written = writer.emit(topic="boundary", producer="p", payload={"i": 3})
                assert live.wait_for(timeout=5.0) == [written]
            finally:
                live.close()

    def test_subscription_stream_backfills_a_reported_gap(self, server: QuayServer):
        with QuayClient(server.socket_path) as writer:
            for index in range(10):
                writer.emit(topic="recover", producer="p", payload={"i": index})
        recovering = SubscriptionStream(
            server.socket_path,
            filters={"topic": "recover"},
            queue_size=32,
            timeout=5.0,
        )
        recovering._last_received_seq = 2  # noqa: SLF001 - deterministic gap seam
        recovering._recover_gap(8)  # noqa: SLF001 - exercise the recovery loop directly
        events = recovering.wait_for(6, timeout=1.0)
        assert [event["seq"] for event in events] == [3, 4, 5, 6, 7, 8]
        assert recovering.recovered_events == 6

    def test_recovery_failure_survives_a_full_consumer_queue(self, tmp_path: Path):
        live = SubscriptionStream(tmp_path / "unused.sock", queue_size=1)
        reader, daemon = socket.socketpair()
        live._sock = reader  # noqa: SLF001 - deterministic reader-loop seam
        assert live._put_event_frame(  # noqa: SLF001
            {"type": "event", "event": {"seq": 1, "kind": "note", "payload": {}}}
        )

        def fail_recovery(until_seq: int) -> None:
            raise ClientError(
                "recovery_queue_full",
                "test recovery failure",
                {"until_seq": until_seq},
            )

        live._recover_gap = fail_recovery  # type: ignore[method-assign]  # noqa: SLF001
        thread = threading.Thread(target=live._read_loop)  # noqa: SLF001
        thread.start()
        try:
            send_frame(
                daemon,
                {"v": "1", "type": "lagged", "sub": 1, "dropped": 1, "last_seq": 2},
            )
            thread.join(timeout=5.0)
            assert not thread.is_alive()

            events = live.events(timeout=None)
            assert next(events)["seq"] == 1
            with pytest.raises(ClientError) as excinfo:
                next(events)
            assert excinfo.value.code == "recovery_queue_full"

            daemon.settimeout(1.0)
            assert daemon.recv(1) == b"", "terminal recovery must close the subscription socket"
        finally:
            live.close()
            daemon.close()

    def test_unsubscribe_stops_delivery(self, client: QuayClient, stream):
        live = stream()
        client.emit(topic="t", producer="p", payload={"n": 1})
        assert live.wait_for(1, timeout=5.0)
        live.unsubscribe()
        client.emit(topic="t", producer="p", payload={"n": 2})
        pending = [frame for frame in live.frames(timeout=0.4) if frame.get("type") == "event"]
        assert pending == []
        assert client.status()["ledger"]["records"] == 2

    def test_unsubscribing_an_unknown_id_is_an_error(self, server: QuayServer):
        raw = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        raw.settimeout(5.0)
        raw.connect(str(server.socket_path))
        try:
            send_frame(raw, make_request("unsubscribe", {"sub": 4242}))
            frame = read_frame(raw)
            assert frame is not None and frame["ok"] is False
            assert frame["error"]["code"] == "unknown_subscription"
        finally:
            raw.close()

    def test_subscription_dies_with_its_connection(self, client: QuayClient, stream, server: QuayServer):
        live = stream()
        assert server.stats()["subscriptions"] == 1
        live.close()
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and server.stats()["subscriptions"] != 0:
            time.sleep(0.05)
        assert server.stats()["subscriptions"] == 0

    def test_heartbeat_reaches_subscribers_only(self, client: QuayClient, stream, server: QuayServer):
        live = stream()
        frames = [frame for frame in live.frames(timeout=1.0)]
        assert any(frame.get("type") == "heartbeat" for frame in frames)
        assert not [frame for frame in client.notices if frame.get("type") == "heartbeat"]

    def test_multiple_subscribers_each_get_their_view(self, client: QuayClient, stream):
        notes = stream({"kind": "note"})
        everything = stream()
        client.emit(topic="t", producer="p", payload={"i": 1}, kind="generic")
        client.emit(topic="t", producer="p", payload={"text": "n"}, kind="note")
        assert [event["kind"] for event in everything.wait_for(2, timeout=5.0)] == ["generic", "note"]
        assert notes.wait_for(1, timeout=5.0)[0]["kind"] == "note"


class TestBackpressure:
    """A stalled subscriber must cost bounded memory and zero latency elsewhere."""

    FLOOD_COUNT = 512
    BLOB_SIZE = 4096

    @classmethod
    def flood(cls, writer: QuayClient, count: int | None = None) -> None:
        blob = "x" * cls.BLOB_SIZE
        for index in range(count or cls.FLOOD_COUNT):
            writer.emit(topic="flood", producer="p", payload={"i": index, "blob": blob})

    def test_slow_subscriber_never_blocks_the_daemon(self, server: QuayServer):
        stalled = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stalled.connect(str(server.socket_path))
        try:
            send_frame(stalled, make_request("subscribe", {"filters": {}}))
            # Read the subscribe response, then stop reading entirely.
            assert read_frame(stalled) is not None

            with QuayClient(server.socket_path, timeout=10.0) as writer:
                self.flood(writer)

            with QuayClient(server.socket_path, timeout=5.0) as probe:
                started = time.monotonic()
                assert probe.request("ping")["pong"] is True
                latency = time.monotonic() - started
                assert latency < 1.0, (
                    f"an unrelated client waited {latency:.3f}s behind a stalled subscriber"
                )
                stats = probe.stats()
                assert stats["pushes_dropped"] > 0, "drops must be counted, not hidden"
                assert probe.status()["ledger"]["records"] == self.FLOOD_COUNT
        finally:
            stalled.close()

    def test_lag_notice_is_delivered_when_the_subscriber_recovers(self, server: QuayServer):
        stalled = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stalled.settimeout(20.0)
        stalled.connect(str(server.socket_path))
        try:
            send_frame(stalled, make_request("subscribe", {"filters": {}}))
            assert read_frame(stalled) is not None

            with QuayClient(server.socket_path, timeout=10.0) as writer:
                self.flood(writer)

            # Recover: drain until the lag notice that accounts for the gap arrives.
            seen_types: list[str] = []
            dropped_total = 0
            deadline = time.monotonic() + 20.0
            while time.monotonic() < deadline:
                frame = read_frame(stalled)
                if frame is None:
                    break
                seen_types.append(str(frame.get("type")))
                if frame.get("type") == "lagged":
                    dropped_total += int(frame.get("dropped", 0))
                    break
            assert "lagged" in seen_types, (
                f"expected a lag notice, saw {len(seen_types)} frames: {seen_types[:10]}"
            )
            assert dropped_total > 0
        finally:
            stalled.close()

    def test_responses_are_never_dropped_for_a_busy_client(self, client: QuayClient):
        for index in range(200):
            assert client.request("ping")["pong"] is True


class TestClientAndServerLifecycle:
    def test_bus_defaults_use_large_outbox_and_normal_sync(self, workspace: Path):
        server = QuayServer(workspace, idle_seconds=0)
        try:
            assert server.subscriber_queue_size == 4096
            synchronous = server.store._conn.execute("PRAGMA synchronous").fetchone()[0]  # noqa: SLF001
            assert synchronous == 1  # SQLite NORMAL
        finally:
            server.close()

    def test_connection_cleanup_removes_subscriptions(self, server: QuayServer):
        for _ in range(3):
            with SubscriptionStream(server.socket_path, timeout=5.0) as live:
                assert live.sub_id is not None
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if server.stats()["subscriptions"] == 0 and server.stats()["connections"] == 0:
                break
            time.sleep(0.05)
        stats = server.stats()
        assert stats["subscriptions"] == 0
        assert stats["connections"] == 0

    def test_daemon_shuts_down_cleanly_on_stop(self, workspace: Path):
        server = QuayServer(workspace, idle_seconds=0, heartbeat_seconds=5.0, synchronous="NORMAL")
        server.start()
        socket_path = server.socket_path
        pid_path = server.paths.pid
        assert socket_path.exists() and pid_path.exists()

        with QuayClient(socket_path, timeout=5.0) as client:
            response = client.request("stop", {"reason": "test"})
            assert response["stopping"] is True

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and socket_path.exists():
            time.sleep(0.05)
        assert not socket_path.exists(), "the socket must be removed on shutdown"
        assert not pid_path.exists(), "the pid file must be removed on shutdown"

    def test_idle_daemon_shuts_itself_down(self, workspace: Path):
        server = QuayServer(
            workspace, idle_seconds=0.4, heartbeat_seconds=5.0, synchronous="NORMAL"
        )
        server.start()
        socket_path = server.socket_path
        with QuayClient(socket_path) as client:
            client.health()
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and socket_path.exists():
            time.sleep(0.05)
        assert not socket_path.exists(), "an idle daemon must exit on its own"
        assert server.stop_reason == "idle"
        server.close()

    def test_active_subscription_inhibits_idle_shutdown(self, workspace: Path):
        server = QuayServer(
            workspace, idle_seconds=0.25, heartbeat_seconds=5.0, synchronous="NORMAL"
        )
        server.start()
        live = SubscriptionStream(server.socket_path, timeout=5.0).open()
        try:
            time.sleep(0.6)
            assert server.socket_path.exists()
            assert server.stop_reason is None
            live.close()
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline and server.socket_path.exists():
                time.sleep(0.05)
            assert server.stop_reason == "idle"
        finally:
            live.close()
            server.close()

    def test_reap_thread_expires_leases_automatically(self, server: QuayServer, client: QuayClient):
        client.request(
            "work_claim",
            {"key": "expiring", "executor": "e", "lease_seconds": 1},
        )
        future = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(time.time() + 7200))
        result = client.request("work_reap", {"now": future})
        assert result["count"] == 1
        assert result["reaped"][0]["payload"]["state"] == "expired"
        assert client.status()["planes"]["work"]["expired"] == 1

    def test_second_start_on_the_same_socket_is_refused(self, workspace: Path):
        server = QuayServer(workspace, idle_seconds=0, synchronous="NORMAL")
        server.start()
        try:
            from quay.workspace import WorkspaceError

            with pytest.raises(WorkspaceError) as excinfo:
                QuayServer(workspace, idle_seconds=0, synchronous="NORMAL").start()
            assert excinfo.value.code == "socket_in_use"
        finally:
            server.request_stop("test")
            server.close()


class TestOperations:
    def test_emit_batch_is_atomic_over_the_wire(self, client: QuayClient):
        events = [
            {"topic": "batch", "producer": "p", "payload": {"i": 1}},
            {"topic": "batch", "producer": "p", "payload": {"i": 2}},
        ]
        written = client.emit_batch(events)
        assert [event["seq"] for event in written] == [1, 2]
        with pytest.raises(ClientError) as excinfo:
            client.emit_batch(
                [
                    {"topic": "batch", "producer": "p", "payload": {"i": 3}},
                    {"topic": "batch", "producer": "p", "payload": {"state": "bogus"}, "kind": "signal"},
                ]
            )
        assert excinfo.value.code == "invalid_state"
        assert client.status()["ledger"]["records"] == 2

    def test_query_log_and_since(self, client: QuayClient):
        for index in range(6):
            client.emit(topic="okf/handoff", producer="factory", payload={"i": index})
        assert [event["seq"] for event in client.query(topic="okf/*", limit=3)] == [1, 2, 3]
        assert [event["seq"] for event in client.log(limit=2)] == [6, 5]
        assert [event["seq"] for event in client.request("since", {"seq": 4})["events"]] == [5, 6]

    def test_work_operations_round_trip(self, client: QuayClient):
        claim = client.request("work_claim", {"key": "k", "executor": "factory", "lease_seconds": 60})["event"]
        assert claim["payload"]["state"] == "claimed"
        token = claim["payload"]["fencing_token"]
        heartbeat = client.request(
            "work_heartbeat",
            {
                "key": "k", "executor": "factory", "causation_id": claim["seq"],
                "lease_seconds": 120, "fencing_token": token,
            },
        )["event"]
        assert heartbeat["payload"]["state"] == "in_flight"
        done = client.request(
            "work_complete",
            {
                "key": "k", "executor": "factory", "causation_id": heartbeat["seq"],
                "evidence": {"commit": "abc"}, "fencing_token": token,
            },
        )["event"]
        assert done["payload"]["state"] == "done"
        listed = client.request("work_list")["work"]
        assert listed[0]["state"] == "done"

    def test_verify_and_prune_over_the_wire(self, client: QuayClient):
        for index in range(10):
            client.emit(topic="t", producer="p", payload={"i": index})
        assert client.request("verify")["ok"] is True
        preview = client.request("prune_preview", {"keep_last": 4})
        assert preview["removable"] == 6
        applied = client.request("prune", {"keep_last": 4})
        assert applied["applied"] is True
        assert client.status()["ledger"]["records"] == 4
        assert client.request("verify")["ok"] is True

    def test_kinds_describes_the_schema(self, client: QuayClient):
        kinds = client.request("kinds")["kinds"]
        assert {item["kind"] for item in kinds} == {
            "signal", "work", "handoff", "note", "metric", "generic"
        }

    def test_operations_list_is_stable(self):
        assert "emit" in OP_NAMES
        assert "subscribe" in OP_NAMES
        assert OP_NAMES == sorted(OP_NAMES)


class TestFilterMatching:
    def test_empty_filter_matches_everything(self):
        event = {"kind": "note", "topic": "a/b", "producer": "p", "payload": {}, "seq": 1}
        assert event_matches(event, {})

    def test_state_filter_reads_the_payload(self):
        event = {"kind": "work", "topic": "t", "producer": "p", "payload": {"state": "done"}, "seq": 1}
        assert event_matches(event, {"state": "done"})
        assert not event_matches(event, {"state": "claimed"})
        assert not event_matches({"kind": "note", "topic": "t", "producer": "p", "payload": {}, "seq": 1}, {"state": "done"})

    def test_since_seq_filter(self):
        event = {"kind": "note", "topic": "t", "producer": "p", "payload": {}, "seq": 5}
        assert event_matches(event, {"since_seq": 5})
        assert not event_matches(event, {"since_seq": 6})

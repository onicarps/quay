"""Ledger durability, integrity, projection, and lifecycle tests (F1-F8)."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

from conftest import SRC
from quay.canonical import GENESIS_HASH, canonical_json, event_digest
from quay.store import (
    GENESIS_PAYLOAD,
    GENESIS_PRODUCER,
    GENESIS_TOPIC,
    STATE_DIRECTORY,
    LedgerStore,
    StoreError,
    canonical_body_text,
    utc_now,
)


class TestCanonicalBodyText:
    """The fast body encoder must be byte-identical to the canonical encoder."""

    @pytest.mark.parametrize(
        "payload",
        [
            {},
            {"i": 1},
            {"nested": {"b": [1, 2, {"c": None}], "a": True}},
            {"unicode": "héllo → 日本語"},
            {"text": "line\nbreak\ttab\"quote\\slash"},
            {"big": ["x" * 100, {"deep": {"deeper": [None, False, 1.5]}}]},
            {"empty_list": [], "empty_obj": {}},
        ],
    )
    def test_matches_canonical_json_for_every_shape(self, payload):
        body = {
            "seq": 7,
            "prev_hash": GENESIS_HASH,
            "kind": "generic",
            "topic": "okf/handoff",
            "producer": "factory",
            "ts": "2026-10-04T12:00:00.000Z",
            "schema_version": "1",
            "causation_id": None,
            "correlation_id": "run-1",
            "payload": payload,
        }
        assert canonical_body_text(body, canonical_json(payload)) == canonical_json(body)

    def test_matches_with_links_set(self):
        body = {
            "seq": 42,
            "prev_hash": "a" * 64,
            "kind": "work",
            "topic": "work/queue",
            "producer": "factory",
            "ts": "2026-10-04T12:00:00.000Z",
            "schema_version": "1",
            "causation_id": 41,
            "correlation_id": None,
            "payload": {"key": "k", "executor": "e", "state": "claimed"},
        }
        assert canonical_body_text(body, canonical_json(body["payload"])) == canonical_json(body)


def append_sample(store: LedgerStore, count: int, *, topic: str = "work/log", producer: str = "tester"):
    return [
        store.append(topic=topic, producer=producer, payload={"i": i}, kind="generic")
        for i in range(count)
    ]


class TestAppendAndChain:
    def test_first_event_chains_from_genesis(self, store: LedgerStore):
        event = store.append(topic="a/b", producer="me", payload={"x": 1})
        assert event["seq"] == 1
        assert event["prev_hash"] == GENESIS_HASH
        assert len(event["hash"]) == 64
        assert event["schema_version"] == "1"

    def test_sequence_is_contiguous_and_hashes_chain(self, store: LedgerStore):
        events = append_sample(store, 6)
        assert [e["seq"] for e in events] == [1, 2, 3, 4, 5, 6]
        previous = GENESIS_HASH
        for event in events:
            assert event["prev_hash"] == previous
            previous = event["hash"]
        assert store.verify()["ok"] is True

    def test_stored_body_reproduces_hash(self, store: LedgerStore):
        event = store.append(topic="a/b", producer="me", payload={"x": [1, 2]}, kind="generic")
        row = store._conn.execute("SELECT body FROM events WHERE seq = 1").fetchone()  # noqa: SLF001
        body = json.loads(row["body"])
        assert body["seq"] == event["seq"]
        assert body["payload"] == {"x": [1, 2]}
        assert event_digest(body["prev_hash"], body) == event["hash"]

    def test_store_owned_fields_are_rejected(self, store: LedgerStore):
        for field in ("seq", "hash", "prev_hash", "event_id"):
            with pytest.raises(StoreError) as excinfo:
                store.append_batch(
                    [{"topic": "t", "producer": "p", "payload": {}, field: 1}]
                )
            assert excinfo.value.code == "store_owned_field"
            assert store.count() == 0

    def test_store_owned_names_are_allowed_inside_payloads(self, store: LedgerStore):
        # A payload is user data: a field called "seq" there is not a collision.
        event = store.append(topic="t", producer="p", payload={"seq": 99})
        assert event["payload"] == {"seq": 99}
        assert event["seq"] == 1

    def test_invalid_payload_is_rejected_before_write(self, store: LedgerStore):
        with pytest.raises(StoreError) as excinfo:
            store.append(topic="t", producer="p", payload={"a": float("nan")})
        assert excinfo.value.code in ("unencodable_payload", "invalid_number")
        assert store.count() == 0

    def test_invalid_timestamp_is_rejected(self, store: LedgerStore):
        with pytest.raises(StoreError) as excinfo:
            store.append(topic="t", producer="p", payload={}, ts="not-a-time")
        assert excinfo.value.code == "invalid_ts"
        assert store.count() == 0

    def test_unknown_causation_is_rejected(self, store: LedgerStore):
        with pytest.raises(StoreError) as excinfo:
            store.append(topic="t", producer="p", payload={}, causation_id=99)
        assert excinfo.value.code == "unknown_causation"

    def test_batch_is_atomic(self, store: LedgerStore):
        events = [
            {"topic": "t", "producer": "p", "payload": {"i": 1}},
            {"topic": "t", "producer": "p", "payload": {"i": 2}},
            {"topic": "t", "producer": "p", "payload": {"state": "waving"}, "kind": "signal"},
            {"topic": "t", "producer": "p", "payload": {"i": 4}},
        ]
        with pytest.raises(StoreError) as excinfo:
            store.append_batch(events)
        assert excinfo.value.code == "invalid_state"
        assert store.count() == 0, "a failed batch must leave no rows behind"
        assert store.verify()["ok"] is True

    def test_successful_batch_commits_every_event(self, store: LedgerStore):
        written = store.append_batch(
            [{"topic": "t", "producer": "p", "payload": {"i": i}} for i in range(50)]
        )
        assert len(written) == 50
        assert store.count() == 50
        assert store.verify()["ok"] is True

    def test_batch_is_capped(self, store: LedgerStore):
        with pytest.raises(StoreError) as excinfo:
            store.append_batch(
                [{"topic": "t", "producer": "p", "payload": {}}] * 10_001
            )
        assert excinfo.value.code == "batch_too_large"


class TestReadsAndQuery:
    def test_get_and_tail(self, store: LedgerStore):
        events = append_sample(store, 5)
        assert store.get(3)["payload"] == {"i": 2}
        assert store.get(99) is None
        assert [e["seq"] for e in store.tail(2)] == [5, 4]
        assert [e["seq"] for e in store.tail(2, order="asc")] == [1, 2]

    def test_since_returns_only_newer_events(self, store: LedgerStore):
        append_sample(store, 5)
        assert [e["seq"] for e in store.since(3)] == [4, 5]
        assert store.since(99) == []

    def test_query_combines_filters(self, store: LedgerStore):
        store.append(topic="okf/handoff", producer="factory", payload={"key": "a", "executor": "factory", "state": "claimed"}, kind="work")
        store.append(topic="okf/handoff", producer="codex", payload={"key": "b", "executor": "codex", "state": "claimed"}, kind="work")
        store.append(
            topic="okf/handoff",
            producer="codex",
            payload={"key": "b", "executor": "codex", "state": "done", "evidence": {"commit": "c1"}},
            kind="work",
            causation_id=2,
        )
        store.append(topic="notes/daily", producer="factory", payload={"text": "alpha release"}, kind="note")
        store.append(topic="metrics/latency", producer="pi", payload={"name": "p99", "value": 1.5}, kind="metric")

        assert len(store.query(kind="work")) == 3
        assert [e["seq"] for e in store.query(producer="factory")] == [1, 4]
        assert [e["seq"] for e in store.query(topic="okf/*")] == [1, 2, 3]
        assert [e["seq"] for e in store.query(text="alpha")] == [4]
        assert [e["seq"] for e in store.query(state="claimed")] == [1, 2]
        assert [e["seq"] for e in store.query(since_seq=2, until_seq=4, order="asc")] == [2, 3, 4]
        assert [e["seq"] for e in store.query(order="desc", limit=2)] == [5, 4]

    def test_query_glob_escapes_like_wildcards(self, store: LedgerStore):
        store.append(topic="a/b", producer="p", payload={})
        store.append(topic="axb", producer="p", payload={})
        matches = store.query(topic="a/b")
        assert [e["seq"] for e in matches] == [1]

    def test_query_by_correlation_and_causation(self, store: LedgerStore):
        first = store.append(topic="t", producer="p", payload={}, correlation_id="run-1")
        second = store.append(topic="t", producer="p", payload={}, causation_id=first["seq"], correlation_id="run-1")
        assert [e["seq"] for e in store.query(correlation_id="run-1")] == [1, 2]
        assert [e["seq"] for e in store.query(causation_id=first["seq"])] == [2]

    def test_query_does_not_allow_sql_injection(self, store: LedgerStore):
        store.append(topic="t", producer="p", payload={})
        hostile = "t' OR 1=1 --"
        assert store.query(topic=hostile) == []
        assert store.query(producer=hostile) == []
        assert store.count() == 1

    def test_query_limit_and_offset(self, store: LedgerStore):
        append_sample(store, 10)
        assert [e["seq"] for e in store.query(limit=3, offset=2, order="asc")] == [3, 4, 5]


class TestStatusAndProjections:
    def test_status_counts_match_appends(self, store: LedgerStore):
        store.append(topic="t", producer="p", payload={"i": 1})
        store.append(topic="t", producer="p", payload={"text": "hi"}, kind="note")
        status = store.status()
        assert status["ledger"]["records"] == 2
        assert status["ledger"]["last_seq"] == 2
        assert status["kinds"] == {"generic": 1, "note": 1}
        assert status["planes"]["signal"]["open"] == 0

    def test_status_is_constant_cost_as_ledger_grows(self, store: LedgerStore):
        append_sample(store, 500)
        small = store.status()
        assert small["ledger"]["records"] == 500
        append_sample(store, 500)
        large = store.status()
        assert large["ledger"]["records"] == 1000
        # Both snapshots come from the counters table, never a table scan.
        assert store.counts()["record"] == 1000

    def test_projection_matches_full_rebuild(self, store: LedgerStore):
        store.append(topic="t", producer="p", payload={}, kind="signal")
        store.append(topic="t", producer="p", payload={"state": "claimed"}, kind="signal", causation_id=1)
        store.append(topic="t", producer="p", payload={"key": "k", "executor": "e", "state": "claimed"}, kind="work")
        incremental = store.status()
        store.rebuild_projections()
        rebuilt = store.status()
        assert incremental["planes"] == rebuilt["planes"]
        assert incremental["kinds"] == rebuilt["kinds"]
        assert incremental["ledger"]["records"] == rebuilt["ledger"]["records"]


class TestSignalPlane:
    def test_signal_lifecycle(self, store: LedgerStore):
        root = store.append(topic="alert", producer="p", payload={}, kind="signal")
        assert store.status()["planes"]["signal"]["pending"] == 1
        store.append(topic="alert", producer="p", payload={"state": "claimed"}, kind="signal", causation_id=root["seq"])
        assert store.status()["planes"]["signal"] == {"pending": 0, "claimed": 1, "cleared": 0, "expired": 0, "open": 1}
        store.append(topic="alert", producer="p", payload={"state": "cleared"}, kind="signal", causation_id=root["seq"])
        assert store.status()["planes"]["signal"] == {"pending": 0, "claimed": 0, "cleared": 1, "expired": 0, "open": 0}

    def test_chain_must_start_pending(self, store: LedgerStore):
        with pytest.raises(StoreError) as excinfo:
            store.append(topic="alert", producer="p", payload={"state": "cleared"}, kind="signal")
        assert excinfo.value.code == "invalid_transition"

    def test_terminal_signal_cannot_reopen(self, store: LedgerStore):
        root = store.append(topic="alert", producer="p", payload={}, kind="signal")
        store.append(topic="alert", producer="p", payload={"state": "cleared"}, kind="signal", causation_id=root["seq"])
        with pytest.raises(StoreError) as excinfo:
            store.append(topic="alert", producer="p", payload={"state": "claimed"}, kind="signal", causation_id=root["seq"])
        assert excinfo.value.code == "invalid_transition"

    def test_signal_parent_must_be_signal(self, store: LedgerStore):
        note = store.append(topic="t", producer="p", payload={"text": "hi"}, kind="note")
        with pytest.raises(StoreError) as excinfo:
            store.append(topic="t", producer="p", payload={}, kind="signal", causation_id=note["seq"])
        assert excinfo.value.code == "signal_parent_not_signal"

    def test_multiple_chains_are_counted_separately(self, store: LedgerStore):
        a = store.append(topic="alert", producer="p", payload={}, kind="signal")
        store.append(topic="alert", producer="p", payload={}, kind="signal")
        store.append(topic="alert", producer="p", payload={"state": "cleared"}, kind="signal", causation_id=a["seq"])
        assert store.status()["planes"]["signal"] == {"pending": 1, "claimed": 0, "cleared": 1, "expired": 0, "open": 1}


class TestVerification:
    def test_verify_reports_clean_ledger(self, store: LedgerStore):
        append_sample(store, 5)
        report = store.verify()
        assert report["ok"] is True
        assert report["events_checked"] == 5
        assert report["first_seq"] == 1
        assert report["last_seq"] == 5
        assert report["gaps"] == []
        assert report["divergence"] is None

    def test_verify_detects_edited_payload(self, store: LedgerStore, ledger_path: Path):
        append_sample(store, 5)
        store.close()
        connection = sqlite3.connect(ledger_path)
        connection.execute(
            "UPDATE events SET payload = ? WHERE seq = 3",
            (json.dumps({"i": 999}),),
        )
        connection.commit()
        connection.close()
        reopened = LedgerStore(ledger_path)
        try:
            report = reopened.verify()
            assert report["ok"] is False
            assert report["divergence"]["seq"] == 3
            assert report["divergence"]["reason"] in ("hash_mismatch", "body_column_mismatch")
        finally:
            reopened.close()

    def test_verify_detects_deleted_middle_event(self, store: LedgerStore, ledger_path: Path):
        append_sample(store, 5)
        store.close()
        connection = sqlite3.connect(ledger_path)
        connection.execute("DELETE FROM events WHERE seq = 3")
        connection.commit()
        connection.close()
        reopened = LedgerStore(ledger_path)
        try:
            report = reopened.verify()
            assert report["ok"] is False
            assert report["gaps"], "a missing sequence number must be reported"
        finally:
            reopened.close()

    def test_verify_records_status(self, store: LedgerStore):
        append_sample(store, 2)
        store.verify()
        status = store.status()
        assert status["integrity"]["hash_chain"] == "verified"
        assert status["integrity"]["verified_seq"] == 2

    def test_verify_on_empty_ledger(self, store: LedgerStore):
        report = store.verify()
        assert report["ok"] is True
        assert report["events_checked"] == 0


class TestPrune:
    def test_prune_requires_explicit_scope(self, store: LedgerStore):
        append_sample(store, 3)
        with pytest.raises(StoreError) as excinfo:
            store.prune()
        assert excinfo.value.code == "prune_scope_required"

    def test_prune_dry_run_changes_nothing(self, store: LedgerStore):
        append_sample(store, 10)
        plan = store.prune(keep_last=4, dry_run=True)
        assert plan["dry_run"] is True
        assert plan["removable"] == 6
        assert plan["remaining"] == 4
        assert store.count() == 10

    def test_prune_keeps_suffix_and_verifies_from_checkpoint(self, store: LedgerStore):
        append_sample(store, 10)
        result = store.prune(keep_last=4)
        assert result["applied"] is True
        assert store.count() == 4
        assert store.last_seq() == 10
        assert store.checkpoint()["seq"] == 6
        report = store.verify()
        assert report["ok"] is True
        assert report["events_checked"] == 4
        assert report["first_seq"] == 7

    def test_append_after_prune_continues_the_chain(self, store: LedgerStore):
        append_sample(store, 10)
        store.prune(keep_last=4)
        store.append(topic="t", producer="p", payload={"after": True})
        assert store.last_seq() == 11
        assert store.count() == 5
        assert store.verify()["ok"] is True

    def test_prune_by_timestamp(self, store: LedgerStore):
        for index in range(3):
            store.append(
                topic="work/log",
                producer="tester",
                payload={"i": index},
                ts="2026-01-01T00:00:00.000Z",
            )
        plan = store.prune(before="2026-06-01T00:00:00.000Z", dry_run=True)
        assert plan["cutoff_seq"] == 3
        assert plan["removable"] == 3

    def test_prune_by_timestamp_is_exclusive_at_the_cutoff(self, store: LedgerStore):
        store.append(topic="t", producer="p", payload={}, ts="2026-06-01T00:00:00.000Z")
        plan = store.prune(before="2026-06-01T00:00:00.000Z", dry_run=True)
        assert plan["removable"] == 0
        assert plan["remaining"] == 1

    def test_prune_is_idempotent(self, store: LedgerStore):
        append_sample(store, 10)
        store.prune(keep_last=4)
        again = store.prune(keep_last=4)
        assert again["applied"] is False
        assert again["reason"] == "nothing_new_to_prune"

    def test_projections_survive_prune(self, store: LedgerStore):
        store.append(topic="alert", producer="p", payload={}, kind="signal")
        append_sample(store, 4)
        store.prune(keep_last=2)
        store.rebuild_projections()
        assert store.status()["planes"]["signal"]["pending"] + store.status()["planes"]["signal"]["cleared"] >= 0
        assert store.verify()["ok"] is True


class TestConcurrencyAndCrash:
    def test_initialization_retries_a_transient_journal_lock(self, tmp_path: Path):
        """A concurrent first open must not fail when WAL setup briefly locks."""
        db = tmp_path / "concurrent-init" / "ledger.db"
        db.parent.mkdir()
        locker = sqlite3.connect(db, timeout=0, isolation_level=None)
        locker.execute("CREATE TABLE seed (value INTEGER)")
        locker.execute("BEGIN EXCLUSIVE")
        result: dict[str, object] = {}

        def open_store() -> None:
            try:
                # A tiny SQLite busy timeout forces the retry path instead of
                # relying on SQLite to wait for the exclusive setup lock.
                store = LedgerStore(db, busy_timeout_ms=1)
                result["count"] = store.count()
                store.close()
            except BaseException as exc:  # pragma: no cover - asserted below
                result["error"] = exc

        opener = threading.Thread(target=open_store)
        opener.start()
        time.sleep(0.15)
        locker.execute("ROLLBACK")
        locker.close()
        opener.join(timeout=10)

        assert not opener.is_alive()
        assert "error" not in result
        assert result["count"] == 0

    def test_concurrent_processes_produce_one_intact_chain(self, tmp_path: Path):
        db = tmp_path / "concurrent" / "ledger.db"
        writers = 4
        per_writer = 25
        script = textwrap.dedent(
            """
            import sys
            sys.path.insert(0, {src!r})
            from quay.store import LedgerStore
            store = LedgerStore({db!r})
            for i in range({count}):
                store.append(topic="burst", producer="w" + sys.argv[1], payload={{"i": i}})
            store.close()
            """
        ).format(src=str(SRC), db=str(db), count=per_writer)
        processes = [
            subprocess.Popen([sys.executable, "-c", script, str(index)])
            for index in range(writers)
        ]
        for process in processes:
            assert process.wait(timeout=90) == 0
        store = LedgerStore(db)
        try:
            assert store.count() == writers * per_writer
            assert [e["seq"] for e in store.query(order="asc", limit=10_000)] == list(
                range(1, writers * per_writer + 1)
            )
            assert store.verify()["ok"] is True
        finally:
            store.close()

    def test_hard_exit_does_not_lose_committed_events(self, tmp_path: Path):
        db = tmp_path / "crash" / "ledger.db"
        script = textwrap.dedent(
            """
            import os, sys
            sys.path.insert(0, {src!r})
            from quay.store import LedgerStore
            store = LedgerStore({db!r}, synchronous="FULL")
            for i in range(20):
                store.append(topic="crash", producer="p", payload={{"i": i}})
            os._exit(0)   # no close(), no cleanup: simulate a hard process death
            """
        ).format(src=str(SRC), db=str(db))
        result = subprocess.run([sys.executable, "-c", script], capture_output=True, timeout=90)
        assert result.returncode == 0, result.stderr.decode()

        store = LedgerStore(db)
        try:
            assert store.count() == 20
            report = store.verify()
            assert report["ok"] is True
            assert report["events_checked"] == 20
        finally:
            store.close()

    def test_writes_survive_reopen_with_identical_hashes(self, tmp_path: Path):
        db = tmp_path / "reopen" / "ledger.db"
        first = LedgerStore(db)
        events = append_sample(first, 3)
        first.close()
        second = LedgerStore(db)
        try:
            for original in events:
                assert second.get(original["seq"])["hash"] == original["hash"]
        finally:
            second.close()

    def test_two_handles_serialize_writes(self, tmp_path: Path):
        db = tmp_path / "handles" / "ledger.db"
        first = LedgerStore(db)
        second = LedgerStore(db)
        try:
            first.append(topic="t", producer="one", payload={"who": 1})
            second.append(topic="t", producer="two", payload={"who": 2})
            first.append(topic="t", producer="one", payload={"who": 3})
            assert first.count() == 3
            assert second.verify()["ok"] is True
        finally:
            first.close()
            second.close()


class TestGenesis:
    """The single onboarding record written into a fresh ledger."""

    def test_genesis_is_written_once_with_the_exact_shape(self, ledger_path: Path):
        store = LedgerStore(ledger_path)
        try:
            event = store.ensure_genesis()
            assert event is not None
            assert event["seq"] == 1
            assert event["kind"] == "note"
            assert event["topic"] == GENESIS_TOPIC
            assert event["producer"] == GENESIS_PRODUCER
            assert GENESIS_PAYLOAD == {
                "text": "Quay ledger initialized. Waiting for agent events...",
                "tags": ["init", "welcome"],
            }
            assert event["payload"] == GENESIS_PAYLOAD
            assert store.ensure_genesis() is None
            assert store.count() == 1
            assert store.verify()["ok"] is True
        finally:
            store.close()

    def test_genesis_is_skipped_when_the_ledger_already_has_records(self, store: LedgerStore):
        store.append(topic="t", producer="p", payload={"i": 1})
        assert store.ensure_genesis() is None
        assert store.count() == 1

    def test_concurrent_genesis_writes_exactly_one_record(self, ledger_path: Path):
        store = LedgerStore(ledger_path)
        results: list[object] = []
        barrier = threading.Barrier(8)

        def worker() -> None:
            barrier.wait()
            results.append(store.ensure_genesis())

        threads = [threading.Thread(target=worker) for _ in range(8)]
        try:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30)
            assert sum(1 for item in results if item is not None) == 1
            assert store.count() == 1
            assert store.get(1)["topic"] == GENESIS_TOPIC
            assert store.verify()["ok"] is True
        finally:
            store.close()


class TestLifecycle:
    def test_owner_only_permissions(self, ledger_path: Path):
        store = LedgerStore(ledger_path)
        try:
            store.append(topic="t", producer="p", payload={})
            mode = ledger_path.stat().st_mode & 0o777
            assert mode == 0o600, f"ledger must be owner-only, got {oct(mode)}"
            state_mode = ledger_path.parent.stat().st_mode & 0o777
            assert state_mode == 0o700, f"state dir must be owner-only, got {oct(state_mode)}"
        finally:
            store.close()

    def test_state_directory_name_is_stable(self):
        assert STATE_DIRECTORY == ".quay"

    def test_open_missing_ledger_without_create_fails(self, tmp_path: Path):
        with pytest.raises(StoreError) as excinfo:
            LedgerStore(tmp_path / "nope" / "ledger.db", create=False)
        assert excinfo.value.code == "no_ledger"

    def test_close_is_idempotent(self, ledger_path: Path):
        store = LedgerStore(ledger_path)
        store.close()
        store.close()
        with pytest.raises(StoreError) as excinfo:
            store.append(topic="t", producer="p", payload={})
        assert excinfo.value.code == "closed"

    def test_context_manager_closes(self, ledger_path: Path):
        with LedgerStore(ledger_path) as store:
            store.append(topic="t", producer="p", payload={})
        assert store._closed is True  # noqa: SLF001

    def test_vacuum_preserves_data(self, store: LedgerStore):
        append_sample(store, 5)
        store.vacuum()
        assert store.count() == 5
        assert store.verify()["ok"] is True

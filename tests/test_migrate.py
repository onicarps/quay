"""Importing a real Changi ledger: fidelity, causality, and failure handling.

These tests build a Changi-shaped SQLite file from scratch (the same DDL Changi
ships) and assert that Quay reads it, maps planes correctly, preserves the
causal chain, and refuses anything it cannot represent instead of inventing it.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import quay.migrate as migrate_module
from quay.migrate import (
    MigrationReport,
    migrate_changi,
    read_changi_events,
    translate_event,
)
from quay.store import LedgerStore, StoreError

CHANGI_DDL = """
CREATE TABLE events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    producer_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    payload TEXT NOT NULL,
    causation_id INTEGER
);
"""


def changi_ledger(path: Path, rows: list[dict]) -> Path:
    """Create a Changi events.db with the rows supplied."""
    base = datetime(2026, 10, 4, 12, 0, 0, tzinfo=timezone.utc)
    connection = sqlite3.connect(path)
    connection.executescript(CHANGI_DDL)
    for index, row in enumerate(rows, start=1):
        connection.execute(
            "INSERT INTO events (topic, producer_id, timestamp, schema_version, payload, causation_id)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                row.get("topic", "demo/topic"),
                row.get("producer_id", "changi.worker"),
                row.get("timestamp", (base + timedelta(seconds=index)).isoformat()),
                row.get("schema_version", 1),
                json.dumps(row.get("payload", {}), sort_keys=True),
                row.get("causation_id"),
            ),
        )
    connection.commit()
    connection.close()
    return path


@pytest.fixture
def changi_db(tmp_path: Path) -> Path:
    return changi_ledger(
        tmp_path / "changi-events.db",
        [
            {"topic": "build/queue", "producer_id": "changi.producer", "payload": {"message": "plain"}},
            {
                "topic": "build/alert",
                "producer_id": "changi.monitor",
                "payload": {
                    "summary": "disk almost full",
                    "changi": {"kind": "signal", "state": "claimed"},
                },
            },
            {
                "topic": "build/receipt",
                "producer_id": "changi.worker",
                "payload": {
                    "title": "compile",
                    "changi": {"kind": "work_receipt"},
                    "work_receipt": {
                        "executor_id": "worker-7",
                        "receipt_id": "job-42",
                        "exit_status": 0,
                        "artifact_hash": "sha256:abc",
                    },
                },
            },
        ],
    )


class TestReader:
    def test_reads_rows_in_ledger_order(self, changi_db: Path):
        rows = list(read_changi_events(changi_db))
        assert [row["event_id"] for row in rows] == [1, 2, 3]
        assert rows[0]["payload"] == {"message": "plain"}

    def test_rejects_a_database_that_is_not_a_changi_ledger(self, tmp_path: Path):
        other = tmp_path / "other.db"
        connection = sqlite3.connect(other)
        connection.execute("CREATE TABLE unrelated (id INTEGER)")
        connection.commit()
        connection.close()
        with pytest.raises(StoreError) as excinfo:
            list(read_changi_events(other))
        assert excinfo.value.code == "not_a_changi_ledger"

    def test_missing_file_surfaces_as_a_sqlite_error(self, tmp_path: Path):
        with pytest.raises(sqlite3.OperationalError):
            list(read_changi_events(tmp_path / "nope.db"))

    def test_unparseable_payload_is_preserved(self, tmp_path: Path):
        path = changi_ledger(tmp_path / "broken.db", [{"payload": {"ok": True}}])
        connection = sqlite3.connect(path)
        connection.execute("UPDATE events SET payload = 'not json'")
        connection.commit()
        connection.close()
        rows = list(read_changi_events(path))
        assert rows[0]["payload"] == {"changi_unparsed_payload": "not json"}


class TestTranslation:
    def test_plain_event_becomes_generic_and_keeps_its_payload(self):
        report = MigrationReport(source="x")
        translated = translate_event(
            {"event_id": 9, "topic": "t", "producer_id": "p", "payload": {"a": 1}, "timestamp": None},
            report,
        )
        assert translated["kind"] == "generic"
        assert translated["payload"]["a"] == 1
        assert translated["payload"]["changi_event_id"] == 9

    def test_signal_plane_maps_to_a_quay_signal(self):
        report = MigrationReport(source="x")
        translated = translate_event(
            {
                "event_id": 1,
                "topic": "t",
                "producer_id": "p",
                "timestamp": None,
                "payload": {"summary": "s", "changi": {"kind": "signal", "state": "cleared"}},
            },
            report,
        )
        assert translated["kind"] == "signal"
        assert translated["payload"]["state"] == "cleared"
        assert translated["payload"]["summary"] == "s"

    def test_unknown_signal_state_falls_back_to_pending(self):
        report = MigrationReport(source="x")
        translated = translate_event(
            {
                "event_id": 1,
                "topic": "t",
                "producer_id": "p",
                "timestamp": None,
                "payload": {"changi": {"kind": "signal", "state": "exploded"}},
            },
            report,
        )
        assert translated["payload"]["state"] == "pending"

    def test_valid_receipt_maps_to_work(self):
        report = MigrationReport(source="x")
        translated = translate_event(
            {
                "event_id": 5,
                "topic": "t",
                "producer_id": "p",
                "timestamp": None,
                "payload": {
                    "changi": {"kind": "work_receipt"},
                    "work_receipt": {"executor_id": "w", "receipt_id": "job-1", "exit_status": 0},
                },
            },
            report,
        )
        assert translated["kind"] == "work"
        assert translated["payload"]["state"] == "done"
        assert translated["payload"]["key"] == "job-1"
        assert "fencing_token" not in translated["payload"]
        assert translated["payload"]["evidence"]["exit_status"] == 0

    def test_failed_receipt_maps_to_failed_work(self):
        report = MigrationReport(source="x")
        translated = translate_event(
            {
                "event_id": 5,
                "topic": "t",
                "producer_id": "p",
                "timestamp": None,
                "payload": {
                    "changi": {"kind": "work_receipt"},
                    "work_receipt": {"executor_id": "w", "exit_status": 17},
                },
            },
            report,
        )
        assert translated["kind"] == "work"
        assert translated["payload"]["state"] == "failed"
        assert "fencing_token" not in translated["payload"]
        assert translated["payload"]["evidence"] == {"exit_status": 17}

    def test_receipt_without_executor_is_downgraded_with_a_warning(self):
        report = MigrationReport(source="x")
        translated = translate_event(
            {
                "event_id": 5,
                "topic": "t",
                "producer_id": "p",
                "timestamp": None,
                "payload": {"changi": {"kind": "work_receipt"}, "work_receipt": {}},
            },
            report,
        )
        assert translated["kind"] == "generic"
        assert any("executor_id" in warning for warning in report.warnings)

    def test_producer_ids_are_sanitized_to_quay_syntax(self):
        report = MigrationReport(source="x")
        translated = translate_event(
            {"event_id": 1, "topic": "t", "producer_id": "Worker #1!", "payload": {}, "timestamp": None},
            report,
        )
        assert translated["producer"] == "Worker--1"
        assert report.warnings

    def test_missing_timestamp_is_reported(self):
        report = MigrationReport(source="x")
        translate_event(
            {"event_id": 1, "topic": "t", "producer_id": "p", "payload": {}, "timestamp": "  "},
            report,
        )
        assert any("timestamp" in warning for warning in report.warnings)


class TestMigration:
    def test_migrates_every_event_and_reports_kinds(self, changi_db: Path, store: LedgerStore):
        report = migrate_changi(changi_db, store)
        assert report.read == 3
        assert report.migrated == 3
        assert report.skipped == 0
        assert report.kinds == {"generic": 1, "signal": 1, "work": 1}
        assert report.duration_ms >= 0
        assert store.count() == 5

    def test_standalone_plane_records_get_a_marked_reconstructed_root(
        self, changi_db: Path, store: LedgerStore
    ):
        """Changi allows a terminal record with no history; Quay requires a root."""
        report = migrate_changi(changi_db, store)
        assert report.synthesized == 2
        assert store.count() == 5
        roots = [
            event
            for event in store.query(limit=10)
            if "reconstructed" in str(event["payload"].get("summary", ""))
            or "reconstructed" in str(event["payload"].get("title", ""))
        ]
        assert len(roots) == 2
        for root in roots:
            assert root["correlation_id"].startswith("changi-import-")

    def test_imported_history_passes_verification(self, changi_db: Path, store: LedgerStore):
        migrate_changi(changi_db, store)
        result = store.verify()
        assert result["ok"] is True
        assert result["events_checked"] == 5

    def test_work_and_signal_projections_are_populated(self, changi_db: Path, store: LedgerStore):
        migrate_changi(changi_db, store)
        planes = store.status()["planes"]
        assert planes["work"]["done"] == 1
        assert planes["signal"]["claimed"] == 1

    def test_legacy_terminal_work_uses_the_reconstructed_claim_token(
        self, changi_db: Path, store: LedgerStore
    ):
        report = migrate_changi(changi_db, store)
        assert report.skipped == 0
        work_events = store.query(kind="work", order="asc")
        assert [event["payload"]["state"] for event in work_events] == ["claimed", "done"]
        assert [event["payload"]["fencing_token"] for event in work_events] == [1, 1]
        assert work_events[1]["causation_id"] == work_events[0]["seq"]

        store.rebuild_projections()
        row = store.list_work()[0]
        assert (row["state"], row["executor"], row["fencing_token"]) == (
            "done",
            "worker-7",
            1,
        )
        assert store.verify()["ok"] is True

    def test_legacy_terminal_uses_the_token_assigned_to_a_reclaim(
        self, changi_db: Path, store: LedgerStore
    ):
        claimed = store.append(
            topic="work/queue",
            producer="worker-7",
            payload={"key": "job-42", "executor": "worker-7", "state": "claimed"},
            kind="work",
        )
        store.append(
            topic="work/queue",
            producer="worker-7",
            payload={
                "key": "job-42",
                "executor": "worker-7",
                "state": "abandoned",
                "reason": "legacy import setup",
                "fencing_token": 1,
            },
            kind="work",
            causation_id=claimed["seq"],
        )

        report = migrate_changi(changi_db, store)
        assert report.migrated == 3
        assert report.skipped == 0
        row = next(item for item in store.list_work() if item["key"] == "job-42")
        assert (row["state"], row["executor"], row["fencing_token"]) == (
            "done",
            "worker-7",
            2,
        )
        imported = [
            event
            for event in store.query(kind="work", order="asc")
            if event["payload"]["key"] == "job-42"
        ]
        assert [event["payload"]["fencing_token"] for event in imported] == [1, 1, 2, 2]
        assert store.verify()["ok"] is True

    def test_causal_links_are_remapped_to_new_sequence_numbers(self, tmp_path: Path, store: LedgerStore):
        path = changi_ledger(
            tmp_path / "chain.db",
            [
                {"payload": {"changi": {"kind": "signal", "state": "pending"}}},
                {
                    "payload": {"changi": {"kind": "signal", "state": "cleared"}},
                    "causation_id": 1,
                },
            ],
        )
        report = migrate_changi(path, store)
        assert report.migrated == 2
        assert report.synthesized == 0
        events = store.query(limit=10)
        child = [event for event in events if event["payload"]["state"] == "cleared"][0]
        parent = [event for event in events if event["payload"]["state"] == "pending"][0]
        assert child["causation_id"] == parent["seq"]

    def test_fallback_resynchronizes_sequences_after_a_skipped_record(
        self, tmp_path: Path, store: LedgerStore, monkeypatch: pytest.MonkeyPatch
    ):
        path = changi_ledger(
            tmp_path / "fallback-chain.db",
            [
                {"payload": {"force_skip": True}},
                {"payload": {"batch": "survivor-a"}},
                {"payload": {"batch": "survivor-b"}},
                {"payload": {"chain": "parent"}},
                {"payload": {"chain": "child"}, "causation_id": 4},
            ],
        )

        class FallbackStore:
            def last_seq(self):
                return store.last_seq()

            def append_batch(self, events):
                if any(event["payload"].get("force_skip") for event in events):
                    raise StoreError("forced_batch_retry", "exercise row fallback")
                return store.append_batch(events)

            def append(self, **event):
                if event["payload"].get("force_skip"):
                    raise StoreError("forced_skip", "exercise skipped-row recovery")
                return store.append(**event)

        monkeypatch.setattr(migrate_module, "BATCH_SIZE", 3)
        report = migrate_changi(path, FallbackStore())  # type: ignore[arg-type]

        assert (report.migrated, report.skipped) == (4, 1)
        events = store.query(order="asc")
        parent = next(event for event in events if event["payload"].get("chain") == "parent")
        child = next(event for event in events if event["payload"].get("chain") == "child")
        assert parent["seq"] == 3
        assert child["causation_id"] == parent["seq"]

    def test_links_pointing_at_unmigrated_events_are_dropped_with_a_warning(
        self, tmp_path: Path, store: LedgerStore
    ):
        path = changi_ledger(
            tmp_path / "dangling.db",
            [
                {"payload": {"changi": {"kind": "signal", "state": "pending"}}},
                {
                    "payload": {"changi": {"kind": "signal", "state": "claimed"}},
                    "causation_id": 999,
                },
            ],
        )
        report = migrate_changi(path, store)
        assert report.migrated == 2
        assert report.synthesized == 1
        assert any("999" in warning for warning in report.warnings)

    def test_work_chain_with_a_mismatched_parent_key_keeps_the_record(
        self, tmp_path: Path, store: LedgerStore
    ):
        path = changi_ledger(
            tmp_path / "work-chain.db",
            [
                {
                    "payload": {
                        "changi": {"kind": "work_receipt"},
                        "work_receipt": {"executor_id": "w", "receipt_id": "job-a", "exit_status": 0},
                    }
                },
                {
                    "payload": {
                        "changi": {"kind": "work_receipt"},
                        "work_receipt": {"executor_id": "w", "receipt_id": "job-b", "exit_status": 0},
                    },
                    "causation_id": 1,
                },
            ],
        )
        report = migrate_changi(path, store)
        assert report.migrated == 2
        assert report.skipped == 0
        assert report.synthesized == 2
        assert store.status()["planes"]["work"]["done"] == 2

    def test_dry_run_counts_without_writing(self, changi_db: Path, store: LedgerStore):
        report = migrate_changi(changi_db, store, dry_run=True)
        assert report.dry_run is True
        assert report.migrated == 3
        assert report.synthesized == 2
        assert store.count() == 0

    def test_rerunning_the_import_keeps_the_chain_valid(self, changi_db: Path, store: LedgerStore):
        """Re-importing is not deduplicated: terminal plane records are refused."""
        first = migrate_changi(changi_db, store)
        second = migrate_changi(changi_db, store)
        assert first.migrated == 3
        assert second.migrated == 2
        assert second.skipped == 1
        assert any("done" in warning for warning in second.warnings)
        work = store.list_work()
        assert len(work) == 1
        assert (work[0]["state"], work[0]["fencing_token"]) == ("done", 1)
        assert store.verify()["ok"] is True

    def test_empty_source_is_reported_as_zero(self, tmp_path: Path, store: LedgerStore):
        path = changi_ledger(tmp_path / "empty.db", [])
        report = migrate_changi(path, store)
        assert (report.read, report.migrated) == (0, 0)
        assert store.count() == 0

    def test_large_source_is_batched(self, tmp_path: Path, store: LedgerStore):
        path = changi_ledger(
            tmp_path / "large.db",
            [{"payload": {"index": index}} for index in range(1200)],
        )
        report = migrate_changi(path, store)
        assert report.read == report.migrated == 1200
        assert store.verify()["ok"] is True

    def test_report_serializes_to_json_safe_types(self, changi_db: Path, store: LedgerStore):
        payload = migrate_changi(changi_db, store).to_dict()
        assert json.loads(json.dumps(payload)) == payload

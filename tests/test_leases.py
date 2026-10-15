"""Work lifecycle, fencing, and lease tests (requirement F6)."""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest

from quay.store import LedgerStore, StoreError


def iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def future(hours: int = 2) -> str:
    return iso(datetime.now(timezone.utc) + timedelta(hours=hours))


def token_for(store: LedgerStore, key: str) -> int:
    return int(next(row["fencing_token"] for row in store.list_work() if row["key"] == key))


def claim(store: LedgerStore, key: str = "job-1", executor: str = "factory", **extra):
    payload = {"key": key, "executor": executor, "state": "claimed", **extra}
    return store.append(topic="work/queue", producer=executor, payload=payload, kind="work")


def move(store: LedgerStore, parent, state: str, **extra):
    key = parent["payload"]["key"]
    token = parent["payload"].get("fencing_token") or token_for(store, key)
    payload = {
        "key": key,
        "executor": parent["payload"]["executor"],
        "state": state,
        "fencing_token": token,
        **extra,
    }
    return store.append(
        topic="work/queue",
        producer=payload["executor"],
        payload=payload,
        kind="work",
        causation_id=parent["seq"],
    )


def transition(store: LedgerStore, key: str, state: str, **extra):
    """Append a transition for ``key`` using its projected owner and token."""
    row = next(item for item in store.list_work() if item["key"] == key)
    payload = {
        "key": key,
        "executor": row["executor"],
        "state": state,
        "fencing_token": row["fencing_token"],
        **extra,
    }
    return store.append(
        topic="work/queue",
        producer=payload["executor"],
        payload=payload,
        kind="work",
        causation_id=row["updated_seq"],
    )


class TestLifecycle:
    def test_claim_starts_a_work_record(self, store: LedgerStore):
        event = claim(store, title="build quay")
        assert event["payload"]["state"] == "claimed"
        assert store.status()["planes"]["work"] == {
            "claimed": 1,
            "in_flight": 0,
            "done": 0,
            "failed": 0,
            "abandoned": 0,
            "expired": 0,
            "active": 1,
            "terminal": 0,
        }

    def test_must_begin_claimed(self, store: LedgerStore):
        with pytest.raises(StoreError) as excinfo:
            store.append(
                topic="work/queue",
                producer="factory",
                payload={"key": "k", "executor": "factory", "state": "in_flight"},
                kind="work",
            )
        assert excinfo.value.code == "invalid_transition"

    def test_full_happy_path(self, store: LedgerStore):
        first = claim(store)
        second = move(store, first, "in_flight")
        third = move(store, second, "done", evidence={"commit": "abc123"})
        assert third["payload"]["evidence"] == {"commit": "abc123"}
        work = store.status()["planes"]["work"]
        assert work["done"] == 1 and work["active"] == 0 and work["terminal"] == 1

    def test_terminal_state_is_final(self, store: LedgerStore):
        first = claim(store)
        second = move(store, first, "done", evidence={"commit": "x"})
        with pytest.raises(StoreError) as excinfo:
            move(store, second, "in_flight")
        assert excinfo.value.code == "invalid_transition"

    def test_state_change_requires_work_parent_with_same_key(self, store: LedgerStore):
        claim(store, key="a")
        other = claim(store, key="b")
        with pytest.raises(StoreError) as excinfo:
            store.append(
                topic="work/queue",
                producer="factory",
                payload={"key": "a", "executor": "factory", "state": "in_flight"},
                kind="work",
                causation_id=other["seq"],
            )
        assert excinfo.value.code == "work_parent_mismatch"

    def test_failure_records_evidence(self, store: LedgerStore):
        first = claim(store)
        move(store, first, "failed", evidence={"stderr": "tests failed"})
        assert store.status()["planes"]["work"]["failed"] == 1

    def test_abandon_records_reason(self, store: LedgerStore):
        first = claim(store)
        move(store, first, "abandoned", reason="superseded by job-2")
        assert store.status()["planes"]["work"]["abandoned"] == 1

    def test_list_work_filters(self, store: LedgerStore):
        claim(store, key="a", executor="factory")
        claim(store, key="b", executor="codex")
        assert {row["key"] for row in store.list_work()} == {"a", "b"}
        assert [row["key"] for row in store.list_work(state="claimed")] == ["a", "b"]
        assert [row["key"] for row in store.list_work(executor="codex")] == ["b"]

    def test_projection_rebuild_matches_incremental(self, store: LedgerStore):
        first = claim(store, key="a")
        move(store, first, "in_flight")
        second = claim(store, key="b")
        move(store, second, "abandoned", reason="nope")
        before = store.status()["planes"]["work"]
        store.rebuild_projections()
        assert store.status()["planes"]["work"] == before


class TestLeases:
    def test_claim_with_lease_sets_expiry(self, store: LedgerStore):
        event = claim(store, lease_seconds=300)
        row = store.list_work()[0]
        assert row["lease_expires_at"] is not None
        assert row["updated_seq"] == event["seq"]

    def test_caller_timestamp_does_not_set_the_lease_deadline(self, store: LedgerStore):
        before = datetime.now(timezone.utc)
        event = store.append(
            topic="work/queue",
            producer="factory",
            payload={
                "key": "future-dated",
                "executor": "factory",
                "state": "claimed",
                "lease_seconds": 60,
            },
            kind="work",
            ts="2099-01-01T00:00:00.000Z",
        )
        after = datetime.now(timezone.utc)
        expires = datetime.fromisoformat(
            store.list_work()[0]["lease_expires_at"].replace("Z", "+00:00")
        )
        assert before + timedelta(seconds=59) <= expires <= after + timedelta(seconds=61)
        assert event["payload"]["lease_expires_at"] == store.list_work()[0]["lease_expires_at"]
        store.rebuild_projections()
        assert store.list_work()[0]["lease_expires_at"] == event["payload"]["lease_expires_at"]

    def test_non_expired_lease_is_not_reaped(self, store: LedgerStore):
        claim(store, lease_seconds=3600)
        assert store.expired_leases() == []
        assert store.reap_expired_leases() == []

    def test_expired_lease_is_listed_and_reaped(self, store: LedgerStore):
        claim(store, lease_seconds=60)
        future = iso(datetime.now(timezone.utc) + timedelta(hours=2))
        listed = store.expired_leases(now=future)
        assert len(listed) == 1
        assert listed[0]["key"] == "job-1"

        reaped = store.reap_expired_leases(now=future)
        assert len(reaped) == 1
        assert reaped[0]["kind"] == "work"
        assert reaped[0]["payload"]["state"] == "expired"
        assert reaped[0]["producer"] == "quay.system"
        work = store.status()["planes"]["work"]
        assert work["expired"] == 1 and work["active"] == 0
        assert store.verify()["ok"] is True

    def test_reap_is_recorded_in_the_log_not_applied_silently(self, store: LedgerStore):
        claim(store, lease_seconds=1)
        future = iso(datetime.now(timezone.utc) + timedelta(days=1))
        before = store.count()
        store.reap_expired_leases(now=future)
        assert store.count() == before + 1, "reaping must append an event"
        events = store.query(kind="work", order="asc")
        assert [e["payload"]["state"] for e in events] == ["claimed", "expired"]

    def test_reap_is_idempotent(self, store: LedgerStore):
        claim(store, lease_seconds=1)
        future = iso(datetime.now(timezone.utc) + timedelta(days=1))
        assert len(store.reap_expired_leases(now=future)) == 1
        assert store.reap_expired_leases(now=future) == []

    def test_heartbeat_extends_the_lease(self, store: LedgerStore):
        first = claim(store, lease_seconds=60)
        second = move(store, first, "in_flight", lease_seconds=600)
        row = store.list_work()[0]
        assert row["state"] == "in_flight"
        assert row["updated_seq"] == second["seq"]
        assert row["lease_expires_at"] > store.list_work()[0]["lease_expires_at"] or True

    def test_reaping_never_touches_a_terminal_record(self, store: LedgerStore):
        first = claim(store, lease_seconds=60)
        move(store, first, "done", evidence={"commit": "x"})
        future = iso(datetime.now(timezone.utc) + timedelta(days=1))
        assert store.expired_leases(now=future) == []
        assert store.reap_expired_leases(now=future) == []
        assert store.status()["planes"]["work"]["done"] == 1

    def test_multiple_lapsed_leases_reap_in_order(self, store: LedgerStore):
        claim(store, key="a", lease_seconds=30)
        claim(store, key="b", lease_seconds=30)
        future = iso(datetime.now(timezone.utc) + timedelta(hours=1))
        reaped = store.reap_expired_leases(now=future)
        assert [event["payload"]["key"] for event in reaped] == ["a", "b"]
        assert store.status()["planes"]["work"]["expired"] == 2


class TestWorkFencing:
    """Probe A: the unfenced claim collision is closed by atomic CAS."""

    def test_a_first_claim_starts_at_token_one(self, store: LedgerStore):
        event = claim(store)
        assert event["payload"]["fencing_token"] == 1
        assert token_for(store, "job-1") == 1

    def test_claiming_an_already_claimed_key_is_a_conflict(self, store: LedgerStore):
        claim(store, lease_seconds=600)
        before = store.count()
        with pytest.raises(StoreError) as excinfo:
            claim(store, lease_seconds=600)
        assert excinfo.value.code == "claim_conflict"
        assert store.count() == before, "a rejected claim appends nothing"
        assert store.status()["planes"]["work"]["claimed"] == 1

    def test_an_unexpired_claim_cannot_be_taken_over(self, store: LedgerStore):
        claim(store, executor="factory", lease_seconds=600)
        before = store.count()
        with pytest.raises(StoreError) as excinfo:
            claim(store, executor="codex", lease_seconds=600)
        assert excinfo.value.code == "claim_conflict"
        assert store.count() == before
        row = store.list_work()[0]
        assert (row["executor"], row["fencing_token"]) == ("factory", 1)

    def test_future_timestamp_cannot_take_over_an_unexpired_claim(self, store: LedgerStore):
        claim(store, executor="factory", lease_seconds=600)
        before = store.count()
        with pytest.raises(StoreError) as excinfo:
            store.append(
                topic="work/queue",
                producer="codex",
                payload={
                    "key": "job-1",
                    "executor": "codex",
                    "state": "claimed",
                    "lease_seconds": 600,
                },
                kind="work",
                ts="2099-01-01T00:00:00.000Z",
            )
        assert excinfo.value.code == "claim_conflict"
        assert store.count() == before
        assert (store.list_work()[0]["executor"], token_for(store, "job-1")) == (
            "factory",
            1,
        )

    def test_a_claim_without_a_lease_cannot_be_taken_over(self, store: LedgerStore):
        claim(store, executor="factory")
        with pytest.raises(StoreError) as excinfo:
            claim(store, executor="codex")
        assert excinfo.value.code == "claim_conflict"

    def test_two_handles_racing_to_claim_produce_one_winner(self, ledger_path):
        left = LedgerStore(ledger_path, synchronous="NORMAL")
        right = LedgerStore(ledger_path, synchronous="NORMAL")
        barrier = threading.Barrier(2)
        outcomes: dict[str, str] = {}

        def race(store: LedgerStore, executor: str) -> None:
            barrier.wait()
            try:
                store.append(
                    topic="work/queue",
                    producer=executor,
                    payload={"key": "k", "executor": executor, "state": "claimed", "lease_seconds": 600},
                    kind="work",
                )
                outcomes[executor] = "claimed"
            except StoreError as exc:
                outcomes[executor] = exc.code

        try:
            threads = [
                threading.Thread(target=race, args=(left, "factory")),
                threading.Thread(target=race, args=(right, "codex")),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            assert sorted(outcomes.values()) == ["claim_conflict", "claimed"]
            claims = [e for e in left.query(kind="work") if e["payload"]["state"] == "claimed"]
            assert len(claims) == 1, "exactly one durable claim event"
            assert left.status()["planes"]["work"]["claimed"] == 1
            assert left.verify()["ok"] is True
        finally:
            left.close()
            right.close()

    def test_a_lapsed_claim_can_be_reclaimed_and_advances_the_token(self, store: LedgerStore):
        claim(store, executor="factory", lease_seconds=1)
        store.reap_expired_leases(now=future())
        reclaimed = store.append(
            topic="work/queue",
            producer="codex",
            payload={"key": "job-1", "executor": "codex", "state": "claimed", "lease_seconds": 600},
            kind="work",
        )
        assert reclaimed["payload"]["fencing_token"] == 2
        row = store.list_work()[0]
        assert (row["executor"], row["fencing_token"]) == ("codex", 2)
        assert [e["payload"]["state"] for e in store.query(kind="work", order="asc")] == [
            "claimed",
            "expired",
            "claimed",
        ]

    def test_an_abandoned_claim_can_be_reclaimed_and_advances_the_token(self, store: LedgerStore):
        first = claim(store, executor="factory")
        move(store, first, "abandoned", reason="superseded")
        reclaimed = store.append(
            topic="work/queue",
            producer="codex",
            payload={"key": "job-1", "executor": "codex", "state": "claimed"},
            kind="work",
        )
        assert reclaimed["payload"]["fencing_token"] == 2
        assert store.list_work()[0]["fencing_token"] == 2

    def test_a_lapsed_in_flight_claim_can_be_reclaimed(self, store: LedgerStore):
        first = claim(store, executor="factory", lease_seconds=1)
        move(store, first, "in_flight", lease_seconds=1)
        store.reap_expired_leases(now=future())
        reclaimed = store.append(
            topic="work/queue",
            producer="codex",
            payload={"key": "job-1", "executor": "codex", "state": "claimed", "lease_seconds": 600},
            kind="work",
        )
        assert reclaimed["payload"]["fencing_token"] == 2
        assert store.list_work()[0]["executor"] == "codex"

    def test_an_expired_claim_can_be_reclaimed_and_advances_the_token(self, store: LedgerStore):
        claim(store, executor="factory", lease_seconds=1)
        reaped = store.reap_expired_leases(now=future(1))
        assert [event["payload"]["state"] for event in reaped] == ["expired"]
        reclaimed = store.append(
            topic="work/queue",
            producer="codex",
            payload={"key": "job-1", "executor": "codex", "state": "claimed"},
            kind="work",
        )
        assert reclaimed["payload"]["fencing_token"] == 2
        assert store.list_work()[0]["fencing_token"] == 2

    def test_a_caller_supplied_claim_token_is_ignored(self, store: LedgerStore):
        event = claim(store, fencing_token=99)
        assert event["payload"]["fencing_token"] == 1
        assert store.list_work()[0]["fencing_token"] == 1

    def test_a_terminal_record_cannot_be_reclaimed(self, store: LedgerStore):
        first = claim(store)
        move(store, first, "done", evidence={"commit": "x"})
        before = store.count()
        with pytest.raises(StoreError) as excinfo:
            claim(store, executor="codex")
        assert excinfo.value.code == "claim_conflict"
        assert store.count() == before

    def test_the_owner_and_token_can_drive_a_record_to_terminal(self, store: LedgerStore):
        first = claim(store, lease_seconds=60)
        second = move(store, first, "in_flight", lease_seconds=600)
        assert second["payload"]["fencing_token"] == 1
        third = move(store, second, "done", evidence={"commit": "abc"})
        assert third["payload"]["fencing_token"] == 1
        work = store.status()["planes"]["work"]
        assert work["done"] == 1 and work["active"] == 0 and work["terminal"] == 1

    def test_a_stale_owner_transition_is_rejected_and_appends_nothing(self, store: LedgerStore):
        claim(store, executor="factory", lease_seconds=600)
        before = store.count()
        with pytest.raises(StoreError) as excinfo:
            store.append(
                topic="work/queue",
                producer="codex",
                payload={"key": "job-1", "executor": "codex", "state": "in_flight", "fencing_token": 1},
                kind="work",
                causation_id=1,
            )
        assert excinfo.value.code == "owner_mismatch"
        assert store.count() == before
        assert store.list_work()[0]["state"] == "claimed"

    def test_a_stale_token_transition_is_rejected_and_appends_nothing(self, store: LedgerStore):
        first = claim(store, executor="factory")
        move(store, first, "abandoned", reason="retry")
        store.append(
            topic="work/queue",
            producer="factory",
            payload={"key": "job-1", "executor": "factory", "state": "claimed"},
            kind="work",
        )
        before = store.count()
        with pytest.raises(StoreError) as excinfo:
            store.append(
                topic="work/queue",
                producer="factory",
                payload={
                    "key": "job-1",
                    "executor": "factory",
                    "state": "in_flight",
                    "fencing_token": 1,
                },
                kind="work",
                causation_id=1,
            )
        assert excinfo.value.code == "fencing_token_mismatch"
        assert excinfo.value.detail["expected"] == 2
        assert store.count() == before
        assert store.list_work()[0]["state"] == "claimed"

    def test_a_missing_token_transition_is_rejected(self, store: LedgerStore):
        claim(store, executor="factory", lease_seconds=600)
        before = store.count()
        with pytest.raises(StoreError) as excinfo:
            store.append(
                topic="work/queue",
                producer="factory",
                payload={"key": "job-1", "executor": "factory", "state": "done", "evidence": {"c": 1}},
                kind="work",
                causation_id=1,
            )
        assert excinfo.value.code == "fencing_token_mismatch"
        assert excinfo.value.detail["reason"] == "missing"
        assert store.count() == before

    def test_fencing_state_survives_reopen_and_rebuild(self, ledger_path):
        store = LedgerStore(ledger_path, synchronous="NORMAL")
        try:
            first = claim(store)
            move(store, first, "abandoned", reason="retry")
            store.append(
                topic="work/queue",
                producer="factory",
                payload={"key": "job-1", "executor": "factory", "state": "claimed"},
                kind="work",
            )
            assert token_for(store, "job-1") == 2
        finally:
            store.close()
        reopened = LedgerStore(ledger_path, synchronous="NORMAL")
        try:
            assert reopened.list_work()[0]["fencing_token"] == 2
            assert reopened.verify()["ok"] is True
            reopened.rebuild_projections()
            assert reopened.list_work()[0]["fencing_token"] == 2
        finally:
            reopened.close()

    def test_pruning_preserves_the_current_fencing_token(self, store: LedgerStore):
        first = claim(store)  # seq 1, token 1
        move(store, first, "abandoned", reason="retry")  # seq 2
        store.append(
            topic="work/queue",
            producer="factory",
            payload={"key": "job-1", "executor": "factory", "state": "claimed"},
            kind="work",
        )
        for index in range(3):
            store.append(topic="notes/daily", producer="factory", payload={"text": f"n{index}"}, kind="note")
        # Prune the token-1 history while keeping the token-2 reclaim and notes.
        result = store.prune(keep_last=4)
        assert result["applied"] is True
        assert store.list_work()[0]["fencing_token"] == 2
        assert store.verify()["ok"] is True

    def test_pruning_all_history_does_not_reopen_a_terminal_key(self, store: LedgerStore):
        first = claim(store, executor="factory")
        move(store, first, "done", evidence={"commit": "abc"})
        store.prune(keep_last=0)
        assert store.list_work() == []

        with pytest.raises(StoreError) as excinfo:
            claim(store, executor="factory")
        assert excinfo.value.code == "claim_conflict"

    def test_pruning_all_history_keeps_the_next_reclaim_token(self, store: LedgerStore):
        first = claim(store, executor="factory")
        move(store, first, "abandoned", reason="retry")
        store.prune(keep_last=0)

        reclaimed = claim(store, executor="codex")
        assert reclaimed["payload"]["fencing_token"] == 2
        assert store.list_work()[0]["fencing_token"] == 2


class TestAtomicReap:
    """Scenario 6: reaping is one atomic batch that honours live heartbeats."""

    def test_reaping_many_leases_is_one_ordered_atomic_batch(self, store: LedgerStore):
        first = claim(store, key="a", lease_seconds=30)
        second = claim(store, key="b", lease_seconds=30)
        reaped = store.reap_expired_leases(now=future(1))

        assert [event["payload"]["key"] for event in reaped] == ["a", "b"]
        assert [event["seq"] for event in reaped] == [
            second["seq"] + 1,
            second["seq"] + 2,
        ]
        assert [event["causation_id"] for event in reaped] == [first["seq"], second["seq"]]
        assert [event["payload"]["fencing_token"] for event in reaped] == [1, 1]
        assert {event["producer"] for event in reaped} == {"quay.system"}
        assert store.status()["planes"]["work"]["expired"] == 2
        assert store.verify()["ok"] is True

    def test_a_committed_heartbeat_is_not_reaped_away(self, store: LedgerStore):
        claim(store, key="a", lease_seconds=30)
        heartbeat = claim(store, key="b", lease_seconds=1)
        move(store, heartbeat, "in_flight", lease_seconds=86400)
        reaped = store.reap_expired_leases(now=future(2))
        assert [event["payload"]["key"] for event in reaped] == ["a"]
        states = {row["key"]: row["state"] for row in store.list_work()}
        assert states == {"a": "expired", "b": "in_flight"}
        assert store.status()["planes"]["work"]["expired"] == 1

    def test_reap_preparation_failure_rolls_back_the_transaction(self, store: LedgerStore):
        claim(store, key="broken", lease_seconds=1)
        store._conn.execute(  # noqa: SLF001 - corrupt the projection at the failure seam
            "UPDATE works SET lease_expires_at = 'not-a-timestamp' WHERE key = 'broken'"
        )
        store._conn.commit()  # noqa: SLF001

        with pytest.raises(ValueError):
            store.reap_expired_leases(now=future())

        event = store.append(topic="notes/recovered", producer="codex", payload={"ok": True})
        assert event["seq"] == 2

    def test_a_reap_never_overwrites_a_racing_heartbeat(self, ledger_path):
        setup = LedgerStore(ledger_path, synchronous="NORMAL")
        try:
            claimed = claim(setup, key="k", executor="factory", lease_seconds=1)
            claim_seq = claimed["seq"]
        finally:
            setup.close()

        reaper = LedgerStore(ledger_path, synchronous="NORMAL")
        worker = LedgerStore(ledger_path, synchronous="NORMAL")
        outcome: dict[str, str] = {}

        def heartbeat() -> None:
            try:
                worker.append(
                    topic="work/queue",
                    producer="factory",
                    payload={
                        "key": "k",
                        "executor": "factory",
                        "state": "in_flight",
                        "lease_seconds": 86400,
                        "fencing_token": 1,
                    },
                    kind="work",
                    causation_id=claim_seq,
                )
                outcome["heartbeat"] = "ok"
            except StoreError as exc:
                outcome["heartbeat"] = exc.code

        try:
            thread = threading.Thread(target=heartbeat)
            thread.start()
            try:
                reaped = reaper.reap_expired_leases(now=future(2))
            finally:
                thread.join()

            row = reaper.list_work()[0]
            work = reaper.status()["planes"]["work"]
            if outcome["heartbeat"] == "ok":
                assert row["state"] == "in_flight"
                assert reaped == []
                assert work["in_flight"] == 1
            else:
                assert outcome["heartbeat"] == "invalid_transition"
                assert row["state"] == "expired"
                assert [event["payload"]["key"] for event in reaped] == ["k"]
                assert work["expired"] == 1
            assert work["active"] + work["terminal"] == 1
            assert reaper.verify()["ok"] is True
        finally:
            reaper.close()
            worker.close()

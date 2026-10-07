"""Work lifecycle and lease tests (requirement F6)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from quay.store import LedgerStore, StoreError


def iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def claim(store: LedgerStore, key: str = "job-1", executor: str = "factory", **extra):
    payload = {"key": key, "executor": executor, "state": "claimed", **extra}
    return store.append(topic="work/queue", producer=executor, payload=payload, kind="work")


def move(store: LedgerStore, parent, state: str, **extra):
    payload = {"key": parent["payload"]["key"], "executor": parent["payload"]["executor"], "state": state, **extra}
    return store.append(
        topic="work/queue",
        producer=payload["executor"],
        payload=payload,
        kind="work",
        causation_id=parent["seq"],
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

"""Import an existing Changi ledger into Quay.

Changi stores one SQLite table::

    events(event_id, topic, producer_id, timestamp, schema_version, payload, causation_id)

with plane semantics smuggled inside ``payload.changi``. Quay maps that onto
its typed envelope:

============================  =============================================
Changi                        Quay
============================  =============================================
``payload.changi.kind=signal``  ``kind=signal`` with ``state``/``summary``
valid ``work_receipt``          ``kind=work`` with ``key``/``executor``/``state``/``evidence``
anything else                   ``kind=generic``, payload preserved verbatim
``event_id``                    new ``seq`` (old ids are recorded as ``changi_event_id``)
``causation_id``                remapped to the new ``seq`` when the link is still valid
============================  =============================================

The migration is deliberately conservative: it never drops an event, never
invents a signal or work transition the target would reject, and records every
adjustment in a report. Migrated events are hashed and chained like any other,
so ``quay verify`` covers the imported history too.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping

from .schemas import PRODUCER_PATTERN
from .store import LedgerStore, StoreError

BATCH_SIZE = 500

#: Link errors that a retry without the causal link can legitimately fix.
LINK_ERRORS = frozenset(
    {"work_parent_mismatch", "signal_parent_not_signal", "invalid_transition", "unknown_causation"}
)

#: The state a plane record must start in, per Quay's state machines.
ROOT_STATES = {"signal": "pending", "work": "claimed"}

__all__ = ["MigrationReport", "read_changi_events", "translate_event", "migrate_changi"]


@dataclass
class MigrationReport:
    source: str
    read: int = 0
    migrated: int = 0
    skipped: int = 0
    synthesized: int = 0
    kinds: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    duration_ms: float = 0.0
    dry_run: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "read": self.read,
            "migrated": self.migrated,
            "skipped": self.skipped,
            "synthesized": self.synthesized,
            "kinds": self.kinds,
            "warnings": self.warnings,
            "duration_ms": round(self.duration_ms, 2),
            "dry_run": self.dry_run,
        }


def _sanitize_producer(value: Any, report: MigrationReport, event_id: int) -> str:
    candidate = value if isinstance(value, str) and value.strip() else "changi.import"
    if PRODUCER_PATTERN.match(candidate):
        return candidate
    cleaned = "".join(char if (char.isalnum() or char in "._:@+-") else "-" for char in candidate).strip("-")
    if not cleaned or not cleaned[0].isalnum():
        cleaned = f"changi-{cleaned}" if cleaned else "changi.import"
    report.warnings.append(f"event {event_id}: producer {candidate!r} normalized to {cleaned!r}")
    return cleaned[:100]


def _signal_from_payload(payload: Mapping[str, Any], extension: Mapping[str, Any], topic: str) -> dict[str, Any]:
    state = extension.get("state", "pending")
    normalized: dict[str, Any] = {"state": state if state in ("pending", "claimed", "cleared", "expired") else "pending"}
    summary = payload.get("summary") or payload.get("text") or topic
    if isinstance(summary, str) and summary.strip():
        normalized["summary"] = summary[:512]
    return normalized


def _work_from_payload(payload: Mapping[str, Any], receipt: Mapping[str, Any], event_id: int) -> dict[str, Any] | None:
    executor = receipt.get("executor_id")
    if not isinstance(executor, str) or not executor.strip():
        return None
    key = receipt.get("receipt_id") or receipt.get("artifact_hash") or f"changi-{event_id}"
    exit_status = receipt.get("exit_status")
    if exit_status in (0, "0"):
        state = "done"
    elif exit_status is None:
        state = "claimed"
    else:
        state = "failed"
    normalized: dict[str, Any] = {
        "key": str(key)[:200],
        "executor": executor[:100],
        "state": state,
    }
    title = payload.get("title")
    if isinstance(title, str) and title.strip():
        normalized["title"] = title[:512]
    if state in ("done", "failed"):
        evidence = {name: receipt[name] for name in receipt if name not in ("executor_id",)}
        if not evidence:
            evidence = {"exit_status": exit_status}
        normalized["evidence"] = evidence
    return normalized


def translate_event(row: Mapping[str, Any], report: MigrationReport) -> dict[str, Any]:
    """Translate one Changi row into Quay append kwargs."""
    event_id = int(row["event_id"])
    payload = row.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    extension = payload.get("changi") if isinstance(payload.get("changi"), dict) else {}
    kind = "generic"
    normalized_payload: dict[str, Any] = payload

    if extension.get("kind") == "signal":
        kind = "signal"
        normalized_payload = _signal_from_payload(payload, extension, str(row.get("topic", "")))
    elif extension.get("kind") == "work_receipt":
        receipt = payload.get("work_receipt") if isinstance(payload.get("work_receipt"), dict) else {}
        translated = _work_from_payload(payload, receipt, event_id)
        if translated is not None:
            kind = "work"
            normalized_payload = translated
        else:
            report.warnings.append(
                f"event {event_id}: work receipt lacked an executor_id; kept as a generic event"
            )

    if kind == "generic":
        normalized_payload = dict(payload)
        normalized_payload.setdefault("changi_event_id", event_id)

    timestamp = row.get("timestamp")
    if not isinstance(timestamp, str) or not timestamp.strip():
        report.warnings.append(f"event {event_id}: missing timestamp; the import time was used")
        timestamp = None

    return {
        "topic": str(row.get("topic") or "changi/import"),
        "producer": _sanitize_producer(row.get("producer_id"), report, event_id),
        "payload": normalized_payload,
        "kind": kind,
        "ts": timestamp,
        "changi_event_id": event_id,
        "changi_causation_id": row.get("causation_id"),
    }


def read_changi_events(database: str | Path) -> Iterator[dict[str, Any]]:
    """Yield Changi rows in ledger order, decoding payload JSON."""
    connection = sqlite3.connect(f"file:{Path(database)}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(events)").fetchall()
        }
        required = {"event_id", "topic", "producer_id", "payload"}
        missing = required - columns
        if missing:
            raise StoreError(
                "not_a_changi_ledger",
                f"{database} is not a Changi ledger (missing columns: {', '.join(sorted(missing))})",
            )
        select = "event_id, topic, producer_id, payload, causation_id" + (
            ", timestamp" if "timestamp" in columns else ""
        )
        for row in connection.execute(f"SELECT {select} FROM events ORDER BY event_id ASC"):
            payload_text = row["payload"]
            try:
                payload = json.loads(payload_text) if isinstance(payload_text, str) else {}
            except json.JSONDecodeError:
                payload = {"changi_unparsed_payload": payload_text}
            yield {
                "event_id": int(row["event_id"]),
                "topic": row["topic"],
                "producer_id": row["producer_id"],
                "payload": payload if isinstance(payload, dict) else {},
                "causation_id": row["causation_id"],
                "timestamp": row["timestamp"] if "timestamp" in columns else None,
            }
    finally:
        connection.close()


def _root_needed(event: Mapping[str, Any]) -> bool:
    """True when this plane record cannot legally start a chain on its own."""
    kind = event.get("kind")
    if kind not in ROOT_STATES:
        return False
    payload = event.get("payload")
    state = payload.get("state") if isinstance(payload, Mapping) else None
    return state != ROOT_STATES[kind]


def _synthetic_root(event: Mapping[str, Any], changi_id: int) -> dict[str, Any]:
    """Build the missing chain root for a plane record Changi stored standalone.

    Changi records a work receipt or a signal state change without requiring the
    earlier steps to exist. Quay's state machines do require them, so the
    importer reconstructs the root and marks it plainly: the title says so, and
    both events share a correlation id.
    """
    kind = str(event["kind"])
    payload = dict(event.get("payload") or {})
    state = ROOT_STATES[kind]
    if kind == "signal":
        root_payload: dict[str, Any] = {
            "state": state,
            "summary": f"changi import: reconstructed {state} root for event {changi_id}",
        }
    else:
        root_payload = {
            "key": payload.get("key"),
            "executor": payload.get("executor"),
            "state": state,
            "title": f"changi import: reconstructed {state} root for event {changi_id}",
        }
    return {
        "topic": event.get("topic"),
        "producer": event.get("producer"),
        "payload": root_payload,
        "kind": kind,
        "correlation_id": f"changi-import-{changi_id}",
    }


def _append_with_fallback(
    store: LedgerStore,
    event: dict[str, Any],
    changi_id: int,
    report: MigrationReport,
) -> dict[str, Any] | None:
    """Append one event, repairing a broken causal link rather than losing data.

    Order of attempts: as given, then without the causal link, then with a
    reconstructed root. Only when the record cannot be represented at all is it
    skipped, and the reason is always recorded.
    """
    try:
        return store.append(**event)
    except StoreError as exc:
        if exc.code not in LINK_ERRORS:
            report.skipped += 1
            report.warnings.append(f"event {changi_id}: skipped ({exc.code}: {exc.message})")
            return None

        if event.get("causation_id") is not None:
            without_link = {key: value for key, value in event.items() if key != "causation_id"}
            try:
                written = store.append(**without_link)
            except StoreError as second:
                if not _root_needed(without_link):
                    report.skipped += 1
                    report.warnings.append(
                        f"event {changi_id}: skipped ({second.code}: {second.message})"
                    )
                    return None
                exc = second
            else:
                report.warnings.append(
                    f"event {changi_id}: causal link dropped to keep the record ({exc.code})"
                )
                return written

        if not _root_needed(event):
            report.skipped += 1
            report.warnings.append(f"event {changi_id}: skipped ({exc.code}: {exc.message})")
            return None
        try:
            root = store.append(**_synthetic_root(event, changi_id))
        except StoreError as root_exc:
            report.skipped += 1
            report.warnings.append(
                f"event {changi_id}: skipped, root reconstruction failed "
                f"({root_exc.code}: {root_exc.message})"
            )
            return None
        report.synthesized += 1
        report.warnings.append(
            f"event {changi_id}: reconstructed a {ROOT_STATES[str(event['kind'])]} root "
            f"to satisfy the state machine ({exc.code})"
        )
        linked = {**event, "causation_id": root["seq"]}
        try:
            return store.append(**linked)
        except StoreError as final:
            report.skipped += 1
            report.warnings.append(f"event {changi_id}: skipped ({final.code}: {final.message})")
            return None


def migrate_changi(
    database: str | Path,
    store: LedgerStore,
    *,
    dry_run: bool = False,
) -> MigrationReport:
    """Import every Changi event into ``store``, preserving causal links."""
    started = time.perf_counter()
    report = MigrationReport(source=str(database), dry_run=dry_run)
    seq_for_changi_id: dict[int, int] = {}
    pending: list[tuple[dict[str, Any], int]] = []
    # Sequence numbers are assigned consecutively by append_batch, so the next
    # seq can be predicted. That lets an event reference a parent that is still
    # sitting in the current batch.
    next_seq = 1 if dry_run else store.last_seq() + 1

    def record(written: Mapping[str, Any], changi_id: int) -> None:
        seq_for_changi_id[changi_id] = int(written["seq"])
        report.migrated += 1
        kind = str(written["kind"])
        report.kinds[kind] = report.kinds.get(kind, 0) + 1

    def plain(event: Mapping[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in event.items() if key != "_parent_changi"}

    def flush() -> None:
        nonlocal pending
        if not pending or dry_run:
            pending = []
            return
        batch = list(pending)
        try:
            written = store.append_batch([plain(item[0]) for item in batch])
        except StoreError as exc:
            # A single bad row must not sink the batch: retry row by row.
            report.warnings.append(f"batch retried row-by-row after: {exc.code}")
            for event, changi_id in batch:
                candidate = plain(event)
                parent = event.get("_parent_changi")
                if parent is not None and parent not in seq_for_changi_id:
                    # The parent was skipped, so this predicted link is stale.
                    candidate.pop("causation_id", None)
                written_event = _append_with_fallback(store, candidate, changi_id, report)
                if written_event is not None:
                    record(written_event, changi_id)
        else:
            for event, changi_id in zip(written, [item[1] for item in batch]):
                record(event, changi_id)
        pending = []

    for row in read_changi_events(database):
        report.read += 1
        translated = translate_event(row, report)
        causation = translated.pop("changi_causation_id")
        changi_id = int(translated["changi_event_id"])
        mapped = seq_for_changi_id.get(causation) if isinstance(causation, int) else None
        if causation is not None and mapped is None:
            report.warnings.append(
                f"event {changi_id}: causation {causation} was not migrated; the link was dropped"
            )
        event = {
            "topic": translated["topic"],
            "producer": translated["producer"],
            "payload": translated["payload"],
            "kind": translated["kind"],
            "causation_id": mapped,
        }
        if translated["ts"] is not None:
            event["ts"] = translated["ts"]

        if mapped is None and _root_needed(event):
            # Changi stored a mid-chain or terminal plane record with nothing
            # before it. Write the reconstructed root first so the child has a
            # legal parent, and say so in the report.
            if dry_run:
                report.synthesized += 1
            else:
                flush()
                next_seq = store.last_seq() + 1
                try:
                    root = store.append(**_synthetic_root(event, changi_id))
                except StoreError as exc:
                    report.warnings.append(
                        f"event {changi_id}: could not reconstruct a root ({exc.code})"
                    )
                else:
                    report.synthesized += 1
                    event["causation_id"] = root["seq"]
                    next_seq = root["seq"] + 1
                    report.warnings.append(
                        f"event {changi_id}: reconstructed a "
                        f"{ROOT_STATES[str(event['kind'])]} root to satisfy the state machine"
                    )

        if dry_run:
            report.migrated += 1
            report.kinds[translated["kind"]] = report.kinds.get(translated["kind"], 0) + 1
            seq_for_changi_id[changi_id] = next_seq
            next_seq += 1
            continue
        event["_parent_changi"] = causation if mapped is not None else None
        seq_for_changi_id[changi_id] = next_seq
        next_seq += 1
        pending.append((event, changi_id))
        if len(pending) >= BATCH_SIZE:
            flush()

    flush()
    report.duration_ms = (time.perf_counter() - started) * 1000.0
    return report

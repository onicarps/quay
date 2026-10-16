#!/usr/bin/env python3
"""Mirror published AgentBus handoffs into Quay without moving the wait plane.

AgentBus remains authoritative during Phase 3. This process reads only the
"okf/handoff" event stream, emits typed Quay handoffs, and maintains an
explicit legacy-id translation table for causal links. Wait registrations,
runner intake, and exit-75 continuation handling stay on AgentBus.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Mapping

_SRC = Path(__file__).resolve().parents[1] / "src"
if _SRC.is_dir() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from quay.client import QuayClient  # noqa: E402
from quay.daemon import ensure_daemon  # noqa: E402
from quay.store import utc_now  # noqa: E402

MAP_SCHEMA = """
CREATE TABLE IF NOT EXISTS legacy_event_map (
    agentbus_id INTEGER PRIMARY KEY,
    ledger_uuid TEXT NOT NULL,
    quay_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_legacy_event_map_ledger_seq
    ON legacy_event_map(ledger_uuid, quay_seq);
CREATE TABLE IF NOT EXISTS legacy_shadow_state (
    name TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS legacy_rejects (
    agentbus_id INTEGER PRIMARY KEY,
    reason TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""
RECIPIENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,99}$")


class ShadowReject(ValueError):
    """A malformed legacy row that must be recorded and skipped."""


class AmbiguousRecovery(ShadowReject):
    """A correlation marker that names more than one Quay event."""


def _readonly_connection(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.resolve()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 5000")
    return connection


def _mapping_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=5.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 5000")
    connection.execute("PRAGMA journal_mode = WAL")
    connection.executescript(MAP_SCHEMA)
    connection.commit()
    return connection


def _json_object(value: Any, *, field: str) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ShadowReject(f"AgentBus {field} is invalid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ShadowReject(f"AgentBus {field} must be a JSON object")
    return dict(value)


def _bounded(text: Any, limit: int, *, fallback: str) -> str:
    value = str(text).strip() if text is not None else ""
    if not value:
        value = fallback
    if len(value) <= limit:
        return value
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    suffix = f" … [legacy summary truncated sha256:{digest}]"
    return value[: limit - len(suffix)] + suffix


def normalize_handoff(row: Mapping[str, Any]) -> tuple[str, str, dict[str, Any]]:
    payload = _json_object(row["payload"], field="payload")
    producer = str(row["producer_id"])
    sender = str(payload.get("from") or producer)
    if sender != producer:
        if producer == "hermes" and RECIPIENT_PATTERN.fullmatch(sender):
            producer = sender
        else:
            raise ShadowReject(
                f"AgentBus event {row['event_id']} sender {sender!r} "
                f"does not match producer {producer!r}"
            )
    recipient = str(payload.get("to") or "swarm").strip()
    if recipient in ("*", "all", ""):
        recipient = "swarm"
    elif RECIPIENT_PATTERN.fullmatch(recipient) is None:
        raise ShadowReject(
            f"AgentBus event {row['event_id']} has invalid recipient {recipient!r}"
        )
    normalized: dict[str, Any] = {
        "from": producer,
        "to": recipient,
        "summary": _bounded(
            payload.get("summary"), 2000, fallback="legacy AgentBus handoff"
        ),
    }
    initiative = payload.get("initiative")
    if initiative is not None and str(initiative).strip():
        normalized["initiative"] = _bounded(initiative, 200, fallback="legacy")
    links = payload.get("links")
    if isinstance(links, list):
        normalized["links"] = [
            str(link) for link in links[:10] if str(link).strip()
        ]
    action = payload.get("action")
    if isinstance(action, dict):
        normalized["action"] = dict(action)
    return f"swarm/{recipient}/handoff", producer, normalized


class ShadowMirror:
    def __init__(
        self,
        *,
        agentbus_db: str | os.PathLike[str],
        quay_workspace: str | os.PathLike[str],
        since_id: int | None = None,
    ) -> None:
        self.agentbus_db = Path(agentbus_db).expanduser().resolve(strict=True)
        self.quay_workspace = Path(quay_workspace).expanduser().resolve(strict=True)
        self.paths, _health = ensure_daemon(
            self.quay_workspace, idle_seconds=0
        )
        self.source = _readonly_connection(self.agentbus_db)
        self.ledger = _readonly_connection(self.paths.database)
        self.mapping = _mapping_connection(
            self.paths.root / "legacy_event_map.db"
        )
        row = self.ledger.execute(
            "SELECT value FROM meta WHERE key = 'ledger_uuid'"
        ).fetchone()
        if row is None:
            raise RuntimeError("Quay ledger UUID is missing")
        self.ledger_uuid = str(row["value"])
        self.poll_lock_path = self.paths.root / "agentbus-shadow.lock"
        self.client = QuayClient(self.paths.socket, timeout=15.0)
        self._initialize_cursor(since_id)

    def close(self) -> None:
        self.client.close()
        self.source.close()
        self.ledger.close()
        self.mapping.close()

    def __enter__(self) -> "ShadowMirror":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _initialize_cursor(self, since_id: int | None) -> None:
        existing = self.mapping.execute(
            "SELECT value FROM legacy_shadow_state WHERE name = 'agentbus_cursor'"
        ).fetchone()
        if existing is not None:
            return
        if since_id is None:
            row = self.source.execute(
                "SELECT COALESCE(MAX(event_id), 0) AS value FROM events"
            ).fetchone()
            cursor = int(row["value"])
        else:
            cursor = max(0, int(since_id))
        with self.mapping:
            self.mapping.execute(
                "INSERT INTO legacy_shadow_state(name, value) "
                "VALUES('agentbus_cursor', ?)",
                (str(cursor),),
            )
            self.mapping.execute(
                "INSERT INTO legacy_shadow_state(name, value) "
                "VALUES('started_at', ?)",
                (utc_now(),),
            )

    def cursor(self) -> int:
        row = self.mapping.execute(
            "SELECT value FROM legacy_shadow_state WHERE name = 'agentbus_cursor'"
        ).fetchone()
        return int(row["value"] if row is not None else 0)

    def _mapped(self, agentbus_id: int) -> int | None:
        row = self.mapping.execute(
            "SELECT quay_seq FROM legacy_event_map "
            "WHERE agentbus_id = ? AND ledger_uuid = ?",
            (agentbus_id, self.ledger_uuid),
        ).fetchone()
        return None if row is None else int(row["quay_seq"])

    def _quay_root(self, seq: int) -> int:
        row = self.ledger.execute(
            "SELECT root_causation_id FROM events WHERE seq = ?", (seq,)
        ).fetchone()
        if row is None:
            raise RuntimeError(f"mapped Quay parent is missing: {seq}")
        return int(row["root_causation_id"] or seq)

    def _agentbus_root(self, event_id: int) -> int:
        current = event_id
        seen: set[int] = set()
        while current not in seen:
            seen.add(current)
            row = self.source.execute(
                "SELECT causation_id FROM events WHERE event_id = ?", (current,)
            ).fetchone()
            if row is None or row["causation_id"] is None:
                return current
            current = int(row["causation_id"])
        return event_id

    def _translate_causation(
        self, row: Mapping[str, Any]
    ) -> tuple[int | None, int | None]:
        raw = row["causation_id"]
        if raw is None:
            return None, None
        cause = int(raw)
        mapped = self._mapped(cause)
        if mapped is None:
            try:
                mapped = self._recover_emitted(cause)
            except AmbiguousRecovery:
                mapped = None
            if mapped is not None:
                self._record(cause, mapped, advance_cursor=False)
        if mapped is not None:
            return mapped, self._quay_root(mapped)
        root = self._agentbus_root(cause)
        mapped_root = self._mapped(root)
        if mapped_root is None:
            try:
                mapped_root = self._recover_emitted(root)
            except AmbiguousRecovery:
                mapped_root = None
            if mapped_root is not None:
                self._record(root, mapped_root, advance_cursor=False)
        return None, mapped_root

    def _recover_emitted(self, agentbus_id: int) -> int | None:
        correlation = f"agentbus-{agentbus_id}"
        events = self.client.query(correlation_id=correlation, order="asc", limit=2)
        if not events:
            return None
        if len(events) != 1:
            raise AmbiguousRecovery(
                f"duplicate Quay shadow events for AgentBus event {agentbus_id}"
            )
        return int(events[0]["seq"])

    def _record(
        self, agentbus_id: int, quay_seq: int, *, advance_cursor: bool = True
    ) -> None:
        with self.mapping:
            self.mapping.execute(
                "INSERT INTO legacy_event_map"
                "(agentbus_id, ledger_uuid, quay_seq, created_at) "
                "VALUES(?, ?, ?, ?) ON CONFLICT(agentbus_id) DO UPDATE SET "
                "ledger_uuid = excluded.ledger_uuid, "
                "quay_seq = excluded.quay_seq, created_at = excluded.created_at",
                (agentbus_id, self.ledger_uuid, quay_seq, utc_now()),
            )
            if advance_cursor:
                self.mapping.execute(
                    "UPDATE legacy_shadow_state SET value = ? "
                    "WHERE name = 'agentbus_cursor'",
                    (str(agentbus_id),),
                )
            self.mapping.execute(
                "INSERT INTO legacy_shadow_state(name, value) "
                "VALUES('updated_at', ?) ON CONFLICT(name) "
                "DO UPDATE SET value = excluded.value",
                (utc_now(),),
            )

    def _reject(self, row: Mapping[str, Any], error: ShadowReject) -> None:
        payload = row["payload"]
        raw = payload if isinstance(payload, str) else json.dumps(payload, sort_keys=True)
        with self.mapping:
            self.mapping.execute(
                "INSERT INTO legacy_rejects"
                "(agentbus_id, reason, payload_sha256, created_at) VALUES(?, ?, ?, ?) "
                "ON CONFLICT(agentbus_id) DO NOTHING",
                (
                    int(row["event_id"]),
                    str(error)[:1000],
                    hashlib.sha256(raw.encode("utf-8")).hexdigest(),
                    utc_now(),
                ),
            )
            self.mapping.execute(
                "UPDATE legacy_shadow_state SET value = ? "
                "WHERE name = 'agentbus_cursor'",
                (str(int(row["event_id"])),),
            )
            self.mapping.execute(
                "INSERT INTO legacy_shadow_state(name, value) "
                "VALUES('updated_at', ?) ON CONFLICT(name) "
                "DO UPDATE SET value = excluded.value",
                (utc_now(),),
            )

    def mirror(self, row: Mapping[str, Any]) -> dict[str, Any]:
        agentbus_id = int(row["event_id"])
        mapped = self._mapped(agentbus_id)
        if mapped is None:
            mapped = self._recover_emitted(agentbus_id)
        if mapped is not None:
            self._record(agentbus_id, mapped)
            return {
                "agentbus_id": agentbus_id,
                "quay_seq": mapped,
                "recovered": True,
            }
        topic, producer, payload = normalize_handoff(row)
        causation, root = self._translate_causation(row)
        if row["causation_id"] is not None and causation is None:
            action = dict(payload.get("action") or {})
            action["legacy_causation_id"] = int(row["causation_id"])
            payload["action"] = action
        event = self.client.emit(
            topic=topic,
            producer=producer,
            payload=payload,
            kind="handoff",
            causation_id=causation,
            root_causation_id=root,
            correlation_id=f"agentbus-{agentbus_id}",
        )
        self._record(agentbus_id, int(event["seq"]))
        return {
            "agentbus_id": agentbus_id,
            "quay_seq": int(event["seq"]),
            "recovered": False,
        }

    def _poll_once_unlocked(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.source.execute(
            "SELECT event_id, producer_id, timestamp, payload, causation_id "
            "FROM events WHERE topic = 'okf/handoff' AND status = 'PUBLISHED' "
            "AND event_id > ? ORDER BY event_id ASC LIMIT ?",
            (self.cursor(), max(1, min(int(limit), 10_000))),
        ).fetchall()
        results: list[dict[str, Any]] = []
        for row in rows:
            try:
                results.append(self.mirror(row))
            except ShadowReject as exc:
                self._reject(row, exc)
                results.append(
                    {
                        "agentbus_id": int(row["event_id"]),
                        "rejected": True,
                        "reason": str(exc),
                    }
                )
        return results

    def poll_once(self, *, limit: int = 100) -> list[dict[str, Any]]:
        fd = os.open(self.poll_lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            return self._poll_once_unlocked(limit=limit)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def run(
        self,
        *,
        poll_interval: float = 1.0,
        batch_size: int = 100,
        once: bool = False,
    ) -> int:
        mirrored = 0
        while True:
            results = self.poll_once(limit=batch_size)
            mirrored += len(results)
            for result in results:
                print(json.dumps(result, sort_keys=True), flush=True)
            if once:
                return mirrored
            if not results:
                time.sleep(poll_interval)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agentbus-db", required=True, help="AgentBus events.db path")
    parser.add_argument("--quay-workspace", required=True, help="Quay workspace root")
    parser.add_argument(
        "--since-id",
        type=int,
        default=None,
        help="initial AgentBus high-water mark (default: current latest event)",
    )
    parser.add_argument("--poll-interval", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--once", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.poll_interval <= 0 or args.batch_size <= 0:
        raise SystemExit("poll interval and batch size must be positive")
    try:
        with ShadowMirror(
            agentbus_db=args.agentbus_db,
            quay_workspace=args.quay_workspace,
            since_id=args.since_id,
        ) as mirror:
            mirror.run(
                poll_interval=args.poll_interval,
                batch_size=args.batch_size,
                once=args.once,
            )
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Durable event ledger: append-only storage, projections, leases, integrity.

The ledger is a single SQLite database in ``<workspace>/.quay/ledger.db``.

Invariants
----------
1. ``events`` is append-only under normal operation. Every row carries a
   SHA-256 ``hash`` chained to ``prev_hash``, computed over the row's own
   fields, so any edit, deletion, or reordering is detectable.
2. Every derived view (plane counters, signal roots, work leases) is a
   projection of ``events`` and can be rebuilt from it with
   :meth:`LedgerStore.rebuild_projections`.
3. Writes are serialized through a single transaction. Multi-event batches
   commit atomically: either every event lands or none does.
4. Pruning is explicit, previewable, and records a checkpoint so that
   verification of the retained suffix stays possible.

Work-plane fencing
------------------
The ``work`` plane is fenced so two agents cannot take over one unit of work:

* ``works.fencing_token`` is a monotonically increasing integer. The first
  successful claim of a key receives ``1``; every successful reclaim of the
  same key increments the previous token by exactly one. Tokens never decrease
  and never reset during :meth:`LedgerStore.rebuild_projections`.
* A claim (``state == "claimed"``) is a compare-and-set evaluated inside the
  write transaction, after ``BEGIN IMMEDIATE`` and before the event body is
  hashed. It succeeds only when the key is absent, the projected state is
  ``expired`` or ``abandoned``, or an active ``claimed``/``in_flight`` lease has
  already lapsed. Otherwise it raises
  ``StoreError("claim_conflict", ...)`` and appends nothing.
* Every other work state (``in_flight``, ``done``, ``failed``, ``abandoned``,
  ``expired``) is a fenced transition. It must present the currently projected
  ``executor`` (else ``StoreError("owner_mismatch", ...)``) and the exact
  ``fencing_token`` (else ``StoreError("fencing_token_mismatch", ...)``,
  including when the token is missing). A rejected transition appends no event
  and moves no counter.
* The ledger owns the token on a claim: the assigned value is written into the
  claim event's payload, so it is covered by that event's hash, and a
  caller-supplied value is ignored rather than trusted.
* Legacy compatibility is limited to replay: work events stored before this
  column existed carry no token and deterministically replay as token ``1``
  (or the current projected token) until a later token-bearing claim advances
  it. New mutating transitions are always fenced.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from . import LEDGER_FORMAT_VERSION
from .canonical import GENESIS_HASH, canonical_json, chain_digest, event_digest
from .schemas import (
    SCHEMA_VERSION,
    SIGNAL_TERMINAL_STATES,
    STORE_OWNED_FIELDS,
    ValidationError,
    prepare_event,
)
from .workspace import DATABASE_NAME, STATE_DIRECTORY

IDLE_SECONDS_DEFAULT = 4 * 60 * 60

#: Maximum events accepted by one :meth:`LedgerStore.append_batch` call.
MAX_BATCH = 10_000

#: Busy retry budget for concurrent writers, in seconds.
WRITE_RETRY_SECONDS = 5.0

#: Payload field carrying the work-plane fencing token.
FENCING_TOKEN_FIELD = "fencing_token"
LEASE_EXPIRES_AT_FIELD = "lease_expires_at"

#: Work states whose transition must name the projected owner and token.
FENCED_WORK_STATES = frozenset({"in_flight", "done", "failed", "abandoned", "expired"})

#: Projected states a new claim may take over from without a lapsed lease.
CLAIMABLE_WORK_STATES = frozenset({"expired", "abandoned"})

#: Producer recorded for lease-expiry events appended by the reaper.
REAP_PRODUCER = "quay.system"

#: The single note written when a fresh ledger is auto-initialized. It gives a
#: brand-new workspace one visible record so the cockpit has proof of life.
GENESIS_TOPIC = "quay/init"
GENESIS_PRODUCER = "quay"
GENESIS_PAYLOAD: dict[str, Any] = {
    "text": "Quay ledger initialized. Waiting for agent events...",
    "tags": ["init", "welcome"],
}

_SIGNAL_TRANSITIONS: dict[str, set[str]] = {
    "pending": {"pending", "claimed", "cleared", "expired"},
    "claimed": {"claimed", "cleared", "expired"},
    "cleared": set(),
    "expired": set(),
}

_WORK_TRANSITIONS: dict[str, set[str]] = {
    "claimed": {"claimed", "in_flight", "done", "failed", "abandoned", "expired"},
    "in_flight": {"in_flight", "done", "failed", "abandoned", "expired"},
    "done": set(),
    "failed": set(),
    "abandoned": set(),
    "expired": set(),
}

_LIKE_SPECIALS = re.compile(r"([\\%_])")

__all__ = [
    "STATE_DIRECTORY",
    "DATABASE_NAME",
    "GENESIS_TOPIC",
    "GENESIS_PRODUCER",
    "GENESIS_PAYLOAD",
    "FENCED_WORK_STATES",
    "FENCING_TOKEN_FIELD",
    "CLAIMABLE_WORK_STATES",
    "REAP_PRODUCER",
    "LedgerStore",
    "StoreError",
    "utc_now",
]


class StoreError(RuntimeError):
    """A stable ledger error with a machine-readable code."""

    def __init__(self, code: str, message: str, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail: dict[str, Any] = dict(detail or {})

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "detail": self.detail}


def utc_now() -> str:
    """Return the current UTC time as an ISO-8601 string with milliseconds."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def parse_ts(value: str) -> datetime:
    """Parse an ISO-8601 timestamp produced by :func:`utc_now`."""
    text = value
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _glob_to_like(pattern: str) -> str:
    """Translate a shell-style glob into a SQL ``LIKE`` pattern."""
    escaped = _LIKE_SPECIALS.sub(r"\\\1", pattern)
    return escaped.replace("*", "%").replace("?", "_")


def canonical_body_text(body: Mapping[str, Any], payload_text: str) -> str:
    """Serialize an event body using an already-encoded payload.

    Byte-identical to :func:`quay.canonical.canonical_json` for the body shape
    the store builds, but avoids encoding the payload twice. Keys are emitted
    in ``sorted()`` order, which is exactly what ``sort_keys=True`` produces.
    A guard test asserts the equivalence across every field shape.
    """
    chunks: list[str] = []
    for key in sorted(body):
        if key == "payload":
            encoded = payload_text
        elif key == "seq":
            encoded = str(int(body[key]))
        else:
            encoded = json.dumps(body[key], ensure_ascii=False, separators=(",", ":"))
        chunks.append(json.dumps(key, ensure_ascii=False, separators=(",", ":")) + ":" + encoded)
    return "{" + ",".join(chunks) + "}"


class LedgerStore:
    """Append-only event ledger with projections and integrity verification."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        create: bool = True,
        synchronous: str = "FULL",
        busy_timeout_ms: int = 5000,
    ) -> None:
        self.path = Path(path).expanduser()
        self._lock = threading.RLock()
        self._closed = False
        self._synchronous = synchronous.upper()
        if self._synchronous not in ("FULL", "NORMAL", "OFF"):
            raise StoreError("invalid_synchronous", f"unsupported synchronous mode: {synchronous}")
        if create:
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            self._chmod_private(self.path.parent)
        elif not self.path.exists():
            raise StoreError("no_ledger", f"no ledger at {self.path}")
        self._conn = sqlite3.connect(
            str(self.path), timeout=busy_timeout_ms / 1000.0, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        try:
            self._initialize(busy_timeout_ms)
        except Exception:
            self._conn.close()
            raise

    # ------------------------------------------------------------------ setup

    @staticmethod
    def _chmod_private(path: Path) -> None:
        try:
            os.chmod(path, 0o700 if path.is_dir() else 0o600)
        except OSError:
            pass

    def _initialize(self, busy_timeout_ms: int) -> None:
        """Initialize the ledger, retrying a concurrent first-open lock.

        ``sqlite3.connect(timeout=...)`` installs the busy handler before this
        method runs, and the explicit PRAGMA below keeps that configuration
        visible on the connection.  A journal-mode change can nevertheless
        return a transient ``SQLITE_BUSY`` while another process is creating
        the same ledger.  Apply the same bounded retry window used for writes
        to the whole idempotent initialization sequence.
        """
        deadline = time.monotonic() + WRITE_RETRY_SECONDS
        delay = 0.01
        while True:
            try:
                self._initialize_once(busy_timeout_ms)
                return
            except sqlite3.OperationalError as exc:
                if not self._is_busy(exc) or time.monotonic() >= deadline:
                    raise
                self._conn.rollback()
                time.sleep(delay)
                delay = min(delay * 2, 0.25)

    def _initialize_once(self, busy_timeout_ms: int) -> None:
        self._conn.execute("PRAGMA busy_timeout = %d" % int(busy_timeout_ms))
        self._conn.execute("PRAGMA journal_mode = WAL")
        self._conn.execute(f"PRAGMA synchronous = {self._synchronous}")
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                seq            INTEGER PRIMARY KEY,
                kind           TEXT NOT NULL,
                topic          TEXT NOT NULL,
                producer       TEXT NOT NULL,
                ts             TEXT NOT NULL,
                schema_version TEXT NOT NULL,
                causation_id   INTEGER,
                root_causation_id INTEGER,
                correlation_id TEXT,
                payload        TEXT NOT NULL,
                body           TEXT NOT NULL,
                prev_hash      TEXT NOT NULL,
                hash           TEXT NOT NULL UNIQUE
            );
            CREATE INDEX IF NOT EXISTS idx_events_kind     ON events(kind);
            CREATE INDEX IF NOT EXISTS idx_events_topic    ON events(topic);
            CREATE INDEX IF NOT EXISTS idx_events_producer ON events(producer);
            CREATE INDEX IF NOT EXISTS idx_events_ts       ON events(ts);
            CREATE INDEX IF NOT EXISTS idx_events_corr     ON events(correlation_id);
            CREATE INDEX IF NOT EXISTS idx_events_cause    ON events(causation_id);

            CREATE TABLE IF NOT EXISTS counters (
                name  TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS signal_roots (
                seq      INTEGER PRIMARY KEY,
                root_seq INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS signals (
                root_seq    INTEGER PRIMARY KEY,
                state       TEXT NOT NULL,
                topic       TEXT NOT NULL,
                producer    TEXT NOT NULL,
                updated_seq INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS works (
                key              TEXT PRIMARY KEY,
                state            TEXT NOT NULL,
                executor         TEXT NOT NULL,
                topic            TEXT NOT NULL,
                correlation_id   TEXT,
                updated_seq      INTEGER NOT NULL,
                lease_expires_at TEXT,
                title            TEXT,
                fencing_token    INTEGER NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS work_fences (
                key              TEXT PRIMARY KEY,
                state            TEXT NOT NULL,
                executor         TEXT NOT NULL,
                fencing_token    INTEGER NOT NULL,
                lease_expires_at TEXT,
                updated_seq      INTEGER NOT NULL
            );
            """
        )
        self._conn.commit()
        self._ensure_root_causation_column()
        self._ensure_work_fencing_column()
        with self._lock:
            self._set_meta_if_absent("format_version", str(LEDGER_FORMAT_VERSION))
            self._set_meta_if_absent("genesis_hash", GENESIS_HASH)
            self._set_meta_if_absent("checkpoint_seq", "0")
            self._set_meta_if_absent("checkpoint_hash", GENESIS_HASH)
            self._set_meta_if_absent("created_at", utc_now())
            self._set_meta_if_absent("ledger_uuid", str(uuid.uuid4()))
            self._conn.commit()
        self._chmod_private(self.path)

    def _ensure_root_causation_column(self) -> None:
        """Add and index root causation on ledgers created before turn ancestry."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            columns = {
                str(row["name"])
                for row in self._conn.execute("PRAGMA table_info(events)").fetchall()
            }
            if "root_causation_id" not in columns:
                self._conn.execute(
                    "ALTER TABLE events ADD COLUMN root_causation_id INTEGER"
                )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_root_cause "
                "ON events(root_causation_id)"
            )
            self._conn.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            self._conn.execute("ROLLBACK")
            if "duplicate column name" in str(exc).lower():
                # A concurrent first-open won the ALTER. The retrying opener
                # will create the idempotent index on its next initialization.
                return
            raise
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def _ensure_work_fencing_column(self) -> None:
        """Add ``works.fencing_token`` to a ledger written before fencing.

        Idempotent: a fresh table already declares the column, and an existing
        ledger is migrated in place with ``DEFAULT 1`` so every projected row
        keeps a valid token. Existing ``events`` rows are untouched.
        """
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            columns = {
                str(row["name"])
                for row in self._conn.execute("PRAGMA table_info(works)").fetchall()
            }
            if FENCING_TOKEN_FIELD not in columns:
                self._conn.execute(
                    f"ALTER TABLE works ADD COLUMN {FENCING_TOKEN_FIELD} "
                    "INTEGER NOT NULL DEFAULT 1"
                )
            # Seed the durable high-water table when upgrading a ledger whose
            # work projection already contains fenced or legacy-token rows.
            self._conn.execute(
                "INSERT INTO work_fences(key, state, executor, fencing_token, "
                "lease_expires_at, updated_seq) "
                "SELECT key, state, executor, fencing_token, lease_expires_at, updated_seq "
                "FROM works WHERE 1 "
                "ON CONFLICT(key) DO UPDATE SET state = excluded.state, "
                "executor = excluded.executor, fencing_token = excluded.fencing_token, "
                "lease_expires_at = excluded.lease_expires_at, updated_seq = excluded.updated_seq "
                "WHERE excluded.fencing_token > work_fences.fencing_token "
                "OR (excluded.fencing_token = work_fences.fencing_token "
                "AND excluded.updated_seq > work_fences.updated_seq)"
            )
            self._conn.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            self._conn.execute("ROLLBACK")
            if "duplicate column name" in str(exc).lower():
                return
            raise
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------- meta helpers

    def _set_meta_if_absent(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO NOTHING",
            (key, value),
        )

    def meta(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return None if row is None else str(row["value"])

    def _set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def checkpoint(self) -> dict[str, Any]:
        """Return the prune checkpoint: events at or below it are gone."""
        return {
            "seq": int(self.meta("checkpoint_seq") or 0),
            "hash": self.meta("checkpoint_hash") or GENESIS_HASH,
        }

    # --------------------------------------------------------------------- read

    def _row_to_event(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "seq": int(row["seq"]),
            "kind": row["kind"],
            "topic": row["topic"],
            "producer": row["producer"],
            "ts": row["ts"],
            "schema_version": row["schema_version"],
            "causation_id": row["causation_id"],
            "root_causation_id": row["root_causation_id"],
            "correlation_id": row["correlation_id"],
            "payload": json.loads(row["payload"]),
            "prev_hash": row["prev_hash"],
            "hash": row["hash"],
        }

    def get(self, seq: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM events WHERE seq = ?", (int(seq),)).fetchone()
            return None if row is None else self._row_to_event(row)

    def last_seq(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COALESCE(MAX(seq), 0) AS m FROM events").fetchone()
            return int(row["m"])

    def count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()
            return int(row["c"])

    def ensure_genesis(self) -> dict[str, Any] | None:
        """Append the genesis note exactly once, when the ledger is empty.

        The emptiness check and the append share one store lock, so two
        callers can never both observe an empty ledger and write two genesis
        records. Returns the written event, or ``None`` when the ledger
        already holds records.
        """
        with self._lock:
            if self.count() != 0:
                return None
            return self.append(
                topic=GENESIS_TOPIC,
                producer=GENESIS_PRODUCER,
                payload=dict(GENESIS_PAYLOAD),
                kind="note",
            )

    def tail(self, limit: int = 50, *, order: str = "desc") -> list[dict[str, Any]]:
        if not 1 <= int(limit) <= 10_000:
            raise StoreError("invalid_limit", "limit must be between 1 and 10000")
        direction = "DESC" if order.lower() == "desc" else "ASC"
        with self._lock:
            rows = self._conn.execute(
                f"SELECT * FROM events ORDER BY seq {direction} LIMIT ?", (int(limit),)
            ).fetchall()
            return [self._row_to_event(row) for row in rows]

    def since(self, seq: int, limit: int = 500) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE seq > ? ORDER BY seq ASC LIMIT ?",
                (int(seq), int(limit)),
            ).fetchall()
            return [self._row_to_event(row) for row in rows]

    def query(
        self,
        *,
        kind: str | None = None,
        topic: str | None = None,
        producer: str | None = None,
        state: str | None = None,
        correlation_id: str | None = None,
        causation_id: int | None = None,
        root_causation_id: int | None = None,
        since_seq: int | None = None,
        until_seq: int | None = None,
        since_ts: str | None = None,
        until_ts: str | None = None,
        text: str | None = None,
        limit: int = 100,
        offset: int = 0,
        order: str = "asc",
    ) -> list[dict[str, Any]]:
        """Return events matching every supplied filter.

        Values are always bound as SQL parameters; only the static clause
        fragments and the sort direction are interpolated.
        """
        clauses: list[str] = []
        params: list[Any] = []
        if kind is not None:
            clauses.append("kind = ?")
            params.append(kind)
        if topic is not None:
            clauses.append("topic LIKE ? ESCAPE '\\'")
            params.append(_glob_to_like(topic))
        if producer is not None:
            clauses.append("producer = ?")
            params.append(producer)
        if correlation_id is not None:
            clauses.append("correlation_id = ?")
            params.append(correlation_id)
        if causation_id is not None:
            clauses.append("causation_id = ?")
            params.append(int(causation_id))
        if root_causation_id is not None:
            clauses.append("root_causation_id = ?")
            params.append(int(root_causation_id))
        if since_seq is not None:
            clauses.append("seq >= ?")
            params.append(int(since_seq))
        if until_seq is not None:
            clauses.append("seq <= ?")
            params.append(int(until_seq))
        if since_ts is not None:
            clauses.append("ts >= ?")
            params.append(since_ts)
        if until_ts is not None:
            clauses.append("ts <= ?")
            params.append(until_ts)
        if state is not None:
            clauses.append("json_extract(payload, '$.state') = ?")
            params.append(state)
        if text is not None:
            clauses.append("instr(payload, ?) > 0")
            params.append(text)
        limit = max(1, min(int(limit), 10_000))
        offset = max(0, int(offset))
        direction = "ASC" if order.lower() != "desc" else "DESC"
        sql = "SELECT * FROM events"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += f" ORDER BY seq {direction} LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        with self._lock:
            return [self._row_to_event(row) for row in self._conn.execute(sql, params).fetchall()]

    def counts(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT name, value FROM counters").fetchall()
            return {str(row["name"]): int(row["value"]) for row in rows}

    def _counter(self, name: str, delta: int) -> None:
        if delta == 0:
            return
        self._conn.execute(
            "INSERT INTO counters(name, value) VALUES(?, ?) "
            "ON CONFLICT(name) DO UPDATE SET value = value + excluded.value",
            (name, delta),
        )

    def status(self) -> dict[str, Any]:
        """Return an O(1) snapshot of ledger and plane state."""
        with self._lock:
            return self._status_locked()

    def _status_locked(self) -> dict[str, Any]:
        counters = self.counts()
        last = self._conn.execute(
            "SELECT seq, ts FROM events ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        checkpoint = self.checkpoint()
        signal = {
            state: counters.get(f"signal.{state}", 0)
            for state in ("pending", "claimed", "cleared", "expired")
        }
        work = {
            state: counters.get(f"work.{state}", 0)
            for state in ("claimed", "in_flight", "done", "failed", "abandoned", "expired")
        }
        work["active"] = work["claimed"] + work["in_flight"]
        work["terminal"] = work["done"] + work["failed"] + work["abandoned"] + work["expired"]
        kinds = {
            name[len("kind.") :]: value
            for name, value in counters.items()
            if name.startswith("kind.")
        }
        return {
            "ledger": {
                "path": str(self.path),
                "uuid": self.meta("ledger_uuid"),
                "format_version": int(self.meta("format_version") or 0),
                "records": counters.get("record", 0),
                "last_seq": int(last["seq"]) if last else 0,
                "last_write_at": last["ts"] if last else None,
                "checkpoint_seq": checkpoint["seq"],
                "size_bytes": self.path.stat().st_size if self.path.exists() else 0,
            },
            "planes": {
                "signal": {
                    "pending": signal["pending"],
                    "claimed": signal["claimed"],
                    "cleared": signal["cleared"],
                    "expired": signal["expired"],
                    "open": signal["pending"] + signal["claimed"],
                },
                "work": work,
            },
            "kinds": kinds,
            "integrity": {
                "hash_chain": self.meta("integrity_chain") or "unverified",
                "verified_at": self.meta("integrity_verified_at"),
                "verified_seq": int(self.meta("integrity_verified_seq") or 0),
            },
        }

    # ------------------------------------------------------------------ writes

    @staticmethod
    def _is_busy(exc: sqlite3.Error) -> bool:
        text = str(exc).lower()
        return "locked" in text or "busy" in text

    def _commit_with_retry(self, action) -> Any:
        """Run a write action, retrying while another writer holds the lock."""
        deadline = time.monotonic() + WRITE_RETRY_SECONDS
        delay = 0.01
        while True:
            try:
                return action()
            except sqlite3.OperationalError as exc:
                if not self._is_busy(exc) or time.monotonic() >= deadline:
                    raise StoreError("write_failed", f"ledger write failed: {exc}") from exc
                time.sleep(delay)
                delay = min(delay * 2, 0.25)

    def _next_seq(self) -> int:
        highest = self.last_seq()
        checkpoint_seq = int(self.meta("checkpoint_seq") or 0)
        return max(highest, checkpoint_seq) + 1

    def _build_body(
        self,
        *,
        seq: int,
        prev_hash: str,
        kind: str,
        topic: str,
        producer: str,
        ts: str,
        causation_id: int | None,
        root_causation_id: int | None,
        correlation_id: str | None,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        body = {
            "seq": seq,
            "prev_hash": prev_hash,
            "kind": kind,
            "topic": topic,
            "producer": producer,
            "ts": ts,
            "schema_version": SCHEMA_VERSION,
            "causation_id": causation_id,
            "correlation_id": correlation_id,
            "payload": payload,
        }
        # Legacy rows predate this envelope field. Omitting a null value keeps
        # their canonical bodies and hashes verifiable after the SQL migration,
        # while a supplied root remains cryptographically bound into new rows.
        if root_causation_id is not None:
            body["root_causation_id"] = root_causation_id
        return body

    def append(
        self,
        *,
        topic: str,
        producer: str,
        payload: Mapping[str, Any] | None = None,
        kind: str = "generic",
        causation_id: int | None = None,
        root_causation_id: int | None = None,
        correlation_id: str | None = None,
        ts: str | None = None,
    ) -> dict[str, Any]:
        """Validate, chain, and durably append one event."""
        return self.append_batch(
            [
                {
                    "topic": topic,
                    "producer": producer,
                    "payload": payload,
                    "kind": kind,
                    "causation_id": causation_id,
                    "root_causation_id": root_causation_id,
                    "correlation_id": correlation_id,
                    "ts": ts,
                }
            ]
        )[0]

    def append_batch(self, events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Append several events in one atomic transaction.

        Sequence numbers and hashes are assigned in order; either all events
        commit or none do.
        """
        if self._closed:
            raise StoreError("closed", "ledger is closed")
        items = list(events)
        if not items:
            return []
        if len(items) > MAX_BATCH:
            raise StoreError("batch_too_large", f"batch exceeds {MAX_BATCH} events")

        prepared: list[dict[str, Any]] = []
        for raw in items:
            for owned in STORE_OWNED_FIELDS:
                if owned in raw:
                    raise StoreError(
                        "store_owned_field",
                        f"'{owned}' is assigned by the ledger and must not be supplied",
                        {"field": owned},
                    )
            try:
                fields, payload_text = prepare_event(
                    topic=raw.get("topic"),
                    producer=raw.get("producer"),
                    payload=raw.get("payload"),
                    kind=raw.get("kind", "generic"),
                    causation_id=raw.get("causation_id"),
                    root_causation_id=raw.get("root_causation_id"),
                    correlation_id=raw.get("correlation_id"),
                )
            except ValidationError as exc:
                raise StoreError(exc.code, exc.message, exc.detail) from exc
            prepared.append({**fields, "ts": raw.get("ts"), "payload_text": payload_text})

        with self._lock:

            def _write() -> list[dict[str, Any]]:
                self._conn.execute("BEGIN IMMEDIATE")
                return self._append_prepared(prepared)

            return self._commit_with_retry(_write)

    def _append_prepared(self, prepared: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        """Write prepared items inside a transaction the caller already opened.

        The caller holds ``self._lock`` and has issued ``BEGIN IMMEDIATE``, so
        the sequence number, the head hash, the claim compare-and-set, and the
        canonical body hash all read one serialized snapshot. On any failure the
        transaction is rolled back: a rejected event neither lands nor moves a
        counter.
        """
        written: list[dict[str, Any]] = []
        deltas: dict[str, int] = {}
        try:
            seq = self._next_seq()
            prev = self._current_head_hash()
            for item in prepared:
                ts = item["ts"] or utc_now()
                if not isinstance(ts, str):
                    raise StoreError("invalid_ts", "ts must be an ISO-8601 string")
                validate_ts(ts)
                self._validate_links(seq, item)
                payload = item["payload"]
                payload_text = item["payload_text"]
                if item["kind"] == "work":
                    # Claim tokens are assigned here, after the write lock is
                    # held and before the body is hashed, so the exact token is
                    # part of the event's own canonical body and digest.
                    payload, payload_text = self._fence_work_payload(payload, payload_text)
                body = self._build_body(
                    seq=seq,
                    prev_hash=prev,
                    kind=item["kind"],
                    topic=item["topic"],
                    producer=item["producer"],
                    ts=ts,
                    causation_id=item["causation_id"],
                    root_causation_id=item["root_causation_id"],
                    correlation_id=item["correlation_id"],
                    payload=payload,
                )
                body_text = canonical_body_text(body, payload_text)
                digest = chain_digest(prev, body_text.encode("utf-8"))
                self._conn.execute(
                    "INSERT INTO events(seq, kind, topic, producer, ts, schema_version, "
                    "causation_id, root_causation_id, correlation_id, payload, body, prev_hash, hash) "
                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        seq,
                        item["kind"],
                        item["topic"],
                        item["producer"],
                        ts,
                        SCHEMA_VERSION,
                        item["causation_id"],
                        item["root_causation_id"],
                        item["correlation_id"],
                        payload_text,
                        body_text,
                        prev,
                        digest,
                    ),
                )
                event = {
                    "seq": seq,
                    "kind": item["kind"],
                    "topic": item["topic"],
                    "producer": item["producer"],
                    "ts": ts,
                    "schema_version": SCHEMA_VERSION,
                    "causation_id": item["causation_id"],
                    "root_causation_id": item["root_causation_id"],
                    "correlation_id": item["correlation_id"],
                    "payload": payload,
                    "prev_hash": prev,
                    "hash": digest,
                }
                self._project(event, deltas)
                written.append(event)
                prev = digest
                seq += 1
            self._apply_deltas(deltas)
            self._conn.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            self._conn.execute("ROLLBACK")
            if self._is_busy(exc):
                # Let the retry loop in _commit_with_retry try again.
                raise
            raise StoreError("write_failed", f"ledger write failed: {exc}") from exc
        except sqlite3.Error as exc:
            self._conn.execute("ROLLBACK")
            raise StoreError("write_failed", f"ledger write failed: {exc}") from exc
        except Exception:
            self._conn.execute("ROLLBACK")
            raise
        return written

    # ------------------------------------------------------------ work fencing

    def _fence_work_payload(
        self, payload: dict[str, Any], payload_text: str
    ) -> tuple[dict[str, Any], str]:
        """Fence one ``work`` payload and return ``(payload, canonical_text)``.

        A claim is a compare-and-set that the ledger tokenizes; any other state
        is a transition that must name the projected owner and token.
        """
        state = str(payload["state"])
        key = str(payload["key"])
        row = self._work_authority(key)
        trusted_now = datetime.now(timezone.utc)
        fenced = dict(payload)
        if state == "claimed":
            fenced[FENCING_TOKEN_FIELD] = self._claim_fencing_token(
                row, key, trusted_now
            )
        elif row is None or state not in FENCED_WORK_STATES:
            # Not a transition of a known record; projection reports it.
            return payload, payload_text
        else:
            projected_token = int(row[FENCING_TOKEN_FIELD])
            supplied = payload.get(FENCING_TOKEN_FIELD)
            if supplied is None:
                raise StoreError(
                    "fencing_token_mismatch",
                    f"work '{key}' transition to '{state}' must present its fencing_token",
                    {"key": key, "state": state, "reason": "missing", "expected": projected_token},
                )
            if str(payload["executor"]) != str(row["executor"]):
                raise StoreError(
                    "owner_mismatch",
                    f"work '{key}' is owned by '{row['executor']}', not '{payload['executor']}'",
                    {
                        "key": key,
                        "state": state,
                        "owner": str(row["executor"]),
                        "executor": str(payload["executor"]),
                    },
                )
            if int(supplied) != projected_token:
                raise StoreError(
                    "fencing_token_mismatch",
                    f"work '{key}' fencing token is {projected_token}, not {int(supplied)}",
                    {
                        "key": key,
                        "state": state,
                        "expected": projected_token,
                        "presented": int(supplied),
                    },
                )

        # Lease authority uses a store-generated wall-clock deadline. The
        # caller's envelope timestamp remains historical metadata and cannot
        # extend a lease or make an existing lease appear expired.
        if state in ("claimed", "in_flight") and payload.get("lease_seconds"):
            fenced[LEASE_EXPIRES_AT_FIELD] = (
                trusted_now + timedelta(seconds=int(payload["lease_seconds"]))
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        else:
            fenced.pop(LEASE_EXPIRES_AT_FIELD, None)
        return fenced, canonical_json(fenced)

    def _work_authority(self, key: str) -> sqlite3.Row | None:
        """Return live projection state or its durable pruned-history fence."""
        row = self._conn.execute(
            f"SELECT state, executor, {FENCING_TOKEN_FIELD}, lease_expires_at "
            "FROM works WHERE key = ?",
            (key,),
        ).fetchone()
        if row is not None:
            return row
        return self._conn.execute(
            f"SELECT state, executor, {FENCING_TOKEN_FIELD}, lease_expires_at "
            "FROM work_fences WHERE key = ?",
            (key,),
        ).fetchone()

    def _claim_fencing_token(self, row: sqlite3.Row | None, key: str, moment: datetime) -> int:
        """Return the token a claim should carry, or reject a live conflict."""
        if row is None:
            return 1
        previous = int(row[FENCING_TOKEN_FIELD])
        state = str(row["state"])
        if state in CLAIMABLE_WORK_STATES:
            return previous + 1
        if state in ("claimed", "in_flight"):
            expires = row["lease_expires_at"]
            if expires is not None and parse_ts(str(expires)) <= moment:
                return previous + 1
            raise StoreError(
                "claim_conflict",
                f"work '{key}' is already claimed by '{row['executor']}'",
                {
                    "key": key,
                    "state": state,
                    "executor": str(row["executor"]),
                    "lease_expires_at": None if expires is None else str(expires),
                    "fencing_token": previous,
                },
            )
        raise StoreError(
            "claim_conflict",
            f"work '{key}' is in terminal state '{state}' and cannot be reclaimed",
            {"key": key, "state": state, "fencing_token": previous},
        )

    def _current_head_hash(self) -> str:
        row = self._conn.execute(
            "SELECT hash FROM events ORDER BY seq DESC LIMIT 1"
        ).fetchone()
        if row is not None:
            return str(row["hash"])
        return self.checkpoint()["hash"]

    def _validate_links(self, seq: int, item: Mapping[str, Any]) -> None:
        causation = item["causation_id"]
        parent: sqlite3.Row | None = None
        if causation is not None:
            parent = self._conn.execute(
                "SELECT seq, kind, correlation_id, payload, root_causation_id "
                "FROM events WHERE seq = ?",
                (int(causation),),
            ).fetchone()
            if parent is None:
                raise StoreError(
                    "unknown_causation",
                    f"causation_id does not exist: {causation}",
                    {"causation_id": causation},
                )
            if item["kind"] == "signal" and parent["kind"] != "signal":
                raise StoreError(
                    "signal_parent_not_signal",
                    "a signal state change must name a signal as its causation",
                    {"causation_id": causation, "parent_kind": parent["kind"]},
                )
            if item["kind"] == "work":
                parent_payload = json.loads(parent["payload"])
                if parent["kind"] != "work" or parent_payload.get("key") != item["payload"].get("key"):
                    raise StoreError(
                        "work_parent_mismatch",
                        "a work state change must name a work event with the same key",
                        {"causation_id": causation, "parent_kind": parent["kind"]},
                    )

        root_causation = item.get("root_causation_id")
        if root_causation is None:
            return
        root = int(root_causation)
        if root >= seq:
            raise StoreError(
                "invalid_root_causation",
                "root_causation_id must reference an earlier event",
                {"root_causation_id": root, "seq": seq},
            )
        checkpoint_seq = int(self.meta("checkpoint_seq") or 0)
        if root > checkpoint_seq:
            root_row = self._conn.execute(
                "SELECT 1 FROM events WHERE seq = ?", (root,)
            ).fetchone()
            if root_row is None:
                raise StoreError(
                    "unknown_root_causation",
                    f"root_causation_id does not exist: {root}",
                    {"root_causation_id": root},
                )
        if parent is not None:
            expected = parent["root_causation_id"]
            expected_root = int(parent["seq"] if expected is None else expected)
            if root != expected_root:
                raise StoreError(
                    "root_causation_mismatch",
                    "root_causation_id must match the causation parent's root",
                    {"root_causation_id": root, "expected": expected_root},
                )

    # -------------------------------------------------------------- projections

    def _project(self, event: Mapping[str, Any], deltas: dict[str, int]) -> None:
        """Update derived tables for one appended event, inside the transaction.

        Counter changes are accumulated in ``deltas`` and applied once per
        batch, so a 500-event append costs one counter write instead of 1000.
        """
        self._bump(deltas, "record", 1)
        self._bump(deltas, f"kind.{event['kind']}", 1)

        if event["kind"] == "signal":
            self._project_signal(event, deltas)
        elif event["kind"] == "work":
            self._project_work(event, deltas)

    @staticmethod
    def _bump(deltas: dict[str, int], name: str, delta: int) -> None:
        if delta:
            deltas[name] = deltas.get(name, 0) + delta

    def _apply_deltas(self, deltas: Mapping[str, int]) -> None:
        for name, delta in deltas.items():
            self._counter(name, delta)

    def _project_signal(self, event: Mapping[str, Any], deltas: dict[str, int]) -> None:
        causation = event["causation_id"]
        if causation is None:
            root = int(event["seq"])
        else:
            row = self._conn.execute(
                "SELECT root_seq FROM signal_roots WHERE seq = ?", (int(causation),)
            ).fetchone()
            if row is None:
                raise StoreError(
                    "signal_parent_not_signal",
                    "signal chain parent is not a signal event",
                    {"causation_id": causation},
                )
            root = int(row["root_seq"])

        state = str(event["payload"].get("state", "pending"))
        previous_row = self._conn.execute(
            "SELECT state FROM signals WHERE root_seq = ?", (root,)
        ).fetchone()
        previous_state = None if previous_row is None else str(previous_row["state"])

        if previous_state is None and state != "pending":
            raise StoreError(
                "invalid_transition",
                "a signal chain must begin in the pending state",
                {"state": state},
            )
        if previous_state is not None:
            allowed = _SIGNAL_TRANSITIONS.get(previous_state, set())
            if state not in allowed:
                raise StoreError(
                    "invalid_transition",
                    f"signal cannot move from '{previous_state}' to '{state}'",
                    {"from": previous_state, "to": state, "allowed": sorted(allowed)},
                )
            self._bump(deltas, f"signal.{previous_state}", -1)
        else:
            # A brand-new chain also needs a record of its own seq as root.
            pass

        self._conn.execute(
            "INSERT INTO signal_roots(seq, root_seq) VALUES(?, ?) "
            "ON CONFLICT(seq) DO UPDATE SET root_seq = excluded.root_seq",
            (int(event["seq"]), root),
        )
        self._conn.execute(
            "INSERT INTO signals(root_seq, state, topic, producer, updated_seq) VALUES(?, ?, ?, ?, ?) "
            "ON CONFLICT(root_seq) DO UPDATE SET state = excluded.state, "
            "topic = excluded.topic, producer = excluded.producer, updated_seq = excluded.updated_seq",
            (
                root,
                state,
                event["topic"],
                event["producer"],
                int(event["seq"]),
            ),
        )
        self._bump(deltas, f"signal.{state}", 1)

    def _project_work(self, event: Mapping[str, Any], deltas: dict[str, int]) -> None:
        payload = event["payload"]
        key = str(payload["key"])
        state = str(payload["state"])
        row = self._conn.execute(
            f"SELECT state, {FENCING_TOKEN_FIELD} FROM works WHERE key = ?", (key,)
        ).fetchone()
        previous_state = None if row is None else str(row["state"])

        if previous_state is None and state != "claimed":
            raise StoreError(
                "invalid_transition",
                "a work record must begin in the claimed state",
                {"key": key, "state": state},
            )
        if previous_state is not None:
            if state != "claimed":
                # A claim is authorized by the atomic compare-and-set in
                # _claim_fencing_token, which is the only path allowed to
                # reclaim a lapsed, abandoned, or terminal record. Every other
                # transition must be legal for the projected state.
                allowed = _WORK_TRANSITIONS.get(previous_state, set())
                if state not in allowed:
                    raise StoreError(
                        "invalid_transition",
                        f"work '{key}' cannot move from '{previous_state}' to '{state}'",
                        {"key": key, "from": previous_state, "to": state, "allowed": sorted(allowed)},
                    )
            self._bump(deltas, f"work.{previous_state}", -1)

        token = payload.get(FENCING_TOKEN_FIELD)
        if isinstance(token, bool) or not isinstance(token, int) or token <= 0:
            # A legacy event without a token keeps the projected token, or
            # starts the key at token 1.
            token = int(row[FENCING_TOKEN_FIELD]) if row is not None else 1

        lease_expires_at = payload.get(LEASE_EXPIRES_AT_FIELD)
        if lease_expires_at is not None:
            lease_expires_at = str(lease_expires_at)

        self._conn.execute(
            "INSERT INTO works(key, state, executor, topic, correlation_id, updated_seq, "
            "lease_expires_at, title, fencing_token) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET state = excluded.state, executor = excluded.executor, "
            "topic = excluded.topic, correlation_id = excluded.correlation_id, "
            "updated_seq = excluded.updated_seq, lease_expires_at = excluded.lease_expires_at, "
            "title = excluded.title, fencing_token = excluded.fencing_token",
            (
                key,
                state,
                str(payload["executor"]),
                event["topic"],
                event["correlation_id"],
                int(event["seq"]),
                lease_expires_at,
                payload.get("title"),
                int(token),
            ),
        )
        self._remember_work_fence(event, int(token), lease_expires_at)
        self._bump(deltas, f"work.{state}", 1)

    def _remember_work_fence(
        self,
        event: Mapping[str, Any],
        token: int,
        lease_expires_at: str | None,
    ) -> None:
        """Advance the per-key fence independently of prunable projections."""
        payload = event["payload"]
        self._conn.execute(
            "INSERT INTO work_fences(key, state, executor, fencing_token, "
            "lease_expires_at, updated_seq) VALUES(?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET state = excluded.state, "
            "executor = excluded.executor, fencing_token = excluded.fencing_token, "
            "lease_expires_at = excluded.lease_expires_at, updated_seq = excluded.updated_seq "
            "WHERE excluded.fencing_token > work_fences.fencing_token "
            "OR (excluded.fencing_token = work_fences.fencing_token "
            "AND excluded.updated_seq > work_fences.updated_seq)",
            (
                str(payload["key"]),
                str(payload["state"]),
                str(payload["executor"]),
                int(token),
                lease_expires_at,
                int(event["seq"]),
            ),
        )

    def rebuild_projections(self) -> dict[str, int]:
        """Recompute every derived table from ``events``.

        Used after pruning and as a repair path. Transition validation is not
        re-applied: rebuild must succeed even for a ledger assembled by an
        older writer.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute("DELETE FROM counters")
                self._conn.execute("DELETE FROM signal_roots")
                self._conn.execute("DELETE FROM signals")
                self._conn.execute("DELETE FROM works")
                deltas: dict[str, int] = {}
                for event in self.query(limit=10_000_000, order="asc"):
                    self._replay_event(event, deltas)
                self._apply_deltas(deltas)
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return self.counts()

    def _replay_event(self, event: Mapping[str, Any], deltas: dict[str, int]) -> None:
        """Projection without transition enforcement, for rebuilds."""
        self._bump(deltas, "record", 1)
        self._bump(deltas, f"kind.{event['kind']}", 1)
        if event["kind"] == "signal":
            causation = event["causation_id"]
            if causation is None:
                root = int(event["seq"])
            else:
                row = self._conn.execute(
                    "SELECT root_seq FROM signal_roots WHERE seq = ?", (int(causation),)
                ).fetchone()
                root = int(row["root_seq"]) if row else int(event["seq"])
            state = str(event["payload"].get("state", "pending"))
            previous = self._conn.execute(
                "SELECT state FROM signals WHERE root_seq = ?", (root,)
            ).fetchone()
            if previous is not None:
                self._bump(deltas, f"signal.{previous['state']}", -1)
            self._conn.execute(
                "INSERT INTO signal_roots(seq, root_seq) VALUES(?, ?) "
                "ON CONFLICT(seq) DO UPDATE SET root_seq = excluded.root_seq",
                (int(event["seq"]), root),
            )
            self._conn.execute(
                "INSERT INTO signals(root_seq, state, topic, producer, updated_seq) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(root_seq) DO UPDATE SET state = excluded.state, "
                "topic = excluded.topic, producer = excluded.producer, updated_seq = excluded.updated_seq",
                (root, state, event["topic"], event["producer"], int(event["seq"])),
            )
            self._bump(deltas, f"signal.{state}", 1)
        elif event["kind"] == "work":
            payload = event["payload"]
            key = str(payload.get("key", "unknown"))
            state = str(payload.get("state", "claimed"))
            previous = self._conn.execute(
                f"SELECT state, {FENCING_TOKEN_FIELD} FROM works WHERE key = ?", (key,)
            ).fetchone()
            if previous is not None:
                self._bump(deltas, f"work.{previous['state']}", -1)
            token = payload.get(FENCING_TOKEN_FIELD)
            if isinstance(token, bool) or not isinstance(token, int) or token <= 0:
                # A pre-fencing event replays as the current projected token,
                # which is 1 until a later token-bearing claim advances it.
                token = int(previous[FENCING_TOKEN_FIELD]) if previous is not None else 1
            lease_expires_at = payload.get(LEASE_EXPIRES_AT_FIELD)
            if lease_expires_at is not None:
                lease_expires_at = str(lease_expires_at)
            elif state in ("claimed", "in_flight") and payload.get("lease_seconds"):
                # Compatibility for pre-0.3 rows, whose trusted deadline was
                # derived from the only timestamp then stored in the event.
                lease_expires_at = (
                    parse_ts(str(event["ts"])) + timedelta(seconds=int(payload["lease_seconds"]))
                ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            self._conn.execute(
                "INSERT INTO works(key, state, executor, topic, correlation_id, updated_seq, "
                "lease_expires_at, title, fencing_token) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET state = excluded.state, executor = excluded.executor, "
                "topic = excluded.topic, correlation_id = excluded.correlation_id, "
                "updated_seq = excluded.updated_seq, lease_expires_at = excluded.lease_expires_at, "
                "title = excluded.title, fencing_token = excluded.fencing_token",
                (
                    key,
                    state,
                    str(payload.get("executor", "unknown")),
                    event["topic"],
                    event["correlation_id"],
                    int(event["seq"]),
                    lease_expires_at,
                    payload.get("title"),
                    int(token),
                ),
            )
            self._remember_work_fence(event, int(token), lease_expires_at)
            self._bump(deltas, f"work.{state}", 1)

    # ------------------------------------------------------------------- leases

    def list_work(self, *, state: str | None = None, executor: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if state is not None:
            clauses.append("state = ?")
            params.append(state)
        if executor is not None:
            clauses.append("executor = ?")
            params.append(executor)
        sql = "SELECT * FROM works"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_seq ASC"
        with self._lock:
            return [dict(row) for row in self._conn.execute(sql, params).fetchall()]

    def expired_leases(self, *, now: str | None = None) -> list[dict[str, Any]]:
        """Return active work rows whose lease has already expired."""
        moment = parse_ts(now) if now else datetime.now(timezone.utc)
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM works WHERE state IN ('claimed', 'in_flight') "
                "AND lease_expires_at IS NOT NULL ORDER BY updated_seq ASC"
            ).fetchall()
        expired: list[dict[str, Any]] = []
        for row in rows:
            expires = parse_ts(str(row["lease_expires_at"]))
            if expires <= moment:
                item = dict(row)
                item["expired_at"] = moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")
                expired.append(item)
        return expired

    def reap_expired_leases(self, *, now: str | None = None) -> list[dict[str, Any]]:
        """Append one ``expired`` work event per lapsed lease, atomically.

        The eligible rows are selected and re-checked inside a single
        ``BEGIN IMMEDIATE`` transaction, so a heartbeat committed before the
        reap is honoured and one arriving during the reap waits for the batch
        to land. Every ``expired`` event is written in one all-or-nothing batch
        in deterministic ``updated_seq ASC`` order, carrying the current
        ``fencing_token`` and the causal ``updated_seq``. Reaping is recorded in
        the log rather than applied as a silent projection edit, so projections
        stay reproducible from events.
        """
        if self._closed:
            raise StoreError("closed", "ledger is closed")
        moment = parse_ts(now) if now else datetime.now(timezone.utc)
        stamped = moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")

        with self._lock:

            def _reap() -> list[dict[str, Any]]:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    prepared = self._prepare_expired_reap(moment, stamped)
                except Exception:
                    self._conn.execute("ROLLBACK")
                    raise
                if not prepared:
                    self._conn.execute("COMMIT")
                    return []
                return self._append_prepared(prepared)

            return self._commit_with_retry(_reap)

    def _prepare_expired_reap(self, moment: datetime, stamped: str) -> list[dict[str, Any]]:
        """Snapshot and re-check lapsed leases inside the open transaction."""
        rows = self._conn.execute(
            "SELECT * FROM works WHERE state IN ('claimed', 'in_flight') "
            "AND lease_expires_at IS NOT NULL ORDER BY updated_seq ASC"
        ).fetchall()
        prepared: list[dict[str, Any]] = []
        for row in rows:
            if parse_ts(str(row["lease_expires_at"])) > moment:
                continue
            payload = {
                "key": str(row["key"]),
                "executor": str(row["executor"]),
                "state": "expired",
                "reason": f"lease expired at {stamped}",
                FENCING_TOKEN_FIELD: int(row[FENCING_TOKEN_FIELD]),
            }
            try:
                fields, payload_text = prepare_event(
                    topic=row["topic"],
                    producer=REAP_PRODUCER,
                    payload=payload,
                    kind="work",
                    causation_id=int(row["updated_seq"]),
                    correlation_id=row["correlation_id"],
                )
            except ValidationError as exc:
                raise StoreError(exc.code, exc.message, exc.detail) from exc
            prepared.append({**fields, "ts": stamped, "payload_text": payload_text})
        return prepared

    # --------------------------------------------------------------- integrity

    def _body_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return self._build_body(
            seq=int(row["seq"]),
            prev_hash=str(row["prev_hash"]),
            kind=str(row["kind"]),
            topic=str(row["topic"]),
            producer=str(row["producer"]),
            ts=str(row["ts"]),
            causation_id=row["causation_id"],
            root_causation_id=row["root_causation_id"],
            correlation_id=row["correlation_id"],
            payload=json.loads(row["payload"]),
        )

    def verify(self, *, record: bool = True) -> dict[str, Any]:
        """Recompute the hash chain over the retained ledger.

        Returns a report; ``ok`` is false if any stored row does not match its
        own recomputed digest, if the chain is broken, or if retained sequence
        numbers are missing.
        """
        checkpoint = self.checkpoint()
        expected_prev = checkpoint["hash"]
        expected_seq = checkpoint["seq"] + 1
        checked = 0
        gaps: list[dict[str, int]] = []
        divergence: dict[str, Any] | None = None

        with self._lock:
            rows = self._conn.execute("SELECT * FROM events ORDER BY seq ASC").fetchall()
        for row in rows:
            seq = int(row["seq"])
            if seq != expected_seq:
                gaps.append({"expected_seq": expected_seq, "found_seq": seq})
                expected_seq = seq
            stored_prev = str(row["prev_hash"])
            if stored_prev != expected_prev and divergence is None:
                divergence = {
                    "seq": seq,
                    "reason": "prev_hash_mismatch",
                    "expected": expected_prev,
                    "found": stored_prev,
                }
            body = self._body_from_row(row)
            recomputed = event_digest(stored_prev, body)
            if recomputed != str(row["hash"]) and divergence is None:
                divergence = {
                    "seq": seq,
                    "reason": "hash_mismatch",
                    "expected": recomputed,
                    "found": str(row["hash"]),
                }
            if str(row["body"]) != canonical_json(body) and divergence is None:
                divergence = {"seq": seq, "reason": "body_column_mismatch"}
            expected_prev = str(row["hash"])
            expected_seq = seq + 1
            checked += 1

        report = {
            "ok": divergence is None and not gaps,
            "events_checked": checked,
            "first_seq": int(rows[0]["seq"]) if rows else None,
            "last_seq": int(rows[-1]["seq"]) if rows else None,
            "checkpoint": checkpoint,
            "head_hash": expected_prev,
            "gaps": gaps,
            "divergence": divergence,
        }
        if record:
            with self._lock:
                self._conn.execute("BEGIN IMMEDIATE")
                try:
                    self._set_meta("integrity_chain", "verified" if report["ok"] else "failed")
                    self._set_meta("integrity_verified_at", utc_now())
                    self._set_meta("integrity_verified_seq", str(report["last_seq"] or 0))
                    self._conn.execute("COMMIT")
                except sqlite3.Error:
                    self._conn.execute("ROLLBACK")
                    raise
        return report

    # ------------------------------------------------------------------ pruning

    def prune_preview(
        self, *, before: str | None = None, keep_last: int | None = None
    ) -> dict[str, Any]:
        """Compute, without applying, which events a prune would remove."""
        checkpoint = self.checkpoint()
        highest = self.last_seq()
        cutoff = checkpoint["seq"]
        if keep_last is not None:
            if int(keep_last) < 0:
                raise StoreError("invalid_keep_last", "keep_last must be >= 0")
            cutoff = max(cutoff, highest - int(keep_last))
        if before is not None:
            parse_ts(before)
        with self._lock:
            if before is not None:
                row = self._conn.execute(
                    "SELECT COALESCE(MAX(seq), ?) AS m FROM events WHERE ts < ?",
                    (checkpoint["seq"], before),
                ).fetchone()
                cutoff = max(cutoff, int(row["m"]))
            cutoff = min(cutoff, highest)
            remaining = self._conn.execute(
                "SELECT COUNT(*) AS c FROM events WHERE seq > ?", (cutoff,)
            ).fetchone()
            removable = self._conn.execute(
                "SELECT COUNT(*) AS c FROM events WHERE seq <= ?", (cutoff,)
            ).fetchone()
        return {
            "cutoff_seq": cutoff,
            "removable": int(removable["c"]),
            "remaining": int(remaining["c"]),
            "current_checkpoint_seq": checkpoint["seq"],
        }

    def prune(
        self,
        *,
        before: str | None = None,
        keep_last: int | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Delete events at or below a cutoff, after recording a checkpoint."""
        if before is None and keep_last is None:
            raise StoreError(
                "prune_scope_required",
                "prune requires --before or --keep-last so the removal is explicit",
            )
        plan = self.prune_preview(before=before, keep_last=keep_last)
        cutoff = plan["cutoff_seq"]
        if cutoff <= plan["current_checkpoint_seq"]:
            result = {**plan, "applied": False, "reason": "nothing_new_to_prune"}
            if dry_run:
                result["dry_run"] = True
            return result
        if dry_run:
            return {**plan, "applied": False, "dry_run": True}

        with self._lock:
            row = self._conn.execute("SELECT hash FROM events WHERE seq = ?", (cutoff,)).fetchone()
        if row is None:
            raise StoreError("prune_cutoff_missing", f"no event at cutoff seq {cutoff}")
        head_hash = str(row["hash"])
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute("DELETE FROM events WHERE seq <= ?", (cutoff,))
                self._conn.execute("DELETE FROM signal_roots WHERE seq <= ?", (cutoff,))
                self._conn.execute("DELETE FROM signals WHERE updated_seq <= ?", (cutoff,))
                self._conn.execute("DELETE FROM works WHERE updated_seq <= ?", (cutoff,))
                self._set_meta("checkpoint_seq", str(cutoff))
                self._set_meta("checkpoint_hash", head_hash)
                self._set_meta("integrity_chain", "unverified")
                self._set_meta("integrity_verified_at", utc_now())
                self._set_meta("integrity_verified_seq", "0")
                self._conn.execute("COMMIT")
            except sqlite3.Error:
                self._conn.execute("ROLLBACK")
                raise
        self.rebuild_projections()
        return {
            **self.prune_preview(before=None, keep_last=0),
            "applied": True,
            "removed": plan["removable"],
            "cutoff_seq": cutoff,
            "checkpoint_hash": head_hash,
        }

    # ---------------------------------------------------------------- lifecycle

    def vacuum(self) -> None:
        with self._lock:
            self._conn.execute("VACUUM")

    def close(self) -> None:
        if self._closed:
            return
        with self._lock:
            self._conn.close()
            self._closed = True

    def __enter__(self) -> "LedgerStore":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def validate_ts(value: str) -> None:
    """Raise :class:`StoreError` unless ``value`` is a parseable ISO timestamp."""
    try:
        parse_ts(value)
    except (ValueError, TypeError) as exc:
        raise StoreError("invalid_ts", f"invalid ISO-8601 timestamp: {value!r}", {"ts": value}) from exc


def work_lease_seconds(payload: Mapping[str, Any]) -> int | None:
    seconds = payload.get("lease_seconds")
    return int(seconds) if isinstance(seconds, int) and not isinstance(seconds, bool) else None


def iter_kinds(events: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    """Count events by kind; a small helper used by the CLI summary output."""
    totals: dict[str, int] = {}
    for event in events:
        totals[str(event["kind"])] = totals.get(str(event["kind"]), 0) + 1
    return totals


def is_terminal_signal(state: str) -> bool:
    return state in SIGNAL_TERMINAL_STATES

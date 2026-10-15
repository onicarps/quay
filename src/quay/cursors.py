"""Durable named-consumer cursors for lossless runner delivery."""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Iterator, Mapping

from .store import StoreError, utc_now

CURSORS_NAME = "cursors.json"
CURSOR_LOCK_NAME = "cursors.lock"
_CONSUMER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@+-]{0,99}$")

__all__ = ["CursorStore", "CURSORS_NAME"]


class CursorStore:
    """Atomic JSON persistence for one-outstanding named consumers."""

    def __init__(self, state_root: str | os.PathLike[str]) -> None:
        self.root = Path(state_root)
        self.path = self.root / CURSORS_NAME
        self.lock_path = self.root / CURSOR_LOCK_NAME

    @staticmethod
    def _consumer(value: Any) -> str:
        if not isinstance(value, str) or _CONSUMER_PATTERN.fullmatch(value) is None:
            raise StoreError("invalid_consumer", "consumer must be a bounded identifier")
        return value

    @staticmethod
    def _positive_seq(value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise StoreError("invalid_cursor_seq", "cursor seq must be a positive integer")
        return value

    @contextlib.contextmanager
    def _locked(self, *, exclusive: bool) -> Iterator[None]:
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _load_unlocked(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        if self.path.is_symlink():
            raise StoreError("unsafe_cursor_path", f"refusing symlinked cursor file: {self.path}")
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise StoreError("invalid_cursor_file", f"cannot read {self.path}: {exc}") from exc
        if not isinstance(value, dict) or not all(
            isinstance(name, str) and isinstance(row, dict) for name, row in value.items()
        ):
            raise StoreError("invalid_cursor_file", "cursors.json must contain an object")
        rows: dict[str, dict[str, Any]] = {}
        for name, row in value.items():
            if _CONSUMER_PATTERN.fullmatch(name) is None:
                raise StoreError(
                    "invalid_cursor_file", f"invalid consumer in cursors.json: {name!r}"
                )
            last_acked = row.get("last_acked_seq", 0)
            if (
                isinstance(last_acked, bool)
                or not isinstance(last_acked, int)
                or last_acked < 0
            ):
                raise StoreError(
                    "invalid_cursor_file",
                    f"cursor {name!r} has an invalid last_acked_seq",
                )
            in_flight = row.get("in_flight")
            if in_flight is not None:
                if not isinstance(in_flight, dict):
                    raise StoreError(
                        "invalid_cursor_file",
                        f"cursor {name!r} has an invalid in_flight record",
                    )
                sequence = in_flight.get("seq")
                idempotency_key = in_flight.get("idempotency_key")
                delivered_at = in_flight.get("delivered_at")
                if (
                    isinstance(sequence, bool)
                    or not isinstance(sequence, int)
                    or sequence <= last_acked
                    or not isinstance(idempotency_key, str)
                    or not idempotency_key.strip()
                    or not isinstance(delivered_at, str)
                    or not delivered_at.strip()
                ):
                    raise StoreError(
                        "invalid_cursor_file",
                        f"cursor {name!r} has a malformed in_flight record",
                    )
            rows[name] = {
                "last_acked_seq": last_acked,
                "in_flight": None if in_flight is None else dict(in_flight),
            }
        return rows

    def _write_unlocked(self, rows: Mapping[str, Mapping[str, Any]]) -> None:
        if self.path.exists() and self.path.is_symlink():
            raise StoreError("unsafe_cursor_path", f"refusing symlinked cursor file: {self.path}")
        fd, temporary = tempfile.mkstemp(prefix=".cursors.", suffix=".tmp", dir=self.root)
        owned_fd: int | None = fd
        try:
            os.fchmod(fd, 0o600)
            stream = os.fdopen(fd, "w", encoding="utf-8", closefd=True)
            owned_fd = None
            with stream:
                json.dump(rows, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            directory_fd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except Exception:
            if owned_fd is not None:
                with contextlib.suppress(OSError):
                    os.close(owned_fd)
            with contextlib.suppress(OSError):
                os.unlink(temporary)
            raise

    def snapshot(self) -> dict[str, dict[str, Any]]:
        with self._locked(exclusive=False):
            return self._load_unlocked()

    def get(self, consumer: str) -> dict[str, Any]:
        name = self._consumer(consumer)
        row = self.snapshot().get(name, {})
        return {
            "last_acked_seq": int(row.get("last_acked_seq", 0)),
            "in_flight": row.get("in_flight"),
        }

    def register(self, consumer: str) -> dict[str, Any]:
        """Persist a durable consumer at sequence zero if it is not registered."""
        name = self._consumer(consumer)
        with self._locked(exclusive=True):
            rows = self._load_unlocked()
            current = rows.get(name)
            if current is None:
                current = {"last_acked_seq": 0, "in_flight": None}
                rows[name] = current
                self._write_unlocked(rows)
            return {
                "last_acked_seq": int(current.get("last_acked_seq", 0)),
                "in_flight": current.get("in_flight"),
            }

    def begin_delivery(
        self,
        consumer: str,
        seq: int,
        idempotency_key: str,
        *,
        delivered_at: str | None = None,
    ) -> dict[str, Any]:
        name = self._consumer(consumer)
        sequence = self._positive_seq(seq)
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise StoreError("invalid_idempotency_key", "idempotency_key must be non-empty")
        with self._locked(exclusive=True):
            rows = self._load_unlocked()
            current = dict(rows.get(name, {}))
            last_acked = int(current.get("last_acked_seq", 0))
            if sequence <= last_acked:
                raise StoreError(
                    "cursor_already_acked",
                    f"seq {sequence} is already acknowledged by {name}",
                    {"last_acked_seq": last_acked},
                )
            in_flight = current.get("in_flight")
            if in_flight is not None:
                if (
                    int(in_flight.get("seq", 0)) == sequence
                    and in_flight.get("idempotency_key") == idempotency_key
                ):
                    return {"last_acked_seq": last_acked, "in_flight": dict(in_flight)}
                raise StoreError(
                    "cursor_in_flight",
                    f"consumer {name} already has an outstanding delivery",
                    {"in_flight": in_flight},
                )
            current = {
                "last_acked_seq": last_acked,
                "in_flight": {
                    "seq": sequence,
                    "idempotency_key": idempotency_key,
                    "delivered_at": delivered_at or utc_now(),
                },
            }
            rows[name] = current
            self._write_unlocked(rows)
            return current

    def acknowledge(
        self, consumer: str, seq: int, *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        name = self._consumer(consumer)
        sequence = self._positive_seq(seq)
        with self._locked(exclusive=True):
            rows = self._load_unlocked()
            current = dict(rows.get(name, {}))
            last_acked = int(current.get("last_acked_seq", 0))
            in_flight = current.get("in_flight")
            if in_flight is None:
                if sequence <= last_acked:
                    return {"last_acked_seq": last_acked, "in_flight": None}
                raise StoreError("cursor_not_in_flight", f"consumer {name} has no delivery to ack")
            if int(in_flight.get("seq", 0)) != sequence:
                raise StoreError(
                    "cursor_ack_mismatch",
                    "ack seq does not match the outstanding delivery",
                    {"expected": in_flight.get("seq"), "actual": sequence},
                )
            if idempotency_key is not None and in_flight.get("idempotency_key") != idempotency_key:
                raise StoreError(
                    "cursor_ack_mismatch",
                    "ack idempotency key does not match the outstanding delivery",
                )
            result = {"last_acked_seq": max(last_acked, sequence), "in_flight": None}
            rows[name] = result
            self._write_unlocked(rows)
            return result

    def minimum_acked_seq(self) -> int | None:
        rows = self.snapshot()
        if not rows:
            return None
        return min(int(row.get("last_acked_seq", 0)) for row in rows.values())

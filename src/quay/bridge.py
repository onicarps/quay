"""Lossless one-outstanding wake spool for headless runners.

The bridge deliberately separates durable delivery state from attention files:
``CursorStore`` owns the acknowledged high-water mark and in-flight event,
while ``wake/<runner>/<seq>.json`` is a replaceable delivery hint.  A cursor is
advanced only after a companion acknowledgement has been persisted.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from .client import QuayClient
from .cursors import CursorStore
from .store import LedgerStore, StoreError, utc_now
from .workspace import LedgerPaths, paths_for

DEFAULT_ACK_TIMEOUT_SECONDS = 60 * 60

_SUCCESS = frozenset({"ack", "completed", "done", "ok", "success", "succeeded"})
_ERROR = frozenset({"error", "failed", "failure", "runner_error"})

__all__ = ["WakeBridge", "DEFAULT_ACK_TIMEOUT_SECONDS"]


def _private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise StoreError("unsafe_bridge_path", f"refusing unsafe bridge directory: {path}")
    with contextlib.suppress(OSError):
        os.chmod(path, 0o700)


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    _private_directory(path.parent)
    if path.exists() and path.is_symlink():
        raise StoreError("unsafe_bridge_path", f"refusing symlinked bridge file: {path}")
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    owned_fd: int | None = fd
    try:
        os.fchmod(fd, 0o600)
        stream = os.fdopen(fd, "w", encoding="utf-8", closefd=True)
        owned_fd = None
        with stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except Exception:
        if owned_fd is not None:
            with contextlib.suppress(OSError):
                os.close(owned_fd)
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise


def _safe_unlink(path: Path) -> None:
    if not path.exists():
        return
    if path.is_symlink() or not path.is_file():
        raise StoreError("unsafe_bridge_path", f"refusing unsafe bridge file: {path}")
    path.unlink()
    _fsync_directory(path.parent)


def _parse_time(value: str) -> float:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


class _RetryState:
    """Small locked sidecar for retry accounting without widening cursor rows."""

    def __init__(self, root: Path, runner: str) -> None:
        self.root = root
        self.path = root / f"bridge.{runner}.json"
        self.lock_path = root / f"bridge.{runner}.lock"

    @contextlib.contextmanager
    def _locked(self):
        _private_directory(self.root)
        fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        if self.path.is_symlink():
            raise StoreError("unsafe_bridge_path", f"refusing symlinked retry state: {self.path}")
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise StoreError("invalid_bridge_state", f"cannot read {self.path}: {exc}") from exc
        if not isinstance(value, dict):
            raise StoreError("invalid_bridge_state", "bridge retry state must be an object")
        return value

    @staticmethod
    def _for_seq(value: Mapping[str, Any], seq: int) -> dict[str, Any]:
        if value.get("seq") != seq:
            return {
                "seq": seq,
                "attempts": 1,
                "failures": 0,
                "pending_failure": None,
                "delivered_at": utc_now(),
                "written": False,
                "ledger_ack_seq": 0,
                "delivery_ledger_seq": 0,
            }
        attempts = value.get("attempts", 1)
        failures = value.get("failures", 0)
        pending = value.get("pending_failure")
        delivered_at = value.get("delivered_at")
        written = value.get("written", False)
        ledger_ack_seq = value.get("ledger_ack_seq", 0)
        delivery_ledger_seq = value.get("delivery_ledger_seq", 0)
        if (
            isinstance(attempts, bool)
            or not isinstance(attempts, int)
            or attempts < 1
            or isinstance(failures, bool)
            or not isinstance(failures, int)
            or failures < 0
            or (pending is not None and not isinstance(pending, str))
            or not isinstance(delivered_at, str)
            or not delivered_at.strip()
            or not isinstance(written, bool)
            or isinstance(ledger_ack_seq, bool)
            or not isinstance(ledger_ack_seq, int)
            or ledger_ack_seq < 0
            or isinstance(delivery_ledger_seq, bool)
            or not isinstance(delivery_ledger_seq, int)
            or delivery_ledger_seq < 0
        ):
            raise StoreError("invalid_bridge_state", "bridge retry counters are malformed")
        return {
            "seq": seq,
            "attempts": attempts,
            "failures": failures,
            "pending_failure": pending,
            "delivered_at": delivered_at,
            "written": written,
            "ledger_ack_seq": ledger_ack_seq,
            "delivery_ledger_seq": delivery_ledger_seq,
        }

    def get(self, seq: int) -> dict[str, Any]:
        with self._locked():
            return self._for_seq(self._read(), seq)

    def contains(self, seq: int) -> bool:
        with self._locked():
            return self._read().get("seq") == seq

    def initialize(
        self, seq: int, *, delivery_ledger_seq: int = 0
    ) -> dict[str, Any]:
        with self._locked():
            current = self._for_seq(self._read(), seq)
            if current["delivery_ledger_seq"] == 0:
                current["delivery_ledger_seq"] = delivery_ledger_seq
            _atomic_json(self.path, current)
            return current

    def fail(
        self,
        seq: int,
        token: str,
        *,
        ledger_ack_seq: int | None = None,
        delivery_ledger_seq: int | None = None,
    ) -> dict[str, Any]:
        with self._locked():
            current = self._for_seq(self._read(), seq)
            changed = False
            if current["pending_failure"] != token:
                current["failures"] += 1
                current["pending_failure"] = token
                changed = True
            if ledger_ack_seq is not None and ledger_ack_seq > current["ledger_ack_seq"]:
                current["ledger_ack_seq"] = ledger_ack_seq
                changed = True
            if (
                delivery_ledger_seq is not None
                and delivery_ledger_seq > current["delivery_ledger_seq"]
            ):
                current["delivery_ledger_seq"] = delivery_ledger_seq
                changed = True
            if changed:
                _atomic_json(self.path, current)
            return current

    def mark_written(self, seq: int) -> dict[str, Any]:
        with self._locked():
            current = self._for_seq(self._read(), seq)
            if not current["written"]:
                current["written"] = True
                _atomic_json(self.path, current)
            return current

    def refresh(
        self, seq: int, *, delivery_ledger_seq: int | None = None
    ) -> dict[str, Any]:
        with self._locked():
            current = self._for_seq(self._read(), seq)
            current["delivered_at"] = utc_now()
            if delivery_ledger_seq is not None:
                current["delivery_ledger_seq"] = delivery_ledger_seq
            _atomic_json(self.path, current)
            return current

    def redelivered(self, seq: int) -> dict[str, Any]:
        with self._locked():
            current = self._for_seq(self._read(), seq)
            if current["pending_failure"] is not None:
                current["attempts"] += 1
                current["pending_failure"] = None
                current["delivered_at"] = utc_now()
                current["written"] = True
                _atomic_json(self.path, current)
            return current

    def clear(self, seq: int) -> None:
        with self._locked():
            current = self._read()
            if current.get("seq") == seq:
                _safe_unlink(self.path)


class WakeBridge:
    """Deliver matching ledger handoffs through a one-outstanding wake spool."""

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        *,
        runner: str,
        wake_dir: str | os.PathLike[str] | None = None,
        ack_dir: str | os.PathLike[str] | None = None,
        max_retries: int = 3,
        ack_timeout_seconds: float = DEFAULT_ACK_TIMEOUT_SECONDS,
    ) -> None:
        if isinstance(max_retries, bool) or int(max_retries) < 1:
            raise StoreError("invalid_max_retries", "max_retries must be a positive integer")
        if ack_timeout_seconds <= 0:
            raise StoreError("invalid_ack_timeout", "ack timeout must be positive")
        self.paths: LedgerPaths = paths_for(workspace, create=False)
        self.runner = CursorStore._consumer(runner)  # noqa: SLF001 - shared identifier contract
        self.wake_dir = (
            Path(wake_dir)
            if wake_dir is not None
            else self.paths.root / "wake" / self.runner
        )
        self.ack_dir = Path(ack_dir) if ack_dir is not None else self.wake_dir / "done"
        _private_directory(self.wake_dir)
        _private_directory(self.ack_dir)
        self.max_retries = int(max_retries)
        self.ack_timeout_seconds = float(ack_timeout_seconds)
        self.cursors = CursorStore(self.paths.root)
        self.cursors.register(self.runner)
        self.retries = _RetryState(self.paths.root, self.runner)
        self.poll_lock_path = self.paths.root / f"bridge.{self.runner}.poll.lock"
        self.store = LedgerStore(self.paths.database, create=False, synchronous="NORMAL")
        self.client = QuayClient(self.paths.socket, timeout=15.0)

    def close(self) -> None:
        self.client.close()
        self.store.close()

    def __enter__(self) -> "WakeBridge":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _wake_path(self, seq: int) -> Path:
        return self.wake_dir / f"{seq}.json"

    def _ack_path(self, seq: int) -> Path:
        return self.ack_dir / f"{seq}.json"

    def _reconcile_wakes(self, cursor: Mapping[str, Any]) -> None:
        in_flight = cursor.get("in_flight")
        expected = (
            int(in_flight["seq"]) if isinstance(in_flight, Mapping) else None
        )
        last_acked = int(cursor["last_acked_seq"])
        unexpected: list[str] = []
        for path in self.wake_dir.glob("*.json"):
            if path.is_symlink() or not path.is_file():
                raise StoreError(
                    "unsafe_bridge_path", f"refusing unsafe wake file: {path}"
                )
            try:
                seq = int(path.stem)
            except ValueError:
                unexpected.append(path.name)
                continue
            if seq == expected:
                continue
            if seq <= last_acked:
                _safe_unlink(path)
            else:
                unexpected.append(path.name)
        if unexpected:
            raise StoreError(
                "bridge_multiple_wakes",
                "wake spool contains an event not owned by the durable cursor",
                {"unexpected": sorted(unexpected), "expected": expected},
            )

    @staticmethod
    def _idempotency_key(ledger_uuid: str, seq: int) -> str:
        return f"quay:{ledger_uuid}:{seq}"

    def _event(self, seq: int) -> dict[str, Any]:
        event = self.store.get(seq)
        if event is None:
            raise StoreError("bridge_event_missing", f"in-flight event no longer exists: {seq}")
        return event

    def _write_wake(
        self,
        event: Mapping[str, Any],
        idempotency_key: str,
        attempt: int,
        *,
        delivered_at: str | None = None,
    ) -> Path:
        seq = int(event["seq"])
        body = {
            "event": dict(event),
            "event_id": seq,
            "idempotency_key": idempotency_key,
            "attempt": attempt,
            "delivered_at": delivered_at or utc_now(),
        }
        target = self._wake_path(seq)
        _atomic_json(target, body)
        return target

    @staticmethod
    def _ack_fingerprint(value: Mapping[str, Any]) -> str:
        encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def _ack_outcome(value: Mapping[str, Any]) -> str:
        event = value.get("event")
        payload: Mapping[str, Any] = value
        if isinstance(event, Mapping) and isinstance(event.get("payload"), Mapping):
            payload = event["payload"]
        action = payload.get("action")
        if isinstance(action, Mapping):
            label = str(action.get("type", "")).strip().lower()
            if label == "runner_ack":
                return "success"
            if label == "runner_error":
                return "error"
        summary = payload.get("summary")
        if isinstance(summary, str):
            upper = summary.lstrip().upper()
            if upper.startswith("RUNNER_ACK"):
                return "success"
            if upper.startswith("RUNNER_ERROR"):
                return "error"
        for key in ("status", "result", "outcome", "type"):
            label = str(value.get(key, "")).strip().lower()
            if label in _SUCCESS:
                return "success"
            if label in _ERROR:
                return "error"
        raise StoreError("invalid_bridge_ack", "ack file is not RUNNER_ACK or RUNNER_ERROR")

    def _read_ack_file(self, seq: int) -> dict[str, Any] | None:
        path = self._ack_path(seq)
        if not path.exists():
            return None
        if path.is_symlink() or not path.is_file():
            raise StoreError("unsafe_bridge_path", f"refusing unsafe ack file: {path}")
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise StoreError("invalid_bridge_ack", f"cannot read {path}: {exc}") from exc
        if not isinstance(value, dict):
            raise StoreError("invalid_bridge_ack", "ack file must contain an object")
        ack_seq = value.get("seq", value.get("event_id", seq))
        if ack_seq != seq:
            raise StoreError("invalid_bridge_ack", f"ack sequence {ack_seq!r} does not match {seq}")
        return value

    def _ledger_ack(self, seq: int) -> dict[str, Any] | None:
        retry = self.retries.get(seq)
        cursor = self.cursors.get(self.runner)
        in_flight = cursor.get("in_flight")
        if not isinstance(in_flight, Mapping) or int(in_flight["seq"]) != seq:
            return None
        idempotency_key = str(in_flight["idempotency_key"])
        after = max(seq, int(retry["ledger_ack_seq"]))
        events = self.store.query(
            kind="handoff",
            producer=self.runner,
            causation_id=seq,
            since_seq=after + 1,
            order="asc",
            limit=10_000,
        )
        for event in events:
            if event.get("producer") != self.runner:
                continue
            if int(event["seq"]) <= int(retry["delivery_ledger_seq"]):
                continue
            payload = event.get("payload")
            action = payload.get("action") if isinstance(payload, Mapping) else None
            if isinstance(action, Mapping):
                attempt = action.get("attempt")
                if attempt is not None and attempt != retry["attempts"]:
                    continue
                ack_key = action.get("idempotency_key")
                if ack_key is not None and ack_key != idempotency_key:
                    continue
            try:
                self._ack_outcome({"event": event})
            except StoreError:
                continue
            persisted = {"seq": seq, "source": "ledger", "event": event}
            _atomic_json(self._ack_path(seq), persisted)
            return persisted
        return None

    def _find_ack(self, seq: int) -> dict[str, Any] | None:
        return self._read_ack_file(seq) or self._ledger_ack(seq)

    def _archive_failure_ack(self, seq: int, failures: int) -> None:
        source = self._ack_path(seq)
        if not source.exists():
            return
        archive = self.ack_dir / "failed"
        _private_directory(archive)
        target = archive / f"{seq}.{failures}.json"
        if target.exists():
            _safe_unlink(source)
            return
        if source.is_symlink():
            raise StoreError("unsafe_bridge_path", f"refusing symlinked ack file: {source}")
        os.replace(source, target)
        _fsync_directory(self.ack_dir)
        _fsync_directory(archive)

    def _poison(self, event: Mapping[str, Any], failures: int) -> None:
        seq = int(event["seq"])
        correlation = f"bridge-{self.runner}-{seq}"
        if self.client.query(correlation_id=correlation, limit=10):
            return
        raw_payload = event.get("payload")
        payload: Mapping[str, Any] = (
            raw_payload if isinstance(raw_payload, Mapping) else {}
        )
        recipient = str(payload.get("from") or "swarm")
        root = int(event.get("root_causation_id") or seq)
        self.client.emit_batch(
            [
                {
                    "topic": "okf/dead-letter",
                    "producer": "quay.bridge",
                    "kind": "generic",
                    "causation_id": seq,
                    "root_causation_id": root,
                    "correlation_id": correlation,
                    "payload": {
                        "reason": "RETRY_EXHAUSTED",
                        "runner": self.runner,
                        "source_seq": seq,
                        "failures": failures,
                        "idempotency_key": self._idempotency_key(
                            str(self.store.meta("ledger_uuid")), seq
                        ),
                    },
                },
                {
                    "topic": f"swarm/{recipient}/handoff",
                    "producer": "quay.bridge",
                    "kind": "handoff",
                    "causation_id": seq,
                    "root_causation_id": root,
                    "correlation_id": correlation,
                    "payload": {
                        "from": "quay.bridge",
                        "to": recipient,
                        "summary": (
                            f"RUNNER_ERROR: {self.runner} exhausted {failures} retries "
                            f"for Quay event #{seq}"
                        ),
                        "action": {
                            "type": "runner_error",
                            "reason": "RETRY_EXHAUSTED",
                            "source_event_id": str(seq),
                        },
                    },
                },
            ]
        )

    def _prune_acks(self, keep_last: int = 100) -> None:
        if not self.ack_dir.exists():
            return
        ack_files: list[tuple[int, Path]] = []
        for path in self.ack_dir.glob("*.json"):
            if path.is_file() and not path.is_symlink() and path.stem.isdigit():
                ack_files.append((int(path.stem), path))
        if len(ack_files) <= keep_last:
            return
        ack_files.sort(key=lambda item: item[0])
        for _seq, path in ack_files[:-keep_last]:
            with contextlib.suppress(OSError):
                path.unlink()
        _fsync_directory(self.ack_dir)

    def _complete(self, seq: int, idempotency_key: str) -> dict[str, Any]:
        cursor = self.cursors.acknowledge(
            self.runner, seq, idempotency_key=idempotency_key
        )
        _safe_unlink(self._wake_path(seq))
        self.retries.clear(seq)
        self._prune_acks()
        return {"action": "acknowledged", "seq": seq, "cursor": cursor}

    def _fail_and_redeliver(
        self,
        event: Mapping[str, Any],
        idempotency_key: str,
        *,
        token: str,
        archive_ack: bool,
        ledger_ack_seq: int | None = None,
    ) -> dict[str, Any]:
        seq = int(event["seq"])
        retry = self.retries.fail(
            seq,
            token,
            ledger_ack_seq=ledger_ack_seq,
            delivery_ledger_seq=self.store.last_seq(),
        )
        failures = int(retry["failures"])
        if failures >= self.max_retries:
            self._poison(event, failures)
            if archive_ack:
                self._archive_failure_ack(seq, failures)
            result = self._complete(seq, idempotency_key)
            return {**result, "action": "dead_lettered", "failures": failures}
        next_attempt = int(retry["attempts"]) + 1
        self._write_wake(event, idempotency_key, next_attempt)
        if archive_ack:
            self._archive_failure_ack(seq, failures)
        retry = self.retries.redelivered(seq)
        return {
            "action": "redelivered",
            "seq": seq,
            "attempt": retry["attempts"],
            "failures": failures,
        }

    def _process_in_flight(
        self, cursor: Mapping[str, Any], *, force_redelivery: bool
    ) -> dict[str, Any]:
        in_flight = cursor["in_flight"]
        assert isinstance(in_flight, Mapping)
        seq = int(in_flight["seq"])
        key = str(in_flight["idempotency_key"])
        event = self._event(seq)
        retry = (
            self.retries.get(seq)
            if self.retries.contains(seq)
            else self.retries.initialize(
                seq, delivery_ledger_seq=self.store.last_seq()
            )
        )
        ack = self._find_ack(seq)
        if ack is not None:
            outcome = self._ack_outcome(ack)
            if outcome == "success":
                return self._complete(seq, key)
            ledger_event = ack.get("event") if ack.get("source") == "ledger" else None
            ledger_ack_seq = (
                int(ledger_event["seq"])
                if isinstance(ledger_event, Mapping)
                else None
            )
            token = (
                f"ledger:{ledger_ack_seq}"
                if ledger_ack_seq is not None
                else f"ack:{self._ack_fingerprint(ack)}"
            )
            return self._fail_and_redeliver(
                event,
                key,
                token=token,
                archive_ack=True,
                ledger_ack_seq=ledger_ack_seq,
            )

        wake = self._wake_path(seq)
        if retry.get("pending_failure") is not None and wake.exists():
            retry = self.retries.redelivered(seq)
        elif not retry["written"] and wake.exists():
            retry = self.retries.mark_written(seq)
        if not wake.exists():
            if not retry["written"]:
                retry = self.retries.refresh(
                    seq, delivery_ledger_seq=self.store.last_seq()
                )
                self._write_wake(event, key, int(retry["attempts"]))
                retry = self.retries.mark_written(seq)
                return {
                    "action": "restored",
                    "seq": seq,
                    "attempt": retry["attempts"],
                }
            token = f"missing:{retry['attempts']}:{in_flight['delivered_at']}"
            return self._fail_and_redeliver(
                event, key, token=token, archive_ack=False
            )

        timed_out = (
            time.time() - _parse_time(str(retry["delivered_at"]))
        ) >= self.ack_timeout_seconds
        if timed_out:
            token = f"timeout:{retry['attempts']}:{retry['delivered_at']}"
            return self._fail_and_redeliver(
                event, key, token=token, archive_ack=False
            )
        if force_redelivery:
            retry = self.retries.refresh(
                seq, delivery_ledger_seq=self.store.last_seq()
            )
            self._write_wake(
                event,
                key,
                int(retry["attempts"]),
                delivered_at=str(retry["delivered_at"]),
            )
            return {"action": "restored", "seq": seq, "attempt": retry["attempts"]}
        return {"action": "waiting", "seq": seq, "attempt": retry["attempts"]}

    def _poll_once_unlocked(
        self, *, force_redelivery: bool = False
    ) -> dict[str, Any]:
        cursor = self.cursors.get(self.runner)
        self._reconcile_wakes(cursor)
        if cursor["in_flight"] is not None:
            return self._process_in_flight(cursor, force_redelivery=force_redelivery)

        events = self.store.query(
            kind="handoff",
            topic=f"swarm/{self.runner}/handoff",
            since_seq=int(cursor["last_acked_seq"]) + 1,
            order="asc",
            limit=1,
        )
        if not events:
            return {"action": "idle", "last_acked_seq": cursor["last_acked_seq"]}
        event = events[0]
        seq = int(event["seq"])
        ledger_uuid = self.store.meta("ledger_uuid")
        if not ledger_uuid:
            raise StoreError("ledger_identity_missing", "ledger UUID is missing")
        key = self._idempotency_key(ledger_uuid, seq)
        cursor = self.cursors.begin_delivery(self.runner, seq, key)
        retry = self.retries.initialize(
            seq, delivery_ledger_seq=self.store.last_seq()
        )
        self._write_wake(event, key, int(retry["attempts"]))
        self.retries.mark_written(seq)
        return {"action": "delivered", "seq": seq, "cursor": cursor, "attempt": 1}

    def poll_once(self, *, force_redelivery: bool = False) -> dict[str, Any]:
        fd = os.open(self.poll_lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            return self._poll_once_unlocked(force_redelivery=force_redelivery)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def run(self, *, poll_interval: float = 1.0, once: bool = False) -> dict[str, Any]:
        if poll_interval <= 0:
            raise StoreError("invalid_poll_interval", "poll interval must be positive")
        result = self.poll_once(force_redelivery=not once)
        if once:
            return result
        while True:
            time.sleep(poll_interval)
            result = self.poll_once()

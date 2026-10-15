"""Client library: blocking request/response and streaming subscriptions.

Two shapes are provided deliberately:

``QuayClient``
    One connection, one request at a time, thread-safe. Use it for command
    style work (``status``, ``emit``, ``query``).

``SubscriptionStream``
    Its own connection and reader thread that turns server pushes into an
    iterator. Use it for live views. The stream never blocks the daemon: if
    the consumer falls behind, pushes are dropped inside the stream's own
    queue and counted.
"""

from __future__ import annotations

import contextlib
import os
import queue
import socket
import threading
import time
from pathlib import Path
from typing import Any, Iterator, Mapping

from .protocol import (
    ProtocolError,
    make_request,
    read_frame,
    send_frame,
    validate_response,
)

__all__ = ["QuayClient", "SubscriptionStream", "ClientError", "connect"]

DEFAULT_TIMEOUT = 15.0


class ClientError(RuntimeError):
    """A request failed or the daemon is unreachable."""

    def __init__(self, code: str, message: str, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail: dict[str, Any] = dict(detail or {})

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "detail": self.detail}


def _connect(socket_path: os.PathLike[str] | str, *, timeout: float) -> socket.socket:
    path = Path(socket_path)
    if path.is_symlink():
        raise ClientError("symlinked_socket", f"refusing symlinked control socket: {path}")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.settimeout(timeout)
        sock.connect(str(path))
    except (FileNotFoundError, ConnectionRefusedError, socket.timeout, OSError) as exc:
        sock.close()
        raise ClientError("connection_failed", f"cannot reach the daemon at {path}: {exc}") from exc
    return sock


def _exchange(
    sock: socket.socket,
    op: str,
    body: Mapping[str, Any] | None,
    *,
    timeout: float,
    notices: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Send one request and read its matching response on an open socket.

    Frames that are not the awaited response (heartbeats, events) are skipped
    and, when ``notices`` is supplied, recorded.
    """
    frame = make_request(op, body)
    request_id = frame["id"]
    send_frame(sock, frame)
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ClientError("timeout", f"no response to '{op}' within {timeout}s")
        sock.settimeout(remaining)
        try:
            incoming = read_frame(sock)
        except (socket.timeout, TimeoutError) as exc:
            raise ClientError("timeout", f"no response to '{op}' within {timeout}s") from exc
        except ProtocolError as exc:
            raise ClientError(exc.code, exc.message, exc.detail) from exc
        except OSError as exc:
            raise ClientError("transport_error", f"transport failure: {exc}") from exc
        if incoming is None:
            raise ClientError("daemon_closed", "the daemon closed the connection")
        if incoming.get("type") != "response":
            if notices is not None:
                notices.append(incoming)
            continue
        if incoming.get("id") != request_id:
            continue
        ok, payload = validate_response(incoming)
        if ok:
            return payload
        raise ClientError(
            str(payload.get("code", "error")),
            str(payload.get("message", "request failed")),
            payload.get("detail") if isinstance(payload.get("detail"), dict) else {},
        )


class QuayClient:
    """Blocking, thread-safe request/response client."""

    def __init__(self, socket_path: os.PathLike[str] | str, *, timeout: float = DEFAULT_TIMEOUT) -> None:
        self.socket_path = Path(socket_path)
        self.timeout = timeout
        self._sock: socket.socket | None = None
        self._lock = threading.RLock()
        self._notices: list[dict[str, Any]] = []

    # ---------------------------------------------------------------- lifetime

    def connect(self) -> "QuayClient":
        with self._lock:
            if self._sock is None:
                self._sock = _connect(self.socket_path, timeout=self.timeout)
        return self

    def close(self) -> None:
        with self._lock:
            if self._sock is not None:
                with contextlib.suppress(OSError):
                    self._sock.shutdown(socket.SHUT_RDWR)
                with contextlib.suppress(OSError):
                    self._sock.close()
                self._sock = None

    @property
    def notices(self) -> list[dict[str, Any]]:
        """Push frames observed while waiting for responses (heartbeats, etc.)."""
        with self._lock:
            return list(self._notices)

    def __enter__(self) -> "QuayClient":
        return self.connect()

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ----------------------------------------------------------------- requests

    def request(self, op: str, body: Mapping[str, Any] | None = None, *, timeout: float | None = None) -> dict[str, Any]:
        """Send one request and return its result, raising :class:`ClientError`."""
        with self._lock:
            self.connect()
            assert self._sock is not None
            effective = self.timeout if timeout is None else timeout
            previous_timeout = self._sock.gettimeout()
            try:
                return _exchange(
                    self._sock, op, body, timeout=effective, notices=self._notices
                )
            except ClientError as exc:
                if exc.code in ("transport_error", "daemon_closed", "timeout"):
                    self.close()
                raise
            finally:
                if self._sock is not None:
                    with contextlib.suppress(OSError):
                        self._sock.settimeout(previous_timeout)

    # --------------------------------------------------------------- convenience

    def health(self) -> dict[str, Any]:
        return self.request("health")

    def status(self) -> dict[str, Any]:
        return self.request("status")

    def emit(self, *, topic: str, producer: str, payload: Mapping[str, Any] | None = None, **extra: Any) -> dict[str, Any]:
        return self.request("emit", {"topic": topic, "producer": producer, "payload": payload, **extra})["event"]

    def emit_batch(self, events: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
        return self.request("emit_batch", {"events": events})["events"]

    def query(self, **filters: Any) -> list[dict[str, Any]]:
        return self.request("query", filters)["events"]

    def log(self, limit: int = 50, *, order: str = "desc") -> list[dict[str, Any]]:
        return self.request("log", {"limit": limit, "order": order})["events"]

    def stats(self) -> dict[str, Any]:
        return self.request("stats")

    def subscribe(self, filters: Mapping[str, Any] | None = None, *, replay: int = 0) -> "SubscriptionStream":
        return SubscriptionStream(self.socket_path, filters=filters, replay=replay, timeout=self.timeout)


class SubscriptionStream:
    """A live push stream with its own connection and reader thread."""

    def __init__(
        self,
        socket_path: os.PathLike[str] | str,
        *,
        filters: Mapping[str, Any] | None = None,
        replay: int = 0,
        timeout: float = DEFAULT_TIMEOUT,
        queue_size: int = 4096,
    ) -> None:
        self.socket_path = Path(socket_path)
        self.filters = dict(filters or {})
        self.replay = int(replay)
        self.timeout = timeout
        self.queue: queue.Queue[dict[str, Any] | None] = queue.Queue(maxsize=queue_size)
        self.dropped = 0
        self.lagged_notices = 0
        self.recovered_events = 0
        self._last_received_seq = 0
        self._sock: socket.socket | None = None
        self._reader: threading.Thread | None = None
        self._closed = threading.Event()
        self._terminal = threading.Event()
        self._terminal_lock = threading.Lock()
        self._terminal_notice: dict[str, Any] | None = None
        self._terminal_notice_delivered = False
        self.sub_id: int | None = None
        self.replayed = 0

    def open(self) -> "SubscriptionStream":
        self._sock = _connect(self.socket_path, timeout=self.timeout)
        early: list[dict[str, Any]] = []
        try:
            result = _exchange(
                self._sock,
                "subscribe",
                {"filters": self.filters, "replay": self.replay},
                timeout=self.timeout,
                notices=early,
            )
        except ClientError:
            with contextlib.suppress(OSError):
                self._sock.close()
            self._sock = None
            raise
        self.sub_id = int(result["sub"])
        self.replayed = int(result.get("replayed", 0))
        # Replayed events arrive before the subscribe response; queue them in
        # order before the reader thread starts so none are lost or reordered.
        for frame in early:
            self._put_event_frame(frame)
        # The server captures this boundary atomically when it registers the
        # subscription. Advancing to it after queuing replay frames prevents a
        # replay-0 recovery from backfilling pre-subscription history.
        self._last_received_seq = max(
            self._last_received_seq, int(result.get("start_seq", 0))
        )
        self._sock.settimeout(None)
        self._reader = threading.Thread(target=self._read_loop, name="quay-sub", daemon=True)
        self._reader.start()
        return self

    def unsubscribe(self) -> None:
        """Ask the daemon to stop this subscription now.

        The reply is not awaited: the reader thread simply sees an extra
        ``response`` frame, which callers that iterate events ignore.
        """
        if self._sock is None or self.sub_id is None or self._closed.is_set():
            return
        try:
            send_frame(self._sock, make_request("unsubscribe", {"sub": self.sub_id}))
        except (OSError, ProtocolError):
            pass
        self.sub_id = None

    def _read_loop(self) -> None:
        assert self._sock is not None
        terminal_notice: dict[str, Any] | None = None
        try:
            while not self._closed.is_set():
                try:
                    frame = read_frame(self._sock)
                except (ProtocolError, OSError):
                    return
                if frame is None:
                    return
                frame_type = frame.get("type")
                if frame_type == "lagged":
                    self.lagged_notices += 1
                    self.dropped += int(frame.get("dropped", 0))
                    try:
                        self._recover_gap(int(frame.get("last_seq") or 0))
                    except ClientError as exc:
                        terminal_notice = {
                            "type": "recovery_error",
                            "error": exc.to_dict(),
                            "last_seq": frame.get("last_seq"),
                        }
                        return
                if frame_type == "shutdown":
                    self._put({"type": "shutdown", "reason": frame.get("reason")})
                    return
                if not self._put_event_frame(frame):
                    return
        finally:
            self._finish(terminal_notice)

    def _put_event_frame(self, frame: dict[str, Any]) -> bool:
        """Queue one frame, suppressing replay/live duplicates by sequence."""
        if frame.get("type") != "event":
            return self._put(frame)
        event = frame.get("event")
        if not isinstance(event, Mapping):
            return self._put(frame)
        seq = int(event.get("seq") or 0)
        if seq > 0 and seq <= self._last_received_seq:
            return True
        if not self._put(frame):
            return False
        if seq > 0:
            self._last_received_seq = seq
        return True

    def _recover_gap(self, until_seq: int) -> None:
        """Backfill matching events dropped by the server before live resumes."""
        if until_seq <= self._last_received_seq:
            return
        cursor = self._last_received_seq
        while cursor < until_seq:
            query = dict(self.filters)
            configured_since = query.get("since_seq")
            lower_bound = int(configured_since) if configured_since is not None else 1
            query["since_seq"] = max(lower_bound, cursor + 1)
            query["until_seq"] = until_seq
            query["limit"] = 5000
            query["order"] = "asc"
            with QuayClient(self.socket_path, timeout=self.timeout) as client:
                events = client.query(**query)
            if not events:
                self._last_received_seq = until_seq
                return
            previous = cursor
            for event in events:
                seq = int(event.get("seq") or 0)
                if seq <= cursor:
                    continue
                if not self._put_event_frame({"type": "event", "event": event, "recovered": True}):
                    raise ClientError(
                        "recovery_queue_full",
                        "subscription queue filled while backfilling a lagged stream",
                        {"cursor": cursor, "until_seq": until_seq},
                    )
                self.recovered_events += 1
                cursor = seq
            if cursor <= previous:
                raise ClientError(
                    "recovery_stalled",
                    "gap recovery made no sequence progress",
                    {"cursor": cursor, "until_seq": until_seq},
                )
            if len(events) < 5000:
                self._last_received_seq = max(self._last_received_seq, until_seq)
                return

    def _put(self, frame: dict[str, Any] | None) -> bool:
        try:
            self.queue.put_nowait(frame)
            return True
        except queue.Full:
            return False

    def _finish(self, notice: dict[str, Any] | None = None) -> None:
        """Record terminal state even when the bounded queue has no free slot."""
        with self._terminal_lock:
            if notice is not None and self._terminal_notice is None:
                self._terminal_notice = notice
            already_terminal = self._terminal.is_set()
            self._terminal.set()
            self._closed.set()
        if self._sock is not None:
            with contextlib.suppress(OSError):
                self._sock.shutdown(socket.SHUT_RDWR)
            with contextlib.suppress(OSError):
                self._sock.close()
        if not already_terminal or notice is not None:
            # Wake a consumer already blocked on an empty queue.  If the queue
            # is full, poll() observes terminal state after draining it.
            self._put(None)

    def _terminal_after_drain(self) -> dict[str, Any] | None:
        """Return a terminal notice once; subsequent calls report end-of-stream."""
        with self._terminal_lock:
            if self._terminal_notice is not None and not self._terminal_notice_delivered:
                self._terminal_notice_delivered = True
                return self._terminal_notice
        return None

    @property
    def is_terminal(self) -> bool:
        """Return whether the stream has ended and will receive no more frames."""
        return self._terminal.is_set()

    def close(self) -> None:
        if self._terminal.is_set():
            return
        self._finish()

    def __enter__(self) -> "SubscriptionStream":
        return self.open()

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def poll(self, timeout: float | None = 1.0) -> dict[str, Any] | None:
        """Return the next raw frame, or None on timeout/end of stream."""
        if self._terminal.is_set() and self.queue.empty():
            notice = self._terminal_after_drain()
            if notice is not None:
                return notice
            if timeout is None:
                return None
            # A terminal marker has already been consumed. Respect the
            # caller's timeout instead of returning immediately on every poll,
            # which would turn retry loops into a CPU busy-spin.
        try:
            frame = self.queue.get(timeout=timeout)
        except queue.Empty:
            if self._terminal.is_set():
                return self._terminal_after_drain()
            return None
        if frame is None and self._terminal.is_set():
            return self._terminal_after_drain()
        return frame

    @staticmethod
    def _raise_recovery_error(frame: Mapping[str, Any]) -> None:
        """Raise the original recovery failure carried by a terminal frame."""
        if frame.get("type") != "recovery_error":
            return
        error = frame.get("error") or {}
        raise ClientError(
            str(error.get("code", "recovery_failed")),
            str(error.get("message", "subscription gap recovery failed")),
            error.get("detail") if isinstance(error.get("detail"), Mapping) else {},
        )

    def frames(self, timeout: float | None = 5.0) -> Iterator[dict[str, Any]]:
        """Yield raw push frames until the stream ends.

        ``timeout`` bounds the whole iteration, not each frame, so a steady
        heartbeat cannot make the loop run forever.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return
            frame = self.poll(remaining)
            if frame is None:
                return
            yield frame

    def events(self, timeout: float | None = 5.0) -> Iterator[dict[str, Any]]:
        """Yield only event payloads, skipping heartbeats and lag notices."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            remaining = None if deadline is None else deadline - time.monotonic()
            if remaining is not None and remaining <= 0:
                return
            frame = self.poll(remaining)
            if frame is None:
                return
            if frame.get("type") == "event":
                yield frame["event"]
            else:
                self._raise_recovery_error(frame)

    def drain_until_lagged(self, *, timeout: float = 10.0) -> int:
        """Read frames until a lag notice arrives; return the total dropped."""
        deadline = time.monotonic() + timeout
        dropped = 0
        while time.monotonic() < deadline:
            frame = self.poll(max(0.0, deadline - time.monotonic()))
            if frame is None:
                break
            if frame.get("type") == "lagged":
                dropped += int(frame.get("dropped", 0))
        return dropped

    def wait_for(self, count: int = 1, *, timeout: float = 5.0) -> list[dict[str, Any]]:
        """Collect ``count`` events or raise :class:`ClientError` on timeout."""
        collected: list[dict[str, Any]] = []
        deadline = time.monotonic() + timeout
        while len(collected) < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ClientError(
                    "timeout", f"received {len(collected)} of {count} events within {timeout}s"
                )
            frame = self.poll(remaining)
            if frame is None:
                raise ClientError("stream_closed", "the subscription stream ended")
            if frame.get("type") == "event":
                collected.append(frame["event"])
            else:
                self._raise_recovery_error(frame)
        return collected


def connect(socket_path: os.PathLike[str] | str) -> QuayClient:
    """Convenience: return a connected client."""
    return QuayClient(socket_path).connect()

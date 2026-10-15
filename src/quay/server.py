"""The Quay daemon: a framed control socket, live subscriptions, and the ledger.

Concurrency model
-----------------
* One accept loop runs in its own thread and only accepts.
* Each client connection gets a reader thread (parses requests) and a writer
  thread (drains a bounded outbound queue). A slow or hostile client therefore
  cannot stall the daemon or any other client.
* All ledger writes are serialized by the store's own lock, so request order
  per connection is preserved and cross-connection order is transactional.

Backpressure
------------
Responses always block their own connection until queued (a client that asked
must get an answer). Server pushes are *droppable*: when a subscriber's queue
is full the frame is discarded, counted, and reported to that subscriber as a
``lagged`` frame once it drains. The daemon never grows memory without bound.
"""

from __future__ import annotations

import contextlib
import fnmatch
import os
import queue
import socket
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Mapping

from . import PROTOCOL_VERSION
from .cursors import CursorStore
from .protocol import (
    ProtocolError,
    make_error,
    make_push,
    make_response,
    read_frame,
    send_frame,
    validate_request,
)
from .schemas import ValidationError, describe_kind
from .store import IDLE_SECONDS_DEFAULT, LedgerStore, StoreError, utc_now
from .workspace import (
    WorkspaceError,
    live_daemon_record,
    new_instance_id,
    paths_for,
    process_start_identity,
    register_workspace,
    unregister_workspace,
    write_owner_file,
    unlink_owned,
)

#: Operations that mutate the ledger; they broadcast to subscribers afterwards.
WRITE_OPS = frozenset({"emit", "emit_batch", "genesis", "work_claim", "work_heartbeat", "work_complete", "work_abandon", "work_reap", "cursor_deliver", "cursor_ack", "prune", "import"})

#: Operations that any client may call.
READ_OPS = frozenset(
    {
        "health",
        "ping",
        "status",
        "stats",
        "query",
        "log",
        "since",
        "get",
        "kinds",
        "verify",
        "work_list",
        "cursor_get",
        "prune_preview",
        "subscribe",
        "unsubscribe",
        "stop",
    }
)

OP_NAMES = sorted(WRITE_OPS | READ_OPS)

#: Bound on a single client's kernel send buffer. Together with the bounded
#: outbox this caps how much memory a client that stops reading can consume.
SEND_BUFFER_BYTES = 64 * 1024

_FILTER_KEYS = ("kind", "topic", "producer", "state", "correlation_id", "since_seq")

__all__ = ["QuayServer", "Connection", "event_matches", "OP_NAMES"]


def event_matches(event: Mapping[str, Any], filters: Mapping[str, Any]) -> bool:
    """Return True if ``event`` satisfies every supplied subscription filter."""
    kind = filters.get("kind")
    if kind is not None and event.get("kind") != kind:
        return False
    producer = filters.get("producer")
    if producer is not None and event.get("producer") != producer:
        return False
    correlation_id = filters.get("correlation_id")
    if correlation_id is not None and event.get("correlation_id") != correlation_id:
        return False
    topic = filters.get("topic")
    if topic is not None and not fnmatch.fnmatchcase(str(event.get("topic", "")), str(topic)):
        return False
    state = filters.get("state")
    if state is not None:
        payload = event.get("payload")
        if not isinstance(payload, Mapping) or payload.get("state") != state:
            return False
    since_seq = filters.get("since_seq")
    if since_seq is not None and int(event.get("seq", 0)) < int(since_seq):
        return False
    return True


def normalize_filters(raw: Any) -> dict[str, Any]:
    """Validate and normalize a subscription filter object."""
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ProtocolError("invalid_filters", "filters must be a JSON object")
    unknown = sorted(set(raw) - set(_FILTER_KEYS))
    if unknown:
        raise ProtocolError(
            "unknown_filter",
            f"unknown filter field(s): {', '.join(unknown)}",
            {"unknown": unknown, "allowed": list(_FILTER_KEYS)},
        )
    filters: dict[str, Any] = {}
    for key, value in raw.items():
        if value is None:
            continue
        if key == "since_seq":
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ProtocolError("invalid_filter", "since_seq must be a non-negative integer")
            filters[key] = value
        elif not isinstance(value, str) or not value:
            raise ProtocolError("invalid_filter", f"filter '{key}' must be a non-empty string")
        else:
            filters[key] = value
    return filters


class Connection:
    """One client connection: reader thread, writer thread, subscriptions."""

    _next_connection_id = 1
    _connection_id_lock = threading.Lock()

    def __init__(self, server: "QuayServer", sock: socket.socket) -> None:
        with Connection._connection_id_lock:
            self.connection_id = Connection._next_connection_id
            Connection._next_connection_id += 1
        self.server = server
        self.sock = sock
        self.outbox: queue.Queue[dict[str, Any] | None] = queue.Queue(
            maxsize=server.subscriber_queue_size
        )
        self.subscriptions: dict[int, dict[str, Any]] = {}
        self.dropped = 0
        self.last_dropped_seq: int | None = None
        self._lag_pending = False
        self._closing = threading.Event()
        self._closed_hard = threading.Event()
        self.reader = threading.Thread(target=self._reader_loop, name=f"quay-read-{self.connection_id}", daemon=True)
        self.writer = threading.Thread(target=self._writer_loop, name=f"quay-write-{self.connection_id}", daemon=True)

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        self.writer.start()
        self.reader.start()

    def close(self) -> None:
        """Close immediately, discarding anything still queued."""
        self._hard_close()

    def close_gracefully(self, *, timeout: float = 2.0) -> None:
        """Let the writer drain queued frames (including an error reply), then close."""
        if not self._closed_hard.is_set() and not self._closing.is_set():
            with contextlib.suppress(queue.Full):
                self.outbox.put_nowait(None)
            if self.writer.is_alive():
                self.writer.join(timeout=timeout)
        self._hard_close()

    def _hard_close(self) -> None:
        if self._closed_hard.is_set():
            return
        self._closed_hard.set()
        self._closing.set()
        with contextlib.suppress(OSError):
            self.sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(OSError):
            self.sock.close()
        with contextlib.suppress(queue.Full):
            self.outbox.put_nowait(None)

    def join(self, timeout: float = 2.0) -> None:
        for thread in (self.writer, self.reader):
            if thread.is_alive():
                thread.join(timeout=timeout)

    # ----------------------------------------------------------------- output

    def enqueue_response(self, frame: dict[str, Any], *, timeout: float = 10.0) -> bool:
        """Queue a response. Responses are never dropped."""
        try:
            self.outbox.put(frame, timeout=timeout)
            return True
        except queue.Full:
            self.close()
            return False

    def enqueue_push(self, frame: dict[str, Any]) -> bool:
        """Queue a server push. Pushes are dropped and accounted, never blocking."""
        try:
            self.outbox.put_nowait(frame)
            return True
        except queue.Full:
            self.dropped += 1
            event = frame.get("event")
            if isinstance(event, Mapping):
                self.last_dropped_seq = int(event.get("seq", 0))
            self._lag_pending = True
            return False

    def _emit_lag_notice(self) -> None:
        if not self._lag_pending:
            return
        dropped, self.dropped = self.dropped, 0
        self._lag_pending = False
        with contextlib.suppress(OSError, ProtocolError):
            send_frame(
                self.sock,
                make_push(
                    "lagged",
                    subs=sorted(self.subscriptions),
                    dropped=dropped,
                    last_seq=self.last_dropped_seq,
                ),
            )

    def _writer_loop(self) -> None:
        try:
            while True:
                frame = self.outbox.get()
                if frame is None:
                    return
                if frame.get("type") == "event":
                    self._emit_lag_notice()
                try:
                    send_frame(self.sock, frame)
                except (OSError, ProtocolError):
                    return
        finally:
            self._hard_close()

    # ------------------------------------------------------------------ input

    def _reader_loop(self) -> None:
        try:
            while not self.server.stopping and not self._closing.is_set():
                try:
                    frame = read_frame(self.sock)
                except ProtocolError as exc:
                    self._reply_error("-", exc)
                    return
                except OSError:
                    return
                if frame is None:
                    return
                try:
                    request_id, op, body = validate_request(frame)
                except ProtocolError as exc:
                    raw_id = frame.get("id")
                    self._reply_error(raw_id if isinstance(raw_id, str) else "-", exc)
                    continue
                self.server.note_activity()
                response, stop = self.server.dispatch(request_id, op, body, self)
                self.enqueue_response(response)
                if stop:
                    self.server.request_stop(str(body.get("reason", op)))
                    return
        finally:
            self.server.forget_connection(self)
            # Let any final response (for example a protocol error) reach the peer.
            self.close_gracefully()

    def _reply_error(self, request_id: str, exc: ProtocolError) -> None:
        self.enqueue_response(
            make_error(request_id, exc.code, exc.message, exc.detail), timeout=2.0
        )


class QuayServer:
    """A workspace-local daemon around one :class:`LedgerStore`."""

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        *,
        socket_path: str | os.PathLike[str] | None = None,
        idle_seconds: float = IDLE_SECONDS_DEFAULT,
        heartbeat_seconds: float = 10.0,
        subscriber_queue_size: int = 4096,
        synchronous: str = "NORMAL",
        reap_interval_seconds: float = 30.0,
    ) -> None:
        self.paths = paths_for(workspace)
        if socket_path is not None:
            self.paths = self.paths.__class__(
                workspace=self.paths.workspace,
                root=self.paths.root,
                database=self.paths.database,
                socket=Path(socket_path),
                pid=self.paths.pid,
                lock=self.paths.lock,
            )
        self.idle_seconds = float(idle_seconds)
        self.heartbeat_seconds = float(heartbeat_seconds)
        self.subscriber_queue_size = int(subscriber_queue_size)
        self.reap_interval_seconds = float(reap_interval_seconds)
        self.store = LedgerStore(self.paths.database, synchronous=synchronous)
        self.cursors = CursorStore(self.paths.root)
        self.instance_id = new_instance_id()
        self.pid = os.getpid()
        self.identity = process_start_identity(self.pid)
        self.started_at = utc_now()
        self.started_monotonic = time.monotonic()
        self.stopping = False
        self.stop_reason: str | None = None
        self.last_activity = time.monotonic()
        self._listener: socket.socket | None = None
        self._connections: dict[int, Connection] = {}
        self._connections_lock = threading.RLock()
        self._accept_thread: threading.Thread | None = None
        self._maintenance_thread: threading.Thread | None = None
        self._watchdog_thread: threading.Thread | None = None
        self._close_lock = threading.Lock()
        self._closed = False
        self._stop_event = threading.Event()
        self._next_subscription_id = 1
        self._write_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        self.total_emitted = 0
        self.total_pushes = 0
        self.total_dropped = 0

    # ---------------------------------------------------------------- startup

    @property
    def socket_path(self) -> Path:
        return self.paths.socket

    def start(self) -> None:
        """Publish identity, bind the socket, and start the background threads.

        Identity is published *before* the socket exists. A client that can
        connect therefore always finds this daemon in the registry, which is
        what stops ``quay daemons`` from missing a daemon that just came up.
        """
        if self._listener is not None:
            raise WorkspaceError("already_started", "server is already started")
        if self.socket_path.exists() or self.socket_path.is_symlink():
            raise WorkspaceError(
                "socket_in_use", f"control socket already exists: {self.socket_path}"
            )
        write_owner_file(
            self.paths.pid,
            '{"instance_id":"%s","pid":%d,"identity":%s,"started_at":"%s"}'
            % (
                self.instance_id,
                self.pid,
                f'"{self.identity}"' if self.identity else "null",
                self.started_at,
            ),
        )
        register_workspace(self.paths, self.instance_id, self.pid, self.identity)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(self.socket_path))
        except OSError as exc:
            listener.close()
            # Never leave a registry row or pid file behind for a socket that
            # does not exist.
            unlink_owned(self.paths.pid)
            with contextlib.suppress(Exception):
                unregister_workspace(self.paths)
            raise WorkspaceError("socket_bind_failed", f"cannot bind control socket: {exc}") from exc
        os.chmod(self.socket_path, 0o600)
        listener.listen(64)
        self._listener = listener
        self.note_activity()
        self._accept_thread = threading.Thread(target=self._accept_loop, name="quay-accept", daemon=True)
        self._accept_thread.start()
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop, name="quay-maintenance", daemon=True
        )
        self._maintenance_thread.start()
        # The watchdog owns teardown: whoever requests a stop, one thread
        # closes the daemon exactly once.
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop, name="quay-watchdog", daemon=True
        )
        self._watchdog_thread.start()

    def _watchdog_loop(self) -> None:
        self._stop_event.wait()
        with contextlib.suppress(Exception):
            self.close()

    def serve_forever(self) -> None:
        """Start and block until the daemon stops."""
        self.start()
        self._stop_event.wait()
        if self._watchdog_thread is not None:
            self._watchdog_thread.join(timeout=10.0)

    def request_stop(self, reason: str) -> None:
        if not self.stopping:
            self.stopping = True
            self.stop_reason = reason
            self._stop_event.set()

    def stop(self, reason: str = "stop") -> None:
        self.request_stop(reason)

    def note_activity(self) -> None:
        self.last_activity = time.monotonic()

    def is_idle(self, *, now: float | None = None) -> bool:
        """Return whether idle shutdown is allowed at this instant.

        Persistent subscriptions are daemon liveness claims: heartbeats are
        intentionally not request frames, so request activity alone cannot be
        used to decide that a bus-mode daemon is unused.
        """
        if self.idle_seconds <= 0:
            return False
        with self._connections_lock:
            if any(connection.subscriptions for connection in self._connections.values()):
                return False
        moment = time.monotonic() if now is None else now
        return moment - self.last_activity >= self.idle_seconds

    def health(self) -> dict[str, Any]:
        return {
            "pid": self.pid,
            "instance_id": self.instance_id,
            "identity": self.identity,
            "started_at": self.started_at,
            "protocol_version": PROTOCOL_VERSION,
            "uptime_seconds": round(time.monotonic() - self.started_monotonic, 3),
            "socket": str(self.socket_path),
            "workspace": str(self.paths.workspace),
        }

    # -------------------------------------------------------------- accept loop

    def _accept_loop(self) -> None:
        assert self._listener is not None
        listener = self._listener
        while not self.stopping:
            try:
                listener.settimeout(0.2)
                client, _ = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                client.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, SEND_BUFFER_BYTES)
            except OSError:
                pass
            connection = Connection(self, client)
            with self._connections_lock:
                self._connections[connection.connection_id] = connection
            try:
                connection.start()
            except RuntimeError:
                connection.close()

    def _maintenance_loop(self) -> None:
        next_heartbeat = time.monotonic() + self.heartbeat_seconds
        next_reap = time.monotonic() + self.reap_interval_seconds
        while not self.stopping:
            now = time.monotonic()
            if now >= next_heartbeat:
                self.broadcast_heartbeat()
                next_heartbeat = now + self.heartbeat_seconds
            if now >= next_reap:
                with contextlib.suppress(StoreError, WorkspaceError):
                    self._reap_expired()
                next_reap = now + self.reap_interval_seconds
            if self.is_idle(now=now):
                self.request_stop("idle")
            time.sleep(0.1)

    def _reap_expired(self) -> None:
        with self._write_lock:
            reaped = self.store.reap_expired_leases()
        for event in reaped:
            self.broadcast_event(event)

    def broadcast_heartbeat(self) -> None:
        """Reassure subscribers that the daemon is alive.

        Heartbeats go only to connections that hold a subscription: a
        request-only client must never have its socket buffer filled by
        frames it did not ask for.
        """
        frame = make_push("heartbeat", ts=utc_now(), seq=self.store.last_seq())
        for connection in self._snapshot():
            if connection.subscriptions:
                connection.enqueue_push(frame)

    def broadcast_event(self, event: Mapping[str, Any]) -> None:
        with self.stats_lock:
            self.total_emitted += 1
        for connection in self._snapshot():
            if not connection.subscriptions:
                continue
            for sub_id, filters in list(connection.subscriptions.items()):
                if event_matches(event, filters):
                    frame = make_push("event", sub=sub_id, event=dict(event))
                    sent = connection.enqueue_push(frame)
                    with self.stats_lock:
                        if sent:
                            self.total_pushes += 1
                        else:
                            self.total_dropped += 1
                    break

    def _snapshot(self) -> list[Connection]:
        with self._connections_lock:
            return list(self._connections.values())

    def forget_connection(self, connection: Connection) -> None:
        with self._connections_lock:
            self._connections.pop(connection.connection_id, None)

    def broadcast_shutdown(self, reason: str) -> None:
        frame = make_push("shutdown", reason=reason)
        connections = self._snapshot()
        for connection in connections:
            connection.enqueue_push(frame)
        # Give writers a moment to flush the notice and the final response.
        deadline = time.monotonic() + 1.0
        for connection in connections:
            while time.monotonic() < deadline and connection.writer.is_alive():
                if connection.outbox.empty():
                    break
                time.sleep(0.01)
        for connection in connections:
            connection.close()
        for connection in connections:
            connection.join(timeout=1.0)
        with self._connections_lock:
            self._connections.clear()

    # ----------------------------------------------------------------- teardown

    def close(self) -> None:
        """Tear the daemon down exactly once, removing socket and PID files."""
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        if self._listener is not None:
            with contextlib.suppress(OSError):
                self._listener.close()
            self._listener = None
        self.broadcast_shutdown(self.stop_reason or "stop")
        for thread in (self._accept_thread, self._maintenance_thread):
            if thread is not None and thread.is_alive():
                thread.join(timeout=2.0)
        self.store.close()
        # Teardown order matters: the control socket is how everyone else
        # decides the daemon is gone, so it is removed last. By the time the
        # socket disappears the registry row and pid file are already clean,
        # which is what makes "socket gone" a reliable signal.
        record = live_daemon_record(self.paths)
        if record and record.get("instance_id") == self.instance_id:
            unlink_owned(self.paths.pid)
        else:
            # Our own record may already be stale-gone; only remove a matching one.
            raw = self.paths.pid
            if raw.exists():
                import json as _json

                with contextlib.suppress(OSError, ValueError):
                    current = _json.loads(raw.read_text(encoding="utf-8"))
                    if current.get("instance_id") == self.instance_id:
                        unlink_owned(raw)
        unregister_workspace(self.paths)
        unlink_owned(self.socket_path)

    def __enter__(self) -> "QuayServer":
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop("context")
        self.close()

    # ------------------------------------------------------------- dispatching

    def stats(self) -> dict[str, Any]:
        with self._connections_lock:
            connections = list(self._connections.values())
        with self.stats_lock:
            emitted, pushes, dropped = self.total_emitted, self.total_pushes, self.total_dropped
        return {
            "connections": len(connections),
            "subscriptions": sum(len(c.subscriptions) for c in connections),
            "clients": [
                {
                    "connection_id": c.connection_id,
                    "subscriptions": sorted(c.subscriptions),
                    "outbox_size": c.outbox.qsize(),
                    "outbox_max": c.outbox.maxsize,
                    "dropped_in_flight": c.dropped,
                }
                for c in connections
            ],
            "events_broadcast": emitted,
            "pushes_delivered": pushes,
            "pushes_dropped": dropped,
        }

    def dispatch(
        self,
        request_id: str,
        op: str,
        body: Mapping[str, Any],
        connection: Connection,
    ) -> tuple[dict[str, Any], bool]:
        """Handle one request, returning ``(response_frame, should_stop)``."""
        handler = getattr(self, f"_op_{op}", None)
        if handler is None or op not in OP_NAMES:
            return (
                make_error(
                    request_id,
                    "unknown_operation",
                    f"unknown operation: {op!r}",
                    {"operation": op, "known": OP_NAMES},
                ),
                False,
            )
        try:
            result, stop = handler(body, connection)
            return make_response(request_id, result), stop
        except ProtocolError as exc:
            return make_error(request_id, exc.code, exc.message, exc.detail), False
        except StoreError as exc:
            return make_error(request_id, exc.code, exc.message, exc.detail), False
        except ValidationError as exc:
            return make_error(request_id, exc.code, exc.message, exc.detail), False
        except WorkspaceError as exc:
            return make_error(request_id, exc.code, exc.message), False
        except Exception as exc:  # pragma: no cover - defensive boundary
            return (
                make_error(
                    request_id,
                    "internal_error",
                    f"{type(exc).__name__}: {exc}",
                    {},
                ),
                False,
            )

    # ------------------------------------------------------------------- read ops

    def _op_health(self, body, connection):
        return self.health(), False

    def _op_ping(self, body, connection):
        return {"pong": True, "ts": utc_now()}, False

    def _op_status(self, body, connection):
        return {"daemon": self.health(), **self.store.status()}, False

    def _op_stats(self, body, connection):
        return self.stats(), False

    def _op_kinds(self, body, connection):
        from .schemas import KINDS

        return {"kinds": [describe_kind(kind) for kind in KINDS]}, False

    def _op_query(self, body, connection):
        allowed = {
            "kind",
            "topic",
            "producer",
            "state",
            "correlation_id",
            "causation_id",
            "root_causation_id",
            "since_seq",
            "until_seq",
            "since_ts",
            "until_ts",
            "text",
            "limit",
            "offset",
            "order",
        }
        unknown = sorted(set(body) - allowed)
        if unknown:
            raise ProtocolError(
                "unknown_query_field",
                f"unknown query field(s): {', '.join(unknown)}",
                {"unknown": unknown, "allowed": sorted(allowed)},
            )
        events = self.store.query(**dict(body))
        return {"events": events, "count": len(events)}, False

    def _op_log(self, body, connection):
        limit = body.get("limit", 50)
        order = body.get("order", "desc")
        if order not in ("asc", "desc"):
            raise ProtocolError("invalid_order", "order must be 'asc' or 'desc'")
        return {"events": self.store.tail(int(limit), order=order)}, False

    def _op_since(self, body, connection):
        seq = body.get("seq", 0)
        if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
            raise ProtocolError("invalid_seq", "seq must be a non-negative integer")
        limit = int(body.get("limit", 500))
        events = self.store.since(seq, limit=limit)
        return {"events": events, "last_seq": self.store.last_seq()}, False

    def _op_get(self, body, connection):
        seq = body.get("seq")
        if isinstance(seq, bool) or not isinstance(seq, int):
            raise ProtocolError("invalid_seq", "seq must be an integer")
        event = self.store.get(seq)
        if event is None:
            raise ProtocolError("not_found", f"no event with seq {seq}", {"seq": seq})
        return {"event": event}, False

    def _op_verify(self, body, connection):
        return self.store.verify(), False

    def _op_work_list(self, body, connection):
        return {
            "work": self.store.list_work(state=body.get("state"), executor=body.get("executor")),
            "expired": self.store.expired_leases(),
        }, False

    def _op_cursor_get(self, body, connection):
        return {
            "consumer": body.get("consumer"),
            "cursor": self.cursors.get(body.get("consumer")),
        }, False

    def _op_cursor_deliver(self, body, connection):
        seq = body.get("seq")
        if isinstance(seq, bool) or not isinstance(seq, int):
            raise ProtocolError("invalid_cursor_seq", "seq must be an integer")
        with self._write_lock:
            if self.store.get(seq) is None:
                raise ProtocolError("not_found", f"no event with seq {seq}", {"seq": seq})
            cursor = self.cursors.begin_delivery(
                body.get("consumer"),
                seq,
                body.get("idempotency_key"),
                delivered_at=body.get("delivered_at"),
            )
        return {"consumer": body.get("consumer"), "cursor": cursor}, False

    def _op_cursor_ack(self, body, connection):
        cursor = self.cursors.acknowledge(
            body.get("consumer"),
            body.get("seq"),
            idempotency_key=body.get("idempotency_key"),
        )
        return {"consumer": body.get("consumer"), "cursor": cursor}, False

    def _op_prune_preview(self, body, connection):
        result = self.store.prune_preview(
            before=body.get("before"), keep_last=body.get("keep_last")
        )
        minimum_acked = self.cursors.minimum_acked_seq()
        result["minimum_acked_seq"] = minimum_acked
        result["cursor_blocked"] = (
            minimum_acked is not None and int(result["cutoff_seq"]) > minimum_acked
        )
        return result, False

    # ------------------------------------------------------------ subscribe ops

    def _op_subscribe(self, body, connection):
        filters = normalize_filters(body.get("filters", body.get("filter")))
        replay = int(body.get("replay", 0) or 0)
        # Serialize registration with appends. A write committed immediately
        # before registration may still broadcast afterward, so the returned
        # boundary lets the client suppress that pre-subscription event.
        with self._write_lock:
            start_seq = self.store.last_seq()
            with self._connections_lock:
                sub_id = self._next_subscription_id
                self._next_subscription_id += 1
                connection.subscriptions[sub_id] = filters
            result: dict[str, Any] = {
                "sub": sub_id,
                "filters": filters,
                "start_seq": start_seq,
            }
            if replay:
                history = self.store.query(
                    kind=filters.get("kind"),
                    topic=filters.get("topic"),
                    producer=filters.get("producer"),
                    state=filters.get("state"),
                    correlation_id=filters.get("correlation_id"),
                    since_seq=filters.get("since_seq"),
                    until_seq=start_seq,
                    limit=max(1, min(replay, 5000)),
                    order="desc",
                )
                for event in reversed(history):
                    connection.enqueue_push(make_push("event", sub=sub_id, event=event))
                result["replayed"] = len(history)
        return result, False

    def _op_unsubscribe(self, body, connection):
        sub_id = body.get("sub")
        if isinstance(sub_id, bool) or not isinstance(sub_id, int):
            raise ProtocolError("invalid_sub", "sub must be an integer subscription id")
        with self._connections_lock:
            removed = connection.subscriptions.pop(sub_id, None)
        if removed is None:
            raise ProtocolError("unknown_subscription", f"no subscription {sub_id}", {"sub": sub_id})
        return {"sub": sub_id, "removed": True}, False

    # ------------------------------------------------------------------ write ops

    def _emit_locked(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        with self._write_lock:
            written = self.store.append_batch(events)
        for event in written:
            self.broadcast_event(event)
        return written

    def _op_emit(self, body, connection):
        event = {
            "topic": body.get("topic"),
            "producer": body.get("producer"),
            "payload": body.get("payload"),
            "kind": body.get("kind", "generic"),
            "causation_id": body.get("causation_id"),
            "root_causation_id": body.get("root_causation_id"),
            "correlation_id": body.get("correlation_id"),
        }
        written = self._emit_locked([event])
        return {"event": written[0]}, False

    def _op_genesis(self, body, connection):
        """Append the genesis note once, when the ledger is empty.

        The count-and-append decision runs inside the daemon's write lock and
        the store's own lock, so a repeated or concurrent onboarding can never
        write a second genesis record.
        """
        with self._write_lock:
            event = self.store.ensure_genesis()
        if event is None:
            return {"created": False, "event": None}, False
        self.broadcast_event(event)
        return {"created": True, "event": event}, False

    def _op_emit_batch(self, body, connection):
        raw = body.get("events")
        if not isinstance(raw, list):
            raise ProtocolError("invalid_events", "emit_batch requires an 'events' array")
        events = []
        for index, item in enumerate(raw):
            if not isinstance(item, dict):
                raise ProtocolError(
                    "invalid_event", f"events[{index}] must be an object", {"index": index}
                )
            events.append(
                {
                    "topic": item.get("topic"),
                    "producer": item.get("producer"),
                    "payload": item.get("payload"),
                    "kind": item.get("kind", "generic"),
                    "causation_id": item.get("causation_id"),
                    "root_causation_id": item.get("root_causation_id"),
                    "correlation_id": item.get("correlation_id"),
                    "ts": item.get("ts"),
                }
            )
        if not events:
            return {"events": [], "count": 0}, False
        written = self._emit_locked(events)
        return {"events": written, "count": len(written)}, False

    def _work_event(self, body, state: str) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "key": body.get("key"),
            "executor": body.get("executor"),
            "state": state,
        }
        for optional in ("title", "lease_seconds", "evidence", "reason", "fencing_token"):
            if optional in body and body[optional] is not None:
                payload[optional] = body[optional]
        return {
            "topic": body.get("topic", "work/queue"),
            "producer": body.get("executor"),
            "payload": payload,
            "kind": "work",
            "causation_id": body.get("causation_id"),
            "root_causation_id": body.get("root_causation_id"),
            "correlation_id": body.get("correlation_id"),
        }

    def _op_work_claim(self, body, connection):
        written = self._emit_locked([self._work_event(body, "claimed")])
        return {"event": written[0]}, False

    def _op_work_heartbeat(self, body, connection):
        written = self._emit_locked([self._work_event(body, "in_flight")])
        return {"event": written[0]}, False

    def _op_work_complete(self, body, connection):
        written = self._emit_locked([self._work_event(body, "done")])
        return {"event": written[0]}, False

    def _op_work_abandon(self, body, connection):
        written = self._emit_locked([self._work_event(body, "abandoned")])
        return {"event": written[0]}, False

    def _op_work_reap(self, body, connection):
        with self._write_lock:
            if body.get("now") is not None:
                reaped = self.store.reap_expired_leases(now=body.get("now"))
            else:
                reaped = self.store.reap_expired_leases()
        for event in reaped:
            self.broadcast_event(event)
        return {"reaped": reaped, "count": len(reaped)}, False

    def _op_prune(self, body, connection):
        dry_run = bool(body.get("dry_run", False))
        with self._write_lock:
            plan = self.store.prune_preview(
                before=body.get("before"), keep_last=body.get("keep_last")
            )
            minimum_acked = self.cursors.minimum_acked_seq()
            if minimum_acked is not None and int(plan["cutoff_seq"]) > minimum_acked:
                raise StoreError(
                    "cursor_prune_conflict",
                    "prune would delete events not acknowledged by every durable consumer",
                    {
                        "cutoff_seq": plan["cutoff_seq"],
                        "minimum_acked_seq": minimum_acked,
                    },
                )
            result = self.store.prune(
                before=body.get("before"), keep_last=body.get("keep_last"), dry_run=dry_run
            )
        return result, False

    def _op_import(self, body, connection):
        """Import a Changi ledger inside the daemon so there is one writer.

        Imported events are deliberately not broadcast: a migration can write
        hundreds of thousands of events, and replaying them to live subscribers
        would turn a bulk load into a fan-out storm. Subscribers can backfill
        with ``since`` when they care.
        """
        source = body.get("source")
        if not isinstance(source, str) or not source.strip():
            raise ProtocolError("invalid_source", "import requires a 'source' path")
        from .migrate import migrate_changi

        dry_run = bool(body.get("dry_run", False))
        if dry_run:
            with tempfile.TemporaryDirectory(prefix="quay-import-") as temporary:
                scratch = LedgerStore(Path(temporary) / "scratch.db", synchronous="OFF")
                try:
                    report = migrate_changi(source, scratch, dry_run=True)
                finally:
                    scratch.close()
        else:
            with self._write_lock:
                report = migrate_changi(source, self.store)
        return {"report": report.to_dict()}, False

    def _op_stop(self, body, connection):
        return {"stopping": True, "reason": str(body.get("reason", "client"))}, True

"""Length-prefixed framed protocol for the Quay control socket.

Every frame on the wire is a 4-byte big-endian unsigned length followed by
that many bytes of UTF-8 JSON. Framing makes reads bounded: a peer can never
make us allocate more than :data:`MAX_FRAME_BYTES`.

Frame shapes
------------
Request::

    {"v": "1", "type": "request", "id": "<token>", "op": "emit", "body": {...}}

Response::

    {"v": "1", "type": "response", "id": "<token>", "ok": true, "result": {...}}
    {"v": "1", "type": "response", "id": "<token>", "ok": false,
     "error": {"code": "...", "message": "...", "detail": {...}}}

Server push::

    {"v": "1", "type": "event",  "sub": 3, "event": {...}}
    {"v": "1", "type": "lagged", "sub": 3, "dropped": 12, "last_seq": 480}
    {"v": "1", "type": "heartbeat", "ts": "2026-10-04T12:00:00.000Z"}
    {"v": "1", "type": "shutdown", "reason": "stop"}
"""

from __future__ import annotations

import json
import socket
import struct
import uuid
from typing import Any, Mapping

PROTOCOL_VERSION = "1"

#: Frame length prefix: unsigned 32-bit big-endian.
HEADER = struct.Struct("!I")

#: Hard ceiling on a single frame body. Payloads are capped well below this
#: by the schema layer (256 KiB); the rest is headroom for batches.
MAX_FRAME_BYTES = 4 * 1024 * 1024

#: Smallest legal frame: ``{}``.
MIN_FRAME_BYTES = 2

REQUEST_TYPES = ("request",)
PUSH_TYPES = ("event", "lagged", "heartbeat", "shutdown")

__all__ = [
    "PROTOCOL_VERSION",
    "MAX_FRAME_BYTES",
    "ProtocolError",
    "encode_frame",
    "read_frame",
    "send_frame",
    "make_request",
    "make_response",
    "make_error",
    "make_push",
    "validate_request",
    "validate_response",
    "new_request_id",
]


class ProtocolError(Exception):
    """A stable protocol-boundary failure with a machine-readable code."""

    def __init__(self, code: str, message: str, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail: dict[str, Any] = dict(detail or {})

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "detail": self.detail}


def new_request_id() -> str:
    return uuid.uuid4().hex


# --------------------------------------------------------------------- encoding


def encode_frame(value: Mapping[str, Any], *, max_bytes: int = MAX_FRAME_BYTES) -> bytes:
    """Serialize one frame, raising :class:`ProtocolError` if it is too large."""
    try:
        raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProtocolError("unencodable_frame", f"frame is not JSON-encodable: {exc}") from exc
    if len(raw) < MIN_FRAME_BYTES or len(raw) > max_bytes:
        raise ProtocolError(
            "frame_too_large",
            f"frame body must be between {MIN_FRAME_BYTES} and {max_bytes} bytes",
            {"size": len(raw)},
        )
    return HEADER.pack(len(raw)) + raw


def send_frame(sock: socket.socket, value: Mapping[str, Any]) -> None:
    """Encode and send one frame; raises ``OSError`` if the peer is gone."""
    sock.sendall(encode_frame(value))


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise ProtocolError(
                "truncated_frame",
                "peer closed the connection mid-frame",
                {"expected": size, "received": size - remaining},
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame(sock: socket.socket, *, max_bytes: int = MAX_FRAME_BYTES) -> dict[str, Any] | None:
    """Read one frame, or ``None`` on a clean end-of-stream between frames."""
    header = b""
    while len(header) < HEADER.size:
        chunk = sock.recv(HEADER.size - len(header))
        if not chunk:
            if not header:
                return None
            raise ProtocolError("truncated_frame", "peer closed while sending a frame header")
        header += chunk
    (size,) = HEADER.unpack(header)
    if size < MIN_FRAME_BYTES or size > max_bytes:
        raise ProtocolError(
            "invalid_frame_size",
            f"frame length {size} is outside the accepted range",
            {"size": size, "max": max_bytes},
        )
    body = _recv_exact(sock, size)
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("invalid_frame_json", f"frame body is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ProtocolError("invalid_frame_shape", "a frame body must be a JSON object")
    return value


# --------------------------------------------------------------------- builders


def make_request(op: str, body: Mapping[str, Any] | None = None, *, request_id: str | None = None) -> dict[str, Any]:
    return {
        "v": PROTOCOL_VERSION,
        "type": "request",
        "id": request_id or new_request_id(),
        "op": op,
        "body": dict(body or {}),
    }


def make_response(request_id: str, result: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        "v": PROTOCOL_VERSION,
        "type": "response",
        "id": request_id,
        "ok": True,
        "result": dict(result or {}),
    }


def make_error(
    request_id: str,
    code: str,
    message: str,
    detail: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "v": PROTOCOL_VERSION,
        "type": "response",
        "id": request_id,
        "ok": False,
        "error": {"code": code, "message": message, "detail": dict(detail or {})},
    }


def make_push(frame_type: str, **fields: Any) -> dict[str, Any]:
    if frame_type not in PUSH_TYPES:
        raise ProtocolError("unknown_push_type", f"unknown push frame type: {frame_type!r}")
    return {"v": PROTOCOL_VERSION, "type": frame_type, **fields}


# ------------------------------------------------------------------- validation


def validate_request(frame: Mapping[str, Any]) -> tuple[str, str, dict[str, Any]]:
    """Validate an inbound request frame and return ``(id, op, body)``."""
    if frame.get("type") != "request":
        raise ProtocolError("not_a_request", "frame is not a request", {"type": frame.get("type")})
    if frame.get("v") != PROTOCOL_VERSION:
        raise ProtocolError(
            "unsupported_protocol",
            f"protocol version {frame.get('v')!r} is not supported (expected {PROTOCOL_VERSION})",
            {"expected": PROTOCOL_VERSION, "found": frame.get("v")},
        )
    request_id = frame.get("id")
    if not isinstance(request_id, str) or not request_id or len(request_id) > 128:
        raise ProtocolError("invalid_request_id", "request id must be a non-empty string")
    op = frame.get("op")
    if not isinstance(op, str) or not op or len(op) > 64:
        raise ProtocolError("invalid_operation", "operation must be a non-empty string")
    body = frame.get("body")
    if not isinstance(body, dict):
        raise ProtocolError("invalid_body", "request body must be a JSON object")
    unknown = sorted(set(frame) - {"v", "type", "id", "op", "body"})
    if unknown:
        raise ProtocolError(
            "unknown_request_field",
            f"unexpected request field(s): {', '.join(unknown)}",
            {"unknown": unknown},
        )
    return request_id, op, body


def validate_response(frame: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
    """Validate an inbound response and return ``(ok, result_or_error)``."""
    if frame.get("type") != "response":
        raise ProtocolError("not_a_response", "frame is not a response", {"type": frame.get("type")})
    if frame.get("v") != PROTOCOL_VERSION:
        raise ProtocolError("unsupported_protocol", "response protocol version mismatch")
    if not isinstance(frame.get("id"), str):
        raise ProtocolError("invalid_request_id", "response is missing a request id")
    ok = frame.get("ok")
    if ok is True:
        result = frame.get("result")
        if not isinstance(result, dict):
            raise ProtocolError("invalid_result", "successful response is missing an object result")
        return True, result
    if ok is False:
        error = frame.get("error")
        if not isinstance(error, dict) or not isinstance(error.get("code"), str):
            raise ProtocolError("invalid_error", "error response is missing a code")
        return False, {
            "code": error["code"],
            "message": str(error.get("message", error["code"])),
            "detail": error.get("detail") if isinstance(error.get("detail"), dict) else {},
        }
    raise ProtocolError("invalid_response", "response 'ok' must be true or false")

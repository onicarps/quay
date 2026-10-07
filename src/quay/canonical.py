"""Deterministic encoding and hashing for ledger events.

Every event that enters the ledger is serialized with :func:`canonical_json`
and bound to its predecessor with a SHA-256 chain. Both operations must be
byte-for-byte reproducible across processes, platforms, and Python patch
releases, otherwise verification would produce false tamper reports.

Design rules:

* Objects are serialized with sorted keys and compact separators.
* Only JSON-representable values are allowed; ``NaN`` and ``Infinity`` are
  rejected because they have no interoperable encoding.
* Floats use Python's shortest round-trip ``repr`` (stable since 3.1). Negative
  zero is normalized to ``0.0`` so it cannot produce two encodings of one value.
* Non-string mapping keys are rejected rather than coerced.
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Iterable

#: Hash of the event before the first event in a chain (and after a prune checkpoint).
GENESIS_HASH = "0" * 64

#: Domain separator between the previous hash and the canonical body.
_CHAIN_SEPARATOR = b"\x1f"

__all__ = [
    "GENESIS_HASH",
    "CanonicalError",
    "canonical_json",
    "canonical_bytes",
    "chain_digest",
    "event_digest",
    "hash_hex",
]


class CanonicalError(ValueError):
    """Raised when a value cannot be encoded canonically."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _check_value(value: Any, path: str) -> None:
    if value is None or value is True or value is False:
        return
    if isinstance(value, str):
        # Lone surrogates cannot be encoded to valid UTF-8.
        try:
            value.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise CanonicalError(
                "invalid_string", f"{path}: string is not valid UTF-8: {exc}"
            ) from exc
        return
    if isinstance(value, int):
        return
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise CanonicalError("invalid_number", f"{path}: {value!r} is not finite")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise CanonicalError(
                    "invalid_key", f"{path}: object key {key!r} is not a string"
                )
            _check_value(item, f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _check_value(item, f"{path}[{index}]")
        return
    raise CanonicalError("invalid_type", f"{path}: {type(value).__name__} is not JSON")


def _normalize(value: Any) -> Any:
    """Return an equivalent value whose encoding is unique.

    Tuples become lists and negative zero becomes positive zero so that two
    semantically equal inputs cannot produce two different hashes.
    """
    if isinstance(value, float):
        return 0.0 if value == 0.0 else value
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    return value


def canonical_json(value: Any) -> str:
    """Serialize ``value`` to its unique canonical JSON text."""
    _check_value(value, "$")
    return json.dumps(
        _normalize(value),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_bytes(value: Any) -> bytes:
    """Return the canonical UTF-8 encoding of ``value``."""
    return canonical_json(value).encode("utf-8")


def hash_hex(data: bytes) -> str:
    """Return the lowercase hex SHA-256 digest of ``data``."""
    return hashlib.sha256(data).hexdigest()


def _validate_prev_hash(prev_hash: str) -> str:
    if not isinstance(prev_hash, str) or len(prev_hash) != 64:
        raise CanonicalError("invalid_prev_hash", "previous hash must be 64 hex chars")
    try:
        int(prev_hash, 16)
    except ValueError as exc:
        raise CanonicalError(
            "invalid_prev_hash", "previous hash must be hexadecimal"
        ) from exc
    return prev_hash


def chain_digest(prev_hash: str, body_bytes: bytes) -> str:
    """Chain already-encoded ``body_bytes`` onto ``prev_hash``.

    Use this on the write path when the canonical body bytes are already in
    hand; :func:`event_digest` is the convenience wrapper that encodes for you.
    """
    _validate_prev_hash(prev_hash)
    if not isinstance(body_bytes, (bytes, bytearray)):
        raise CanonicalError("invalid_body", "body must be bytes")
    return hash_hex(prev_hash.encode("ascii") + _CHAIN_SEPARATOR + bytes(body_bytes))


def event_digest(prev_hash: str, body: Any) -> str:
    """Chain ``body`` onto ``prev_hash`` and return the resulting digest.

    ``body`` is any canonically encodable value; the ledger passes the full
    event body, including its sequence number and previous hash.
    """
    return chain_digest(prev_hash, canonical_bytes(body))


def digest_many(prev_hash: str, bodies: Iterable[Any]) -> list[str]:
    """Chain several bodies from ``prev_hash`` and return each digest in order."""
    digests: list[str] = []
    current = prev_hash
    for body in bodies:
        current = event_digest(current, body)
        digests.append(current)
    return digests

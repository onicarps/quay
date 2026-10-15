"""Typed event envelopes and the declarative schema registry.

An event is an envelope with a declared ``kind`` and a validated ``payload``.
Validation happens at the boundary, before anything is written: a malformed
event is rejected with a stable code rather than normalized into the ledger.

Kinds
-----
``signal``   A recorded request for attention with an explicit lifecycle
             (pending -> claimed -> cleared, or expired).
``work``     A unit of work with a lifecycle and an optional lease
             (claimed -> in_flight -> done/failed/abandoned/expired).
``note``     A free-text annotation.
``metric``   A named numeric sample.
``generic``  Any other JSON object; the escape hatch for callers who do not
             need plane semantics.

Envelope fields that the store owns (``seq``, ``hash``, ``prev_hash``,
``event_id``) are rejected if a caller supplies them.
"""

from __future__ import annotations

import re
from typing import Any, Mapping

from .canonical import CanonicalError, canonical_json

SCHEMA_VERSION = "1"

KINDS = ("signal", "work", "handoff", "note", "metric", "generic")

SIGNAL_STATES = ("pending", "claimed", "cleared", "expired")
SIGNAL_TERMINAL_STATES = ("cleared", "expired")

WORK_STATES = ("claimed", "in_flight", "done", "failed", "abandoned", "expired")
WORK_TERMINAL_STATES = ("done", "failed", "abandoned", "expired")
WORK_ACTIVE_STATES = ("claimed", "in_flight")

#: Fields the store assigns. A caller must never supply them.
STORE_OWNED_FIELDS = ("seq", "hash", "prev_hash", "event_id")

_MISSING = object()

TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$")
PRODUCER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@+-]{0,99}$")
CORRELATION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
HANDOFF_TOPIC_PATTERN = re.compile(r"^swarm/([A-Za-z0-9][A-Za-z0-9._:-]{0,99})/handoff$")

MAX_TOPIC_LENGTH = 200
MAX_CANONICAL_PAYLOAD_BYTES = 256 * 1024

__all__ = [
    "SCHEMA_VERSION",
    "KINDS",
    "SIGNAL_STATES",
    "SIGNAL_TERMINAL_STATES",
    "WORK_STATES",
    "WORK_TERMINAL_STATES",
    "WORK_ACTIVE_STATES",
    "STORE_OWNED_FIELDS",
    "ValidationError",
    "prepare_event",
    "normalize_event",
    "validate_payload",
    "describe_kind",
]


class ValidationError(ValueError):
    """A stable, user-facing schema rejection."""

    def __init__(self, code: str, message: str, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail: dict[str, Any] = dict(detail or {})

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "detail": self.detail}


def _require_object(payload: Any, kind: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValidationError(
            "payload_not_object",
            f"{kind} payload must be a JSON object",
            {"kind": kind, "type": type(payload).__name__},
        )
    return payload


def _require_str(payload: Mapping[str, Any], field: str, kind: str, *, max_length: int = 512) -> str:
    value = payload.get(field, _MISSING)
    if value is _MISSING or not isinstance(value, str) or not value.strip():
        raise ValidationError(
            "missing_field", f"{kind} payload requires a non-empty '{field}' string", {"field": field}
        )
    if len(value) > max_length:
        raise ValidationError(
            "field_too_long",
            f"{kind} payload field '{field}' exceeds {max_length} characters",
            {"field": field, "length": len(value)},
        )
    return value


def _require_enum(payload: Mapping[str, Any], field: str, allowed: tuple[str, ...], kind: str, default: str | None = None) -> str:
    value = payload.get(field, _MISSING)
    if value is _MISSING:
        if default is None:
            raise ValidationError(
                "missing_field",
                f"{kind} payload requires '{field}' (one of {', '.join(allowed)})",
                {"field": field, "allowed": list(allowed)},
            )
        return default
    if value not in allowed:
        raise ValidationError(
            "invalid_state",
            f"{kind} payload field '{field}' must be one of {', '.join(allowed)}",
            {"field": field, "value": value, "allowed": list(allowed)},
        )
    return str(value)


def _reject_unknown_fields(payload: Mapping[str, Any], allowed: tuple[str, ...], kind: str) -> None:
    unknown = sorted(set(payload) - set(allowed))
    if unknown:
        raise ValidationError(
            "unknown_field",
            f"{kind} payload has unknown field(s): {', '.join(unknown)}",
            {"kind": kind, "unknown": unknown, "allowed": list(allowed)},
        )


def _validate_signal(payload: dict[str, Any]) -> dict[str, Any]:
    _reject_unknown_fields(payload, ("state", "summary", "severity", "assignee"), "signal")
    state = _require_enum(payload, "state", SIGNAL_STATES, "signal", default="pending")
    normalized: dict[str, Any] = {"state": state}
    if "summary" in payload:
        normalized["summary"] = _require_str(payload, "summary", "signal")
    if "severity" in payload:
        normalized["severity"] = _require_enum(payload, "severity", ("info", "warning", "critical"), "signal")
    if "assignee" in payload:
        normalized["assignee"] = _require_str(payload, "assignee", "signal", max_length=100)
    return normalized


def _validate_work(payload: dict[str, Any]) -> dict[str, Any]:
    allowed = (
        "key",
        "state",
        "executor",
        "title",
        "lease_seconds",
        "evidence",
        "reason",
        "fencing_token",
    )
    _reject_unknown_fields(payload, allowed, "work")
    normalized: dict[str, Any] = {
        "key": _require_str(payload, "key", "work", max_length=200),
        "executor": _require_str(payload, "executor", "work", max_length=100),
        "state": _require_enum(payload, "state", WORK_STATES, "work"),
    }
    if "title" in payload:
        normalized["title"] = _require_str(payload, "title", "work")
    if "lease_seconds" in payload:
        seconds = payload["lease_seconds"]
        if not isinstance(seconds, int) or isinstance(seconds, bool) or seconds <= 0:
            raise ValidationError(
                "invalid_lease",
                "work payload 'lease_seconds' must be a positive integer",
                {"value": seconds},
            )
        normalized["lease_seconds"] = seconds
    if "evidence" in payload:
        evidence = payload["evidence"]
        if not isinstance(evidence, dict) or not evidence:
            raise ValidationError(
                "invalid_evidence", "work payload 'evidence' must be a non-empty object"
            )
        normalized["evidence"] = evidence
    if "reason" in payload:
        normalized["reason"] = _require_str(payload, "reason", "work")
    if "fencing_token" in payload:
        token = payload["fencing_token"]
        if isinstance(token, bool) or not isinstance(token, int) or token <= 0:
            raise ValidationError(
                "invalid_fencing_token",
                "work payload 'fencing_token' must be a positive integer",
                {"value": token},
            )
        normalized["fencing_token"] = token
    if normalized["state"] in ("done", "failed"):
        if "evidence" not in normalized:
            raise ValidationError(
                "missing_evidence",
                f"work payload in state '{normalized['state']}' requires 'evidence'",
                {"state": normalized["state"]},
            )
    if normalized["state"] in ("abandoned", "expired") and "reason" not in normalized and "evidence" not in normalized:
        raise ValidationError(
            "missing_reason",
            f"work payload in state '{normalized['state']}' requires 'reason' or 'evidence'",
            {"state": normalized["state"]},
        )
    return normalized


def _validate_handoff(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate the bounded swarm routing payload.

    Topic/producer consistency is checked in :func:`prepare_event`, where the
    envelope fields are available.  Keeping the payload validator context-free
    preserves the public ``validate_payload`` API.
    """
    allowed = ("from", "to", "summary", "initiative", "links", "action")
    _reject_unknown_fields(payload, allowed, "handoff")
    normalized: dict[str, Any] = {
        "from": _require_str(payload, "from", "handoff", max_length=100),
        "to": _require_str(payload, "to", "handoff", max_length=100),
        "summary": _require_str(payload, "summary", "handoff", max_length=2000),
    }
    if "initiative" in payload:
        normalized["initiative"] = _require_str(
            payload, "initiative", "handoff", max_length=200
        )
    if "links" in payload:
        links = payload["links"]
        if (
            not isinstance(links, list)
            or len(links) > 10
            or not all(isinstance(link, str) and link.strip() for link in links)
        ):
            raise ValidationError(
                "invalid_links",
                "handoff payload 'links' must be a list of at most 10 non-empty strings",
            )
        normalized["links"] = [link.replace("\\", "/") for link in links]
    if "action" in payload:
        action = payload["action"]
        if not isinstance(action, dict):
            raise ValidationError("invalid_action", "handoff payload 'action' must be an object")
        normalized["action"] = dict(action)
    return normalized


def _validate_note(payload: dict[str, Any]) -> dict[str, Any]:
    _reject_unknown_fields(payload, ("text", "tags"), "note")
    normalized: dict[str, Any] = {"text": _require_str(payload, "text", "note", max_length=4096)}
    if "tags" in payload:
        tags = payload["tags"]
        if not isinstance(tags, list) or not all(isinstance(tag, str) and tag.strip() for tag in tags):
            raise ValidationError("invalid_tags", "note payload 'tags' must be a list of non-empty strings")
        normalized["tags"] = list(tags)
    return normalized


def _validate_metric(payload: dict[str, Any]) -> dict[str, Any]:
    _reject_unknown_fields(payload, ("name", "value", "unit"), "metric")
    normalized: dict[str, Any] = {"name": _require_str(payload, "name", "metric", max_length=200)}
    value = payload.get("value", _MISSING)
    if value is _MISSING or isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError("invalid_value", "metric payload 'value' must be a number", {"value": value})
    normalized["value"] = value
    if "unit" in payload:
        normalized["unit"] = _require_str(payload, "unit", "metric", max_length=32)
    return normalized


def _validate_generic(payload: dict[str, Any]) -> dict[str, Any]:
    return payload


_VALIDATORS = {
    "signal": _validate_signal,
    "work": _validate_work,
    "handoff": _validate_handoff,
    "note": _validate_note,
    "metric": _validate_metric,
    "generic": _validate_generic,
}


def normalize_kind(value: Any) -> str:
    """Return a known kind, degrading unknown labels to ``generic``.

    Unknown kinds are a compatibility feature, not an error: a newer producer
    must be able to write to an older ledger. The original label is preserved
    in the payload as ``original_kind`` so nothing is lost.
    """
    if not isinstance(value, str):
        return "generic"
    candidate = value.strip().lower()
    if candidate in KINDS:
        return candidate
    return "generic"


def validate_payload(kind: str, payload: Any) -> dict[str, Any]:
    """Validate and normalize a payload for ``kind``.

    Returns a new dict containing only known fields, in canonical form.
    Raises :class:`ValidationError` on rejection.
    """
    if kind not in _VALIDATORS:
        raise ValidationError("unknown_kind", f"unknown event kind: {kind!r}", {"kind": kind})
    return _VALIDATORS[kind](_require_object(payload, kind))


def prepare_event(
    *,
    topic: Any,
    producer: Any,
    payload: Any,
    kind: Any = "generic",
    causation_id: Any = None,
    root_causation_id: Any = None,
    correlation_id: Any = None,
) -> tuple[dict[str, Any], str]:
    """Validate caller input and return ``(event_fields, canonical_payload_text)``.

    The payload is canonicalized exactly once here, and the encoded text is
    returned so the write path never has to encode it again. The event fields
    contain only what a caller controls; ``seq``, ``hash``, and ``prev_hash``
    are assigned by the store.
    """
    if not isinstance(topic, str) or not TOPIC_PATTERN.match(topic or ""):
        raise ValidationError(
            "invalid_topic",
            "topic must start with an alphanumeric character and may contain . _ : / -",
            {"topic": topic},
        )
    if not isinstance(producer, str) or not PRODUCER_PATTERN.match(producer or ""):
        raise ValidationError(
            "invalid_producer",
            "producer must start with an alphanumeric character and may contain . _ : @ + -",
            {"producer": producer},
        )
    if causation_id is not None:
        if isinstance(causation_id, bool) or not isinstance(causation_id, int) or causation_id <= 0:
            raise ValidationError(
                "invalid_causation", "causation_id must be a positive integer", {"causation_id": causation_id}
            )
    if root_causation_id is not None:
        if (
            isinstance(root_causation_id, bool)
            or not isinstance(root_causation_id, int)
            or root_causation_id <= 0
        ):
            raise ValidationError(
                "invalid_root_causation",
                "root_causation_id must be a positive integer",
                {"root_causation_id": root_causation_id},
            )
    if correlation_id is not None:
        if not isinstance(correlation_id, str) or not CORRELATION_PATTERN.match(correlation_id):
            raise ValidationError(
                "invalid_correlation",
                "correlation_id must be an alphanumeric identifier",
                {"correlation_id": correlation_id},
            )

    resolved_kind = normalize_kind(kind)
    raw_payload = payload if payload is not None else {}
    normalized_payload = validate_payload(resolved_kind, raw_payload)

    if resolved_kind == "handoff":
        match = HANDOFF_TOPIC_PATTERN.fullmatch(topic)
        if match is None:
            raise ValidationError(
                "invalid_handoff_topic",
                "handoff topic must be 'swarm/<recipient>/handoff'",
                {"topic": topic},
            )
        recipient = match.group(1)
        if normalized_payload["from"] != producer:
            raise ValidationError(
                "handoff_sender_mismatch",
                "handoff payload 'from' must equal the envelope producer",
                {"from": normalized_payload["from"], "producer": producer},
            )
        if normalized_payload["to"] != recipient:
            raise ValidationError(
                "handoff_recipient_mismatch",
                "handoff payload 'to' must equal the topic recipient",
                {"to": normalized_payload["to"], "recipient": recipient},
            )

    label = kind if isinstance(kind, str) else "generic"
    if resolved_kind == "generic" and label.strip().lower() not in KINDS:
        normalized_payload = dict(normalized_payload)
        normalized_payload.setdefault("original_kind", label)

    try:
        encoded_text = canonical_json(normalized_payload)
    except CanonicalError as exc:
        raise ValidationError("unencodable_payload", f"payload is not encodable: {exc}", {"reason": exc.code}) from exc

    encoded_size = len(encoded_text.encode("utf-8"))
    if encoded_size > MAX_CANONICAL_PAYLOAD_BYTES:
        raise ValidationError(
            "payload_too_large",
            f"payload exceeds {MAX_CANONICAL_PAYLOAD_BYTES} bytes",
            {"size": encoded_size},
        )

    return (
        {
            "kind": resolved_kind,
            "topic": topic,
            "producer": producer,
            "payload": normalized_payload,
            "causation_id": causation_id,
            "root_causation_id": root_causation_id,
            "correlation_id": correlation_id,
        },
        encoded_text,
    )


def normalize_event(
    *,
    topic: Any,
    producer: Any,
    payload: Any,
    kind: Any = "generic",
    causation_id: Any = None,
    root_causation_id: Any = None,
    correlation_id: Any = None,
) -> dict[str, Any]:
    """Validate caller input and return the canonical pre-insert event fields.

    The result contains exactly the fields a caller controls. It never
    contains ``seq``, ``hash``, or ``prev_hash``; the store adds those.
    """
    return prepare_event(
        topic=topic,
        producer=producer,
        payload=payload,
        kind=kind,
        causation_id=causation_id,
        root_causation_id=root_causation_id,
        correlation_id=correlation_id,
    )[0]


def describe_kind(kind: str) -> dict[str, Any]:
    """Return a machine-readable description of one kind's contract."""
    descriptions: dict[str, dict[str, Any]] = {
        "signal": {"states": list(SIGNAL_STATES), "required": ["state"], "purpose": "request for attention"},
        "work": {"states": list(WORK_STATES), "required": ["key", "executor", "state"], "purpose": "unit of work"},
        "handoff": {
            "required": ["from", "to", "summary"],
            "purpose": "bounded swarm coordination handoff",
        },
        "note": {"required": ["text"], "purpose": "free-text annotation"},
        "metric": {"required": ["name", "value"], "purpose": "named numeric sample"},
        "generic": {"required": [], "purpose": "any JSON object"},
    }
    if kind not in descriptions:
        raise ValidationError("unknown_kind", f"unknown event kind: {kind!r}", {"kind": kind})
    return {"kind": kind, **descriptions[kind]}

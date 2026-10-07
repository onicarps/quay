"""Envelope and payload schema validation tests (requirements F3, F4)."""

from __future__ import annotations

import pytest

from quay.schemas import (
    KINDS,
    MAX_CANONICAL_PAYLOAD_BYTES,
    SCHEMA_VERSION,
    ValidationError,
    describe_kind,
    normalize_event,
    normalize_kind,
    validate_payload,
)


def test_schema_version_is_pinned():
    assert SCHEMA_VERSION == "1"


def test_declared_kinds_are_stable():
    assert KINDS == ("signal", "work", "note", "metric", "generic")


@pytest.mark.parametrize(
    "label,expected",
    [
        ("signal", "signal"),
        ("SIGNAL", "signal"),
        ("  work ", "work"),
        ("note", "note"),
        ("metric", "metric"),
        ("generic", "generic"),
        ("telemetry", "generic"),
        ("", "generic"),
        (None, "generic"),
        (7, "generic"),
    ],
)
def test_unknown_kinds_degrade_to_generic(label, expected):
    assert normalize_kind(label) == expected


def test_generic_kind_preserves_original_label():
    event = normalize_event(
        topic="t", producer="p", payload={"x": 1}, kind="telemetry"
    )
    assert event["kind"] == "generic"
    assert event["payload"]["original_kind"] == "telemetry"


def test_known_generic_kind_has_no_original_label():
    event = normalize_event(topic="t", producer="p", payload={"x": 1}, kind="generic")
    assert "original_kind" not in event["payload"]


def test_payload_must_be_object():
    with pytest.raises(ValidationError) as excinfo:
        validate_payload("generic", ["not", "an", "object"])
    assert excinfo.value.code == "payload_not_object"


class TestSignal:
    def test_default_state_is_pending(self):
        assert validate_payload("signal", {}) == {"state": "pending"}

    def test_accepts_full_shape(self):
        payload = validate_payload(
            "signal", {"state": "claimed", "summary": "build broken", "severity": "critical", "assignee": "pi"}
        )
        assert payload == {
            "state": "claimed",
            "summary": "build broken",
            "severity": "critical",
            "assignee": "pi",
        }

    def test_rejects_unknown_state(self):
        with pytest.raises(ValidationError) as excinfo:
            validate_payload("signal", {"state": "waving"})
        assert excinfo.value.code == "invalid_state"

    def test_rejects_unknown_field(self):
        with pytest.raises(ValidationError) as excinfo:
            validate_payload("signal", {"urgency": "high"})
        assert excinfo.value.code == "unknown_field"

    def test_rejects_empty_summary(self):
        with pytest.raises(ValidationError) as excinfo:
            validate_payload("signal", {"summary": "   "})
        assert excinfo.value.code == "missing_field"


class TestWork:
    def test_minimal_shape(self):
        payload = validate_payload("work", {"key": "k", "executor": "e", "state": "claimed"})
        assert payload == {"key": "k", "executor": "e", "state": "claimed"}

    def test_done_requires_evidence(self):
        with pytest.raises(ValidationError) as excinfo:
            validate_payload("work", {"key": "k", "executor": "e", "state": "done"})
        assert excinfo.value.code == "missing_evidence"

    def test_failed_requires_evidence(self):
        with pytest.raises(ValidationError) as excinfo:
            validate_payload("work", {"key": "k", "executor": "e", "state": "failed"})
        assert excinfo.value.code == "missing_evidence"

    def test_terminal_state_accepts_evidence(self):
        payload = validate_payload(
            "work", {"key": "k", "executor": "e", "state": "done", "evidence": {"commit": "abc"}}
        )
        assert payload["evidence"] == {"commit": "abc"}

    def test_abandoned_requires_reason_or_evidence(self):
        with pytest.raises(ValidationError) as excinfo:
            validate_payload("work", {"key": "k", "executor": "e", "state": "abandoned"})
        assert excinfo.value.code == "missing_reason"

    def test_abandoned_accepts_reason(self):
        payload = validate_payload(
            "work", {"key": "k", "executor": "e", "state": "abandoned", "reason": "superseded"}
        )
        assert payload["reason"] == "superseded"

    def test_lease_seconds_must_be_positive_integer(self):
        for bad in (0, -5, "30", 2.5, True):
            with pytest.raises(ValidationError) as excinfo:
                validate_payload(
                    "work", {"key": "k", "executor": "e", "state": "claimed", "lease_seconds": bad}
                )
            assert excinfo.value.code == "invalid_lease"

    def test_evidence_must_be_non_empty_object(self):
        with pytest.raises(ValidationError) as excinfo:
            validate_payload(
                "work", {"key": "k", "executor": "e", "state": "done", "evidence": {}}
            )
        assert excinfo.value.code == "invalid_evidence"

    def test_missing_key_is_rejected(self):
        with pytest.raises(ValidationError) as excinfo:
            validate_payload("work", {"executor": "e", "state": "claimed"})
        assert excinfo.value.code == "missing_field"


class TestNoteMetricGeneric:
    def test_note_requires_text(self):
        with pytest.raises(ValidationError) as excinfo:
            validate_payload("note", {"tags": ["a"]})
        assert excinfo.value.code == "missing_field"

    def test_note_accepts_tags(self):
        assert validate_payload("note", {"text": "hi", "tags": ["a", "b"]}) == {
            "text": "hi",
            "tags": ["a", "b"],
        }

    def test_note_rejects_bad_tags(self):
        with pytest.raises(ValidationError) as excinfo:
            validate_payload("note", {"text": "hi", "tags": ["ok", ""]})
        assert excinfo.value.code == "invalid_tags"

    def test_metric_requires_numeric_value(self):
        with pytest.raises(ValidationError) as excinfo:
            validate_payload("metric", {"name": "latency", "value": "fast"})
        assert excinfo.value.code == "invalid_value"

    def test_metric_accepts_int_and_float(self):
        assert validate_payload("metric", {"name": "a", "value": 1})["value"] == 1
        assert validate_payload("metric", {"name": "a", "value": 1.5})["value"] == 1.5

    def test_metric_rejects_bool_value(self):
        with pytest.raises(ValidationError):
            validate_payload("metric", {"name": "a", "value": True})

    def test_generic_accepts_arbitrary_object(self):
        payload = {"anything": [1, {"nested": True}]}
        assert validate_payload("generic", payload) == payload


class TestEnvelope:
    def test_normalize_event_returns_only_caller_fields(self):
        event = normalize_event(topic="okf/handoff", producer="factory", payload={"a": 1})
        assert set(event) == {"kind", "topic", "producer", "payload", "causation_id", "correlation_id"}
        assert event["correlation_id"] is None
        assert event["causation_id"] is None

    def test_topic_is_validated(self):
        for bad in ("", "-leading", "has space", "x" * 201, None, 5):
            with pytest.raises(ValidationError) as excinfo:
                normalize_event(topic=bad, producer="p", payload={})
            assert excinfo.value.code == "invalid_topic"

    def test_producer_is_validated(self):
        for bad in ("", "-x", "has space", "x" * 101, None):
            with pytest.raises(ValidationError) as excinfo:
                normalize_event(topic="t", producer=bad, payload={})
            assert excinfo.value.code == "invalid_producer"

    def test_causation_id_must_be_positive_int(self):
        for bad in (0, -1, "3", 1.0, True):
            with pytest.raises(ValidationError) as excinfo:
                normalize_event(topic="t", producer="p", payload={}, causation_id=bad)
            assert excinfo.value.code == "invalid_causation"

    def test_correlation_id_is_validated(self):
        with pytest.raises(ValidationError) as excinfo:
            normalize_event(topic="t", producer="p", payload={}, correlation_id="has space")
        assert excinfo.value.code == "invalid_correlation"

    def test_payload_size_is_bounded(self):
        with pytest.raises(ValidationError) as excinfo:
            normalize_event(
                topic="t",
                producer="p",
                payload={"blob": "x" * (MAX_CANONICAL_PAYLOAD_BYTES + 1)},
            )
        assert excinfo.value.code == "payload_too_large"

    def test_unencodable_payload_is_rejected(self):
        with pytest.raises(ValidationError) as excinfo:
            normalize_event(topic="t", producer="p", payload={"bad": float("nan")})
        assert excinfo.value.code == "unencodable_payload"


def test_describe_kind_covers_every_kind():
    for kind in KINDS:
        described = describe_kind(kind)
        assert described["kind"] == kind
        assert "purpose" in described
    with pytest.raises(ValidationError):
        describe_kind("nope")


def test_validation_error_serializes_to_stable_envelope():
    error = ValidationError("invalid_state", "bad", {"field": "state"})
    assert error.to_dict() == {
        "code": "invalid_state",
        "message": "bad",
        "detail": {"field": "state"},
    }

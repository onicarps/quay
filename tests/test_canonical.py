"""Canonical encoding and hash-chain tests (requirement F2, F3)."""

from __future__ import annotations

import hashlib
import json

import pytest

from quay.canonical import (
    GENESIS_HASH,
    CanonicalError,
    canonical_bytes,
    canonical_json,
    digest_many,
    event_digest,
    hash_hex,
)


def test_key_order_does_not_change_encoding():
    assert canonical_json({"b": 1, "a": 2}) == canonical_json({"a": 2, "b": 1})
    assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'


def test_encoding_has_no_insignificant_whitespace():
    encoded = canonical_json({"a": [1, 2], "b": {"c": None}})
    assert " " not in encoded
    assert "\n" not in encoded


def test_nested_structures_are_canonical():
    left = {"z": [{"b": 1, "a": 2}], "a": {"d": 4, "c": 3}}
    right = {"a": {"c": 3, "d": 4}, "z": [{"a": 2, "b": 1}]}
    assert canonical_bytes(left) == canonical_bytes(right)


def test_unicode_is_preserved_and_stable():
    assert canonical_json({"k": "héllo → ok"}) == '{"k":"héllo → ok"}'
    assert canonical_bytes({"k": "日本語"}) == '{"k":"日本語"}'.encode("utf-8")


def test_nan_and_infinity_are_rejected():
    for bad in (float("nan"), float("inf"), float("-inf")):
        with pytest.raises(CanonicalError) as excinfo:
            canonical_json({"v": bad})
        assert excinfo.value.code == "invalid_number"


def test_non_string_keys_are_rejected():
    with pytest.raises(CanonicalError) as excinfo:
        canonical_json({1: "value"})
    assert excinfo.value.code == "invalid_key"


def test_unencodable_types_are_rejected():
    with pytest.raises(CanonicalError) as excinfo:
        canonical_json({"v": object()})
    assert excinfo.value.code == "invalid_type"


def test_lone_surrogate_is_rejected():
    with pytest.raises(CanonicalError) as excinfo:
        canonical_json({"v": "\ud800"})
    assert excinfo.value.code == "invalid_string"


def test_tuples_encode_like_lists():
    assert canonical_json((1, 2)) == canonical_json([1, 2])


def test_negative_zero_normalizes_to_zero():
    assert canonical_json({"v": -0.0}) == canonical_json({"v": 0.0})


def test_event_digest_matches_manual_sha256():
    body = {"seq": 1, "payload": {"a": 1}}
    digest = event_digest(GENESIS_HASH, body)
    expected = hashlib.sha256(
        GENESIS_HASH.encode("ascii") + b"\x1f" + canonical_bytes(body)
    ).hexdigest()
    assert digest == expected
    assert digest == hash_hex(GENESIS_HASH.encode("ascii") + b"\x1f" + canonical_bytes(body))


def test_digest_changes_with_any_body_change():
    base = {"seq": 1, "payload": {"a": 1}}
    assert event_digest(GENESIS_HASH, base) != event_digest(GENESIS_HASH, {**base, "seq": 2})
    assert event_digest(GENESIS_HASH, base) != event_digest(GENESIS_HASH, {"seq": 1, "payload": {"a": 2}})


def test_digest_depends_on_previous_hash():
    body = {"seq": 2}
    assert event_digest(GENESIS_HASH, body) != event_digest("a" * 64, body)


def test_event_digest_rejects_malformed_previous_hash():
    for bad in ("", "xyz", "a" * 63, "g" * 64, None, 5):
        with pytest.raises(CanonicalError) as excinfo:
            event_digest(bad, {"seq": 1})  # type: ignore[arg-type]
        assert excinfo.value.code == "invalid_prev_hash"


def test_digest_many_chains_in_order():
    bodies = [{"seq": 1}, {"seq": 2}, {"seq": 3}]
    digests = digest_many(GENESIS_HASH, bodies)
    assert len(digests) == 3
    assert digests[0] == event_digest(GENESIS_HASH, bodies[0])
    assert digests[1] == event_digest(digests[0], bodies[1])
    assert digests[2] == event_digest(digests[1], bodies[2])


def test_encoding_round_trips_through_json():
    value = {"a": [1, 2.5, None, True], "b": {"c": "x"}}
    assert json.loads(canonical_json(value)) == value

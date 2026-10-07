"""Frame and envelope validation tests (requirements F9, F12)."""

from __future__ import annotations

import json
import socket
import struct

import pytest

from quay.protocol import (
    HEADER,
    MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    ProtocolError,
    encode_frame,
    make_error,
    make_push,
    make_request,
    make_response,
    read_frame,
    send_frame,
    validate_request,
    validate_response,
)


def socket_pair() -> tuple[socket.socket, socket.socket]:
    return socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)


class TestEncoding:
    def test_round_trip(self):
        sender, receiver = socket_pair()
        try:
            payload = {"type": "request", "id": "abc", "op": "health", "body": {"nested": [1, 2, {"a": "é"}]}}
            send_frame(sender, payload)
            assert read_frame(receiver) == payload
        finally:
            sender.close()
            receiver.close()

    def test_length_prefix_is_big_endian(self):
        frame = encode_frame({"a": 1})
        (size,) = HEADER.unpack(frame[:4])
        assert size == len(frame) - 4
        assert json.loads(frame[4:].decode()) == {"a": 1}

    def test_frame_is_deterministic(self):
        assert encode_frame({"b": 1, "a": 2}) == encode_frame({"a": 2, "b": 1})

    def test_unencodable_frame_is_rejected(self):
        with pytest.raises(ProtocolError) as excinfo:
            encode_frame({"bad": {1, 2}})
        assert excinfo.value.code == "unencodable_frame"

    def test_oversized_frame_is_rejected_before_sending(self):
        with pytest.raises(ProtocolError) as excinfo:
            encode_frame({"blob": "x" * (MAX_FRAME_BYTES + 10)})
        assert excinfo.value.code == "frame_too_large"

    def test_frame_at_the_limit_is_accepted(self):
        # Exercise the boundary explicitly with a small cap so the test stays fast.
        cap = 64
        overhead = len(b'{"blob":""}')
        body = b'{"blob":"' + b"x" * (cap - overhead) + b'"}'
        assert len(body) == cap
        assert encode_frame(json.loads(body), max_bytes=cap) == HEADER.pack(cap) + body
        sender, receiver = socket_pair()
        try:
            sender.sendall(HEADER.pack(cap) + body)
            value = read_frame(receiver, max_bytes=cap)
            assert value is not None and len(value["blob"]) == cap - overhead
        finally:
            sender.close()
            receiver.close()

    def test_frame_one_byte_over_the_limit_is_rejected(self):
        cap = 64
        with pytest.raises(ProtocolError) as excinfo:
            encode_frame({"blob": "x" * cap}, max_bytes=cap)
        assert excinfo.value.code == "frame_too_large"

    def test_oversized_length_prefix_is_refused_without_allocating(self):
        sender, receiver = socket_pair()
        try:
            sender.sendall(HEADER.pack(MAX_FRAME_BYTES + 1))
            with pytest.raises(ProtocolError) as excinfo:
                read_frame(receiver)
            assert excinfo.value.code == "invalid_frame_size"
        finally:
            sender.close()
            receiver.close()

    def test_clean_eof_between_frames_returns_none(self):
        sender, receiver = socket_pair()
        sender.close()
        try:
            assert read_frame(receiver) is None
        finally:
            receiver.close()

    def test_truncated_header_is_reported(self):
        sender, receiver = socket_pair()
        try:
            sender.sendall(b"\x00\x00")
            sender.shutdown(socket.SHUT_WR)
            with pytest.raises(ProtocolError) as excinfo:
                read_frame(receiver)
            assert excinfo.value.code == "truncated_frame"
        finally:
            sender.close()
            receiver.close()

    def test_truncated_body_is_reported(self):
        sender, receiver = socket_pair()
        try:
            sender.sendall(HEADER.pack(50) + b'{"partial"')
            sender.shutdown(socket.SHUT_WR)
            with pytest.raises(ProtocolError) as excinfo:
                read_frame(receiver)
            assert excinfo.value.code == "truncated_frame"
            assert excinfo.value.detail["received"] == len(b'{"partial"')
        finally:
            sender.close()
            receiver.close()

    def test_invalid_json_body_is_reported(self):
        sender, receiver = socket_pair()
        try:
            body = b"{not json}"
            sender.sendall(HEADER.pack(len(body)) + body)
            with pytest.raises(ProtocolError) as excinfo:
                read_frame(receiver)
            assert excinfo.value.code == "invalid_frame_json"
        finally:
            sender.close()
            receiver.close()

    def test_non_object_body_is_reported(self):
        sender, receiver = socket_pair()
        try:
            body = b"[1,2,3]"
            sender.sendall(HEADER.pack(len(body)) + body)
            with pytest.raises(ProtocolError) as excinfo:
                read_frame(receiver)
            assert excinfo.value.code == "invalid_frame_shape"
        finally:
            sender.close()
            receiver.close()


class TestBuilders:
    def test_make_request_shape(self):
        frame = make_request("emit", {"topic": "t"})
        assert frame["type"] == "request"
        assert frame["v"] == PROTOCOL_VERSION
        assert frame["op"] == "emit"
        assert frame["body"] == {"topic": "t"}
        assert len(frame["id"]) == 32

    def test_make_request_ids_are_unique(self):
        assert make_request("a")["id"] != make_request("a")["id"]

    def test_make_response_shape(self):
        frame = make_response("abc", {"ok": 1})
        assert frame == {
            "v": PROTOCOL_VERSION,
            "type": "response",
            "id": "abc",
            "ok": True,
            "result": {"ok": 1},
        }

    def test_make_error_shape(self):
        frame = make_error("abc", "bad_thing", "it broke", {"x": 1})
        assert frame["ok"] is False
        assert frame["error"] == {"code": "bad_thing", "message": "it broke", "detail": {"x": 1}}

    def test_make_push_rejects_unknown_type(self):
        with pytest.raises(ProtocolError):
            make_push("gossip", x=1)

    def test_protocol_error_serializes(self):
        assert ProtocolError("c", "m", {"a": 1}).to_dict() == {
            "code": "c",
            "message": "m",
            "detail": {"a": 1},
        }


class TestValidateRequest:
    def test_returns_id_op_body(self):
        frame = make_request("emit", {"a": 1}, request_id="fixed")
        assert validate_request(frame) == ("fixed", "emit", {"a": 1})

    @pytest.mark.parametrize(
        "mutation,code",
        [
            ({"type": "event"}, "not_a_request"),
            ({"v": "99"}, "unsupported_protocol"),
            ({"id": ""}, "invalid_request_id"),
            ({"id": 12}, "invalid_request_id"),
            ({"op": ""}, "invalid_operation"),
            ({"op": None}, "invalid_operation"),
            ({"body": []}, "invalid_body"),
            ({"extra": 1}, "unknown_request_field"),
        ],
    )
    def test_rejections(self, mutation, code):
        frame = make_request("health", request_id="x")
        frame.update(mutation)
        with pytest.raises(ProtocolError) as excinfo:
            validate_request(frame)
        assert excinfo.value.code == code

    def test_request_id_length_is_bounded(self):
        frame = make_request("health")
        frame["id"] = "a" * 129
        with pytest.raises(ProtocolError) as excinfo:
            validate_request(frame)
        assert excinfo.value.code == "invalid_request_id"


class TestValidateResponse:
    def test_accepts_success(self):
        ok, payload = validate_response(make_response("x", {"a": 1}))
        assert ok is True and payload == {"a": 1}

    def test_accepts_error(self):
        ok, payload = validate_response(make_error("x", "boom", "it broke"))
        assert ok is False
        assert payload["code"] == "boom"

    @pytest.mark.parametrize(
        "frame,code",
        [
            ({"type": "event"}, "not_a_response"),
            ({"type": "response", "v": "x", "id": "a", "ok": True, "result": {}}, "unsupported_protocol"),
            ({"type": "response", "v": PROTOCOL_VERSION, "ok": True, "result": {}}, "invalid_request_id"),
            ({"type": "response", "v": PROTOCOL_VERSION, "id": "a", "ok": True}, "invalid_result"),
            ({"type": "response", "v": PROTOCOL_VERSION, "id": "a", "ok": False}, "invalid_error"),
            ({"type": "response", "v": PROTOCOL_VERSION, "id": "a", "ok": "yes"}, "invalid_response"),
        ],
    )
    def test_rejections(self, frame, code):
        with pytest.raises(ProtocolError) as excinfo:
            validate_response(frame)
        assert excinfo.value.code == code


class TestConcurrencyOfFraming:
    def test_many_frames_in_one_stream_are_delimited(self):
        sender, receiver = socket_pair()
        try:
            expected = [{"seq": i, "payload": "x" * 100} for i in range(50)]
            for value in expected:
                send_frame(sender, value)
            received = [read_frame(receiver) for _ in range(50)]
            assert received == expected
        finally:
            sender.close()
            receiver.close()

    def test_partial_write_is_reassembled(self):
        sender, receiver = socket_pair()
        try:
            frame = encode_frame({"big": "y" * 5000})
            for index in range(0, len(frame), 97):
                sender.sendall(frame[index : index + 97])
            assert read_frame(receiver) == {"big": "y" * 5000}
        finally:
            sender.close()
            receiver.close()

    def test_struct_header_size(self):
        assert struct.calcsize("!I") == HEADER.size == 4

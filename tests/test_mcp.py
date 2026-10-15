"""End-to-end tests for the standard-library stdio MCP bridge (``quay mcp``).

Every test drives a real subprocess over stdin/stdout against a real workspace
daemon. Nothing is unit-tested through private helpers, so the wire behaviour
is what a client (Cursor, Claude Code, Codex, ...) actually sees.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from quay.daemon import stop_daemon

SRC = Path(__file__).resolve().parents[1] / "src"

TOOL_NAMES = ["quay_emit", "quay_log", "quay_status", "quay_work_claim", "quay_work_complete"]


def bridge_env() -> dict[str, str]:
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{SRC}{os.pathsep}{existing}" if existing else str(SRC)
    return env


class McpSession:
    """A live ``quay mcp`` subprocess speaking line-delimited JSON-RPC."""

    def __init__(self, workspace: Path) -> None:
        self.process = subprocess.Popen(
            [sys.executable, "-m", "quay.cli", "mcp", "--workspace", str(workspace)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=bridge_env(),
        )
        self._next_id = 0

    # ------------------------------------------------------------------ write

    def _send(self, message: dict) -> dict:
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()
        return self._read()

    def _read(self) -> dict:
        assert self.process.stdout is not None
        line = self.process.stdout.readline()
        assert line, f"bridge closed early: {self.process.stderr.read() if self.process.stderr else ''}"
        return json.loads(line)

    def request(self, method: str, params: object = None) -> dict:
        self._next_id += 1
        message: dict = {"jsonrpc": "2.0", "id": self._next_id, "method": method}
        if params is not None:
            message["params"] = params
        return self._send(message)

    def notify(self, method: str, params: object = None) -> None:
        message: dict = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def send_raw(self, raw: str) -> dict:
        assert self.process.stdin is not None
        self.process.stdin.write(raw + "\n")
        self.process.stdin.flush()
        return self._read()

    def call(self, name: str, arguments: dict) -> dict:
        return self.request("tools/call", {"name": name, "arguments": arguments})

    # ----------------------------------------------------------------- teardown

    def close(self) -> None:
        if self.process.stdin is not None:
            self.process.stdin.close()
        self.process.wait(timeout=30)

    def __enter__(self) -> "McpSession":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


@pytest.fixture()
def workspace(tmp_path: Path) -> Path:
    root = tmp_path / "mcp-workspace"
    root.mkdir()
    return root


@pytest.fixture()
def session(workspace: Path):
    item = McpSession(workspace)
    try:
        yield item
    finally:
        item.close()
        stop_daemon(workspace)


def text_of(response: dict) -> str:
    return response["result"]["content"][0]["text"]


def payload_of(response: dict) -> dict:
    return json.loads(text_of(response))


class TestMcpProtocol:
    def test_initialize_negotiates_the_proposed_version(self, session: McpSession):
        response = session.request(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0"},
            },
        )
        result = response["result"]
        assert result["protocolVersion"] == "2025-06-18"
        assert result["serverInfo"]["name"] == "quay"
        assert "tools" in result["capabilities"]

    def test_initialize_falls_back_when_no_version_is_proposed(self, session: McpSession):
        result = session.request("initialize", {})["result"]
        assert result["protocolVersion"]

    def test_tools_list_exposes_exactly_the_documented_tools(self, session: McpSession):
        tools = session.request("tools/list")["result"]["tools"]
        assert [tool["name"] for tool in tools] == TOOL_NAMES
        for tool in tools:
            schema = tool["inputSchema"]
            assert schema["type"] == "object"
            assert isinstance(schema["required"], list)

    def test_tools_list_identifies_the_required_fields(self, session: McpSession):
        schemas = {tool["name"]: tool["inputSchema"] for tool in session.request("tools/list")["result"]["tools"]}
        assert schemas["quay_emit"]["required"] == ["topic", "payload"]
        assert schemas["quay_log"]["required"] == ["limit"]
        assert schemas["quay_status"]["required"] == []
        assert schemas["quay_work_claim"]["required"] == ["key", "executor"]
        assert schemas["quay_work_complete"]["required"] == [
            "key", "executor", "evidence", "fencing_token"
        ]

    def test_notifications_get_no_reply(self, session: McpSession):
        session.notify("notifications/initialized")
        response = session.request("tools/list")
        assert response["id"] == 1  # the notification produced no line of its own

    def test_unknown_method_is_a_jsonrpc_error(self, session: McpSession):
        response = session.request("does/not/exist")
        assert response["error"]["code"] == -32601
        assert "does/not/exist" in response["error"]["message"]

    def test_malformed_json_is_a_parse_error(self, session: McpSession):
        response = session.send_raw("{not json")
        assert response["error"]["code"] == -32700
        assert response["id"] is None

    def test_non_object_params_are_rejected(self, session: McpSession):
        response = session.request("tools/call", "nope")
        assert response["error"]["code"] == -32602

    def test_missing_jsonrpc_version_is_rejected(self, session: McpSession):
        response = session.send_raw(json.dumps({"id": 1, "method": "tools/list"}))
        assert response["error"]["code"] == -32600

    def test_the_server_survives_a_bad_request(self, session: McpSession):
        session.send_raw("{not json")
        assert session.request("tools/list")["result"]["tools"]


class TestMcpTools:
    def test_status_reports_the_auto_initialized_workspace(self, session: McpSession, workspace: Path):
        status = payload_of(session.call("quay_status", {}))
        assert status["daemon"]["workspace"] == str(workspace)
        assert status["ledger"]["records"] == 1
        assert status["integrity"]["hash_chain"] in ("unverified", "verified")

    def test_emit_appends_a_typed_event(self, session: McpSession):
        event = payload_of(
            session.call(
                "quay_emit",
                {"topic": "quay/demo", "payload": {"state": "pending", "summary": "hello"}, "kind": "signal"},
            )
        )["event"]
        assert event["kind"] == "signal"
        assert event["topic"] == "quay/demo"
        assert event["producer"] == "quay"
        assert event["payload"] == {"state": "pending", "summary": "hello"}

    def test_log_returns_the_bounded_ledger(self, session: McpSession):
        session.call("quay_emit", {"topic": "t", "payload": {"i": 1}})
        result = payload_of(session.call("quay_log", {"limit": 2, "order": "asc"}))
        assert [event["seq"] for event in result["events"]] == [1, 2]

    def test_work_claim_then_complete(self, session: McpSession):
        claimed = payload_of(session.call("quay_work_claim", {"key": "k", "executor": "factory"}))["event"]
        assert claimed["payload"]["state"] == "claimed"
        assert claimed["kind"] == "work"
        done = payload_of(
            session.call(
                "quay_work_complete",
                {
                    "key": "k",
                    "executor": "factory",
                    "evidence": {"ok": True},
                    "causation_id": claimed["seq"],
                    "fencing_token": claimed["payload"]["fencing_token"],
                },
            )
        )["event"]
        assert done["payload"]["state"] == "done"
        assert done["payload"]["evidence"] == {"ok": True}

    def test_domain_validation_failure_is_a_tool_error(self, session: McpSession):
        response = session.call("quay_emit", {"topic": "t", "payload": {"nope": 1}, "kind": "signal"})
        assert response["result"]["isError"] is True
        assert "unknown_field" in text_of(response)

    def test_missing_required_field_is_a_tool_error(self, session: McpSession):
        response = session.call("quay_emit", {"topic": "t"})
        assert response["result"]["isError"] is True
        assert "payload" in text_of(response)

    def test_unknown_tool_is_a_tool_error(self, session: McpSession):
        response = session.call("nope", {})
        assert response["result"]["isError"] is True
        assert "unknown tool" in text_of(response)

    def test_log_limit_is_bounded_with_a_tool_error(self, session: McpSession):
        response = session.call("quay_log", {"limit": 0})
        assert response["result"]["isError"] is True
        assert "limit" in text_of(response)

    def test_work_complete_requires_non_empty_evidence_in_a_tool_error(self, session: McpSession):
        response = session.call(
            "quay_work_complete",
            {"key": "k", "executor": "e", "evidence": {}, "fencing_token": 1},
        )
        assert response["result"]["isError"] is True
        assert "evidence" in text_of(response)

    def test_unknown_arguments_are_returned_as_a_tool_error(self, session: McpSession):
        response = session.call("quay_status", {"path": "/etc/passwd"})
        assert response["result"]["isError"] is True
        assert "unknown field" in text_of(response)

    def test_unknown_tools_call_parameter_is_a_protocol_error(self, session: McpSession):
        response = session.request("tools/call", {"name": "quay_status", "arguments": {}, "extra": True})
        assert response["error"]["code"] == -32602

    def test_invalid_request_id_is_a_protocol_error(self, session: McpSession):
        response = session.send_raw(
            json.dumps({"jsonrpc": "2.0", "id": {"not": "valid"}, "method": "tools/list"})
        )
        assert response["error"]["code"] == -32600
        assert response["id"] is None

"""A dependency-free stdio MCP bridge for the Quay ledger.

The bridge speaks line-delimited JSON-RPC 2.0 on stdin/stdout and exposes a
small set of typed tools. Every tool writes through the same workspace daemon
the CLI uses, so schema validation, the hash chain, and the plane projections
behave identically. Only the Python standard library and Quay's own client are
used; no MCP package is required.

Protocol traffic stays on stdout. Diagnostics go to stderr, so a client can
parse stdout without filtering.

Run it directly::

    quay mcp --workspace /path/to/workspace
    python -m quay.mcp --workspace /path/to/workspace
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Callable, Mapping, Sequence

from . import __version__
from .client import ClientError, QuayClient
from .daemon import ensure_daemon
from .onboarding import CLIENT_TIMEOUT_SECONDS, onboard
from .store import StoreError
from .workspace import WorkspaceError

#: JSON-RPC 2.0 error codes used by the bridge.
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

JSONRPC_VERSION = "2.0"

#: MCP protocol revision advertised when the client does not propose one.
DEFAULT_PROTOCOL_VERSION = "2025-06-18"

SERVER_NAME = "quay"

#: Default producer for events that do not name one.
DEFAULT_PRODUCER = "quay"

MAX_LOG_LIMIT = 10_000

__all__ = ["Bridge", "TOOLS", "serve", "main", "EXIT_USAGE"]

EXIT_OK = 0
EXIT_USAGE = 2

#: Tools exposed to the client. ``required`` is the contract the bridge
#: enforces before a call reaches the daemon.
TOOLS: tuple[dict[str, Any], ...] = (
    {
        "name": "quay_emit",
        "description": "Append one typed event to the workspace ledger.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string", "description": "event topic, for example 'quay/demo'"},
                "payload": {"type": "object", "description": "typed event payload"},
                "kind": {
                    "type": "string",
                    "description": "signal, work, note, metric, or generic (default: generic)",
                },
                "producer": {"type": "string", "description": "producer identity (default: quay)"},
                "causation_id": {"type": "integer", "minimum": 1},
                "correlation_id": {"type": "string"},
            },
            "required": ["topic", "payload"],
            "additionalProperties": False,
        },
    },
    {
        "name": "quay_log",
        "description": "Return the most recent ledger events.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_LOG_LIMIT,
                    "description": "maximum events to return",
                },
                "order": {"type": "string", "enum": ["asc", "desc"], "description": "default: desc"},
            },
            "required": ["limit"],
            "additionalProperties": False,
        },
    },
    {
        "name": "quay_status",
        "description": "Return daemon, ledger, integrity, and plane status.",
        "inputSchema": {"type": "object", "properties": {}, "required": [], "additionalProperties": False},
    },
    {
        "name": "quay_work_claim",
        "description": "Claim a unit of work, starting its lease.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key": {"type": "string"},
                "executor": {"type": "string"},
                "topic": {"type": "string", "description": "default: work/queue"},
                "title": {"type": "string"},
                "lease_seconds": {"type": "integer", "minimum": 1},
                "causation_id": {"type": "integer", "minimum": 1},
                "correlation_id": {"type": "string"},
            },
            "required": ["key", "executor"],
            "additionalProperties": False,
        },
    },
    {
        "name": "quay_work_complete",
        "description": "Mark a unit of work done with non-empty evidence.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "key": {"type": "string"},
                "executor": {"type": "string"},
                "evidence": {"type": "object", "minProperties": 1},
                "topic": {"type": "string", "description": "default: work/queue"},
                "causation_id": {"type": "integer", "minimum": 1},
                "correlation_id": {"type": "string"},
            },
            "required": ["key", "executor", "evidence"],
            "additionalProperties": False,
        },
    },
)

TOOL_BY_NAME: dict[str, dict[str, Any]] = {tool["name"]: tool for tool in TOOLS}


class JsonRpcError(Exception):
    """A protocol-level failure that must be reported as a JSON-RPC error."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


# --------------------------------------------------------------- validation


def _check_type(expected: str, value: Any, spec: Mapping[str, Any]) -> str | None:
    if expected == "object":
        if not isinstance(value, dict):
            return "must be a JSON object"
        if spec.get("minProperties") and not value:
            return "must be a non-empty object"
    elif expected == "string":
        if not isinstance(value, str) or not value:
            return "must be a non-empty string"
        allowed = spec.get("enum")
        if allowed is not None and value not in allowed:
            return f"must be one of {', '.join(allowed)}"
    elif expected == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            return "must be an integer"
        if "minimum" in spec and value < spec["minimum"]:
            return f"must be >= {spec['minimum']}"
        if "maximum" in spec and value > spec["maximum"]:
            return f"must be <= {spec['maximum']}"
    elif expected == "boolean":
        if not isinstance(value, bool):
            return "must be a boolean"
    return None


def check_arguments(schema: Mapping[str, Any], arguments: Mapping[str, Any]) -> str | None:
    """Return a human-readable problem with ``arguments``, or None if valid."""
    properties = schema.get("properties") or {}
    for field in schema.get("required") or ():
        if field not in arguments:
            return f"missing required field: {field}"
    if schema.get("additionalProperties") is False:
        unknown = sorted(set(arguments) - set(properties))
        if unknown:
            return f"unknown field(s): {', '.join(unknown)}"
    for name, value in arguments.items():
        spec = properties.get(name)
        if not isinstance(spec, Mapping) or "type" not in spec:
            continue
        problem = _check_type(str(spec["type"]), value, spec)
        if problem is not None:
            return f"'{name}' {problem}"
    return None


# ---------------------------------------------------------------- transport


def _write_message(stream, message: Mapping[str, Any]) -> None:
    stream.write(json.dumps(message, ensure_ascii=False) + "\n")
    stream.flush()


def _response(request_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": JSONRPC_VERSION, "id": request_id, "result": result}


def _error(request_id: Any, code: int, message: str, data: Any = None) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": JSONRPC_VERSION, "id": request_id, "error": error}


def _tool_text(value: Any) -> dict[str, Any]:
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True, ensure_ascii=False)
    return {"content": [{"type": "text", "text": text}], "isError": False}


def _tool_error(message: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": message}], "isError": True}


# ------------------------------------------------------------------ bridge


class Bridge:
    """One MCP session bound to one workspace."""

    def __init__(self, workspace: str | os.PathLike[str] | None = None, *, idle_seconds: float | None = None) -> None:
        self.onboarding = onboard(workspace if workspace is not None else os.getcwd(), idle_seconds=idle_seconds)
        self.root = self.onboarding.root
        self._client: QuayClient | None = None
        self._methods: dict[str, Callable[[Mapping[str, Any]], Any]] = {
            "initialize": self._method_initialize,
            "ping": self._method_ping,
            "tools/list": self._method_tools_list,
            "tools/call": self._method_tools_call,
        }

    # ------------------------------------------------------------- lifecycle

    def client(self) -> QuayClient:
        if self._client is None:
            self._client = QuayClient(self.onboarding.paths.socket, timeout=CLIENT_TIMEOUT_SECONDS).connect()
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def request(self, op: str, body: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Call the daemon, reconnecting once if the socket went away."""
        try:
            return self.client().request(op, body)
        except ClientError as exc:
            if exc.code not in ("connection_failed", "daemon_closed", "transport_error", "timeout"):
                raise
            self.close()
            paths, _health = ensure_daemon(self.root)
            self._client = QuayClient(paths.socket, timeout=CLIENT_TIMEOUT_SECONDS).connect()
            return self._client.request(op, body)

    # -------------------------------------------------------------- protocol

    def handle(self, message: Any) -> dict[str, Any] | None:
        """Handle one decoded JSON-RPC message, returning a reply or None."""
        if not isinstance(message, dict):
            return _error(None, INVALID_REQUEST, "request must be a JSON object")
        has_id = "id" in message
        request_id = message.get("id") if has_id else None
        if has_id and (
            isinstance(request_id, bool)
            or not isinstance(request_id, (str, int, float, type(None)))
        ):
            return _error(None, INVALID_REQUEST, "id must be a string, number, or null")
        if message.get("jsonrpc") != JSONRPC_VERSION:
            return _error(request_id, INVALID_REQUEST, "jsonrpc must be '2.0'")
        method = message.get("method")
        if not isinstance(method, str) or not method:
            return _error(request_id, INVALID_REQUEST, "method must be a non-empty string")
        params = message.get("params")
        if params is None:
            params = {}
        if not isinstance(params, dict):
            return _error(request_id, INVALID_PARAMS, "params must be a JSON object")
        if not has_id:
            # Notifications never receive a reply, including unknown ones.
            return None
        handler = self._methods.get(method)
        if handler is None:
            return _error(request_id, METHOD_NOT_FOUND, f"unknown method: {method}")
        try:
            result = handler(params)
        except JsonRpcError as exc:
            return _error(request_id, exc.code, exc.message, exc.data)
        except (ClientError, StoreError, WorkspaceError) as exc:
            return _error(request_id, INTERNAL_ERROR, f"{type(exc).__name__}: {exc}")
        except Exception as exc:  # pragma: no cover - defensive boundary
            return _error(request_id, INTERNAL_ERROR, f"{type(exc).__name__}: {exc}")
        return _response(request_id, result)

    # --------------------------------------------------------------- methods

    def _method_initialize(self, params: Mapping[str, Any]) -> dict[str, Any]:
        proposed = params.get("protocolVersion")
        version = proposed if isinstance(proposed, str) and proposed else DEFAULT_PROTOCOL_VERSION
        return {
            "protocolVersion": version,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": SERVER_NAME, "version": __version__},
        }

    def _method_ping(self, params: Mapping[str, Any]) -> dict[str, Any]:
        return {}

    def _method_tools_list(self, params: Mapping[str, Any]) -> dict[str, Any]:
        return {"tools": [dict(tool) for tool in TOOLS]}

    def _method_tools_call(self, params: Mapping[str, Any]) -> dict[str, Any]:
        unknown = sorted(set(params) - {"name", "arguments"})
        if unknown:
            raise JsonRpcError(INVALID_PARAMS, f"unknown tools/call parameter(s): {', '.join(unknown)}")
        name = params.get("name")
        if not isinstance(name, str) or not name:
            raise JsonRpcError(INVALID_PARAMS, "tools/call requires a 'name' string")
        arguments = params.get("arguments")
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, dict):
            raise JsonRpcError(INVALID_PARAMS, "'arguments' must be a JSON object")
        tool = TOOL_BY_NAME.get(name)
        if tool is None:
            return _tool_error(f"unknown tool: {name}")
        problem = check_arguments(tool["inputSchema"], arguments)
        if problem is not None:
            return _tool_error(f"invalid arguments for {name}: {problem}")
        try:
            return _tool_text(_TOOL_HANDLERS[name](self, arguments))
        except ClientError as exc:
            # Domain rejections (schema, leases, unknown fields) are tool results.
            return _tool_error(f"{exc.code}: {exc.message}")
        except (StoreError, WorkspaceError) as exc:
            return _tool_error(f"{getattr(exc, 'code', 'error')}: {exc}")


# ------------------------------------------------------------ tool handlers


def _tool_quay_emit(bridge: Bridge, arguments: Mapping[str, Any]) -> dict[str, Any]:
    body = {
        "topic": arguments["topic"],
        "producer": arguments.get("producer") or DEFAULT_PRODUCER,
        "payload": arguments.get("payload", {}),
        "kind": arguments.get("kind", "generic"),
        "causation_id": arguments.get("causation_id"),
        "correlation_id": arguments.get("correlation_id"),
    }
    return {"event": bridge.request("emit", body)["event"]}


def _tool_quay_log(bridge: Bridge, arguments: Mapping[str, Any]) -> dict[str, Any]:
    body = {"limit": arguments["limit"], "order": arguments.get("order", "desc")}
    return bridge.request("log", body)


def _tool_quay_status(bridge: Bridge, arguments: Mapping[str, Any]) -> dict[str, Any]:
    return bridge.request("status", {})


def _tool_quay_work_claim(bridge: Bridge, arguments: Mapping[str, Any]) -> dict[str, Any]:
    body: dict[str, Any] = {"key": arguments["key"], "executor": arguments["executor"]}
    for optional in ("topic", "title", "lease_seconds", "causation_id", "correlation_id"):
        if arguments.get(optional) is not None:
            body[optional] = arguments[optional]
    return {"event": bridge.request("work_claim", body)["event"]}


def _tool_quay_work_complete(bridge: Bridge, arguments: Mapping[str, Any]) -> dict[str, Any]:
    body: dict[str, Any] = {
        "key": arguments["key"],
        "executor": arguments["executor"],
        "evidence": arguments["evidence"],
    }
    for optional in ("topic", "causation_id", "correlation_id"):
        if arguments.get(optional) is not None:
            body[optional] = arguments[optional]
    return {"event": bridge.request("work_complete", body)["event"]}


_TOOL_HANDLERS: dict[str, Callable[[Bridge, Mapping[str, Any]], Any]] = {
    "quay_emit": _tool_quay_emit,
    "quay_log": _tool_quay_log,
    "quay_status": _tool_quay_status,
    "quay_work_claim": _tool_quay_work_claim,
    "quay_work_complete": _tool_quay_work_complete,
}


# ------------------------------------------------------------------- server


def serve(
    workspace: str | os.PathLike[str] | None = None,
    *,
    stdin=None,
    stdout=None,
    stderr=None,
    idle_seconds: float | None = None,
) -> int:
    """Serve the workspace over stdio until stdin closes."""
    input_stream = stdin if stdin is not None else sys.stdin
    output_stream = stdout if stdout is not None else sys.stdout
    error_stream = stderr if stderr is not None else sys.stderr

    try:
        bridge = Bridge(workspace, idle_seconds=idle_seconds)
    except (WorkspaceError, StoreError, ClientError, OSError) as exc:
        print(f"quay mcp: error: {exc}", file=error_stream)
        return EXIT_USAGE

    try:
        for raw in input_stream:
            line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError as exc:
                _write_message(output_stream, _error(None, PARSE_ERROR, f"parse error: {exc.msg}"))
                continue
            reply = bridge.handle(message)
            if reply is not None:
                _write_message(output_stream, reply)
    except KeyboardInterrupt:  # pragma: no cover - interactive interrupt
        return EXIT_OK
    finally:
        bridge.close()
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for ``python -m quay.mcp``."""
    parser = argparse.ArgumentParser(prog="quay mcp", description="Serve a Quay workspace over stdio MCP.")
    parser.add_argument("--workspace", default=None, help="workspace root (default: current directory)")
    parser.add_argument("--idle-seconds", type=float, default=None, help="daemon idle timeout")
    parser.add_argument("--version", action="version", version=f"quay mcp (quay {__version__})")
    args = parser.parse_args(list(argv) if argv is not None else None)
    return serve(args.workspace, idle_seconds=args.idle_seconds)


if __name__ == "__main__":  # pragma: no cover - exercised via subprocess
    raise SystemExit(main())

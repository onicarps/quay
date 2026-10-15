"""The ``quay`` command line interface.

Output policy
-------------
Every command can emit machine-readable JSON with ``--json``, and Quay emits
JSON automatically when stdout is not a terminal. That means a pipeline gets
parseable output with no extra flags, and a human gets a readable line.

Exit codes
----------
``0`` success · ``1`` the requested check failed (for example ``verify``)
``2`` usage, validation, or workspace error · ``3`` the daemon is unreachable
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import io
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from . import __version__
from .client import ClientError, QuayClient, SubscriptionStream
from .daemon import all_daemon_status, daemon_health, ensure_daemon, stop_daemon
from .onboarding import onboard
from .store import LedgerStore, StoreError, utc_now
from .tui import TuiApp
from .workspace import (
    STATE_DIRECTORY,
    WorkspaceError,
    canonical_workspace,
    git_advisory,
    paths_for,
)

EXIT_OK = 0
EXIT_CHECK_FAILED = 1
EXIT_USAGE = 2
EXIT_UNAVAILABLE = 3

__all__ = ["main", "build_parser", "emit_result"]


# --------------------------------------------------------------------- helpers


def _json_mode(args: argparse.Namespace) -> bool:
    """Decide the output format.

    Precedence: explicit ``--no-json`` wins, then explicit ``--json``, then the
    default of "JSON when stdout is not a terminal".
    """
    if getattr(args, "no_json", False):
        return False
    explicit = getattr(args, "json", None)
    if explicit is True:
        return True
    if explicit is False:
        return False
    return not sys.stdout.isatty()


def emit_result(data: Any, *, json_mode: bool, human: str | None = None) -> None:
    """Write a command result as JSON or as a human line."""
    if json_mode:
        json.dump(data, sys.stdout, sort_keys=True, ensure_ascii=False)
        sys.stdout.write("\n")
    else:
        sys.stdout.write((human if human is not None else json.dumps(data, sort_keys=True)) + "\n")
    sys.stdout.flush()


def emit_error(code: str, message: str, *, json_mode: bool, detail: Mapping[str, Any] | None = None) -> None:
    payload = {"error": {"code": code, "message": message, "detail": dict(detail or {})}}
    if json_mode:
        json.dump(payload, sys.stderr, sort_keys=True, ensure_ascii=False)
        sys.stderr.write("\n")
    else:
        sys.stderr.write(f"quay: error: {message}\n")
    sys.stderr.flush()


def workspace_path(args: argparse.Namespace, *, create: bool = True) -> Path:
    root = canonical_workspace(getattr(args, "workspace", None) or os.getcwd())
    if create:
        paths_for(root, create=True)
    return root


def connected(args: argparse.Namespace) -> QuayClient:
    """Return a client for the workspace daemon, starting one if needed."""
    root = workspace_path(args)
    paths, _health = ensure_daemon(root, idle_seconds=getattr(args, "idle_seconds", None))
    return QuayClient(paths.socket, timeout=getattr(args, "timeout", 15.0)).connect()


def _payload_from_text(text: str | None) -> dict[str, Any]:
    if text is None or text == "":
        return {}
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise WorkspaceError("invalid_payload", f"payload is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise WorkspaceError("invalid_payload", "payload must be a JSON object")
    return value


# ---------------------------------------------------------------------- parser


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # pragma: no cover - argparse path
        emit_error("usage", message, json_mode=not sys.stdout.isatty())
        raise SystemExit(EXIT_USAGE)


def _add_cockpit_parser(
    subparsers: argparse._SubParsersAction,
    common: argparse.ArgumentParser,
    name: str,
    help_text: str,
) -> argparse.ArgumentParser:
    """Register one cockpit command (``watch`` or its ``monitor`` alias).

    Both names are built from this one definition and share :func:`cmd_watch`,
    so ``monitor`` stays a true alias instead of a divergent copy.
    """
    node = subparsers.add_parser(name, parents=[common], help=help_text)
    node.add_argument("path", nargs="?", default=None, help="workspace directory (default: current directory)")
    node.add_argument("--kind", default=None, help="initial kind filter")
    node.add_argument("--plain", action="store_true", help="stream JSON lines instead of the TUI")
    node.add_argument("--history", type=int, default=200)
    node.add_argument("--max-events", type=int, default=None, help="plain mode: stop after N events")
    node.add_argument("--for", dest="duration", type=float, default=None, help="plain mode: stop after N seconds")
    return node


def build_parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="quay",
        description="Quay: a local-first streaming event ledger and terminal cockpit.",
    )
    parser.add_argument("--version", action="version", version=f"quay {__version__}")
    parser.add_argument(
        "--workspace",
        default=None,
        help=f"workspace root (default: current directory; state lives in ./{STATE_DIRECTORY})",
    )
    json_group = parser.add_mutually_exclusive_group()
    json_group.add_argument("--json", action="store_true", dest="json", default=None, help="force JSON output")
    json_group.add_argument("--no-json", action="store_false", dest="json", help="force human output")
    parser.add_argument("--timeout", type=float, default=15.0, help="client request timeout in seconds")

    subparsers = parser.add_subparsers(dest="command")

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--workspace", default=argparse.SUPPRESS)
    common.add_argument("--json", action="store_true", dest="json", default=argparse.SUPPRESS)
    common.add_argument("--no-json", action="store_true", dest="no_json", default=argparse.SUPPRESS)
    common.add_argument("--timeout", type=float, default=argparse.SUPPRESS)

    init_parser = subparsers.add_parser(
        "init", parents=[common], help="initialize the runtime ledger and tracked OKF knowledge plane"
    )
    init_parser.add_argument(
        "--bare", action="store_true", help="initialize runtime state without tracked OKF files"
    )
    # Retained as compatibility no-ops: explicit init now ignores .quay/ safely by default.
    init_parser.add_argument("--git-ignore", action="store_true", help=argparse.SUPPRESS)
    init_parser.add_argument("--yes", action="store_true", help=argparse.SUPPRESS)

    distill = subparsers.add_parser(
        "distill", parents=[common], help="materialize acknowledged handoffs into bounded OKF files"
    )
    output = distill.add_mutually_exclusive_group()
    output.add_argument("--stdout", action="store_true", help="render Markdown without writing files")
    output.add_argument("--out", default=None, help="knowledge-plane output root (default: okf/)")
    distill.add_argument(
        "--force-worktree", action="store_true", help="allow writes from a linked Git worktree"
    )
    distill.add_argument("--limit", type=int, default=100, help="maximum handoffs per run (1-1000)")

    start = subparsers.add_parser("start", parents=[common], help="start the daemon for this workspace")
    start.add_argument("--idle-seconds", type=float, default=None, help="idle shutdown timeout (0 disables)")

    stop = subparsers.add_parser("stop", parents=[common], help="stop this workspace's daemon")
    stop.add_argument("--all", action="store_true", help="stop every registered workspace daemon")
    stop.add_argument("--yes", action="store_true", help="confirm stopping all daemons")

    subparsers.add_parser("restart", parents=[common], help="stop and start the daemon")
    subparsers.add_parser("status", parents=[common], help="daemon and plane status")
    subparsers.add_parser("daemons", parents=[common], help="list registered workspace daemons")
    subparsers.add_parser("doctor", parents=[common], help="check the local install and workspace health")
    subparsers.add_parser("kinds", parents=[common], help="describe the event schemas")
    subparsers.add_parser("verify", parents=[common], help="recompute the ledger hash chain")

    emit = subparsers.add_parser("emit", parents=[common], help="append one event")
    emit.add_argument("topic")
    emit.add_argument("payload", nargs="?", help="JSON object payload")
    emit.add_argument("--kind", default="generic")
    emit.add_argument("--producer", default=None)
    emit.add_argument("--causation", type=int, default=None)
    emit.add_argument("--root-causation", type=int, default=None)
    emit.add_argument("--correlation", default=None)
    emit.add_argument("--correlation-id", dest="correlation", default=None)

    work = subparsers.add_parser("work", parents=[common], help="manage the work plane")
    work_sub = work.add_subparsers(dest="work_command")
    for name, state, help_text in (
        ("claim", "claimed", "claim a unit of work (starts the lease)"),
        ("heartbeat", "in_flight", "extend an in-flight lease"),
        ("complete", "done", "mark work done with evidence"),
        ("abandon", "abandoned", "abandon work with a reason"),
    ):
        node = work_sub.add_parser(name, parents=[common], help=help_text)
        node.add_argument("key")
        node.add_argument("--executor", required=True)
        node.add_argument("--topic", default="work/queue")
        node.add_argument("--title", default=None)
        node.add_argument("--lease-seconds", type=int, default=None)
        node.add_argument("--causation", type=int, default=None)
        node.add_argument("--root-causation", type=int, default=None)
        node.add_argument("--correlation", default=None)
        if state != "claimed":
            node.add_argument("--fencing-token", type=int, required=True)
        if state in ("done", "failed"):
            node.add_argument("--evidence", required=True, help="JSON object of proof")
        if state in ("abandoned", "expired"):
            node.add_argument("--reason", required=True)
    list_parser = work_sub.add_parser("list", parents=[common], help="list work records and expired leases")
    list_parser.add_argument("--state", default=None)
    list_parser.add_argument("--executor", default=None)
    work_sub.add_parser("reap", parents=[common], help="append expired events for lapsed leases")

    query = subparsers.add_parser("query", parents=[common], help="search the ledger")
    query.add_argument("--kind", default=None)
    query.add_argument("--topic", default=None, help="topic glob, e.g. 'okf/*'")
    query.add_argument("--producer", default=None)
    query.add_argument("--state", default=None)
    query.add_argument("--correlation", dest="correlation_id", default=None)
    query.add_argument("--causation", type=int, default=None)
    query.add_argument("--root-causation", type=int, default=None)
    query.add_argument("--since-seq", type=int, default=None)
    query.add_argument("--until-seq", type=int, default=None)
    query.add_argument("--since", dest="since_ts", default=None, help="ISO timestamp lower bound")
    query.add_argument("--until", dest="until_ts", default=None, help="ISO timestamp upper bound")
    query.add_argument("--text", default=None)
    query.add_argument("--limit", type=int, default=50)
    query.add_argument("--offset", type=int, default=0)
    query.add_argument("--order", choices=("asc", "desc"), default="asc")
    query.add_argument("--count-only", action="store_true")
    query.add_argument("--export", choices=("jsonl", "csv"), default=None)

    log = subparsers.add_parser("log", parents=[common], help="show recent events")
    log.add_argument("--limit", type=int, default=30)
    log.add_argument("--order", choices=("asc", "desc"), default="desc")
    log.add_argument("--follow", action="store_true", help="keep streaming new events")
    log.add_argument("--interval", type=float, default=0.5, help="poll interval for --follow")
    log.add_argument("--for", dest="duration", type=float, default=None, help="stop after N seconds")
    log.add_argument("--max-events", type=int, default=None, help="stop after N streamed events")

    _add_cockpit_parser(subparsers, common, "watch", "attach the live cockpit")
    _add_cockpit_parser(subparsers, common, "monitor", "attach the live cockpit (alias for watch)")

    subparsers.add_parser("mcp", parents=[common], help="serve the workspace over a stdio MCP bridge")

    prune = subparsers.add_parser("prune", parents=[common], help="delete old events after a checkpoint")
    prune.add_argument("--keep-last", type=int, default=None)
    prune.add_argument("--before", default=None, help="delete events older than this ISO timestamp")
    prune.add_argument("--dry-run", action="store_true")
    prune.add_argument("--yes", action="store_true", help="confirm the deletion")

    bench = subparsers.add_parser("bench", parents=[common], help="run the benchmark suite")
    bench.add_argument("--scale", choices=("quick", "full"), default="quick")
    bench.add_argument("--out", default=None, help="also write the JSON report to this path")

    import_parser = subparsers.add_parser(
        "import", parents=[common], help="import an existing Changi ledger into this workspace"
    )
    import_parser.add_argument("source", help="path to the Changi events.db (or its .changi directory)")
    import_parser.add_argument("--dry-run", action="store_true", help="report without writing")
    import_parser.add_argument("--max-warnings", type=int, default=20, help="warnings to print in human output")

    return parser


# ------------------------------------------------------------------- commands


def cmd_init(args: argparse.Namespace) -> int:
    from .cursors import CursorStore
    from .scaffold import ensure_runtime_ignored, scaffold_workspace

    json_mode = _json_mode(args)
    root = workspace_path(args, create=False)
    scaffold_time = utc_now()
    try:
        existing_paths = paths_for(root, create=False)
        existing_ledger = LedgerStore(existing_paths.database, create=False, synchronous="NORMAL")
    except (WorkspaceError, StoreError) as exc:
        if getattr(exc, "code", None) not in ("no_state", "no_ledger"):
            raise
    else:
        try:
            scaffold_time = existing_ledger.meta("created_at") or scaffold_time
        finally:
            existing_ledger.close()
    if not args.bare:
        scaffold_workspace(root, generated_at=scaffold_time, check_only=True)
    paths = paths_for(root, create=True)
    ledger = LedgerStore(paths.database, synchronous="NORMAL")
    created_at = ledger.meta("created_at")
    ledger.close()
    # Prefer the ledger's persisted creation time. On a new ledger it differs
    # from the preflight time only by milliseconds; no files exist yet.
    scaffold_time = created_at or scaffold_time
    scaffold = (
        {
            "template_version": None,
            "manifest": None,
            "created": [],
            "unchanged": [],
            "conflicts": [],
            "bare": True,
        }
        if args.bare
        else {**scaffold_workspace(paths.workspace, generated_at=scaffold_time), "bare": False}
    )
    if not args.bare:
        CursorStore(paths.root).register("quay.distill")
    ignore = ensure_runtime_ignored(root, bare=bool(args.bare))
    advisory = git_advisory(root)
    result = {
        "workspace": str(root),
        "state_dir": str(paths.root),
        "ledger": str(paths.database),
        "socket": str(paths.socket),
        "git_advisory": advisory,
        "git_ignore_applied": ignore["applied"],
        "git_ignore": ignore,
        "scaffold": scaffold,
    }
    human = f"workspace ready: {paths.root}"
    if scaffold["bare"]:
        human += "\nknowledge scaffold skipped (--bare)"
    else:
        human += f"\nscaffold: {len(scaffold['created'])} created, {len(scaffold['unchanged'])} unchanged"
    if ignore["path"]:
        verb = "updated" if ignore["applied"] else "already configured"
        human += f"\n{verb}: {ignore['path']}"
    if advisory:
        human += f"\n{advisory}"
    emit_result(result, json_mode=json_mode, human=human)
    return EXIT_OK


def cmd_distill(args: argparse.Namespace) -> int:
    from .distill import distill_workspace

    root = workspace_path(args, create=False)
    output_root = None
    if args.out is not None:
        candidate = Path(args.out).expanduser()
        output_root = candidate if candidate.is_absolute() else root / candidate
    result = distill_workspace(
        root,
        output_root=output_root,
        stdout=bool(args.stdout),
        force_worktree=bool(args.force_worktree),
        limit=int(args.limit),
    )
    if args.stdout:
        for index, document in enumerate(result.pop("documents")):
            if index:
                sys.stdout.write("\n")
            sys.stdout.write(document)
        sys.stdout.flush()
        return EXIT_OK
    emit_result(
        result,
        json_mode=_json_mode(args),
        human=(
            f"distilled {result['count']} handoff(s) through seq {result['high_water_seq']} "
            f"into {result['output_root']}"
        ),
    )
    return EXIT_OK


def cmd_start(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    root = workspace_path(args)
    paths, health = ensure_daemon(root, idle_seconds=args.idle_seconds)
    emit_result(
        {"paths": {"socket": str(paths.socket), "pid": str(paths.pid)}, "health": health},
        json_mode=json_mode,
        human=f"daemon pid={health['pid']} socket={paths.socket}",
    )
    return EXIT_OK


def cmd_stop(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    if args.all:
        rows = [row for row in all_daemon_status() if row.get("running")]
        targets = [str(row["workspace"]) for row in rows]
        if not args.yes:
            emit_result(
                {"workspaces": targets, "stopped": []},
                json_mode=json_mode,
                human="rerun with --yes to stop: " + ", ".join(targets or ["(none)"]),
            )
            return EXIT_OK if not targets else EXIT_USAGE
        stopped = []
        for target in targets:
            with contextlib.suppress(WorkspaceError):
                if stop_daemon(target):
                    stopped.append(target)
        emit_result(
            {"workspaces": targets, "stopped": stopped},
            json_mode=json_mode,
            human=f"stopped {len(stopped)} daemon(s)",
        )
        return EXIT_OK
    root = workspace_path(args, create=False)
    stopped = stop_daemon(root)
    emit_result(
        {"workspace": str(root), "stopped": stopped},
        json_mode=json_mode,
        human="daemon stopped" if stopped else "no daemon was running",
    )
    return EXIT_OK


def cmd_restart(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    root = workspace_path(args)
    with contextlib.suppress(WorkspaceError):
        stop_daemon(root)
    paths, health = ensure_daemon(root, idle_seconds=getattr(args, "idle_seconds", None))
    emit_result(
        {"health": health, "socket": str(paths.socket)},
        json_mode=json_mode,
        human=f"daemon restarted pid={health['pid']}",
    )
    return EXIT_OK


def cmd_status(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    with connected(args) as client:
        status = client.status()
    ledger = status["ledger"]
    planes = status["planes"]
    emit_result(
        status,
        json_mode=json_mode,
        human=(
            f"pid={status['daemon']['pid']} records={ledger['records']} last_seq={ledger['last_seq']} "
            f"signal_open={planes['signal']['open']} work_active={planes['work']['active']} "
            f"work_done={planes['work']['done']} chain={status['integrity']['hash_chain']}"
        ),
    )
    return EXIT_OK


def cmd_daemons(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    rows = all_daemon_status()
    human = "\n".join(
        f"{'up  ' if row['running'] else 'down'} pid={row['pid']} {row['workspace']}" for row in rows
    ) or "(no registered workspaces)"
    emit_result({"daemons": rows, "count": len(rows)}, json_mode=json_mode, human=human)
    return EXIT_OK


def cmd_kinds(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    with connected(args) as client:
        kinds = client.request("kinds")["kinds"]
    human = "\n".join(
        f"{item['kind']:<8s} {'/'.join(item.get('states', []) or item.get('required', []))}"
        for item in kinds
    )
    emit_result({"kinds": kinds}, json_mode=json_mode, human=human)
    return EXIT_OK


def cmd_verify(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    with connected(args) as client:
        report = client.request("verify")
    human = (
        f"chain ok · {report['events_checked']} events · head {report['head_hash'][:16]}"
        if report["ok"]
        else f"chain BROKEN · {json.dumps(report['divergence'] or report['gaps'])}"
    )
    emit_result(report, json_mode=json_mode, human=human)
    return EXIT_OK if report["ok"] else EXIT_CHECK_FAILED


def cmd_doctor(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    checks: list[dict[str, Any]] = []

    python_ok = sys.version_info >= (3, 11)
    checks.append({"check": "python>=3.11", "ok": python_ok, "detail": sys.version.split()[0]})
    checks.append({"check": "unix-socket", "ok": hasattr(socket, "AF_UNIX"), "detail": sys.platform})

    try:
        root = workspace_path(args)
        paths = paths_for(root)
        checks.append({"check": "workspace", "ok": True, "detail": str(root)})
        checks.append(
            {
                "check": "workspace-resolution",
                "ok": True,
                "detail": {"requested": str(root), "shared_root": str(paths.workspace)},
            }
        )
    except WorkspaceError as exc:
        checks.append({"check": "workspace", "ok": False, "detail": str(exc)})
        emit_result({"checks": checks}, json_mode=json_mode)
        return EXIT_USAGE

    health = daemon_health(root)
    checks.append(
        {"check": "daemon", "ok": health is not None, "detail": (health or {}).get("pid", "not running")}
    )
    if health is not None:
        try:
            with QuayClient(paths.socket, timeout=5.0) as client:
                report = client.request("verify")
            checks.append(
                {
                    "check": "ledger-integrity",
                    "ok": bool(report["ok"]),
                    "detail": f"{report['events_checked']} events checked",
                }
            )
        except ClientError as exc:
            checks.append({"check": "ledger-integrity", "ok": False, "detail": exc.code})
    checks.append(
        {"check": "socket-mode", "ok": True, "detail": oct(paths.socket.stat().st_mode & 0o777) if paths.socket.exists() else "n/a"}
    )

    ok = all(check["ok"] for check in checks)
    human = "\n".join(f"{'ok  ' if check['ok'] else 'FAIL'} {check['check']}: {check['detail']}" for check in checks)
    emit_result({"checks": checks, "ok": ok}, json_mode=json_mode, human=human)
    return EXIT_OK if ok else EXIT_CHECK_FAILED


def cmd_emit(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    payload = _payload_from_text(args.payload)
    producer = args.producer or os.environ.get("QUAY_PRODUCER") or os.environ.get("USER") or "user"
    body = {
        "topic": args.topic,
        "producer": producer,
        "payload": payload,
        "kind": args.kind,
        "causation_id": args.causation,
        "root_causation_id": args.root_causation,
        "correlation_id": args.correlation,
    }
    with connected(args) as client:
        event = client.request("emit", body)["event"]
    emit_result(
        {"event": event},
        json_mode=json_mode,
        human=f"#{event['seq']} {event['kind']} {event['topic']} {event['hash'][:12]}",
    )
    return EXIT_OK


def cmd_work(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    sub = getattr(args, "work_command", None)
    if sub is None:
        emit_error("usage", "work requires a subcommand: claim, heartbeat, complete, abandon, list, reap", json_mode=json_mode)
        return EXIT_USAGE
    if sub in ("list", "reap"):
        with connected(args) as client:
            if sub == "list":
                result = client.request(
                    "work_list",
                    {"state": getattr(args, "state", None), "executor": getattr(args, "executor", None)},
                )
                human = "\n".join(
                    f"{row['state']:<10s} {row['key']:<24s} {row['executor']:<12s} "
                    f"lease={row['lease_expires_at'] or '-'}"
                    for row in result["work"]
                ) or "(no work records)"
            else:
                result = client.request("work_reap")
                human = f"reaped {result['count']} lease(s)"
        emit_result(result, json_mode=json_mode, human=human)
        return EXIT_OK

    evidence = None
    if getattr(args, "evidence", None):
        evidence = _payload_from_text(args.evidence)
        if not evidence:
            emit_error("invalid_evidence", "evidence must be a non-empty JSON object", json_mode=json_mode)
            return EXIT_USAGE
    body: dict[str, Any] = {
        "key": args.key,
        "executor": args.executor,
        "topic": args.topic,
        "causation_id": args.causation,
        "root_causation_id": args.root_causation,
        "correlation_id": args.correlation,
    }
    if args.title:
        body["title"] = args.title
    if args.lease_seconds:
        body["lease_seconds"] = args.lease_seconds
    if evidence:
        body["evidence"] = evidence
    if getattr(args, "reason", None):
        body["reason"] = args.reason
    if getattr(args, "fencing_token", None) is not None:
        body["fencing_token"] = args.fencing_token
    operation = {"claim": "work_claim", "heartbeat": "work_heartbeat", "complete": "work_complete", "abandon": "work_abandon"}[sub]
    with connected(args) as client:
        event = client.request(operation, body)["event"]
    emit_result(
        {"event": event},
        json_mode=json_mode,
        human=f"#{event['seq']} work {args.key} -> {event['payload']['state']}",
    )
    return EXIT_OK


def _query_filters(args: argparse.Namespace) -> dict[str, Any]:
    return {
        key: value
        for key, value in {
            "kind": args.kind,
            "topic": args.topic,
            "producer": args.producer,
            "state": args.state,
            "correlation_id": args.correlation_id,
            "causation_id": args.causation,
            "root_causation_id": args.root_causation,
            "since_seq": args.since_seq,
            "until_seq": args.until_seq,
            "since_ts": args.since_ts,
            "until_ts": args.until_ts,
            "text": args.text,
        }.items()
        if value is not None
    }


def cmd_query(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    filters = _query_filters(args)
    with connected(args) as client:
        events = client.request(
            "query", {**filters, "limit": args.limit, "offset": args.offset, "order": args.order}
        )["events"]
    if args.count_only:
        emit_result({"count": len(events)}, json_mode=json_mode, human=str(len(events)))
        return EXIT_OK
    if args.export == "jsonl":
        for event in events:
            sys.stdout.write(json.dumps(event, sort_keys=True, ensure_ascii=False) + "\n")
        sys.stdout.flush()
        return EXIT_OK
    if args.export == "csv":
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(
            [
                "seq",
                "ts",
                "kind",
                "topic",
                "producer",
                "causation_id",
                "root_causation_id",
                "correlation_id",
                "hash",
                "payload",
            ]
        )
        for event in events:
            writer.writerow(
                [
                    event["seq"],
                    event["ts"],
                    event["kind"],
                    event["topic"],
                    event["producer"],
                    event["causation_id"] if event["causation_id"] is not None else "",
                    event.get("root_causation_id")
                    if event.get("root_causation_id") is not None
                    else "",
                    event["correlation_id"] or "",
                    event["hash"],
                    json.dumps(event["payload"], sort_keys=True, ensure_ascii=False),
                ]
            )
        sys.stdout.write(buffer.getvalue())
        sys.stdout.flush()
        return EXIT_OK
    human = "\n".join(
        f"#{event['seq']:<6d} {event['ts']} {event['kind']:<7s} {event['topic']:<24s} "
        f"{event['producer']:<10s} {json.dumps(event['payload'], sort_keys=True, ensure_ascii=False)}"
        for event in events
    ) or "(no matching events)"
    emit_result({"events": events, "count": len(events)}, json_mode=json_mode, human=human)
    return EXIT_OK


def _print_event_lines(events: Sequence[Mapping[str, Any]], *, json_mode: bool) -> None:
    if json_mode:
        for event in events:
            sys.stdout.write(json.dumps(event, sort_keys=True, ensure_ascii=False) + "\n")
    else:
        for event in events:
            sys.stdout.write(
                f"#{event['seq']} {event['kind']} {event['topic']} {event['producer']} "
                f"{json.dumps(event['payload'], sort_keys=True, ensure_ascii=False)}\n"
            )
    sys.stdout.flush()


def human_event_lines(events: Sequence[Mapping[str, Any]]) -> str:
    """Render events as aligned human-readable lines."""
    return "\n".join(
        f"#{event['seq']:<6d} {event['ts']} {event['kind']:<7s} {event['topic']:<22s} "
        f"{event['producer']:<10s} {json.dumps(event['payload'], sort_keys=True, ensure_ascii=False)}"
        for event in events
    ) or "(no events yet)"


def cmd_log(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    if not args.follow:
        with connected(args) as client:
            events = client.request("log", {"limit": args.limit, "order": args.order})["events"]
        emit_result(
            {"events": events, "count": len(events)},
            json_mode=json_mode,
            human=human_event_lines(events),
        )
        return EXIT_OK
    return _follow(args, json_mode=json_mode, initial_limit=args.limit)


def _follow(args: argparse.Namespace, *, json_mode: bool, initial_limit: int) -> int:
    """Print a backlog, then stream live without polling or gaps.

    Backfill uses a subscription with ``replay``, so the backlog and the live
    feed come from the same ordered stream. There is no window in which an
    event could be missed between "read history" and "start listening".
    """
    deadline = time.monotonic() + args.duration if getattr(args, "duration", None) else None
    max_events = getattr(args, "max_events", None)
    delivered = 0
    with connected(args) as client:
        with SubscriptionStream(
            client.socket_path, filters={}, replay=max(0, int(initial_limit)), timeout=5.0
        ) as live:
            while True:
                if deadline is not None and time.monotonic() >= deadline:
                    break
                if max_events is not None and delivered >= max_events:
                    break
                frame = live.poll(0.2)
                if frame is None:
                    if live.is_terminal:
                        break
                    continue
                if frame.get("type") != "event":
                    SubscriptionStream._raise_recovery_error(frame)
                    continue
                _print_event_lines([frame["event"]], json_mode=json_mode)
                delivered += 1
    return EXIT_OK


def cmd_watch(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    if getattr(args, "path", None) is not None:
        if getattr(args, "workspace", None):
            emit_error(
                "conflicting_workspace",
                "give either a positional path or --workspace, not both",
                json_mode=json_mode,
            )
            return EXIT_USAGE
        args.workspace = args.path
    root = workspace_path(args)
    # The implicit cockpit (`quay [PATH]`, bare `quay`) and `quay monitor` are
    # the zero-friction path: they may create the workspace and seed it.
    auto_init = bool(getattr(args, "auto_init", False)) or getattr(args, "command", None) == "monitor"
    if auto_init:
        paths = onboard(root, idle_seconds=getattr(args, "idle_seconds", None)).paths
    else:
        paths, _health = ensure_daemon(root, idle_seconds=getattr(args, "idle_seconds", None))
    terminal_is_dumb = os.environ.get("TERM", "").lower() == "dumb"
    monitor_plain_fallback = getattr(args, "command", None) == "monitor" and (
        not sys.stdout.isatty() or terminal_is_dumb
    )
    plain = bool(args.plain or monitor_plain_fallback)
    if monitor_plain_fallback and not args.plain:
        sys.stderr.write(
            "quay: monitor has no interactive terminal; streaming JSON lines in plain mode\n"
        )
    interactive = (
        sys.stdin.isatty()
        and sys.stdout.isatty()
        and not terminal_is_dumb
        and not plain
    )
    if interactive:
        app = TuiApp(
            paths.socket,
            workspace=str(root),
            filter_kind=args.kind,
            history=args.history,
        )
        return app.run()

    if not plain:
        # No TTY and no explicit --plain: print a snapshot instead of
        # streaming forever, which is what a script or CI job wants.
        sys.stderr.write("quay: no TTY; printing a status snapshot instead of the cockpit\n")
        with QuayClient(paths.socket, timeout=getattr(args, "timeout", 15.0)) as client:
            status = client.status()
        emit_result(status, json_mode=True)
        return EXIT_OK
    return _follow(args, json_mode=True if json_mode else False, initial_limit=int(args.history))


def cmd_mcp(args: argparse.Namespace) -> int:
    from .mcp import serve

    return serve(getattr(args, "workspace", None) or os.getcwd())


def cmd_import(args: argparse.Namespace) -> int:
    """Import a Changi ledger, writing through the running daemon when possible."""
    json_mode = _json_mode(args)
    source = Path(args.source).expanduser()
    if source.is_dir():
        source = source / "events.db"
    if not source.exists():
        emit_error("source_missing", f"no Changi ledger at {source}", json_mode=json_mode)
        return EXIT_USAGE

    with connected(args) as client:
        result = client.request("import", {"source": str(source), "dry_run": bool(args.dry_run)})
    payload = result["report"]
    warnings = payload["warnings"]
    shown = warnings[: max(0, args.max_warnings)]
    human_lines = [
        f"imported {payload['migrated']} of {payload['read']} event(s) from {payload['source']} "
        f"in {payload['duration_ms']:.0f}ms",
        f"kinds: {json.dumps(payload['kinds'], sort_keys=True)}",
    ]
    if payload["skipped"]:
        human_lines.append(f"skipped: {payload['skipped']}")
    for warning in shown:
        human_lines.append(f"warning: {warning}")
    if len(warnings) > len(shown):
        human_lines.append(f"… {len(warnings) - len(shown)} more warning(s)")
    emit_result(payload, json_mode=json_mode, human="\n".join(human_lines))
    return EXIT_OK


def cmd_bench(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    from .bench import format_report, run_all

    report = run_all(scale=args.scale)
    if args.out:
        target = Path(args.out).expanduser()
        target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        report["report_path"] = str(target)
    emit_result(report, json_mode=json_mode, human=format_report(report))
    return EXIT_OK


def cmd_prune(args: argparse.Namespace) -> int:
    json_mode = _json_mode(args)
    if args.keep_last is None and args.before is None:
        emit_error("prune_scope_required", "prune requires --keep-last or --before", json_mode=json_mode)
        return EXIT_USAGE
    apply_now = not args.dry_run
    if apply_now and not args.yes:
        emit_error(
            "confirmation_required",
            "prune deletes events; rerun with --yes to apply the preview (or use --dry-run)",
            json_mode=json_mode,
        )
        return EXIT_USAGE
    with connected(args) as client:
        if apply_now:
            result = client.request("prune", {"keep_last": args.keep_last, "before": args.before})
        else:
            result = client.request("prune_preview", {"keep_last": args.keep_last, "before": args.before})
            result = {**result, "dry_run": True, "applied": False}
    emit_result(
        result,
        json_mode=json_mode,
        human=(
            f"would remove {result['removable']} event(s); {result['remaining']} remain"
            if not apply_now
            else f"removed {result.get('removed', 0)} event(s); cutoff seq {result.get('cutoff_seq')}"
        ),
    )
    return EXIT_OK


COMMANDS: dict[str, Callable[[argparse.Namespace], int]] = {
    "init": cmd_init,
    "distill": cmd_distill,
    "start": cmd_start,
    "stop": cmd_stop,
    "restart": cmd_restart,
    "status": cmd_status,
    "daemons": cmd_daemons,
    "doctor": cmd_doctor,
    "kinds": cmd_kinds,
    "verify": cmd_verify,
    "emit": cmd_emit,
    "work": cmd_work,
    "query": cmd_query,
    "log": cmd_log,
    "watch": cmd_watch,
    "monitor": cmd_watch,
    "mcp": cmd_mcp,
    "prune": cmd_prune,
    "bench": cmd_bench,
    "import": cmd_import,
}


def _first_free_token(argv: Sequence[str]) -> int | None:
    """Index of the first token that is neither an option nor an option value."""
    takes_value = ("--workspace", "--timeout")
    index = 0
    while index < len(argv):
        token = argv[index]
        if token == "--":
            return None
        if token in takes_value:
            index += 2
            continue
        if token.startswith("-"):
            index += 1
            continue
        return index
    return None


def _normalize_positional_workspace(argv: Sequence[str]) -> tuple[list[str], bool]:
    """Rewrite ``quay PATH`` into ``quay watch --workspace PATH``.

    Only the first free token is considered, and only when it resolves to an
    existing directory and is not a registered command. Everything else falls
    through to argparse's ordinary usage error, so a typo can never silently
    create a workspace.
    """
    tokens = list(argv)
    if "--workspace" in tokens or any(token.startswith("--workspace=") for token in tokens):
        return tokens, False
    index = _first_free_token(tokens)
    if index is None:
        return tokens, False
    candidate = tokens[index]
    if candidate in COMMANDS:
        return tokens, False
    try:
        if not Path(candidate).expanduser().is_dir():
            return tokens, False
    except OSError:  # pragma: no cover - defensive; is_dir rarely raises
        return tokens, False
    normalized = tokens[:index] + ["watch", "--workspace", candidate] + tokens[index + 1 :]
    return normalized, True


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns a process exit code.

    ``--help`` and ``--version`` keep argparse's usual behaviour and exit the
    process; usage errors are reported and returned as ``EXIT_USAGE``.
    """
    parser = build_parser()
    normalized, implicit_workspace = _normalize_positional_workspace(
        list(argv) if argv is not None else sys.argv[1:]
    )
    try:
        args = parser.parse_args(normalized)
    except SystemExit as exit_request:
        code = exit_request.code if isinstance(exit_request.code, int) else EXIT_USAGE
        if code == 0:
            raise
        return code
    command = getattr(args, "command", None)
    if command is None:
        # Bare `quay` behaves like `quay watch`, matching a local companion.
        args.plain = False
        args.kind = None
        args.history = 200
        args.max_events = None
        args.duration = None
        command = "watch"
        args.command = command
        implicit_workspace = True
    if implicit_workspace:
        # `quay [PATH]` may safely create the workspace it points at.
        args.auto_init = True
    handler = COMMANDS.get(command)
    if handler is None:
        emit_error("unknown_command", f"unknown command: {command}", json_mode=_json_mode(args))
        return EXIT_USAGE
    args.json = getattr(args, "json", None)
    try:
        code = handler(args)
    except (WorkspaceError, StoreError) as exc:
        emit_error(getattr(exc, "code", "error"), str(exc), json_mode=_json_mode(args))
        return EXIT_USAGE
    except ClientError as exc:
        json_mode = _json_mode(args)
        emit_error(exc.code, exc.message, json_mode=json_mode, detail=exc.detail)
        return EXIT_UNAVAILABLE if exc.code in ("connection_failed", "timeout", "daemon_closed") else EXIT_USAGE
    except BrokenPipeError:  # pragma: no cover - pipeline closed early
        with contextlib.suppress(OSError):
            sys.stdout.close()
        return EXIT_OK
    return code


if __name__ == "__main__":  # pragma: no cover - exercised via subprocess
    raise SystemExit(main())

"""Deterministic materialization of acknowledged Quay handoffs into OKF."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import html
import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any, Iterable

from .cursors import CursorStore
from .scaffold import MANIFEST_PATH
from .store import LedgerStore
from .workspace import WorkspaceError, canonical_workspace, git_worktree_root, paths_for

DISTILLER_VERSION = "1"
STATE_NAME = ".distill-state.json"
LOG_LIMIT = 50

__all__ = ["DISTILLER_VERSION", "distill_workspace", "event_uri"]


def event_uri(ledger_uuid: str, seq: int) -> str:
    """Return the stable URI for an event in one persistent ledger."""
    return f"quay://{ledger_uuid}/events/{int(seq)}"


def _yaml_string(value: Any) -> str:
    return json.dumps(str(value), ensure_ascii=False)


def _one_line(value: Any) -> str:
    return html.escape(" ".join(str(value).split()))


def _source_id(ledger_uuid: str, seq: int) -> str:
    return f"quay-{ledger_uuid}-{int(seq)}"


def _month_parts(timestamp: str) -> tuple[str, str]:
    if (
        len(timestamp) < 7
        or not timestamp[:4].isdigit()
        or timestamp[4] != "-"
        or not timestamp[5:7].isdigit()
    ):
        raise WorkspaceError("invalid_event_timestamp", f"cannot partition event timestamp: {timestamp!r}")
    year, month = timestamp[:4], timestamp[5:7]
    if not 1 <= int(month) <= 12:
        raise WorkspaceError("invalid_event_timestamp", f"cannot partition event timestamp: {timestamp!r}")
    return year, month


def _handoff_relative(event: dict[str, Any]) -> Path:
    year, month = _month_parts(str(event["ts"]))
    return Path("handoffs") / year / month / f"{int(event['seq']):020d}-{str(event['hash'])[:12]}.md"


def _render_handoff(event: dict[str, Any], ledger_uuid: str) -> str:
    payload = event["payload"]
    seq = int(event["seq"])
    source_id = _source_id(ledger_uuid, seq)
    uri = event_uri(ledger_uuid, seq)
    summary_lines = str(payload["summary"]).splitlines() or [""]
    quoted_summary = "\n".join(
        f"> {html.escape(line)}" if line else ">" for line in summary_lines
    )
    links = payload.get("links") or []
    link_section = ""
    if links:
        link_section = "\n## Links\n\n" + "\n".join(f"- {_one_line(link)}" for link in links) + "\n"
    initiative = payload.get("initiative")
    initiative_line = (
        f"- **Initiative:** {_one_line(initiative)}\n" if initiative is not None else ""
    )
    return (
        "---\n"
        "type: Handoff\n"
        f"title: {_yaml_string(f'Quay handoff {seq}')}\n"
        "description: Deterministic materialization of an acknowledged Quay handoff.\n"
        "status: stable\n"
        "generated:\n"
        f"  by: {_yaml_string(str(event['producer']))}\n"
        f"  at: {_yaml_string(str(event['ts']))}\n"
        "distilled:\n"
        f"  by: quay/distill-v{DISTILLER_VERSION}\n"
        "sources:\n"
        f"  - id: {source_id}\n"
        f"    resource: {uri}\n"
        f"    title: {_yaml_string(f'Quay ledger event {seq}')}\n"
        "---\n\n"
        f"# Handoff {seq}\n\n"
        f"- **From:** {_one_line(payload['from'])}\n"
        f"- **To:** {_one_line(payload['to'])}\n"
        f"- **Topic:** {_one_line(event['topic'])}\n"
        f"{initiative_line}"
        f"- **Source:** [{uri}]({uri})[^{source_id}]\n\n"
        "## Summary\n\n"
        f"{quoted_summary}\n"
        f"{link_section}\n"
        f"[^{source_id}]: Source declared in frontmatter.\n"
    )


def _entry(event: dict[str, Any], relative: Path) -> dict[str, Any]:
    payload = event["payload"]
    return {
        "seq": int(event["seq"]),
        "ts": str(event["ts"]),
        "from": str(payload["from"]),
        "to": str(payload["to"]),
        "summary": str(payload["summary"]),
        "path": relative.as_posix(),
    }


def _render_log(entries: Iterable[dict[str, Any]], ledger_uuid: str) -> str:
    rows = sorted(entries, key=lambda row: int(row["seq"]))[-LOG_LIMIT:]
    if not rows:
        raise WorkspaceError("distill_empty_log", "cannot render an empty distilled log")
    latest = rows[-1]
    sources = []
    body = []
    footnotes = []
    for row in reversed(rows):
        seq = int(row["seq"])
        source_id = _source_id(ledger_uuid, seq)
        uri = event_uri(ledger_uuid, seq)
        sources.extend(
            [
                f"  - id: {source_id}",
                f"    resource: {uri}",
                f"    title: {_yaml_string(f'Quay ledger event {seq}')}",
            ]
        )
        body.append(
            f"- `{row['ts']}` — [#{seq}]({row['path']}) "
            f"{_one_line(row['from'])} → {_one_line(row['to'])}: "
            f"{_one_line(row['summary'])}[^{source_id}]"
        )
        footnotes.append(f"[^{source_id}]: Source declared in frontmatter.")
    return (
        "---\n"
        "type: Handoff Log\n"
        "title: Recent distilled handoffs\n"
        f"description: Bounded deterministic index through Quay event {int(latest['seq'])}.\n"
        "status: stable\n"
        "generated:\n"
        f"  by: quay/distill-v{DISTILLER_VERSION}\n"
        f"  at: {_yaml_string(latest['ts'])}\n"
        "sources:\n"
        + "\n".join(sources)
        + "\n---\n\n# Recent handoffs\n\n"
        + "\n".join(body)
        + "\n\n"
        + "\n".join(footnotes)
        + "\n"
    )


def _default_state(ledger_uuid: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "distiller_version": DISTILLER_VERSION,
        "ledger_uuid": ledger_uuid,
        "high_water_seq": 0,
        "entries": [],
    }


def _load_state(path: Path, ledger_uuid: str) -> dict[str, Any]:
    if not path.exists() and not path.is_symlink():
        return _default_state(ledger_uuid)
    if path.is_symlink() or not path.is_file():
        raise WorkspaceError("unsafe_distill_path", f"refusing distill state path: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise WorkspaceError("invalid_distill_state", f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise WorkspaceError("invalid_distill_state", f"unsupported distill state in {path}")
    if value.get("ledger_uuid") != ledger_uuid:
        raise WorkspaceError(
            "distill_ledger_mismatch",
            f"distill state belongs to ledger {value.get('ledger_uuid')!r}, not {ledger_uuid!r}",
        )
    high_water = value.get("high_water_seq")
    entries = value.get("entries")
    if isinstance(high_water, bool) or not isinstance(high_water, int) or high_water < 0:
        raise WorkspaceError("invalid_distill_state", "high_water_seq must be a non-negative integer")
    if not isinstance(entries, list) or not all(isinstance(row, dict) for row in entries):
        raise WorkspaceError("invalid_distill_state", "entries must be a list of objects")
    for row in entries:
        seq = row.get("seq")
        relative = row.get("path")
        if isinstance(seq, bool) or not isinstance(seq, int) or seq <= 0:
            raise WorkspaceError("invalid_distill_state", "entry seq must be a positive integer")
        if not all(isinstance(row.get(key), str) for key in ("ts", "from", "to", "summary")):
            raise WorkspaceError("invalid_distill_state", "entry metadata must contain strings")
        if not isinstance(relative, str):
            raise WorkspaceError("invalid_distill_state", "entry path must be a string")
        candidate = Path(relative)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise WorkspaceError("invalid_distill_state", f"unsafe entry path: {relative!r}")
    return value


def _assert_safe_output(root: Path, target: Path) -> None:
    try:
        relative = target.relative_to(root)
    except ValueError as exc:
        raise WorkspaceError("unsafe_distill_path", f"distill target escapes output root: {target}") from exc
    current = root
    for part in relative.parts[:-1]:
        current = current / part
        if current.is_symlink():
            raise WorkspaceError("unsafe_distill_path", f"refusing symlinked output path: {current}")
        if current.exists() and not current.is_dir():
            raise WorkspaceError("unsafe_distill_path", f"output parent is not a directory: {current}")
    if target.is_symlink():
        raise WorkspaceError("unsafe_distill_path", f"refusing symlinked output file: {target}")


def _atomic_replace(target: Path, text: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.parent.is_symlink() or target.is_symlink():
        raise WorkspaceError("unsafe_distill_path", f"refusing symlinked output path: {target}")
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    owned_fd: int | None = fd
    try:
        os.fchmod(fd, 0o644)
        stream = os.fdopen(fd, "w", encoding="utf-8", closefd=True)
        owned_fd = None
        with stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        if target.is_symlink():
            raise WorkspaceError("unsafe_distill_path", f"refusing symlinked output file: {target}")
        os.replace(temporary, target)
        directory_fd = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        if owned_fd is not None:
            try:
                os.close(owned_fd)
            except OSError:
                pass
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _is_acknowledged(event: dict[str, Any], cursors: dict[str, dict[str, Any]]) -> bool:
    recipient = str(event["payload"].get("to", ""))
    row = cursors.get(recipient)
    return row is not None and int(row.get("last_acked_seq", 0)) >= int(event["seq"])


def _collect(
    store: LedgerStore,
    cursors: dict[str, dict[str, Any]],
    *,
    high_water: int,
    until_seq: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int, int | None]:
    selected: list[dict[str, Any]] = []
    cursor = int(high_water)
    blocked_seq: int | None = None
    while len(selected) < limit and cursor < until_seq:
        batch = store.since(cursor, limit=min(500, max(1, limit - len(selected) + 32)))
        if not batch:
            break
        for event in batch:
            if int(event["seq"]) > until_seq:
                return selected, cursor, blocked_seq
            if event["kind"] == "handoff":
                if not _is_acknowledged(event, cursors):
                    blocked_seq = int(event["seq"])
                    return selected, cursor, blocked_seq
                selected.append(event)
            cursor = int(event["seq"])
            if len(selected) >= limit:
                break
    return selected, cursor, blocked_seq


def _consumer_name(output_root: Path, *, default_output: bool) -> str:
    if default_output:
        return "quay.distill"
    digest = hashlib.sha256(str(output_root).encode("utf-8")).hexdigest()[:16]
    return f"quay.distill.{digest}"


def _output_path(
    output_root: str | os.PathLike[str] | None, *, primary: Path
) -> Path:
    candidate = Path(output_root).expanduser() if output_root is not None else primary / "okf"
    if candidate.is_symlink():
        raise WorkspaceError("unsafe_distill_path", f"refusing symlinked output root: {candidate}")
    return candidate.resolve()


def _advance_cursor(cursors: CursorStore, consumer: str, target: int) -> None:
    while True:
        current = cursors.get(consumer)
        last_acked = int(current["last_acked_seq"])
        if last_acked >= target:
            return
        in_flight = current.get("in_flight")
        if in_flight is not None:
            sequence = int(in_flight["seq"])
            if sequence > target:
                raise WorkspaceError(
                    "distill_cursor_ahead",
                    f"distill cursor {consumer} has in-flight seq {sequence} beyond state {target}",
                )
            cursors.acknowledge(
                consumer, sequence, idempotency_key=str(in_flight["idempotency_key"])
            )
            continue
        key = f"distill:{target}"
        cursors.begin_delivery(consumer, target, key)
        cursors.acknowledge(consumer, target, idempotency_key=key)


@contextlib.contextmanager
def _distill_lock(state_root: Path, consumer: str):
    path = state_root / f"{consumer}.lock"
    if path.is_symlink():
        raise WorkspaceError("unsafe_distill_lock", f"refusing symlinked distill lock: {path}")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        raise WorkspaceError("distill_lock_unavailable", f"cannot open {path}: {exc}") from exc
    try:
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _distill_once(
    workspace: str | os.PathLike[str],
    *,
    output_root: str | os.PathLike[str] | None = None,
    stdout: bool = False,
    force_worktree: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    """Distill acknowledged handoffs in sequence order without skipping gaps."""
    if not 1 <= int(limit) <= 1000:
        raise WorkspaceError("invalid_distill_limit", "distill limit must be between 1 and 1000")
    requested = canonical_workspace(workspace)
    paths = paths_for(requested, create=False)
    current_worktree = git_worktree_root(requested) or requested
    primary = paths.workspace
    if not stdout and current_worktree.resolve() != primary.resolve() and not force_worktree:
        raise WorkspaceError(
            "distill_linked_worktree",
            "refusing knowledge-plane writes from a linked worktree; run in the primary "
            "worktree or pass --force-worktree",
        )
    out = _output_path(output_root, primary=primary)
    default_output = out.resolve() == (primary / "okf").resolve()
    if default_output and not stdout and not (primary / MANIFEST_PATH).is_file():
        raise WorkspaceError(
            "knowledge_plane_missing", "run explicit 'quay init' before distilling handoffs"
        )
    if out.is_symlink():
        raise WorkspaceError("unsafe_distill_path", f"refusing symlinked output root: {out}")

    store = LedgerStore(paths.database, create=False, synchronous="NORMAL")
    try:
        ledger_uuid = store.meta("ledger_uuid")
        if not ledger_uuid:
            raise WorkspaceError("ledger_identity_missing", "ledger has no persistent UUID")
        try:
            if str(uuid.UUID(ledger_uuid)) != ledger_uuid:
                raise ValueError
        except (ValueError, AttributeError) as exc:
            raise WorkspaceError(
                "ledger_identity_invalid", f"ledger UUID is invalid: {ledger_uuid!r}"
            ) from exc
        integrity = store.verify(record=False)
        if not integrity["ok"]:
            raise WorkspaceError(
                "ledger_integrity_failed",
                f"refusing to distill a ledger with integrity divergence: {integrity['divergence'] or integrity['gaps']}",
            )
        snapshot_seq = int(integrity["last_seq"] or integrity["checkpoint"]["seq"])
        state_path = out / STATE_NAME
        state = _load_state(state_path, ledger_uuid)
        high_water = int(state["high_water_seq"])
        checkpoint_seq = int(integrity["checkpoint"]["seq"])
        if high_water < checkpoint_seq:
            raise WorkspaceError(
                "distill_history_pruned",
                f"distill state {high_water} trails ledger checkpoint {checkpoint_seq}",
            )
        if high_water > snapshot_seq:
            raise WorkspaceError(
                "invalid_distill_state",
                f"distill state {high_water} is ahead of ledger snapshot {snapshot_seq}",
            )
        cursor_store = CursorStore(paths.root)
        consumer = _consumer_name(out, default_output=default_output)
        if not stdout:
            durable = cursor_store.register(consumer)
            if int(durable["last_acked_seq"]) > high_water:
                raise WorkspaceError(
                    "distill_cursor_ahead",
                    f"distill cursor {consumer} is ahead of tracked state {high_water}",
                )
        cursor_snapshot = cursor_store.snapshot()
        selected, high_water, blocked_seq = _collect(
            store,
            cursor_snapshot,
            high_water=high_water,
            until_seq=snapshot_seq,
            limit=int(limit),
        )
    finally:
        store.close()

    documents: list[tuple[Path, str, dict[str, Any]]] = []
    for event in selected:
        relative = _handoff_relative(event)
        documents.append((relative, _render_handoff(event, ledger_uuid), _entry(event, relative)))
    if stdout:
        return {
            "ledger_uuid": ledger_uuid,
            "snapshot_seq": snapshot_seq,
            "high_water_seq": high_water,
            "blocked_seq": blocked_seq,
            "count": len(documents),
            "documents": [text for _relative, text, _entry_row in documents],
        }

    try:
        out.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise WorkspaceError("distill_output_unavailable", f"cannot create {out}: {exc}") from exc
    if out.is_symlink():
        raise WorkspaceError("unsafe_distill_path", f"refusing symlinked output root: {out}")
    conflicts: list[str] = []
    created: list[str] = []
    unchanged: list[str] = []
    for relative, text, _entry_row in documents:
        target = out / relative
        _assert_safe_output(out, target)
        if target.exists():
            if not target.is_file() or target.read_text(encoding="utf-8") != text:
                conflicts.append(relative.as_posix())
            else:
                unchanged.append(relative.as_posix())
        else:
            created.append(relative.as_posix())
    if conflicts:
        raise WorkspaceError(
            "distill_conflict",
            "refusing to overwrite non-identical distilled handoffs: " + ", ".join(conflicts),
        )

    for relative, text, _entry_row in documents:
        if relative.as_posix() in created:
            _atomic_replace(out / relative, text)

    previous_entries = [dict(row) for row in state.get("entries", [])]
    combined = {int(row["seq"]): row for row in previous_entries}
    for _relative, _text, entry in documents:
        combined[int(entry["seq"])] = entry
    entries = [combined[key] for key in sorted(combined)][-LOG_LIMIT:]
    if documents:
        _atomic_replace(out / "log.md", _render_log(entries, ledger_uuid))
    updated_state = {
        "schema_version": 1,
        "distiller_version": DISTILLER_VERSION,
        "ledger_uuid": ledger_uuid,
        "high_water_seq": high_water,
        "entries": entries,
    }
    _atomic_replace(
        state_path, json.dumps(updated_state, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    )
    _advance_cursor(cursor_store, consumer, high_water)
    return {
        "ledger_uuid": ledger_uuid,
        "snapshot_seq": snapshot_seq,
        "output_root": str(out),
        "high_water_seq": high_water,
        "blocked_seq": blocked_seq,
        "count": len(documents),
        "created": created,
        "unchanged": unchanged,
        "state": str(state_path),
    }


def distill_workspace(
    workspace: str | os.PathLike[str],
    *,
    output_root: str | os.PathLike[str] | None = None,
    stdout: bool = False,
    force_worktree: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    """Distill acknowledged handoffs, serializing writers per output cursor."""
    if stdout:
        return _distill_once(
            workspace,
            output_root=output_root,
            stdout=True,
            force_worktree=force_worktree,
            limit=limit,
        )
    requested = canonical_workspace(workspace)
    paths = paths_for(requested, create=False)
    primary = paths.workspace
    out = _output_path(output_root, primary=primary)
    default_output = out.resolve() == (primary / "okf").resolve()
    consumer = _consumer_name(out, default_output=default_output)
    with _distill_lock(paths.root, consumer):
        return _distill_once(
            requested,
            output_root=out,
            stdout=False,
            force_worktree=force_worktree,
            limit=limit,
        )

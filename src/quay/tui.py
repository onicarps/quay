"""Incremental terminal cockpit: a real TUI with diffed redraws.

Design
------
The TUI is split into two layers so it can be tested without a terminal:

* :class:`Screen` is a width x height grid of ``(character, style)`` cells.
  :func:`render_diff` turns the difference between two screens into the
  smallest ANSI update that reproduces the new one. Identical frames produce
  an empty string, so an idle daemon costs nothing to display.
* :class:`Dashboard` builds a screen from plain data (health, status, events,
  selection, filter). It is a pure function of its inputs.

:class:`TuiApp` wires the two to a real terminal: alternate screen, raw mode,
key handling, and a live subscription.

Redrawing only changed cells is what makes this a cockpit rather than a
reprint: Changi clears and rewrites the whole screen once per second, which
tears under load; Quay writes only what changed, as fast as events arrive.
"""

from __future__ import annotations

import codecs
import contextlib
import io
import os
import re
import select
import shutil
import signal
import sys
import termios
import time
import tty
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable, Mapping, Sequence

if TYPE_CHECKING:
    from .client import SubscriptionStream

#: 256-colour styles keep the TUI readable on every modern terminal.
STYLES: dict[str, str] = {
    "default": "\x1b[0m",
    "dim": "\x1b[38;5;243m",
    "bold": "\x1b[1m",
    "title": "\x1b[1;38;5;45m",
    "accent": "\x1b[38;5;214m",
    "ok": "\x1b[38;5;42m",
    "warn": "\x1b[38;5;214m",
    "bad": "\x1b[38;5;203m",
    "border": "\x1b[38;5;240m",
    "selected": "\x1b[7;38;5;231m",
    "signal": "\x1b[38;5;203m",
    "work": "\x1b[38;5;39m",
    "handoff": "\x1b[38;5;45m",
    "note": "\x1b[38;5;150m",
    "metric": "\x1b[38;5;183m",
    "generic": "\x1b[38;5;245m",
}
RESET = "\x1b[0m"

#: Wipe the screen and park the cursor at the home position. Written whenever
#: the physical screen may no longer match the cells we believe are on it.
CLEAR_SCREEN = "\x1b[2J\x1b[H"

#: Ctrl-L: repaint the screen without touching the model or the ledger.
REFRESH_KEY = "\x0c"

#: Alternate-screen and cursor toggles live in one place so every exit path
#: leaves the terminal the way the cockpit found it.
ALT_SCREEN_ON = "\x1b[?1049h"
ALT_SCREEN_OFF = "\x1b[?1049l"
CURSOR_HIDE = "\x1b[?25l"
CURSOR_SHOW = "\x1b[?25h"

#: SGR 1006 mouse reporting: 1000 reports button presses, 1002 adds drag
#: tracking, and 1006 selects the ``\x1b[<b;col;row`` coordinate encoding that
#: makes a click addressable instead of a cursor-relative guess.
MOUSE_ON = "\x1b[?1000h\x1b[?1002h\x1b[?1006h"
MOUSE_OFF = "\x1b[?1006l\x1b[?1002l\x1b[?1000l"

#: The event stream always starts on row 3: title, filter bar, column header.
STREAM_TOP = 3

#: Payload lines the inspect overlay shows before it says how many it hid.
_INSPECT_PAYLOAD_LINES = 14

#: How far right of the header a click counts as hitting the PAUSED marker.
_PAUSED_CLICK_ZONE = 30

#: Reading a burst: wait this long for the rest of an unfinished escape
#: sequence, at most this many times, and never buffer more than this much.
_TAIL_READ_TIMEOUT = 0.02
_MAX_TAIL_READS = 8
_MAX_INPUT_BYTES = 4096

KIND_STYLE = {
    "signal": "signal",
    "work": "work",
    "handoff": "handoff",
    "note": "note",
    "metric": "metric",
    "generic": "generic",
}

#: Filter presets cycled with the ``f`` key.
FILTER_PRESETS: tuple[tuple[str, str | None], ...] = (
    ("all", None),
    ("signal", "signal"),
    ("work", "work"),
    ("handoff", "handoff"),
    ("note", "note"),
    ("metric", "metric"),
)

_CONTROL_CHARS = {chr(code) for code in range(32)} | {chr(127)}


def sanitize(text: str) -> str:
    """Make a string safe to place in a cell grid."""
    return "".join("·" if char in _CONTROL_CHARS else char for char in text)


@dataclass
class Screen:
    """A grid of styled cells."""

    width: int
    height: int
    cells: list[list[tuple[str, str]]] = field(init=False)

    def __post_init__(self) -> None:
        self.width = max(1, int(self.width))
        self.height = max(1, int(self.height))
        self.cells = [[(" ", "default") for _ in range(self.width)] for _ in range(self.height)]

    def put(self, row: int, col: int, text: str, style: str = "default") -> None:
        """Write ``text`` at ``(row, col)``, clipping anything outside the grid."""
        if row < 0 or row >= self.height:
            return
        column = col
        for char in sanitize(str(text)):
            if column >= self.width:
                return
            if column >= 0:
                self.cells[row][column] = (char, style)
            column += 1

    def fill(self, row: int, style: str = "default", char: str = " ") -> None:
        if 0 <= row < self.height:
            self.cells[row] = [(char, style) for _ in range(self.width)]

    def text_at(self, row: int) -> str:
        return "".join(char for char, _style in self.cells[row])


def render_diff(
    previous: Sequence[Sequence[tuple[str, str]]] | None,
    current: Sequence[Sequence[tuple[str, str]]],
) -> str:
    """Return the ANSI byte sequence that turns ``previous`` into ``current``."""
    if not current:
        return ""
    height = len(current)
    width = len(current[0]) if current else 0
    output: list[str] = []
    for row in range(height):
        cells = current[row]
        row_width = min(width, len(cells))
        run_start: int | None = None
        run_style: str | None = None
        run_text: list[str] = []

        def flush() -> None:
            nonlocal run_start, run_style, run_text
            if run_start is None:
                return
            style = STYLES.get(run_style or "default", STYLES["default"])
            output.append(f"\x1b[{row + 1};{run_start + 1}H{style}{''.join(run_text)}{RESET}")
            run_start = None
            run_style = None
            run_text = []

        for col in range(row_width):
            cell = cells[col]
            same = (
                previous is not None
                and row < len(previous)
                and col < len(previous[row])
                and previous[row][col] == cell
            )
            if same:
                flush()
                continue
            if run_start is None:
                run_start = col
                run_style = cell[1]
            elif cell[1] != run_style:
                flush()
                run_start = col
                run_style = cell[1]
            run_text.append(cell[0])
        flush()
    return "".join(output)


def _shorten(value: str, limit: int) -> str:
    if limit <= 1:
        return value[:1]
    return value if len(value) <= limit else value[: limit - 1] + "…"


def _int_or(value: object, default: int = 0) -> int:
    """Coerce an untrusted model value to ``int`` without ever raising.

    The cockpit renders whatever the daemon sends, so a count that arrives as a
    string (or as ``None`` after a schema change) must degrade to a number
    instead of taking the whole frame down with a ``ValueError``.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _mapping(value: object) -> Mapping[str, Any]:
    """Return ``value`` when it is a mapping, otherwise an empty one."""
    return value if isinstance(value, Mapping) else {}


def _events_of(model: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
    """Return the model's event list, or an empty one when it is unusable."""
    events = model.get("events")
    return events if isinstance(events, (list, tuple)) else ()


def _seq_column(value: object) -> str:
    """Format a sequence number for the stream column, whatever it holds."""
    if isinstance(value, int) and not isinstance(value, bool):
        return f"{value:<5d}"
    text = "?" if value is None else str(value)
    return f"{text:<5.5s}"


def summarize_event(event: Mapping[str, Any], limit: int = 64) -> str:
    """Return a one-line human summary of an event's payload."""
    payload = event.get("payload")
    if isinstance(payload, Mapping):
        for key in ("text", "summary", "title", "reason"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return _shorten(value, limit)
        if "key" in payload and "state" in payload:
            return _shorten(f"{payload['key']} [{payload['state']}]", limit)
        if "name" in payload and "value" in payload:
            unit = f" {payload['unit']}" if payload.get("unit") else ""
            return _shorten(f"{payload['name']} = {payload['value']}{unit}", limit)
        if "state" in payload and len(payload) == 1:
            return _shorten(f"state={payload['state']}", limit)
    try:
        import json

        compact = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    except (TypeError, ValueError):
        compact = str(payload)
    return _shorten(compact, limit)


def age_of(timestamp: object, *, now: float | None = None) -> str:
    """Return a compact age such as ``3s`` or ``2m`` for an ISO timestamp."""
    if not isinstance(timestamp, str) or not timestamp:
        return "  -"
    try:
        from .store import parse_ts

        moment = parse_ts(timestamp).timestamp()
    except (ValueError, TypeError, OverflowError, OSError):
        return "  -"
    seconds = max(0.0, (time.time() if now is None else now) - moment)
    if seconds < 60:
        return f"{int(seconds):>2d}s"
    if seconds < 3600:
        return f"{int(seconds // 60):>2d}m"
    if seconds < 86400:
        return f"{int(seconds // 3600):>2d}h"
    return f"{int(seconds // 86400):>2d}d"


def _ledger_records(status: Mapping[str, Any] | None) -> int | None:
    """Return the ledger record count from a status snapshot, if present."""
    if not isinstance(status, Mapping):
        return None
    ledger = status.get("ledger")
    if not isinstance(ledger, Mapping):
        return None
    records = ledger.get("records")
    if isinstance(records, bool) or not isinstance(records, int):
        return None
    return records


def _event_seq(event: Mapping[str, Any]) -> int:
    """Extract integer event sequence number safely for sorting."""
    val = event.get("seq")
    try:
        return int(val) if val is not None else 0
    except (TypeError, ValueError):
        return 0


HELP_LINES = (
    "  q / Ctrl-C   detach (the daemon keeps running)",
    "  ↑ / ↓, k / j move the selection cursor",
    "  mouse click  select event / cycle filter",
    "  mouse wheel  scroll events up / down",
    "  Enter        inspect full event payload",
    "  PgUp / PgDn  scroll by page",
    "  g / G        jump to oldest / newest",
    "  space        pause or resume live updates",
    "  f            cycle the kind filter",
    "  1 - 6        jump to a specific kind filter",
    "  d            toggle the detail pane",
    "  s            inject a sample agent event",
    "  v            verify the ledger hash chain",
    "  b            reap expired work leases",
    "  Ctrl-L       force a full screen refresh",
    "  ?            close this help",
)

#: Shown in place of the key hints until the ledger has more than
#: :data:`FIRST_RUN_RECORDS` records, so a fresh cockpit teaches itself.
FIRST_RUN_TIP = "Tip: Press [s] to inject a sample agent event | [v] verify hash chain | [q] detach"
FIRST_RUN_RECORDS = 2

#: The demo event the ``s`` key injects through the live client.
SAMPLE_TOPIC = "quay/demo"
SAMPLE_PRODUCER = "quay"

#: Kept under 100 columns so the tail stays visible on a narrow terminal; the
#: help overlay lists the full set of keys, including the detail toggle.
HINT_LINE = " q detach  ↑↓/click move  enter inspect  space pause  f filter  s sample  v verify  ctrl-l refresh  ? help"

#: One SGR mouse report: ``\x1b[<button;column;row`` closed by ``M`` for a press
#: or a wheel tick, and by ``m`` for a release.
_SGR_MOUSE_PATTERN = re.compile(r"\x1b\[<([0-9]+);([0-9]+);([0-9]+)([Mm])")
#: Any complete CSI sequence: arrows, page keys, function keys, private modes.
_CSI_PATTERN = re.compile(r"\x1b\[[0-9;?]*[A-Za-z~]")
#: SS3 cursor keys, which some terminals still send instead of CSI arrows.
_SS3_PATTERN = re.compile(r"\x1bO[A-Za-z]")
#: Prefixes that begin an escape sequence we may not have received in full.
_ESCAPE_PREFIXES = ("\x1b[", "\x1bO")


def _tokenize(raw: str) -> tuple[list[str], str]:
    """Split ``raw`` into complete keys, returning an unfinished tail as well.

    A sequence that is cut off mid-flight is never broken into its own bytes:
    handing ``\\x1b[<0;15;5`` to the key handler as ``0``, ``1`` and ``5``
    would type those digits into the app, and *every* digit is a bound filter
    shortcut. The caller that can read more bytes may complete the tail; the
    caller that cannot must drop it.
    """
    tokens: list[str] = []
    index = 0
    total = len(raw)
    while index < total:
        char = raw[index]
        if char != "\x1b":
            tokens.append(char)
            index += 1
            continue
        match = (
            _SGR_MOUSE_PATTERN.match(raw, index)
            or _CSI_PATTERN.match(raw, index)
            or _SS3_PATTERN.match(raw, index)
        )
        if match is not None:
            tokens.append(match.group(0))
            index = match.end()
            continue
        if raw.startswith(_ESCAPE_PREFIXES, index):
            return tokens, raw[index:]
        # A lone ESC: Esc itself, or the prefix of an Alt-<key> chord.
        tokens.append(char)
        index += 1
    return tokens, ""


def split_keys(raw: str) -> list[str]:
    """Split a raw input buffer into individual keys and mouse events.

    Every byte is accounted for. An escape sequence cut off mid-way comes back
    as one opaque token, so a partial report can never masquerade as the keys
    its bytes happen to resemble.
    """
    if not raw:
        return []
    tokens, pending = _tokenize(raw)
    if pending:
        tokens.append(pending)
    return tokens


def stream_rows_for(height: int) -> int:
    """Return how many event rows a terminal ``height`` lines tall can show.

    The loop and the dashboard both size the viewport through this helper, so
    the click hit-test and the painted grid can never disagree about where the
    stream begins and ends.
    """
    return max(0, max(2, _int_or(height, 24) - 3) - STREAM_TOP + 1)


class Dashboard:
    """Builds a :class:`Screen` from plain data. No terminal required."""

    def __init__(self, *, detail: bool = True) -> None:
        self.detail = detail

    def render(self, model: Mapping[str, Any]) -> Screen:
        # Every field is treated as untrusted: a frame that cannot be drawn is
        # worse than a frame with a blank count, so nothing here may raise.
        width = max(1, _int_or(model.get("width"), 80))
        height = max(1, _int_or(model.get("height"), 24))
        screen = Screen(width, height)
        events = _events_of(model)
        selection = _int_or(model.get("selection"), 0)
        health = _mapping(model.get("health"))
        status = _mapping(model.get("status"))
        ledger = _mapping(status.get("ledger"))

        self._header(screen, model, health, ledger)
        body_top = STREAM_TOP
        stream_rows = stream_rows_for(height)
        show_side = bool(model.get("detail", self.detail)) and width >= 72
        stream_width = width - 34 if show_side else width

        scroll_offset = _int_or(model.get("scroll_offset"), 0)
        if stream_rows <= 0:
            # Nothing is visible, so no offset is meaningful.
            scroll_offset = 0
        elif scroll_offset <= 0 and selection >= stream_rows:
            scroll_offset = min(selection - stream_rows + 1, max(0, len(events) - stream_rows))
        scroll_offset = max(0, min(scroll_offset, max(0, len(events) - stream_rows)))

        scroll_info = ""
        if len(events) > stream_rows and stream_rows > 0:
            start = scroll_offset + 1
            end = min(scroll_offset + stream_rows, len(events))
            scroll_info = f"[{start}-{end}/{len(events)}]"

        self._columns(screen, body_top - 1, stream_width, scroll_info)
        self._stream(screen, body_top, stream_rows, stream_width, events, selection, model, scroll_offset)
        if show_side:
            self._planes(screen, body_top, stream_rows, width - 34, status, events, selection)
        self._footer(screen, height - 2, model, status)
        # The overlay is drawn last so the body cannot paint over it; its box
        # never reaches the footer row, so the verify banner stays visible.
        if model.get("help"):
            self._overlay(screen, "keys", HELP_LINES)
        elif model.get("inspecting") and 0 <= selection < len(events):
            self._render_inspect(screen, events[selection])
        return screen

    def _columns(self, screen: Screen, row: int, width: int, scroll_info: str = "") -> None:
        if 0 <= row < screen.height:
            header_text = " age   seq  kind     topic                  producer   summary"
            if scroll_info:
                header_text = f"{header_text:<60s} {scroll_info}"
            screen.put(row, 0, _shorten(header_text, width), "border")

    def _render_inspect(self, screen: Screen, event: Mapping[str, Any]) -> None:
        import json

        seq = event.get("seq", "?")
        kind = event.get("kind", "?")
        topic = event.get("topic", "")
        producer = event.get("producer", "")
        ts = event.get("ts", "")
        causation = event.get("causation_id")
        root_causation = event.get("root_causation_id")
        h = event.get("hash", "")
        lines = [
            f"Seq: #{seq}  Kind: {kind}  Producer: {producer}",
            f"Topic: {topic}",
            f"Timestamp: {ts}",
            f"Causation: #{causation}  Root: #{root_causation}" if causation else f"Root: #{root_causation}",
            f"Hash: {h}",
            "Payload:",
        ]
        payload = event.get("payload")
        try:
            payload_json = json.dumps(payload, indent=2, ensure_ascii=False, default=str)
            payload_lines = payload_json.splitlines()
            for pl_line in payload_lines[:_INSPECT_PAYLOAD_LINES]:
                lines.append(f"  {pl_line}")
            hidden = len(payload_lines) - _INSPECT_PAYLOAD_LINES
            if hidden > 0:
                # Say so rather than letting a long payload look complete.
                lines.append(f"  … {hidden} more line(s)")
        except Exception:
            lines.append(f"  {payload}")
        lines.append("")
        lines.append("Press Enter or Esc to dismiss")
        self._overlay(screen, f"inspect event #{seq}", lines)

    # ------------------------------------------------------------------ pieces

    def _header(self, screen: Screen, model: Mapping[str, Any], health: Mapping[str, Any], ledger: Mapping[str, Any]) -> None:
        screen.fill(0, "title")
        workspace = str(model.get("workspace", "?"))
        pid = health.get("pid", "-")
        uptime = health.get("uptime_seconds")
        uptime_text = f"{uptime:.0f}s" if isinstance(uptime, (int, float)) else "-"
        paused = "  PAUSED" if model.get("paused") else ""
        screen.put(0, 0, f" quay  {workspace}", "title")
        right = f"pid {pid}  up {uptime_text}  seq {ledger.get('last_seq', 0)}  records {ledger.get('records', 0)}{paused} "
        screen.put(0, max(0, screen.width - len(right)), right, "title")

        screen.fill(1, "dim")
        filter_label = str(model.get("filter_label", "all"))
        message = str(model.get("message") or "")
        left = f" filter: {filter_label}   events: {len(_events_of(model))}   dropped: {model.get('dropped', 0)}"
        screen.put(1, 0, left, "accent" if message else "dim")
        if message:
            screen.put(1, min(screen.width - 1, len(left) + 2), message, "accent")

    def _stream(
        self,
        screen: Screen,
        top: int,
        rows: int,
        width: int,
        events: Sequence[Mapping[str, Any]],
        selection: int,
        model: Mapping[str, Any],
        scroll_offset: int = 0,
    ) -> None:
        if rows <= 0:
            return
        for offset in range(rows):
            screen.fill(top + offset, "default")

        if not events:
            screen.put(top, 1, "waiting for events…", "dim")
            return
        visible = events[scroll_offset : scroll_offset + rows]
        for offset, event in enumerate(visible):
            row = top + offset
            actual_index = scroll_offset + offset
            selected = actual_index == selection
            style = "selected" if selected else KIND_STYLE.get(str(event.get("kind")), "default")
            seq = _seq_column(event.get("seq"))
            kind = str(event.get("kind", "?"))
            topic = str(event.get("topic", ""))
            producer = str(event.get("producer", ""))
            age = age_of(event.get("ts"))
            prefix = f"{age} #{seq} {kind:<7s} {topic:<22s} {producer:<10s} "
            line = prefix + summarize_event(event, max(8, width - len(prefix) - 2))
            screen.put(row, 0, _shorten(line, width), style)

    def _planes(
        self,
        screen: Screen,
        top: int,
        rows: int,
        col: int,
        status: Mapping[str, Any],
        events: Sequence[Mapping[str, Any]] = (),
        selection: int = -1,
    ) -> None:
        planes = _mapping(status.get("planes"))
        signal = _mapping(planes.get("signal"))
        work = _mapping(planes.get("work"))
        kinds = _mapping(status.get("kinds"))
        integrity = _mapping(status.get("integrity"))

        for offset in range(rows):
            screen.put(top + offset, col, "│", "border")
        cursor = top
        screen.put(cursor, col + 2, " planes", "bold")
        cursor += 1
        for label, value, style in (
            ("signal open", signal.get("open", 0), "signal"),
            ("signal pending", signal.get("pending", 0), "signal"),
            ("work active", work.get("active", 0), "work"),
            ("work done", work.get("done", 0), "ok"),
            ("work failed", work.get("failed", 0), "bad"),
            ("work expired", work.get("expired", 0), "warn"),
        ):
            if cursor >= top + rows:
                break
            screen.put(cursor, col + 2, f"{label:<15s}{_int_or(value):>6d}", style)
            cursor += 1
        cursor += 1
        if cursor < top + rows:
            screen.put(cursor, col + 2, " kinds", "bold")
            cursor += 1
        for kind, count in sorted(kinds.items(), key=lambda item: str(item[0])):
            if cursor >= top + rows:
                break
            label = str(kind)
            screen.put(cursor, col + 2, f"{label:<15s}{_int_or(count):>6d}", KIND_STYLE.get(label, "default"))
            cursor += 1
        cursor += 1
        if cursor < top + rows:
            chain = str(integrity.get("hash_chain", "unverified"))
            style = "ok" if chain == "verified" else ("bad" if chain == "failed" else "dim")
            screen.put(cursor, col + 2, f"chain: {chain}", style)
            cursor += 1
            screen.put(cursor, col + 2, f"verified seq: {integrity.get('verified_seq', 0)}", "dim")
            cursor += 1

        if 0 <= selection < len(events) and cursor < top + rows - 2:
            sel_event = events[selection]
            cursor += 1
            if cursor < top + rows:
                screen.put(cursor, col + 2, " selected event", "bold")
                cursor += 1
            seq = sel_event.get("seq", "?")
            kind = str(sel_event.get("kind", "?"))
            if cursor < top + rows:
                screen.put(cursor, col + 2, f"#{seq} ({kind})", KIND_STYLE.get(kind, "title"))
                cursor += 1
            if cursor < top + rows:
                topic = str(sel_event.get("topic", ""))
                screen.put(cursor, col + 2, _shorten(f"topic: {topic}", 30), "default")
                cursor += 1
            if cursor < top + rows:
                producer = str(sel_event.get("producer", ""))
                screen.put(cursor, col + 2, _shorten(f"from: {producer}", 30), "dim")
                cursor += 1
            if cursor < top + rows:
                causation = sel_event.get("causation_id")
                if causation:
                    screen.put(cursor, col + 2, f"cause: #{causation}", "dim")
                    cursor += 1
            if cursor < top + rows:
                payload = sel_event.get("payload")
                if isinstance(payload, Mapping):
                    summary = payload.get("summary") or payload.get("text") or payload.get("name")
                    if summary:
                        screen.put(cursor, col + 2, _shorten(f"info: {summary}", 30), "accent")
                        cursor += 1

    def _footer(self, screen: Screen, row: int, model: Mapping[str, Any], status: Mapping[str, Any]) -> None:
        if row < 0 or row >= screen.height:
            return
        screen.fill(row, "dim")
        if model.get("verify_banner"):
            screen.put(row, 0, f" {model['verify_banner']}", "ok" if str(model["verify_banner"]).startswith("chain ok") else "bad")
            return
        records = _ledger_records(status)
        if records is not None and records <= FIRST_RUN_RECORDS:
            screen.put(row, 0, FIRST_RUN_TIP, "accent")
            return
        screen.put(row, 0, HINT_LINE, "dim")

    def _overlay(self, screen: Screen, title: str, lines: Iterable[str]) -> None:
        body = list(lines)
        # A terminal too small to hold a frame gets no frame, rather than a
        # torn one painted over rows that do not exist.
        box_width = max(0, min(screen.width - 2, 62))
        box_height = max(0, min(screen.height - 2, len(body) + 3))
        if box_width <= 0 or box_height <= 0:
            return
        top = max(0, (screen.height - box_height) // 2)
        left = max(0, (screen.width - box_width) // 2)
        for offset in range(box_height):
            screen.fill(top + offset, "default")
            screen.put(top + offset, left, "│", "border")
        screen.put(top, left, "┌" + "─" * (box_width - 2) + "┐", "border")
        screen.put(top, left + 2, f" {title} ", "bold")
        for index, line in enumerate(body):
            if top + 1 + index >= top + box_height - 1:
                break
            screen.put(top + 1 + index, left + 2, line, "default")
        screen.put(top + box_height - 1, left, "└" + "─" * (box_width - 2) + "┘", "border")


class TuiApp:
    """Attaches the dashboard to a live subscription on a real terminal."""

    def __init__(
        self,
        socket_path: os.PathLike[str] | str,
        *,
        workspace: str = "",
        filter_kind: str | None = None,
        history: int = 200,
        refresh_seconds: float = 1.0,
    ) -> None:
        self.socket_path = socket_path
        self.workspace = workspace
        self.filter_index = next(
            (index for index, (_label, kind) in enumerate(FILTER_PRESETS) if kind == filter_kind), 0
        )
        self.history = int(history)
        self.refresh_seconds = refresh_seconds
        self.dashboard = Dashboard()
        self.selection = 0
        self.scroll_offset = 0
        self.inspecting = False
        self.stream_rows = 20
        self.last_width = 80
        self.last_height = 24
        self.paused = False
        self.help = False
        self.message = ""
        self.verify_banner = ""
        self.events: list[dict[str, Any]] = []
        self.dropped = 0
        self.status: dict[str, Any] = {}
        self.health: dict[str, Any] = {}
        self.redraw_requested = False

    # ------------------------------------------------------------------ model

    @property
    def filter_kind(self) -> str | None:
        return FILTER_PRESETS[self.filter_index][1]

    @property
    def filter_label(self) -> str:
        return FILTER_PRESETS[self.filter_index][0]

    def model(self, *, width: int, height: int) -> dict[str, Any]:
        return {
            "width": width,
            "height": height,
            "workspace": self.workspace,
            "events": self.events,
            "selection": self.selection,
            "scroll_offset": self.scroll_offset,
            "filter_label": self.filter_label,
            "paused": self.paused,
            "help": self.help,
            "inspecting": self.inspecting,
            "message": self.message,
            "verify_banner": self.verify_banner,
            "dropped": self.dropped,
            "health": self.health,
            "status": self.status,
            "detail": self.dashboard.detail,
        }

    # ------------------------------------------------------------------- state

    def ingest(self, event: Mapping[str, Any]) -> None:
        """Add one pushed event to the visible history."""
        if self.paused:
            return
        kind = self.filter_kind
        if kind is not None and event.get("kind") != kind:
            return
        seq = event.get("seq")
        if seq is not None and any(e.get("seq") == seq for e in self.events):
            return
        self.events.insert(0, dict(event))
        self.events.sort(key=_event_seq, reverse=True)
        del self.events[self.history :]

    def reload_history(self, client) -> None:
        """Re-read history after a filter change."""
        try:
            events = client.query(kind=self.filter_kind, limit=self.history, order="desc")
        except Exception as exc:  # pragma: no cover - surfaced in the UI
            self.message = f"history reload failed: {exc}"
            return
        self.events = [dict(event) for event in events]
        self.events.sort(key=_event_seq, reverse=True)
        self.selection = 0
        self.scroll_offset = 0

    # ------------------------------------------------------------------ redraw

    def request_redraw(self) -> None:
        """Ask the loop to clear the screen and repaint every cell.

        Both the SIGWINCH handler and the Ctrl-L binding funnel through here, so
        the request path stays cheap and free of terminal I/O and of model
        changes. The loop honours it on its next iteration.
        """
        self.redraw_requested = True

    def take_redraw_request(self) -> bool:
        """Consume a pending redraw request, clearing it only once honoured."""
        if not self.redraw_requested:
            return False
        self.redraw_requested = False
        return True

    def ensure_selection_visible(self) -> None:
        """Keep the selection in range and the selected row in the viewport."""
        # Clamp first: a shrinking history (or a page key on an empty ledger)
        # can strand the cursor past the end, where nothing renders as selected.
        self.selection = max(0, min(self.selection, len(self.events) - 1))
        rows = self.stream_rows
        if rows <= 0 or not self.events:
            self.scroll_offset = 0
            return
        max_offset = max(0, len(self.events) - rows)
        if self.selection < self.scroll_offset:
            self.scroll_offset = max(0, min(self.selection, max_offset))
        elif self.selection >= self.scroll_offset + rows:
            self.scroll_offset = max(0, min(self.selection - rows + 1, max_offset))

    def handle_key(self, key: str, client) -> bool:
        """Apply one key press or mouse event. Returns False when the UI should exit."""
        if not key:
            return True
        if key in ("q", "\x03"):
            if self.inspecting or self.help:
                self.inspecting = False
                self.help = False
                self.request_redraw()
                return True
            return False

        if key == "\x1b":
            if self.inspecting or self.help:
                self.inspecting = False
                self.help = False
                self.request_redraw()
            return True

        if key in ("\r", "\n"):
            if self.inspecting:
                self.inspecting = False
            elif self.events and 0 <= self.selection < len(self.events):
                self.inspecting = True
            self.request_redraw()
            return True

        if key == "?":
            self.help = not self.help
            if self.help:
                self.inspecting = False
            self.request_redraw()
            return True

        # SGR Mouse event: \x1b[<b;x;y;M or \x1b[<b;x;y;m
        if key.startswith("\x1b[<") and (key.endswith("M") or key.endswith("m")):
            self._handle_mouse(key, client)
            return True

        # Dismiss overlays on any navigation key
        if self.help or self.inspecting:
            self.help = False
            self.inspecting = False
            self.request_redraw()

        if key in ("\x1b[B", "\x1bOB", "j") and self.events:
            self.selection = min(self.selection + 1, len(self.events) - 1)
            self.ensure_selection_visible()
        elif key in ("\x1b[A", "\x1bOA", "k"):
            self.selection = max(0, self.selection - 1)
            self.ensure_selection_visible()
        elif key == "\x1b[5~":  # Page Up
            self.selection = max(0, self.selection - max(1, self.stream_rows - 2))
            self.ensure_selection_visible()
        elif key == "\x1b[6~":  # Page Down
            self.selection = max(0, min(len(self.events) - 1, self.selection + max(1, self.stream_rows - 2)))
            self.ensure_selection_visible()
        elif key in ("g", "\x1b[F", "\x1b[4~", "\x1bOF"):
            self.selection = max(0, len(self.events) - 1)
            self.ensure_selection_visible()
        elif key in ("G", "\x1b[H", "\x1b[1~", "\x1bOH"):
            self.selection = 0
            self.ensure_selection_visible()
        elif key == " ":
            self.paused = not self.paused
            self.message = "paused" if self.paused else ""
        elif key == "f":
            self.filter_index = (self.filter_index + 1) % len(FILTER_PRESETS)
            self.reload_history(client)
        elif len(key) == 1 and key in "123456":
            self.filter_index = int(key) - 1
            self.reload_history(client)
        elif key == "d":
            self.dashboard.detail = not self.dashboard.detail
        elif key == "s":
            self.run_sample(client)
        elif key == "v":
            self.run_verify(client)
        elif key == "b":
            self.run_reap(client)
        elif key == REFRESH_KEY:
            # Ctrl-L: repaint only. No model change, no ledger event.
            self.request_redraw()
        return True

    def _handle_mouse(self, key: str, client) -> None:
        if self.help or self.inspecting:
            self.help = False
            self.inspecting = False
            self.request_redraw()
            return

        match = _SGR_MOUSE_PATTERN.fullmatch(key)
        if match is None:
            # Anything that is not a whole SGR report (a truncated packet, a
            # future extension) is ignored rather than guessed at.
            return
        button = int(match.group(1))
        col = max(0, int(match.group(2)) - 1)
        row = max(0, int(match.group(3)) - 1)
        is_release = match.group(4) == "m"

        if button & 32:
            # Motion/drag reporting is enabled by 1002; only presses act.
            return
        # Mask the modifier bits (shift 4, alt 8, ctrl 16) so shift+wheel still
        # scrolls and shift+click still selects.
        base = button & ~(4 | 8 | 16)

        if base & 64:  # Wheel: 64 up, 65 down, 66/67 sideways
            if not self.events:
                return
            direction = base - 64
            if direction == 0:
                self.selection = max(0, self.selection - 1)
            elif direction == 1:
                self.selection = min(len(self.events) - 1, self.selection + 1)
            else:
                return  # horizontal wheel has nothing to scroll here
            self.ensure_selection_visible()
            self.request_redraw()
            return

        if base != 0 or is_release:  # only a plain left-button press acts
            return

        body_top = STREAM_TOP
        if body_top <= row < body_top + self.stream_rows and self.events:
            offset = row - body_top
            target = self.scroll_offset + offset
            if 0 <= target < len(self.events):
                self.selection = target
                self.ensure_selection_visible()
                self.request_redraw()
        elif row == 1:
            # Clicked on filter bar
            self.filter_index = (self.filter_index + 1) % len(FILTER_PRESETS)
            self.reload_history(client)
            self.request_redraw()
        elif row == 0 and col >= max(0, self.last_width - _PAUSED_CLICK_ZONE):
            # Clicked near PAUSED
            self.paused = not self.paused
            self.message = "paused" if self.paused else ""
            self.request_redraw()

    def run_sample(self, client) -> None:
        """Inject one schema-valid demo event so the planes visibly move."""
        try:
            event = client.emit(
                topic=SAMPLE_TOPIC,
                producer=SAMPLE_PRODUCER,
                payload={"state": "pending", "summary": "sample agent event from the cockpit"},
                kind="signal",
            )
        except Exception as exc:  # pragma: no cover - surfaced in the UI
            self.message = f"sample event failed: {exc}"
            return
        self.message = f"injected sample event #{event.get('seq')} on {SAMPLE_TOPIC}"

    def run_verify(self, client) -> None:
        try:
            report = client.request("verify")
        except Exception as exc:  # pragma: no cover - surfaced in the UI
            self.verify_banner = f"verify failed: {exc}"
            return
        if report.get("ok"):
            self.verify_banner = f"chain ok · {report['events_checked']} events · head {str(report['head_hash'])[:12]}"
        else:
            divergence = report.get("divergence") or {"reason": "gaps"}
            self.verify_banner = f"chain BROKEN at seq {divergence.get('seq')}: {divergence.get('reason')}"

    def run_reap(self, client) -> None:
        try:
            result = client.request("work_reap")
        except Exception as exc:  # pragma: no cover - surfaced in the UI
            self.message = f"reap failed: {exc}"
            return
        self.message = f"reaped {result.get('count', 0)} expired lease(s)"

    # ------------------------------------------------------------------- loop

    def run(self) -> int:
        from .client import ClientError, QuayClient, SubscriptionStream

        try:
            with QuayClient(self.socket_path, timeout=5.0) as client:
                with SubscriptionStream(
                    self.socket_path, filters={"kind": self.filter_kind}, replay=self.history
                ) as live:
                    self._prime(client)
                    return self._loop(client, live)
        except ClientError as exc:
            print(f"quay: error: {exc}", file=sys.stderr)
            return 3

    def _prime(self, client) -> None:
        with contextlib.suppress(Exception):
            self.health = client.health()
        with contextlib.suppress(Exception):
            self.status = client.status()
        self.reload_history(client)

    def _loop(
        self,
        client,
        live: SubscriptionStream,
        *,
        reader_factory=None,
        output=None,
        max_iterations: int | None = None,
        size_getter=None,
    ) -> int:
        """Render until the user detaches.

        ``reader_factory``, ``output``, ``max_iterations``, and ``size_getter``
        exist so the loop can be driven headlessly in tests; production always
        passes the real terminal session and stdout.
        """
        stream = output if output is not None else sys.stdout
        factory = reader_factory or terminal_session
        get_size = size_getter or shutil.get_terminal_size
        size = get_size(fallback=(100, 30))
        self.last_width = size.columns
        self.last_height = size.lines
        self.stream_rows = stream_rows_for(size.lines)
        # The geometry is tracked separately from ``previous``: the cell cache
        # only describes frames we drew, while the terminal may have cleared or
        # reflowed the screen behind our back.
        last_geometry = (size.columns, size.lines)
        previous: list[list[tuple[str, str]]] | None = None
        last_status = 0.0
        iterations = 0
        stream_ended = False
        with factory(on_resize=self.request_redraw) as reader:
            write = stream.write
            try:
                write(f"{ALT_SCREEN_ON}{CLEAR_SCREEN}{CURSOR_HIDE}{MOUSE_ON}")
                while True:
                    size = get_size(fallback=size)
                    geometry = (size.columns, size.lines)
                    self.last_width = size.columns
                    self.last_height = size.lines
                    self.stream_rows = stream_rows_for(size.lines)
                    # Always consume the request: a resize and SIGWINCH often
                    # arrive together, and a lingering flag would repaint twice.
                    redraw_requested = self.take_redraw_request()
                    if geometry != last_geometry or redraw_requested:
                        # The viewport changed, or the user/SIGWINCH asked for a
                        # repaint. tmux and the terminal emulator clear or
                        # reflow on resize, so every cached cell is suspect:
                        # wipe the screen and force a full paint instead of
                        # letting render_diff suppress "unchanged" cells.
                        last_geometry = geometry
                        write(CLEAR_SCREEN)
                        previous = None
                    now = time.monotonic()
                    if now - last_status >= self.refresh_seconds:
                        with contextlib.suppress(Exception):
                            self.status = client.status()
                        with contextlib.suppress(Exception):
                            self.health = client.health()
                        last_status = now
                    frame = None if stream_ended else live.poll(0.0)
                    while frame is not None:
                        if frame.get("type") == "event":
                            self.ingest(frame["event"])
                        elif frame.get("type") == "lagged":
                            self.dropped += int(frame.get("dropped", 0))
                            self.message = f"daemon dropped {self.dropped} push(es); re-read history"
                        elif frame.get("type") == "shutdown":
                            self.message = "daemon is shutting down"
                            stream_ended = True
                            frame = None
                            break
                        elif frame.get("type") == "recovery_error":
                            error = frame.get("error") or {}
                            self.message = (
                                "stream ended: "
                                f"{error.get('code', 'recovery_failed')}"
                            )
                            stream_ended = True
                            frame = None
                            break
                        frame = live.poll(0.0)

                    screen = self.dashboard.render(
                        self.model(width=size.columns, height=size.lines)
                    )
                    diff = render_diff(previous, screen.cells)
                    if diff:
                        write(diff)
                        write("\x1b[H")
                        stream.flush()
                    previous = screen.cells

                    iterations += 1
                    if max_iterations is not None and iterations >= max_iterations:
                        return 0

                    key = reader(0.1)
                    if key is None:
                        continue
                    # Escape sequences usually arrive in one burst, but a slow
                    # pty can hand a mouse report over in pieces. Keep reading
                    # while the buffer ends in an unfinished sequence; the caps
                    # bound both the extra latency and the memory a stuck
                    # sequence can hold.
                    buffer = key
                    tokens, pending = _tokenize(buffer)
                    for _ in range(_MAX_TAIL_READS):
                        if not pending or len(buffer) >= _MAX_INPUT_BYTES:
                            break
                        tail = reader(_TAIL_READ_TIMEOUT)
                        if not tail:
                            break
                        buffer += tail
                        tokens, pending = _tokenize(buffer)
                    if pending:
                        # Still cut off. Hand it over as one opaque token: a bare
                        # ESC keeps dismissing overlays, and a half-received
                        # report stays inert instead of typing its coordinates.
                        tokens.append(pending)
                    for token in tokens:
                        if not self.handle_key(token, client):
                            return 0
            except KeyboardInterrupt:
                return 0
            finally:
                # Restoration must never mask the reason the loop ended, and it
                # runs on every path: detach, Ctrl-C, resize, or a raise.
                with contextlib.suppress(OSError, ValueError):
                    write(f"{MOUSE_OFF}{CURSOR_SHOW}{ALT_SCREEN_OFF}")
                    stream.flush()


@contextlib.contextmanager
def terminal_session(*, on_resize=None):
    """Put the terminal in cbreak mode and yield a non-blocking key reader.

    Falls back to a no-op when stdin is not a terminal (pipes, test runners),
    where every read simply reports "no key".

    When ``on_resize`` is supplied and the platform provides ``SIGWINCH``, a
    narrowly scoped handler asks the app for a redraw and nudges the blocking
    reader awake through a self-pipe. The handler performs no terminal I/O, and
    the previous handler is restored on every exit path.
    """
    try:
        stdin = sys.stdin.fileno()
    except (OSError, ValueError, AttributeError, io.UnsupportedOperation):
        stdin = None
    saved = None
    if stdin is not None:
        try:
            saved = termios.tcgetattr(stdin)
            tty.setcbreak(stdin)
        except (termios.error, ValueError, OSError):
            saved = None

    wake_read: int | None = None
    wake_write: int | None = None
    winch = getattr(signal, "SIGWINCH", None)
    previous_handler = None
    if on_resize is not None and winch is not None:
        try:
            wake_read, wake_write = os.pipe()
            os.set_blocking(wake_read, False)
            os.set_blocking(wake_write, False)
        except (OSError, AttributeError):  # pragma: no cover - exotic platforms
            wake_read = wake_write = None

        def _on_winch(_signum, _frame) -> None:
            on_resize()
            if wake_write is not None:
                with contextlib.suppress(OSError, ValueError):
                    os.write(wake_write, b"\x00")

        try:
            previous_handler = signal.getsignal(winch)
            signal.signal(winch, _on_winch)
        except (OSError, ValueError, RuntimeError):
            # Signals can only be trapped from the main thread; a worker thread
            # keeps the polling reader and simply misses the prompt wakeup.
            previous_handler = None

    # Bytes of a code point split across two reads are held here rather than
    # decoded into a replacement character, so a pasted or fast-typed non-ASCII
    # key survives the read boundary intact.
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    def read_key(timeout: float) -> str | None:
        if stdin is None:
            time.sleep(min(timeout, 0.05))
            return None
        watched = [stdin] if wake_read is None else [stdin, wake_read]
        try:
            ready, _, _ = select.select(watched, [], [], timeout)
        except (OSError, ValueError):
            return None
        if wake_read is not None and wake_read in ready:
            with contextlib.suppress(OSError, ValueError):
                os.read(wake_read, 4096)
            if stdin not in ready:
                return None
        if not ready:
            return None
        try:
            raw = os.read(stdin, 1024)
        except (OSError, ValueError, io.UnsupportedOperation):
            # A stdin we cannot read by descriptor (a wrapper, a test harness).
            # Only touch it when a byte is already waiting: a blocking read here
            # would freeze the cockpit.
            try:
                if not select.select([stdin], [], [], 0)[0]:
                    return None
                return sys.stdin.read(1) or None
            except Exception:
                return None
        if not raw:
            # EOF. Yield the loop's slice instead of spinning on a closed fd.
            time.sleep(min(timeout, 0.05))
            return None
        text = decoder.decode(raw)
        return text or None

    try:
        yield read_key
    finally:
        if previous_handler is not None and winch is not None:
            with contextlib.suppress(OSError, ValueError):
                signal.signal(winch, previous_handler)
        for descriptor in (wake_read, wake_write):
            if descriptor is not None:
                with contextlib.suppress(OSError):
                    os.close(descriptor)
        if saved is not None:
            with contextlib.suppress(termios.error, ValueError, OSError):
                termios.tcsetattr(stdin, termios.TCSADRAIN, saved)

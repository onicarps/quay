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

import contextlib
import io
import os
import shutil
import select
import sys
import termios
import time
import tty
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

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
    "note": "\x1b[38;5;150m",
    "metric": "\x1b[38;5;183m",
    "generic": "\x1b[38;5;245m",
}
RESET = "\x1b[0m"

KIND_STYLE = {
    "signal": "signal",
    "work": "work",
    "note": "note",
    "metric": "metric",
    "generic": "generic",
}

#: Filter presets cycled with the ``f`` key.
FILTER_PRESETS: tuple[tuple[str, str | None], ...] = (
    ("all", None),
    ("signal", "signal"),
    ("work", "work"),
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

        for col in range(width):
            cell = current[row][col]
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


def age_of(timestamp: str | None, *, now: float | None = None) -> str:
    """Return a compact age such as ``3s`` or ``2m`` for an ISO timestamp."""
    if not timestamp:
        return "  -"
    try:
        from .store import parse_ts

        moment = parse_ts(timestamp).timestamp()
    except (ValueError, TypeError):
        return "  -"
    seconds = max(0.0, (time.time() if now is None else now) - moment)
    if seconds < 60:
        return f"{int(seconds):>2d}s"
    if seconds < 3600:
        return f"{int(seconds // 60):>2d}m"
    if seconds < 86400:
        return f"{int(seconds // 3600):>2d}h"
    return f"{int(seconds // 86400):>2d}d"


HELP_LINES = (
    "  q / Ctrl-C   detach (the daemon keeps running)",
    "  ↑ / ↓, k / j move the selection",
    "  g / G        jump to oldest / newest",
    "  space        pause or resume live updates",
    "  f            cycle the kind filter",
    "  1 - 5        jump to a specific kind filter",
    "  d            toggle the detail pane",
    "  v            verify the ledger hash chain",
    "  b            reap expired work leases",
    "  ?            close this help",
)


class Dashboard:
    """Builds a :class:`Screen` from plain data. No terminal required."""

    def __init__(self, *, detail: bool = True) -> None:
        self.detail = detail

    def render(self, model: Mapping[str, Any]) -> Screen:
        width = int(model.get("width", 80))
        height = int(model.get("height", 24))
        screen = Screen(width, height)
        events: Sequence[Mapping[str, Any]] = model.get("events", ())
        selection = int(model.get("selection", 0))
        health = model.get("health") or {}
        status = model.get("status") or {}
        ledger = status.get("ledger", {}) if isinstance(status, Mapping) else {}

        self._header(screen, model, health, ledger)
        if model.get("help"):
            self._overlay(screen, "keys", HELP_LINES)
        body_top = 3
        body_bottom = height - 3
        if body_bottom < body_top:
            body_bottom = body_top - 1
        stream_rows = max(0, body_bottom - body_top + 1)
        show_side = bool(model.get("detail", self.detail)) and width >= 72
        stream_width = width - 34 if show_side else width
        self._columns(screen, body_top - 1, stream_width)
        self._stream(screen, body_top, stream_rows, stream_width, events, selection, model)
        if show_side:
            self._planes(screen, body_top, stream_rows, width - 34, status)
        self._footer(screen, height - 2, model, status)
        return screen

    def _columns(self, screen: Screen, row: int, width: int) -> None:
        if 0 <= row < screen.height:
            screen.put(row, 0, _shorten(" age   seq  kind     topic                  producer   summary", width), "border")

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
        left = f" filter: {filter_label}   events: {len(model.get('events', ()))}   dropped: {model.get('dropped', 0)}"
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
    ) -> None:
        if rows <= 0:
            return
        for offset in range(rows):
            screen.fill(top + offset, "default")

        if not events:
            screen.put(top, 1, "waiting for events…", "dim")
            return
        visible = events[:rows]
        for index, event in enumerate(visible):
            row = top + index
            selected = index == selection
            style = "selected" if selected else KIND_STYLE.get(str(event.get("kind")), "default")
            seq = event.get("seq", "?")
            kind = str(event.get("kind", "?"))
            topic = str(event.get("topic", ""))
            producer = str(event.get("producer", ""))
            age = age_of(event.get("ts"))
            prefix = f"{age} #{seq:<5d} {kind:<7s} {topic:<22s} {producer:<10s} "
            line = prefix + summarize_event(event, max(8, width - len(prefix) - 2))
            screen.put(row, 0, _shorten(line, width), style)

    def _planes(self, screen: Screen, top: int, rows: int, col: int, status: Mapping[str, Any]) -> None:
        planes = status.get("planes", {}) if isinstance(status, Mapping) else {}
        signal = planes.get("signal", {}) if isinstance(planes, Mapping) else {}
        work = planes.get("work", {}) if isinstance(planes, Mapping) else {}
        kinds = status.get("kinds", {}) if isinstance(status, Mapping) else {}
        integrity = status.get("integrity", {}) if isinstance(status, Mapping) else {}

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
            screen.put(cursor, col + 2, f"{label:<15s}{value:>6d}", style)
            cursor += 1
        cursor += 1
        if cursor < top + rows:
            screen.put(cursor, col + 2, " kinds", "bold")
            cursor += 1
        for kind, count in sorted(kinds.items()):
            if cursor >= top + rows:
                break
            screen.put(cursor, col + 2, f"{kind:<15s}{count:>6d}", KIND_STYLE.get(kind, "default"))
            cursor += 1
        cursor += 1
        if cursor < top + rows:
            chain = str(integrity.get("hash_chain", "unverified"))
            style = "ok" if chain == "verified" else ("bad" if chain == "failed" else "dim")
            screen.put(cursor, col + 2, f"chain: {chain}", style)
            cursor += 1
            screen.put(cursor, col + 2, f"verified seq: {integrity.get('verified_seq', 0)}", "dim")

    def _footer(self, screen: Screen, row: int, model: Mapping[str, Any], status: Mapping[str, Any]) -> None:
        if row < 0 or row >= screen.height:
            return
        screen.fill(row, "dim")
        if model.get("verify_banner"):
            screen.put(row, 0, f" {model['verify_banner']}", "ok" if str(model["verify_banner"]).startswith("chain ok") else "bad")
            return
        hints = " q detach  ↑↓ move  space pause  f filter  1-5 kind  d detail  v verify  b reap  ? help"
        screen.put(row, 0, hints, "dim")

    def _overlay(self, screen: Screen, title: str, lines: Iterable[str]) -> None:
        body = list(lines)
        box_width = min(screen.width - 2, 62)
        box_height = min(screen.height - 2, len(body) + 3)
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
        self.paused = False
        self.help = False
        self.message = ""
        self.verify_banner = ""
        self.events: list[dict[str, Any]] = []
        self.dropped = 0
        self.status: dict[str, Any] = {}
        self.health: dict[str, Any] = {}

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
            "filter_label": self.filter_label,
            "paused": self.paused,
            "help": self.help,
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
        self.events.insert(0, dict(event))
        del self.events[self.history :]

    def reload_history(self, client) -> None:
        """Re-read history after a filter change."""
        try:
            events = client.query(kind=self.filter_kind, limit=self.history, order="desc")
        except Exception as exc:  # pragma: no cover - surfaced in the UI
            self.message = f"history reload failed: {exc}"
            return
        self.events = [dict(event) for event in events]
        self.selection = 0

    def handle_key(self, key: str, client) -> bool:
        """Apply one key press. Returns False when the UI should exit."""
        if key in ("q", "\x03"):
            return False
        if key == "?":
            self.help = not self.help
        elif key in ("\x1b[B", "j") and self.events:
            self.selection = min(self.selection + 1, len(self.events) - 1)
        elif key in ("\x1b[A", "k"):
            self.selection = max(0, self.selection - 1)
        elif key == "g":
            self.selection = max(0, len(self.events) - 1)
        elif key == "G":
            self.selection = 0
        elif key == " ":
            self.paused = not self.paused
            self.message = "paused" if self.paused else ""
        elif key == "f":
            self.filter_index = (self.filter_index + 1) % len(FILTER_PRESETS)
            self.reload_history(client)
        elif key in "12345":
            self.filter_index = int(key) - 1
            self.reload_history(client)
        elif key == "d":
            self.dashboard.detail = not self.dashboard.detail
        elif key == "v":
            self.run_verify(client)
        elif key == "b":
            self.run_reap(client)
        return True

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
    ) -> int:
        """Render until the user detaches.

        ``reader_factory``, ``output``, and ``max_iterations`` exist so the loop
        can be driven headlessly in tests; production always passes the real
        terminal session and stdout.
        """
        stream = output if output is not None else sys.stdout
        factory = reader_factory or terminal_session
        size = shutil.get_terminal_size(fallback=(100, 30))
        previous: list[list[tuple[str, str]]] | None = None
        last_status = 0.0
        iterations = 0
        with factory() as reader:
            write = stream.write
            write("\x1b[?1049h\x1b[2J\x1b[H\x1b[?25l")
            try:
                while True:
                    size = shutil.get_terminal_size(fallback=size)
                    now = time.monotonic()
                    if now - last_status >= self.refresh_seconds:
                        with contextlib.suppress(Exception):
                            self.status = client.status()
                        with contextlib.suppress(Exception):
                            self.health = client.health()
                        last_status = now
                    frame = live.poll(0.0)
                    while frame is not None:
                        if frame.get("type") == "event":
                            self.ingest(frame["event"])
                        elif frame.get("type") == "lagged":
                            self.dropped += int(frame.get("dropped", 0))
                            self.message = f"daemon dropped {self.dropped} push(es); re-read history"
                        elif frame.get("type") == "shutdown":
                            self.message = "daemon is shutting down"
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
                    # Escape sequences arrive in one burst; read the tail too.
                    if key == "\x1b":
                        tail = reader(0.01)
                        if tail:
                            key += tail
                    if not self.handle_key(key, client):
                        return 0
            except KeyboardInterrupt:
                return 0
            finally:
                write("\x1b[?25h\x1b[?1049l")
                stream.flush()


@contextlib.contextmanager
def terminal_session():
    """Put the terminal in cbreak mode and yield a non-blocking key reader.

    Falls back to a no-op when stdin is not a terminal (pipes, test runners),
    where every read simply reports "no key".
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

    def read_key(timeout: float) -> str | None:
        if stdin is None:
            time.sleep(min(timeout, 0.05))
            return None
        try:
            ready, _, _ = select.select([stdin], [], [], timeout)
        except (OSError, ValueError):
            return None
        if not ready:
            return None
        try:
            return sys.stdin.read(1)
        except (OSError, ValueError, io.UnsupportedOperation):
            return None

    try:
        yield read_key
    finally:
        if saved is not None:
            with contextlib.suppress(termios.error, ValueError, OSError):
                termios.tcsetattr(stdin, termios.TCSADRAIN, saved)

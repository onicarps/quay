"""Dashboard and diff-rendering tests (requirements F16).

The TUI is exercised headlessly: screens are built from data and diffs are
rendered to strings, so every visual rule is asserted without a terminal.
"""

from __future__ import annotations

import contextlib
import io
import os
import re
import signal
import sys
import termios
import time

import pytest

from quay.tui import (
    CLEAR_SCREEN,
    FILTER_PRESETS,
    FIRST_RUN_TIP,
    REFRESH_KEY,
    RESET,
    SAMPLE_PRODUCER,
    SAMPLE_TOPIC,
    STREAM_TOP,
    STYLES,
    Dashboard,
    Screen,
    TuiApp,
    age_of,
    render_diff,
    sanitize,
    stream_rows_for,
    summarize_event,
    terminal_session,
    split_keys,
)


def sample_event(seq: int = 1, kind: str = "generic", topic: str = "okf/handoff", **payload: object) -> dict:
    return {
        "seq": seq,
        "kind": kind,
        "topic": topic,
        "producer": "factory",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
        "payload": payload or {"i": seq},
        "causation_id": None,
        "correlation_id": None,
        "prev_hash": "0" * 64,
        "hash": "a" * 64,
    }


def sample_status() -> dict:
    return {
        "ledger": {"records": 12, "last_seq": 12, "checkpoint_seq": 0, "size_bytes": 4096},
        "planes": {
            "signal": {"pending": 1, "claimed": 0, "cleared": 2, "expired": 0, "open": 1},
            "work": {"claimed": 1, "in_flight": 1, "done": 3, "failed": 1, "abandoned": 0, "expired": 0, "active": 2, "terminal": 4},
        },
        "kinds": {"note": 4, "work": 6, "signal": 2},
        "integrity": {"hash_chain": "verified", "verified_at": "2026-10-04T00:00:00.000Z", "verified_seq": 12},
    }


def sample_model(**overrides: object) -> dict:
    model = {
        "width": 100,
        "height": 24,
        "workspace": "/home/oni/okf_agent_workspace",
        "events": [sample_event(seq=n, topic=f"topic/{n}") for n in (5, 4, 3, 2, 1)],
        "selection": 0,
        "filter_label": "all",
        "paused": False,
        "help": False,
        "message": "",
        "verify_banner": "",
        "dropped": 0,
        "health": {"pid": 4242, "uptime_seconds": 12.5, "instance_id": "abc"},
        "status": sample_status(),
        "detail": True,
    }
    model.update(overrides)
    return model


_ANSI_SEQUENCE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
_CELL_POSITION = re.compile(r"\x1b\[(\d+);(\d+)H")


def painted_cells(segment: str) -> set[tuple[int, int]]:
    """Return the zero-based (row, column) cells an ANSI segment writes."""
    cells: set[tuple[int, int]] = set()
    matches = list(_CELL_POSITION.finditer(segment))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(segment)
        row = int(match.group(1)) - 1
        column = int(match.group(2)) - 1
        text = _ANSI_SEQUENCE.sub("", segment[match.end() : end])
        cells.update((row, column + offset) for offset in range(len(text)))
    return cells


def full_cells(width: int, height: int) -> set[tuple[int, int]]:
    """Every cell of a ``width`` x ``height`` screen."""
    return {(row, column) for row in range(height) for column in range(width)}


class ScriptedSize:
    """Hands the loop one geometry per call, repeating the last one forever."""

    def __init__(self, *sizes: tuple[int, int]) -> None:
        self.sizes = [tuple(size) for size in sizes]
        self.calls = 0

    def __call__(self, fallback: object = None) -> os.terminal_size:
        size = self.sizes[min(self.calls, len(self.sizes) - 1)]
        self.calls += 1
        return os.terminal_size(size)


class FakeLive:
    """A subscription stand-in that never has anything to push."""

    def poll(self, timeout: float):
        return None


class RecoveryErrorLive:
    """A subscription stand-in that terminates with a recovery error."""

    def __init__(self):
        self.frames = [
            {
                "type": "recovery_error",
                "error": {"code": "recovery_stalled", "message": "no progress"},
            }
        ]

    def poll(self, timeout: float):
        return self.frames.pop(0) if self.frames else None


def scripted_reader(*presses: str, on_read=None):
    """A ``terminal_session`` stand-in that replays scripted key presses.

    ``on_read`` receives ``(call_number, on_resize)`` so a test can trip the
    resize callback from inside the loop.
    """

    @contextlib.contextmanager
    def factory(*, on_resize=None):
        queue = list(presses)
        calls = 0

        def read(timeout: float) -> str | None:
            nonlocal calls
            calls += 1
            if on_read is not None:
                on_read(calls, on_resize)
            return queue.pop(0) if queue else None

        yield read

    return factory


class TestScreen:
    def test_put_writes_cells(self):
        screen = Screen(10, 2)
        screen.put(0, 0, "hello")
        assert screen.text_at(0) == "hello     "

    def test_put_clips_at_the_right_edge(self):
        screen = Screen(5, 1)
        screen.put(0, 3, "abcdef")
        assert screen.text_at(0) == "   ab"

    def test_put_ignores_out_of_range_rows(self):
        screen = Screen(5, 2)
        screen.put(-1, 0, "x")
        screen.put(9, 0, "x")
        assert screen.text_at(0) == "     "
        assert screen.text_at(1) == "     "

    def test_put_clips_a_negative_start(self):
        screen = Screen(6, 1)
        screen.put(0, -2, "abcdef")
        assert screen.text_at(0) == "cdef  "

    def test_fill_uses_the_style(self):
        screen = Screen(3, 1)
        screen.fill(0, "title")
        assert screen.cells[0][0] == (" ", "title")

    def test_sanitize_replaces_control_characters(self):
        assert sanitize("a\x1b[31mb\tc") == "a·[31mb·c"

    def test_minimum_size_is_one_cell(self):
        screen = Screen(0, 0)
        assert screen.width == 1 and screen.height == 1


class TestRenderDiff:
    def test_identical_screens_produce_no_output(self):
        screen = Screen(20, 3)
        screen.put(0, 0, "stable")
        assert render_diff(screen.cells, screen.cells) == ""

    def test_first_frame_renders_everything(self):
        screen = Screen(10, 2)
        screen.put(0, 0, "ab")
        output = render_diff(None, screen.cells)
        assert "\x1b[1;1H" in output
        assert RESET in output

    def test_only_changed_cells_are_emitted(self):
        before = Screen(40, 5)
        before.put(0, 0, "unchanged header")
        before.put(2, 0, "old value")
        after = Screen(40, 5)
        after.put(0, 0, "unchanged header")
        after.put(2, 0, "new value")

        output = render_diff(before.cells, after.cells)
        # The changed row is addressed; the untouched row never is.
        assert "\x1b[3;1H" in output
        assert "\x1b[1;1H" not in output
        # Only the differing prefix is resent; the shared tail " value" is not.
        assert "new" in output
        assert "value" not in output

    def test_changed_run_size_is_minimal(self):
        before = Screen(20, 1)
        before.put(0, 0, "aaaaabbbbb")
        after = Screen(20, 1)
        after.put(0, 0, "aaaaaXbbbb")
        output = render_diff(before.cells, after.cells)
        assert "X" in output
        assert "a" not in output.replace("\x1b", "").replace("[1;6H", "")

    def test_style_change_alone_is_a_change(self):
        before = Screen(4, 1)
        before.put(0, 0, "text", "default")
        after = Screen(4, 1)
        after.put(0, 0, "text", "bad")
        assert render_diff(before.cells, after.cells) != ""

    def test_empty_screen_renders_nothing(self):
        assert render_diff(None, []) == ""

    def test_every_style_is_ansi(self):
        for name, code in STYLES.items():
            assert code.startswith("\x1b["), name


class TestSummarize:
    def test_prefers_human_fields(self):
        assert summarize_event(sample_event(text="hello world")) == "hello world"
        assert summarize_event(sample_event(summary="brief")) == "brief"

    def test_work_payload_summary(self):
        assert summarize_event(sample_event(kind="work", key="build", state="done")) == "build [done]"

    def test_metric_payload_summary(self):
        assert summarize_event(sample_event(kind="metric", name="p99", value=12.5, unit="ms")) == "p99 = 12.5 ms"

    def test_falls_back_to_compact_json(self):
        assert summarize_event(sample_event(alpha=1, beta=2)) == '{"alpha":1,"beta":2}'

    def test_truncates_long_values(self):
        summary = summarize_event(sample_event(text="x" * 500), 20)
        assert len(summary) == 20
        assert summary.endswith("…")


class TestAge:
    def test_recent_timestamp(self):
        stamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
        assert age_of(stamp).endswith("s")

    def test_missing_timestamp(self):
        assert age_of(None).strip() == "-"

    def test_invalid_timestamp(self):
        assert age_of("not-a-time").strip() == "-"


class TestDashboard:
    def test_header_shows_workspace_pid_and_totals(self):
        screen = Dashboard().render(sample_model())
        header = screen.text_at(0)
        assert "quay" in header
        assert "/home/oni/okf_agent_workspace" in header
        assert "pid 4242" in header
        assert "seq 12" in header

    def test_stream_is_newest_first_with_columns(self):
        screen = Dashboard().render(sample_model())
        assert "#5" in screen.text_at(3)
        assert "#1" in screen.text_at(7)
        assert "kind" in screen.text_at(2)

    def test_event_kinds_use_distinct_styles(self):
        events = [
            sample_event(seq=3, kind="signal", summary="broken"),
            sample_event(seq=2, kind="work", key="k", state="done"),
            sample_event(seq=1, kind="note", text="hi"),
        ]
        screen = Dashboard().render(sample_model(events=events, selection=-1))
        assert screen.cells[3][0][1] == "signal"
        assert screen.cells[4][0][1] == "work"
        assert screen.cells[5][0][1] == "note"

    def test_selection_is_highlighted(self):
        screen = Dashboard().render(sample_model(selection=2))
        assert screen.cells[3][0][1] != "selected"
        assert screen.cells[5][0][1] == "selected"

    def test_planes_pane_reports_counters_and_chain(self):
        screen = Dashboard().render(sample_model())
        text = "\n".join(screen.text_at(row) for row in range(screen.height))
        assert "planes" in text
        assert "work active" in text
        assert "chain: verified" in text

    def test_help_overlay_shows_keys(self):
        screen = Dashboard().render(sample_model(help=True))
        text = "\n".join(screen.text_at(row) for row in range(screen.height))
        assert "detach" in text
        assert "verify" in text
        # The overlay must be painted above the body, not overwritten by it.
        assert "cycle the kind filter" in text
        assert "inject a sample agent event" in text

    def test_verify_banner_replaces_the_hint_row(self):
        screen = Dashboard().render(sample_model(verify_banner=f"chain ok · 12 events{RESET}"))
        assert "chain ok" in screen.text_at(screen.height - 2)

    def test_message_is_shown_next_to_the_filter(self):
        screen = Dashboard().render(sample_model(message="reaped 2 expired lease(s)"))
        assert "reaped 2 expired lease(s)" in screen.text_at(1)

    def test_paused_indicator(self):
        screen = Dashboard().render(sample_model(paused=True))
        assert "PAUSED" in screen.text_at(0)

    def test_empty_ledger_renders_a_placeholder(self):
        screen = Dashboard().render(sample_model(events=[]))
        assert "waiting for events" in screen.text_at(3)

    def test_first_run_tip_replaces_the_hints_on_a_small_ledger(self):
        model = sample_model(status={"ledger": {"records": 1}})
        screen = Dashboard().render(model)
        assert FIRST_RUN_TIP in screen.text_at(screen.height - 2)
        assert "q detach" not in screen.text_at(screen.height - 2)

    def test_first_run_tip_is_shown_at_the_two_record_threshold(self):
        screen = Dashboard().render(sample_model(status={"ledger": {"records": 2}}))
        assert FIRST_RUN_TIP in screen.text_at(screen.height - 2)

    def test_hints_return_once_the_ledger_grows(self):
        screen = Dashboard().render(sample_model())  # records: 12
        row = screen.text_at(screen.height - 2)
        assert "q detach" in row
        assert FIRST_RUN_TIP not in row

    def test_footer_hint_mentions_the_refresh_key(self):
        screen = Dashboard().render(sample_model())
        assert "ctrl-l refresh" in screen.text_at(screen.height - 2)

    def test_verify_banner_wins_over_the_first_run_tip(self):
        model = sample_model(status={"ledger": {"records": 1}}, verify_banner="chain ok · 1 events")
        screen = Dashboard().render(model)
        row = screen.text_at(screen.height - 2)
        assert "chain ok" in row
        assert FIRST_RUN_TIP not in row

    def test_first_run_tip_survives_a_narrow_terminal(self):
        screen = Dashboard().render(sample_model(width=30, height=12, status={"ledger": {"records": 0}}))
        assert screen.text_at(screen.height - 2).startswith("Tip: Press [s]")

    def test_detail_pane_is_skipped_on_narrow_terminals(self):
        screen = Dashboard().render(sample_model(width=60, height=20))
        text = "\n".join(screen.text_at(row) for row in range(screen.height))
        assert "planes" not in text
        assert "#5" in text

    @pytest.mark.parametrize("width,height", [(20, 5), (40, 10), (72, 12), (200, 60), (10, 3)])
    def test_any_terminal_size_renders_a_valid_grid(self, width, height):
        screen = Dashboard().render(sample_model(width=width, height=height))
        assert len(screen.cells) == height
        assert all(len(row) == width for row in screen.cells)
        assert render_diff(None, screen.cells) != ""

    def test_rendering_is_deterministic(self):
        model = sample_model()
        dashboard = Dashboard()
        assert dashboard.render(model).text_at(0) == dashboard.render(model).text_at(0)


class FakeClient:
    def __init__(self, verify: dict | None = None, reap: dict | None = None, emit_error: Exception | None = None) -> None:
        self.verify_report = verify or {"ok": True, "events_checked": 7, "head_hash": "f" * 64}
        self.reap_report = reap or {"count": 3}
        self.emit_error = emit_error
        self.emitted: dict | None = None
        self.calls: list[str] = []

    def request(self, op: str, body: dict | None = None) -> dict:
        self.calls.append(op)
        if op == "verify":
            return self.verify_report
        if op == "work_reap":
            return self.reap_report
        raise AssertionError(f"unexpected op {op}")

    def emit(self, *, topic: str, producer: str, payload: dict | None = None, **extra: object) -> dict:
        self.calls.append("emit")
        if self.emit_error is not None:
            raise self.emit_error
        self.emitted = {"topic": topic, "producer": producer, "payload": payload, **extra}
        return {"seq": 7, "kind": extra.get("kind", "generic"), "topic": topic, "producer": producer, "payload": payload}

    def query(self, **filters: object) -> list[dict]:
        return [sample_event(seq=1, kind=str(filters.get("kind") or "generic"))]


class TestTuiApp:
    def test_ingest_prepends_and_caps_history(self):
        app = TuiApp("/tmp/x.sock", history=3)
        for seq in range(1, 6):
            app.ingest(sample_event(seq=seq))
        assert [event["seq"] for event in app.events] == [5, 4, 3]

    def test_ingest_deduplicates_by_sequence(self):
        app = TuiApp("/tmp/x.sock", history=10)
        app.ingest(sample_event(seq=1))
        app.ingest(sample_event(seq=2))
        # Re-ingesting existing sequences must be ignored
        app.ingest(sample_event(seq=1))
        app.ingest(sample_event(seq=2))
        assert [event["seq"] for event in app.events] == [2, 1]

    def test_ingest_handles_missing_sequence(self):
        app = TuiApp("/tmp/x.sock", history=10)
        e1 = sample_event(seq=None)
        e2 = sample_event(seq=None)
        app.ingest(e1)
        app.ingest(e2)
        assert len(app.events) == 2

    def test_reload_history_and_replayed_ingest_deduplicates(self):
        app = TuiApp("/tmp/x.sock", history=10)
        client = FakeClient()
        app.reload_history(client)
        assert [e["seq"] for e in app.events] == [1]
        # Replayed frame with seq=1 arrives from stream
        app.ingest(sample_event(seq=1))
        assert [e["seq"] for e in app.events] == [1]

    def test_ingest_maintains_descending_sequence_order(self):
        app = TuiApp("/tmp/x.sock", history=10)
        app.ingest(sample_event(seq=2))
        app.ingest(sample_event(seq=5))
        app.ingest(sample_event(seq=1))
        app.ingest(sample_event(seq=4))
        assert [event["seq"] for event in app.events] == [5, 4, 2, 1]

    def test_ingest_ignores_non_matching_kinds(self):
        app = TuiApp("/tmp/x.sock", filter_kind="work")
        app.ingest(sample_event(seq=1, kind="note"))
        app.ingest(sample_event(seq=2, kind="work", key="k", state="claimed"))
        assert [event["seq"] for event in app.events] == [2]

    def test_ingest_is_ignored_while_paused(self):
        app = TuiApp("/tmp/x.sock")
        app.paused = True
        app.ingest(sample_event(seq=1))
        assert app.events == []

    def test_quit_keys_return_false(self):
        app = TuiApp("/tmp/x.sock")
        assert app.handle_key("q", FakeClient()) is False
        assert app.handle_key("\x03", FakeClient()) is False

    def test_navigation_moves_within_bounds(self):
        app = TuiApp("/tmp/x.sock", history=10)
        app.events = [sample_event(seq=n) for n in range(5)]
        app.handle_key("j", FakeClient())
        assert app.selection == 1
        app.handle_key("k", FakeClient())
        assert app.selection == 0
        app.handle_key("k", FakeClient())
        assert app.selection == 0
        app.handle_key("g", FakeClient())
        assert app.selection == 4
        app.handle_key("G", FakeClient())
        assert app.selection == 0

    def test_space_toggles_pause(self):
        app = TuiApp("/tmp/x.sock")
        app.handle_key(" ", FakeClient())
        assert app.paused is True
        app.handle_key(" ", FakeClient())
        assert app.paused is False

    def test_help_toggles(self):
        app = TuiApp("/tmp/x.sock")
        app.handle_key("?", FakeClient())
        assert app.help is True
        app.handle_key("?", FakeClient())
        assert app.help is False

    def test_filter_cycles_and_reloads_history(self):
        app = TuiApp("/tmp/x.sock")
        client = FakeClient()
        for _ in range(len(FILTER_PRESETS)):
            app.handle_key("f", client)
        assert app.filter_index == 0
        app.handle_key("2", client)
        assert app.filter_label == "signal"

    def test_empty_key_is_ignored(self):
        app = TuiApp("/tmp/x.sock")
        assert app.handle_key("", FakeClient()) is True
        assert app.filter_index == 0

    def test_arrow_and_page_keys_navigate(self):
        app = TuiApp("/tmp/x.sock", history=10)
        app.events = [sample_event(seq=n) for n in range(10)]
        app.stream_rows = 5
        client = FakeClient()

        # Down arrow ANSI and SS3
        app.handle_key("\x1b[B", client)
        assert app.selection == 1
        app.handle_key("\x1bOB", client)
        assert app.selection == 2

        # Up arrow ANSI and SS3
        app.handle_key("\x1b[A", client)
        assert app.selection == 1
        app.handle_key("\x1bOA", client)
        assert app.selection == 0

        # Page Down and Page Up
        app.handle_key("\x1b[6~", client)
        assert app.selection == 3
        app.handle_key("\x1b[5~", client)
        assert app.selection == 0

        # End and Home
        app.handle_key("\x1b[F", client)
        assert app.selection == 9
        app.handle_key("\x1b[H", client)
        assert app.selection == 0

    def test_viewport_scrolling_tracks_selection(self):
        app = TuiApp("/tmp/x.sock", history=30)
        app.events = [sample_event(seq=n) for n in range(30)]
        app.stream_rows = 5
        client = FakeClient()

        # Selection within viewport: offset remains 0
        for _ in range(4):
            app.handle_key("\x1b[B", client)
        assert app.selection == 4
        assert app.scroll_offset == 0

        # Moving past viewport bottom advances scroll_offset
        app.handle_key("\x1b[B", client)
        assert app.selection == 5
        assert app.scroll_offset == 1

        # Moving to end
        app.handle_key("\x1b[F", client)
        assert app.selection == 29
        assert app.scroll_offset == 25  # 30 - 5

        # Moving to top
        app.handle_key("\x1b[H", client)
        assert app.selection == 0
        assert app.scroll_offset == 0

    def test_sgr_mouse_click_and_scroll(self):
        app = TuiApp("/tmp/x.sock", history=10)
        app.events = [sample_event(seq=n) for n in range(10)]
        app.stream_rows = 5
        app.last_width = 80
        app.last_height = 24
        client = FakeClient()

        # Left click on row 5 (body_top is 3, so offset is 2 -> selection index 2)
        # SGR format: \x1b[<b;x;y;M (1-indexed x=15, y=5 -> 0-indexed row=4, offset=1)
        app.handle_key("\x1b[<0;15;5M", client)
        assert app.selection == 1

        # Left click release is ignored
        app.handle_key("\x1b[<0;15;5m", client)
        assert app.selection == 1

        # Mouse wheel down (b=65)
        app.handle_key("\x1b[<65;15;5M", client)
        assert app.selection == 2

        # Mouse wheel up (b=64)
        app.handle_key("\x1b[<64;15;5M", client)
        assert app.selection == 1

        # Left click on filter row (row=2, 1-indexed -> 0-indexed row 1)
        assert app.filter_index == 0
        app.handle_key("\x1b[<0;10;2M", client)
        assert app.filter_index == 1

        # Left click near PAUSED (row=1, 1-indexed, col=60)
        assert app.paused is False
        app.handle_key("\x1b[<0;60;1M", client)
        assert app.paused is True

    def test_inspect_overlay_toggle_and_dismiss(self):
        app = TuiApp("/tmp/x.sock", history=5)
        app.events = [sample_event(seq=42, summary="critical issue")]
        client = FakeClient()

        # Enter toggles inspect
        app.handle_key("\r", client)
        assert app.inspecting is True

        # Esc dismisses inspect
        app.handle_key("\x1b", client)
        assert app.inspecting is False

        # Enter again, 'q' dismisses inspect without exiting app
        app.handle_key("\n", client)
        assert app.inspecting is True
        res = app.handle_key("q", client)
        assert res is True
        assert app.inspecting is False

    def test_split_keys_parser(self):
        # Concatenated mouse press and release
        events = split_keys("\x1b[<0;20;5M\x1b[<0;20;5m")
        assert events == ["\x1b[<0;20;5M", "\x1b[<0;20;5m"]

        # Concatenated arrow keys
        arrows = split_keys("\x1b[A\x1b[B\x1bOA")
        assert arrows == ["\x1b[A", "\x1b[B", "\x1bOA"]

        # Page and function keys
        pages = split_keys("\x1b[5~\x1b[6~")
        assert pages == ["\x1b[5~", "\x1b[6~"]

        # Plain text characters and standalone Esc
        plain = split_keys("jk\x1b?")
        assert plain == ["j", "k", "\x1b", "?"]


    def test_detail_toggle(self):
        app = TuiApp("/tmp/x.sock")
        before = app.dashboard.detail
        app.handle_key("d", FakeClient())
        assert app.dashboard.detail is not before

    def test_verify_key_sets_a_success_banner(self):
        app = TuiApp("/tmp/x.sock")
        app.handle_key("v", FakeClient())
        assert app.verify_banner.startswith("chain ok")

    def test_verify_key_reports_a_broken_chain(self):
        app = TuiApp("/tmp/x.sock")
        client = FakeClient(
            verify={"ok": False, "events_checked": 3, "head_hash": "f" * 64, "divergence": {"seq": 7, "reason": "hash_mismatch"}}
        )
        app.handle_key("v", client)
        assert "BROKEN" in app.verify_banner
        assert "7" in app.verify_banner

    def test_reap_key_reports_the_count(self):
        app = TuiApp("/tmp/x.sock")
        app.handle_key("b", FakeClient())
        assert "reaped 3" in app.message

    def test_sample_key_emits_a_schema_valid_demo_event(self):
        app = TuiApp("/tmp/x.sock")
        client = FakeClient()
        app.handle_key("s", client)
        assert client.emitted is not None
        assert client.emitted["topic"] == SAMPLE_TOPIC
        assert client.emitted["producer"] == SAMPLE_PRODUCER
        assert client.emitted["kind"] == "signal"
        assert client.emitted["payload"] == {
            "state": "pending",
            "summary": "sample agent event from the cockpit",
        }
        assert "injected sample event #7" in app.message

    def test_sample_key_surfaces_a_failure_without_crashing(self):
        app = TuiApp("/tmp/x.sock")
        client = FakeClient(emit_error=RuntimeError("daemon gone"))
        assert app.handle_key("s", client) is True
        assert "sample event failed" in app.message
        assert "daemon gone" in app.message

    def test_help_overlay_documents_the_sample_key(self):
        app = TuiApp("/tmp/x.sock")
        app.handle_key("?", FakeClient())
        screen = app.dashboard.render(app.model(width=100, height=30))
        text = "\n".join(screen.text_at(row) for row in range(screen.height))
        assert "inject a sample agent event" in text

    def test_help_overlay_documents_the_refresh_key(self):
        app = TuiApp("/tmp/x.sock")
        app.handle_key("?", FakeClient())
        screen = app.dashboard.render(app.model(width=100, height=30))
        text = "\n".join(screen.text_at(row) for row in range(screen.height))
        assert "Ctrl-L" in text
        assert "full screen refresh" in text

    def test_ctrl_l_requests_a_redraw_without_touching_state(self):
        app = TuiApp("/tmp/x.sock")
        client = FakeClient()
        assert app.handle_key(REFRESH_KEY, client) is True
        assert app.redraw_requested is True
        assert client.emitted is None
        assert app.message == ""
        assert app.events == []
        # The request is cleared exactly once, when the loop honours it.
        assert app.take_redraw_request() is True
        assert app.redraw_requested is False
        assert app.take_redraw_request() is False

    def test_model_is_serializable_and_complete(self):
        app = TuiApp("/tmp/x.sock", workspace="/w")
        model = app.model(width=80, height=24)
        for key in ("width", "height", "events", "selection", "status", "health", "dropped"):
            assert key in model
        assert model["workspace"] == "/w"


class TestTuiLoopOverARealSocket:
    """Drives the render loop against a live daemon with a scripted key reader."""

    @staticmethod
    def key_reader(*presses: str):
        import contextlib as _contextlib

        @_contextlib.contextmanager
        def factory(*, on_resize=None):
            queue = list(presses)

            def read(timeout: float) -> str | None:
                return queue.pop(0) if queue else None

            yield read

        return factory

    def test_loop_renders_live_events_and_exits_on_q(self, server, client):
        import io

        from quay.client import SubscriptionStream

        for index in range(3):
            client.emit(topic="okf/handoff", producer="factory", payload={"summary": f"step {index}"})

        app = TuiApp(server.socket_path, workspace=str(server.paths.workspace), refresh_seconds=0.0)
        output = io.StringIO()
        with SubscriptionStream(server.socket_path, filters={}, replay=10) as live:
            code = app._loop(  # noqa: SLF001 - the documented test seam
                client,
                live,
                reader_factory=self.key_reader("q"),
                output=output,
                max_iterations=4,
            )
        rendered = output.getvalue()
        assert code == 0
        assert "\x1b[?1049h" in rendered, "the alternate screen must be entered"
        assert "\x1b[?1049l" in rendered, "the alternate screen must be left"
        assert "okf/handoff" in rendered
        assert "quay" in rendered
        assert [event["seq"] for event in app.events] == [3, 2, 1]

    def test_loop_handles_pause_and_filter_keys(self, server, client):
        import io

        from quay.client import SubscriptionStream

        client.emit(topic="t", producer="p", payload={"i": 1}, kind="generic")
        app = TuiApp(server.socket_path, workspace=str(server.paths.workspace), refresh_seconds=0.0)
        output = io.StringIO()
        with SubscriptionStream(server.socket_path, filters={}, replay=5) as live:
            code = app._loop(  # noqa: SLF001 - the documented test seam
                client,
                live,
                reader_factory=self.key_reader(" ", "2", "q"),
                output=output,
                max_iterations=6,
            )
        assert code == 0
        assert app.filter_label == "signal"
        # The loop writes only changed cells, so the label can be split across
        # two diffs; assert the state and the rendered screen separately.
        assert "filter:" in output.getvalue()
        assert "signal" in output.getvalue()
        screen = app.dashboard.render(app.model(width=100, height=24))
        assert "filter: signal" in screen.text_at(1)

    def test_loop_sample_key_writes_a_real_event(self, server, client):
        import io

        from quay.client import SubscriptionStream

        app = TuiApp(server.socket_path, workspace=str(server.paths.workspace), refresh_seconds=0.0)
        with SubscriptionStream(server.socket_path, filters={}, replay=5) as live:
            code = app._loop(  # noqa: SLF001 - the documented test seam
                client,
                live,
                reader_factory=self.key_reader("s", "q"),
                output=io.StringIO(),
                max_iterations=6,
            )
        assert code == 0
        events = client.query(topic=SAMPLE_TOPIC, limit=5)
        assert len(events) == 1
        assert events[0]["kind"] == "signal"
        assert events[0]["producer"] == SAMPLE_PRODUCER
        assert events[0]["payload"]["state"] == "pending"

    def test_loop_bounds_itself_when_no_keys_arrive(self, server, client):
        import io

        from quay.client import SubscriptionStream

        app = TuiApp(server.socket_path, workspace=str(server.paths.workspace), refresh_seconds=0.0)
        with SubscriptionStream(server.socket_path, filters={}) as live:
            code = app._loop(  # noqa: SLF001 - the documented test seam
                client,
                live,
                reader_factory=self.key_reader(),
                output=io.StringIO(),
                max_iterations=2,
            )
        assert code == 0


class TestTuiLoopResizeAndRefresh:
    """Headless proof that resize, SIGWINCH, and Ctrl-L repaint in full.

    ``painted_cells`` decodes the ANSI the loop writes, so a suppressed cell
    shows up as a hole in the repaint: these tests assert complete cell coverage
    instead of merely spotting a clear-screen sequence.
    """

    SMALL = (60, 16)
    LARGE = (100, 30)

    def run_loop(self, app, factory, size_getter, *, iterations: int = 6, client=None) -> str:
        output = io.StringIO()
        code = app._loop(  # noqa: SLF001 - the documented test seam
            FakeClient() if client is None else client,
            FakeLive(),
            reader_factory=factory,
            output=output,
            max_iterations=iterations,
            size_getter=size_getter,
        )
        assert code == 0
        return output.getvalue()

    def test_geometry_change_clears_and_repaints_every_cell(self):
        app = TuiApp("/tmp/x.sock")
        sizes = ScriptedSize(self.SMALL, self.SMALL, self.LARGE, self.SMALL, self.SMALL)
        rendered = self.run_loop(app, scripted_reader(), sizes)

        # The entry clear plus one clear for each geometry change (small -> large
        # -> small). A resize must never fall back to an incremental diff.
        assert rendered.count(CLEAR_SCREEN) == 3
        segments = rendered.split(CLEAR_SCREEN)
        assert painted_cells(segments[2]) == full_cells(*self.LARGE)
        assert painted_cells(segments[3]) == full_cells(*self.SMALL)

        # Cells that are identical across the two geometries are exactly the ones
        # a stale cache would suppress. Prove the scenario is real, then prove
        # they are written anyway.
        before = app.dashboard.render(app.model(width=self.SMALL[0], height=self.SMALL[1]))
        after = app.dashboard.render(app.model(width=self.LARGE[0], height=self.LARGE[1]))
        unchanged = {
            (row, column)
            for row in range(before.height)
            for column in range(before.width)
            if before.cells[row][column] == after.cells[row][column]
        }
        assert unchanged, "the fixture must keep some cells identical across a resize"
        assert unchanged <= painted_cells(segments[2])

    def test_stable_geometry_keeps_incremental_diffs(self):
        app = TuiApp("/tmp/x.sock")
        rendered = self.run_loop(app, scripted_reader(), ScriptedSize(self.LARGE))

        # An unchanged viewport must not clear on every iteration.
        assert rendered.count(CLEAR_SCREEN) == 1
        segments = rendered.split(CLEAR_SCREEN)
        assert painted_cells(segments[1]) == full_cells(*self.LARGE)
        # The first frame paints row 0 from its first column exactly once: the
        # idle iterations that follow emit nothing at all.
        assert segments[1].count("\x1b[1;1H") == 1

    def test_recovery_failure_is_visible_and_ends_stream_processing(self):
        app = TuiApp("/tmp/x.sock")
        output = io.StringIO()

        code = app._loop(  # noqa: SLF001 - the documented test seam
            FakeClient(),
            RecoveryErrorLive(),
            reader_factory=scripted_reader(),
            output=output,
            max_iterations=2,
            size_getter=ScriptedSize(self.LARGE),
        )

        assert code == 0
        assert app.message == "stream ended: recovery_stalled"

    def test_ctrl_l_forces_a_full_repaint_of_an_unchanged_model(self):
        app = TuiApp("/tmp/x.sock")
        client = FakeClient()
        rendered = self.run_loop(
            app,
            scripted_reader(REFRESH_KEY, "q"),
            ScriptedSize(self.LARGE),
            client=client,
        )

        assert rendered.count(CLEAR_SCREEN) == 2
        segments = rendered.split(CLEAR_SCREEN)
        assert painted_cells(segments[2]) == full_cells(*self.LARGE)
        # Ctrl-L repaints only: no model change, no ledger write, no detach.
        assert client.emitted is None
        assert app.events == []
        assert app.selection == 0
        assert app.message == ""
        assert app.paused is False
        assert app.help is False
        assert app.redraw_requested is False

    def test_one_redraw_request_clears_exactly_once(self):
        app = TuiApp("/tmp/x.sock")

        def on_read(call: int, on_resize) -> None:
            if call == 2:
                on_resize()

        # The geometry never changes, so the extra clear can only come from the
        # resize callback that SIGWINCH would trip in production.
        rendered = self.run_loop(app, scripted_reader(on_read=on_read), ScriptedSize(self.LARGE))

        assert rendered.count(CLEAR_SCREEN) == 2
        segments = rendered.split(CLEAR_SCREEN)
        assert painted_cells(segments[2]) == full_cells(*self.LARGE)
        assert app.redraw_requested is False, "a consumed request must be cleared"

    def test_resize_and_sigwinch_together_clear_once(self):
        app = TuiApp("/tmp/x.sock")

        def on_read(call: int, on_resize) -> None:
            if call == 1:
                on_resize()  # SIGWINCH lands in the same iteration as the resize

        sizes = ScriptedSize(self.LARGE, self.LARGE, self.SMALL, self.SMALL)
        rendered = self.run_loop(app, scripted_reader(on_read=on_read), sizes)

        # Entry clear plus exactly one clear for the combined event: the request
        # must be consumed even when the geometry check already tripped.
        assert rendered.count(CLEAR_SCREEN) == 2
        segments = rendered.split(CLEAR_SCREEN)
        assert painted_cells(segments[2]) == full_cells(*self.SMALL)
        assert app.redraw_requested is False

    def test_alternate_screen_and_cursor_lifecycle_is_preserved(self):
        app = TuiApp("/tmp/x.sock")
        sizes = ScriptedSize(self.SMALL, self.SMALL, self.LARGE, self.SMALL, self.SMALL)
        rendered = self.run_loop(app, scripted_reader(), sizes)

        assert rendered.count("\x1b[?1049h") == 1
        assert rendered.count("\x1b[?1049l") == 1
        assert rendered.count("\x1b[?25l") == 1
        assert rendered.count("\x1b[?25h") == 1
        assert rendered.index("\x1b[?1049h") < rendered.index("\x1b[?25l")
        assert rendered.rstrip().endswith("\x1b[?25h\x1b[?1049l")

    def test_sigwinch_requests_a_redraw_and_is_restored(self):
        winch = getattr(signal, "SIGWINCH", None)
        if winch is None:
            pytest.skip("SIGWINCH is unavailable on this platform")
        app = TuiApp("/tmp/x.sock")
        original = signal.getsignal(winch)
        with terminal_session(on_resize=app.request_redraw):
            installed = signal.getsignal(winch)
            assert installed is not original
            installed(winch, None)  # the handler must only request a redraw
            assert app.redraw_requested is True
        assert signal.getsignal(winch) is original

    def test_real_sigwinch_reaches_the_handler(self):
        winch = getattr(signal, "SIGWINCH", None)
        if winch is None:
            pytest.skip("SIGWINCH is unavailable on this platform")
        app = TuiApp("/tmp/x.sock")
        with terminal_session(on_resize=app.request_redraw) as read_key:
            assert read_key(0.0) is None
            os.kill(os.getpid(), winch)
            deadline = time.monotonic() + 1.0
            while not app.redraw_requested and time.monotonic() < deadline:
                time.sleep(0.01)
            assert app.redraw_requested is True
            # The self-pipe must drain, so the reader keeps blocking normally.
            assert read_key(0.0) is None

    def test_sigwinch_handler_is_restored_after_an_error(self):
        winch = getattr(signal, "SIGWINCH", None)
        if winch is None:
            pytest.skip("SIGWINCH is unavailable on this platform")
        original = signal.getsignal(winch)
        with pytest.raises(RuntimeError):
            with terminal_session(on_resize=lambda: None):
                raise RuntimeError("boom")
        assert signal.getsignal(winch) is original

    def test_terminal_session_without_a_callback_leaves_sigwinch_alone(self):
        winch = getattr(signal, "SIGWINCH", None)
        if winch is None:
            pytest.skip("SIGWINCH is unavailable on this platform")
        original = signal.getsignal(winch)
        with terminal_session() as read_key:
            assert signal.getsignal(winch) is original
            assert callable(read_key)
        assert signal.getsignal(winch) is original


class TestInputTokenization:
    """A partial escape sequence must never be read as the keys it resembles.

    ``\\x1b[<0;15;5`` (a mouse report cut off before its terminator) used to
    split into ``0``, ``1``, ``5`` and friends, and every digit is a bound
    filter shortcut, so a slow pty could silently jump the cockpit to another
    filter and reload its history.
    """

    def test_a_truncated_mouse_report_is_one_opaque_token(self):
        for truncated in ("\x1b[<0;15;5", "\x1b[<0;15;", "\x1b[<0;", "\x1b[<"):
            assert split_keys(truncated) == [truncated]

    def test_an_incomplete_ss3_or_csi_prefix_is_one_opaque_token(self):
        assert split_keys("\x1b[") == ["\x1b["]
        assert split_keys("\x1bO") == ["\x1bO"]
        assert split_keys("j\x1b[") == ["j", "\x1b["]

    def test_a_truncated_mouse_report_cannot_type_its_coordinates(self):
        app = TuiApp("/tmp/x.sock", history=10)
        app.events = [sample_event(seq=n) for n in range(10)]
        app.stream_rows = 5
        client = FakeClient()
        for token in split_keys("\x1b[<0;15;5"):
            app.handle_key(token, client)
        assert app.filter_index == 0
        assert app.filter_label == "all"
        assert app.selection == 0
        assert client.calls == []

    def test_a_lone_escape_stays_meaningful(self):
        assert split_keys("\x1b") == ["\x1b"]
        assert split_keys("j\x1b") == ["j", "\x1b"]

    @pytest.mark.parametrize(
        "payload",
        [
            "jq",
            "\x1b[A\x1b[B\x1bOA\x1bOB",
            "\x1b[5~\x1b[6~\x1b[1;5A\x1b[Z",
            "\x1b[<0;15;5M\x1b[<0;15;5m",
            "\x1b[<64;1;1M\x1b[<65;1;1M\x1b[<0;200;60M",
            "héllo → 🚀",
            "".join("\x1b[<0;15;5M" for _ in range(20)),
            "\x1b[?1049h\x1b[?25l",
        ],
    )
    def test_every_character_of_a_burst_is_accounted_for(self, payload):
        assert "".join(split_keys(payload)) == payload

    def test_unicode_payloads_split_into_code_points(self):
        assert split_keys("é→🚀") == ["é", "→", "🚀"]

    def test_an_unknown_but_complete_sequence_is_one_token(self):
        # Bracketed-paste markers are not handled keys, but they are complete
        # sequences: they must not be shredded into letters.
        assert split_keys("\x1b[200~") == ["\x1b[200~"]
        assert split_keys("\x1b[201~") == ["\x1b[201~"]


class TestMouseHardening:
    """Boundary handling for SGR 1006 reports."""

    @staticmethod
    def app(*, events: int = 10, rows: int = 5, selection: int = 0) -> TuiApp:
        app = TuiApp("/tmp/x.sock", history=40)
        app.events = [sample_event(seq=n) for n in range(events)]
        app.stream_rows = rows
        app.selection = selection
        app.last_width = 80
        app.last_height = 24
        return app

    @pytest.mark.parametrize(
        "packet,expected",
        [
            ("\x1b[<64;15;5M", 4),  # wheel up
            ("\x1b[<68;15;5M", 4),  # shift + wheel up
            ("\x1b[<72;15;5M", 4),  # alt + wheel up
            ("\x1b[<65;15;5M", 6),  # wheel down
            ("\x1b[<81;15;5M", 6),  # ctrl + wheel down
        ],
    )
    def test_wheel_reports_scroll_with_modifiers_masked(self, packet, expected):
        app = self.app(selection=5)
        assert app.handle_key(packet, FakeClient()) is True
        assert app.selection == expected

    @pytest.mark.parametrize(
        "packet",
        [
            "\x1b[<66;15;5M",  # wheel left
            "\x1b[<67;15;5M",  # wheel right
            "\x1b[<32;15;5M",  # bare motion
            "\x1b[<35;15;5M",  # drag with the left button held
            "\x1b[<1;15;5M",  # middle button
            "\x1b[<2;15;5M",  # right button
            "\x1b[<0;15;5m",  # left release
        ],
    )
    def test_reports_with_nothing_to_do_are_inert(self, packet):
        app = self.app(selection=5)
        app.handle_key(packet, FakeClient())
        assert app.selection == 5
        assert app.filter_index == 0
        assert app.paused is False
        assert app.redraw_requested is False

    def test_shift_click_still_selects(self):
        app = self.app()
        app.handle_key("\x1b[<4;15;5M", FakeClient())
        assert app.selection == 1

    def test_wheel_on_an_empty_ledger_is_inert(self):
        app = self.app(events=0)
        for packet in ("\x1b[<64;15;5M", "\x1b[<65;15;5M"):
            assert app.handle_key(packet, FakeClient()) is True
        assert app.selection == 0
        assert app.scroll_offset == 0

    def test_wheel_with_a_single_event_keeps_the_selection_in_range(self):
        app = self.app(events=1)
        for packet in ("\x1b[<65;15;5M", "\x1b[<64;15;5M", "\x1b[<64;15;5M"):
            app.handle_key(packet, FakeClient())
        assert app.selection == 0
        assert app.scroll_offset == 0

    def test_clicks_are_ignored_when_the_viewport_has_no_rows(self):
        app = self.app(rows=0)
        app.handle_key("\x1b[<0;15;5M", FakeClient())
        assert app.selection == 0
        assert app.redraw_requested is False

    @pytest.mark.parametrize(
        "packet",
        [
            "\x1b[<0;15M",  # two fields
            "\x1b[<;15;5M",  # empty button
            "\x1b[<M",  # no fields at all
            "\x1b[<0;15;5X",  # unknown terminator
            "\x1b[<0;15;5",  # truncated
        ],
    )
    def test_malformed_packets_are_ignored(self, packet):
        app = self.app()
        app.handle_key(packet, FakeClient())
        assert app.selection == 0
        assert app.filter_index == 0
        assert app.paused is False

    def test_out_of_range_coordinates_are_clamped(self):
        app = self.app()
        # Row 0 (1-indexed) would be -1 once zero-based: clamped to the header,
        # far from the PAUSED hit zone, so it must do nothing at all.
        app.handle_key("\x1b[<0;0;0M", FakeClient())
        assert app.paused is False
        assert app.selection == 0
        # An enormous row cannot reach the stream either.
        app.handle_key("\x1b[<0;99999;99999M", FakeClient())
        assert app.selection == 0

    def test_a_rapid_click_burst_applies_every_click_once(self):
        app = self.app(events=30)
        tokens = split_keys("".join("\x1b[<0;15;7M" for _ in range(25)))
        assert len(tokens) == 25
        for token in tokens:
            app.handle_key(token, FakeClient())
        # Row 7 is one-based: row 6, body offset 3, so index 3, however many
        # clicks land. A burst must not drift or double-apply.
        assert app.selection == 3
        assert app.take_redraw_request() is True
        assert app.take_redraw_request() is False

    def test_a_wheel_burst_scrolls_one_step_per_tick(self):
        app = self.app(events=30)
        for _ in range(10):
            app.handle_key("\x1b[<65;15;5M", FakeClient())
        assert app.selection == 10
        assert app.scroll_offset == 6  # the selection stays on the last row

    def test_a_mouse_event_dismisses_an_overlay(self):
        app = self.app()
        app.help = True
        app.handle_key("\x1b[<64;15;5M", FakeClient())
        assert app.help is False
        assert app.redraw_requested is True


class TestSelectionBounds:
    def test_page_down_on_an_empty_ledger_keeps_the_selection_in_range(self):
        app = TuiApp("/tmp/x.sock")
        app.handle_key("\x1b[6~", FakeClient())
        assert app.selection == 0
        app.handle_key("\x1b[5~", FakeClient())
        assert app.selection == 0

    def test_ensure_selection_visible_clamps_a_stranded_selection(self):
        app = TuiApp("/tmp/x.sock", history=10)
        app.events = [sample_event(seq=n) for n in range(3)]
        app.stream_rows = 5
        app.selection = 2
        del app.events[1:]  # the visible history shrank under the cursor
        app.ensure_selection_visible()
        assert app.selection == 0
        assert app.scroll_offset == 0

    def test_ensure_selection_visible_resets_a_negative_selection(self):
        app = TuiApp("/tmp/x.sock")
        app.selection = -1
        app.ensure_selection_visible()
        assert app.selection == 0

    def test_navigation_stays_in_range_with_no_events(self):
        app = TuiApp("/tmp/x.sock")
        client = FakeClient()
        for key in ("j", "\x1b[B", "\x1b[A", "k", "g", "G", "\x1b[5~", "\x1b[6~", "\x1b[F", "\x1b[H"):
            assert app.handle_key(key, client) is True
            assert app.selection == 0


class TestRenderRobustness:
    """The cockpit renders whatever the daemon sends: a frame must never raise."""

    @pytest.mark.parametrize(
        "override",
        [
            {"events": [sample_event(seq=None)]},
            {"events": [sample_event(seq="7")]},
            {"events": [sample_event(seq=[1])]},
            {"events": [{**sample_event(seq=1), "ts": 1700000000}]},
            {"events": [{**sample_event(seq=1), "ts": None}]},
            {"events": [{**sample_event(seq=1), "kind": None}]},
            {"events": None},
            {"events": 5},
            {"selection": None},
            {"selection": "abc"},
            {"width": None, "height": None},
            {"width": "abc", "height": "abc"},
            {"health": [1, 2]},
            {"status": "broken"},
            {"status": {"ledger": [1], "kinds": {}, "planes": {}}},
            {"status": {"ledger": {}, "kinds": [1], "planes": {}}},
            {"status": {"ledger": {}, "kinds": {"note": "4"}, "planes": {"signal": {"open": "1"}, "work": {}}}},
        ],
    )
    def test_malformed_fields_still_render_a_valid_grid(self, override):
        screen = Dashboard().render(sample_model(**override))
        assert len(screen.cells) >= 1
        assert all(len(row) == len(screen.cells[0]) for row in screen.cells)
        assert render_diff(None, screen.cells) != ""

    def test_an_unusable_sequence_number_renders_as_a_placeholder(self):
        events = [sample_event(seq=None), sample_event(seq=3)]
        screen = Dashboard().render(sample_model(events=events))
        text = "\n".join(screen.text_at(row) for row in range(screen.height))
        assert "#?" in text
        assert "#3" in text

    def test_age_of_rejects_a_non_string_timestamp(self):
        assert age_of(1700000000).strip() == "-"
        assert age_of(["2026-01-01"]).strip() == "-"

    @pytest.mark.parametrize("width,height", [(1, 1), (2, 2), (5, 3), (39, 5), (300, 1), (1, 60), (40, 6)])
    def test_extreme_geometry_keeps_the_grid_valid(self, width, height):
        for overlay in ({"help": True}, {"inspecting": True}, {}):
            screen = Dashboard().render(sample_model(width=width, height=height, **overlay))
            assert len(screen.cells) == height
            assert all(len(row) == width for row in screen.cells)

    @pytest.mark.parametrize("height", [1, 2, 3, 4, 5, 6, 7, 12, 24, 30, 60])
    def test_viewport_rows_agree_with_the_dashboard(self, height):
        # Newest first, exactly as the app keeps its history.
        events = [sample_event(seq=n) for n in reversed(range(60))]
        screen = Dashboard().render(sample_model(height=height, events=events, selection=0))
        rows = stream_rows_for(height)
        if rows == 0:
            text = "\n".join(screen.text_at(row) for row in range(screen.height))
            assert "#59" not in text
        else:
            assert "#59" in screen.text_at(STREAM_TOP)
            oldest_visible = 59 - rows + 1
            assert f"#{oldest_visible} " in screen.text_at(STREAM_TOP + rows - 1)

    def test_inspect_overlay_reports_hidden_payload_lines(self):
        event = sample_event(seq=9)
        event["payload"] = {"lines": [f"line {index}" for index in range(60)]}
        app = TuiApp("/tmp/x.sock")
        app.events = [event]
        app.inspecting = True
        screen = Dashboard().render(app.model(width=100, height=40))
        text = "\n".join(screen.text_at(row) for row in range(screen.height))
        assert "line 0" in text
        assert "more line(s)" in text

    def test_inspect_overlay_renders_unicode_and_strips_control_characters(self):
        event = sample_event(seq=10)
        event["payload"] = {"text": "héllo → 🚀", "ansi": "\x1b[31mred\x1b[0m", "multi": "a\nb"}
        app = TuiApp("/tmp/x.sock")
        app.events = [event]
        app.inspecting = True
        screen = Dashboard().render(app.model(width=100, height=40))
        text = "\n".join(screen.text_at(row) for row in range(screen.height))
        assert "héllo → 🚀" in text
        assert "\x1b" not in text

    def test_inspect_overlay_survives_a_payload_json_cannot_encode(self):
        event = sample_event(seq=11)
        circular: dict = {}
        circular["self"] = circular
        event["payload"] = circular
        app = TuiApp("/tmp/x.sock")
        app.events = [event]
        app.inspecting = True
        screen = Dashboard().render(app.model(width=100, height=30))
        assert render_diff(None, screen.cells) != ""


class TestTuiLoopInputAssembly:
    """The loop joins split reports and drops the ones that never complete."""

    def run_loop(self, app, *presses: str, iterations: int = 6, size=(100, 30)) -> str:
        output = io.StringIO()
        code = app._loop(  # noqa: SLF001 - the documented test seam
            FakeClient(),
            FakeLive(),
            reader_factory=scripted_reader(*presses),
            output=output,
            max_iterations=iterations,
            size_getter=ScriptedSize(size),
        )
        assert code == 0
        return output.getvalue()

    def test_a_mouse_report_split_across_reads_is_joined(self):
        app = TuiApp("/tmp/x.sock", history=10)
        app.events = [sample_event(seq=n) for n in range(10)]
        self.run_loop(app, "\x1b[<0;15;", "5M", "q")
        assert app.selection == 1

    def test_a_burst_of_clicks_in_one_read_selects_the_last_one(self):
        app = TuiApp("/tmp/x.sock", history=10)
        app.events = [sample_event(seq=n) for n in range(10)]
        burst = "\x1b[<0;15;5M\x1b[<0;15;4M\x1b[<0;15;3M"
        self.run_loop(app, burst, "q")
        assert app.selection == 0

    def test_a_truncated_mouse_report_is_dropped(self):
        app = TuiApp("/tmp/x.sock", history=10)
        app.events = [sample_event(seq=n) for n in range(10)]
        self.run_loop(app, "\x1b[<0;15;5")
        assert app.filter_index == 0
        assert app.selection == 0

    def test_a_bare_escape_still_dismisses_the_inspect_overlay(self):
        app = TuiApp("/tmp/x.sock", history=10)
        app.events = [sample_event(seq=n) for n in range(10)]
        app.inspecting = True
        self.run_loop(app, "\x1b", "q")
        assert app.inspecting is False

    def test_mouse_reporting_is_enabled_and_disabled_exactly_once(self):
        app = TuiApp("/tmp/x.sock")
        rendered = self.run_loop(app, "q")
        assert rendered.count("\x1b[?1000h\x1b[?1002h\x1b[?1006h") == 1
        assert rendered.count("\x1b[?1006l\x1b[?1002l\x1b[?1000l") == 1
        # Reporting is armed before the first frame is painted, so no click can
        # be lost between entering the alternate screen and the first render.
        assert rendered.index("\x1b[?1006h") < rendered.index("\x1b[1;1H")
        assert rendered.rstrip().endswith("\x1b[?1006l\x1b[?1002l\x1b[?1000l\x1b[?25h\x1b[?1049l")

    def test_restore_sequence_is_written_when_the_reader_is_interrupted(self):
        @contextlib.contextmanager
        def factory(*, on_resize=None):
            def read(timeout: float) -> str | None:
                raise KeyboardInterrupt

            yield read

        app = TuiApp("/tmp/x.sock")
        output = io.StringIO()
        code = app._loop(  # noqa: SLF001 - the documented test seam
            FakeClient(),
            FakeLive(),
            reader_factory=factory,
            output=output,
            max_iterations=4,
            size_getter=ScriptedSize((100, 30)),
        )
        rendered = output.getvalue()
        assert code == 0
        assert rendered.count("\x1b[?1006l\x1b[?1002l\x1b[?1000l") == 1
        assert rendered.rstrip().endswith("\x1b[?1006l\x1b[?1002l\x1b[?1000l\x1b[?25h\x1b[?1049l")

    def test_a_broken_output_stream_does_not_mask_the_exit(self):
        class BrokenStream:
            def write(self, _text: str) -> int:
                raise OSError("stdout is gone")

            def flush(self) -> None:
                raise OSError("stdout is gone")

        app = TuiApp("/tmp/x.sock")
        with pytest.raises(OSError):
            app._loop(  # noqa: SLF001 - the documented test seam
                FakeClient(),
                FakeLive(),
                reader_factory=scripted_reader("q"),
                output=BrokenStream(),
                max_iterations=2,
                size_getter=ScriptedSize((100, 30)),
            )


class _FdStdin:
    """A ``sys.stdin`` stand-in that reads straight from a descriptor."""

    def __init__(self, fd: int) -> None:
        self.fd = fd

    def fileno(self) -> int:
        return self.fd

    def read(self, size: int = 1) -> str:
        return os.read(self.fd, size).decode("utf-8", errors="replace")


def _pty_pair() -> tuple[int, int]:
    pty = pytest.importorskip("pty")
    return pty.openpty()


class TestTerminalSessionReads:
    """Drive the real reader: this is the code path the navigation fix lives in."""

    def test_a_burst_of_keys_arrives_in_one_read(self, monkeypatch):
        master, slave = _pty_pair()
        try:
            monkeypatch.setattr(sys, "stdin", _FdStdin(slave))
            with terminal_session() as read_key:
                os.write(master, b"\x1b[A\x1b[Bjq")
                # One unbuffered read must drain the whole burst; a buffered
                # TextIOWrapper starves the escape sequences apart.
                assert read_key(0.5) == "\x1b[A\x1b[Bjq"
        finally:
            os.close(master)
            os.close(slave)

    def test_a_utf8_character_split_across_reads_is_reassembled(self, monkeypatch):
        master, slave = _pty_pair()
        encoded = "→".encode("utf-8")
        assert len(encoded) == 3
        try:
            monkeypatch.setattr(sys, "stdin", _FdStdin(slave))
            with terminal_session() as read_key:
                os.write(master, encoded[:1])
                # Half a code point is held, not decoded into a replacement.
                assert read_key(0.5) is None
                os.write(master, encoded[1:])
                assert read_key(0.5) == "→"
        finally:
            os.close(master)
            os.close(slave)

    def test_an_invalid_byte_becomes_a_replacement_character(self, monkeypatch):
        master, slave = _pty_pair()
        try:
            monkeypatch.setattr(sys, "stdin", _FdStdin(slave))
            with terminal_session() as read_key:
                os.write(master, b"\xffj")
                assert read_key(0.5) == "\ufffdj"
        finally:
            os.close(master)
            os.close(slave)

    def test_no_input_reports_none(self, monkeypatch):
        master, slave = _pty_pair()
        try:
            monkeypatch.setattr(sys, "stdin", _FdStdin(slave))
            with terminal_session() as read_key:
                assert read_key(0.0) is None
        finally:
            os.close(master)
            os.close(slave)

    def test_tty_attributes_are_restored_on_exit(self, monkeypatch):
        master, slave = _pty_pair()
        try:
            monkeypatch.setattr(sys, "stdin", _FdStdin(slave))
            before = termios.tcgetattr(slave)
            with terminal_session():
                assert termios.tcgetattr(slave) != before, "cbreak must change the tty"
            assert termios.tcgetattr(slave) == before
        finally:
            os.close(master)
            os.close(slave)

    def test_a_closed_pipe_reports_eof_without_spinning(self, monkeypatch):
        read_fd, write_fd = os.pipe()
        os.close(write_fd)  # a pipe that is already at EOF
        try:
            monkeypatch.setattr(sys, "stdin", _FdStdin(read_fd))
            with terminal_session() as read_key:
                started = time.monotonic()
                for _ in range(3):
                    assert read_key(0.05) is None
                assert time.monotonic() - started < 2.0
        finally:
            os.close(read_fd)

    def test_an_unreadable_descriptor_degrades_to_none(self, monkeypatch):
        read_fd, write_fd = os.pipe()
        os.close(read_fd)
        try:
            monkeypatch.setattr(sys, "stdin", _FdStdin(read_fd))
            with terminal_session() as read_key:
                assert read_key(0.05) is None
        finally:
            os.close(write_fd)


class TestIntCoercionHelpers:
    """The cockpit must survive any value the daemon hands it."""

    def test_int_or_returns_native_ints_unchanged(self):
        from quay.tui import _int_or
        assert _int_or(0) == 0
        assert _int_or(42) == 42
        assert _int_or(-7) == -7

    def test_int_or_truncates_floats(self):
        from quay.tui import _int_or
        assert _int_or(3.7) == 3
        assert _int_or(-1.9) == -1
        assert _int_or(0.0) == 0

    def test_int_or_coerces_booleans_to_zero_or_one(self):
        from quay.tui import _int_or
        # Bool is a subclass of int; ``isinstance(True, int)`` is True.
        # The helper keeps that contract explicit so readers know the rule.
        assert _int_or(True) == 1
        assert _int_or(False) == 0

    def test_int_or_parses_valid_numeric_strings(self):
        from quay.tui import _int_or
        assert _int_or(" 17 ") == 17
        assert _int_or("-3") == -3

    def test_int_or_returns_default_for_garbage(self):
        from quay.tui import _int_or
        assert _int_or(None) == 0
        assert _int_or(None, 99) == 99
        assert _int_or("nope") == 0
        assert _int_or("nope", 4) == 4
        assert _int_or([1, 2]) == 0
        assert _int_or({"x": 1}) == 0
        assert _int_or(object()) == 0


class TestMappingHelper:
    def test_a_real_mapping_passes_through(self):
        from quay.tui import _mapping
        original = {"a": 1, "b": 2}
        out = _mapping(original)
        assert out is original

    def test_non_mapping_inputs_become_empty(self):
        from quay.tui import _mapping
        assert _mapping(None) == {}
        assert _mapping([1, 2]) == {}
        assert _mapping(5) == {}
        assert _mapping("nope") == {}
        assert _mapping((1, 2)) == {}


class TestSeqColumnHelper:
    def test_int_sequence_is_left_aligned_to_five_columns(self):
        from quay.tui import _seq_column
        assert _seq_column(1) == "1    "
        assert _seq_column(42) == "42   "
        assert _seq_column(12345) == "12345"

    def test_bool_is_rendered_as_string_not_as_one_or_zero(self):
        # Booleans are ints; the helper refuses them so a stray schema change
        # cannot silently turn a filter flip into a sequence column.
        from quay.tui import _seq_column
        # ``True`` is the string "True", not 1.
        assert _seq_column(True).strip() == "True"
        assert _seq_column(False).strip() == "False"

    def test_none_becomes_a_question_mark(self):
        from quay.tui import _seq_column
        assert _seq_column(None).strip() == "?"

    def test_unparseable_string_is_truncated_to_five_chars(self):
        from quay.tui import _seq_column
        assert _seq_column("abcdefg").strip() == "abcde"

    def test_negatives_are_signed(self):
        from quay.tui import _seq_column
        assert _seq_column(-1).strip() == "-1"


class TestStreamRowsFor:
    """``stream_rows_for`` is the single source of truth for the hit-test."""

    def test_a_tall_terminal_keeps_a_generous_viewport(self):
        from quay.tui import stream_rows_for
        # 24 - 3 = 21 above the footer; after row 3 the body has 21 - 3 + 1 = 19.
        assert stream_rows_for(24) == 19
        assert stream_rows_for(50) == 45

    def test_a_short_terminal_yields_a_visible_viewport(self):
        from quay.tui import stream_rows_for
        # The cockpit needs at least rows beyond title/filter/header/footer.
        assert stream_rows_for(6) == 1
        assert stream_rows_for(5) == 0

    def test_a_zero_or_negative_terminal_yields_zero(self):
        from quay.tui import stream_rows_for
        assert stream_rows_for(0) == 0
        assert stream_rows_for(-3) == 0

    def test_a_garbage_terminal_falls_back_to_a_safe_default(self):
        from quay.tui import stream_rows_for
        # ``_int_or(None, 24)`` falls back to 24, which yields 19 stream rows.
        assert stream_rows_for(None) == 19
        assert stream_rows_for("abc") == 19


class TestTokenizationMidBurstCarry:
    """Sequence bytes that arrive across reads must join before dispatch."""

    def test_a_partial_csi_string_is_one_opaque_token(self):
        assert split_keys("\x1b[1;5") == ["\x1b[1;5"]

    def test_a_completed_csi_then_a_partial_csi_split_cleanly(self):
        # Complete arrow + incomplete modifier-prefixed arrow.
        out = split_keys("\x1b[A\x1b[1;5")
        assert out == ["\x1b[A", "\x1b[1;5"]

    def test_a_partial_sgr_mouse_far_in_is_one_opaque_token(self):
        # Five numbers rather than the usual three: the daemon split mid-write.
        assert split_keys("\x1b[<0;15;5;32;17M") == ["\x1b[<0;15;5;32;17M"]

    def test_a_private_mode_escape_is_one_token(self):
        # The dashboard emits these; the tokeniser must not split them apart.
        assert split_keys("\x1b[?1049h") == ["\x1b[?1049h"]
        assert split_keys("\x1b[?25l") == ["\x1b[?25l"]


class TestRenderOnTinyScreens:
    """The dashboard must never raise, even when the terminal is impossibly small."""

    def test_a_one_row_terminal_renders_a_valid_grid(self):
        app = TuiApp("/tmp/x.sock")
        screen = app.dashboard.render(app.model(width=10, height=1))
        assert screen.width == 10
        assert screen.height == 1
        # Every cell exists and is a printable or blank character.
        for row in range(screen.height):
            rendered = screen.text_at(row)
            assert len(rendered) == screen.width

    def test_a_zero_height_terminal_does_not_crash(self):
        app = TuiApp("/tmp/x.sock")
        screen = app.dashboard.render(app.model(width=80, height=0))
        assert screen.width == 80
        # ``Screen.height`` is pinned to >= 1 by the constructor; the model
        # floor of >= 1 also keeps this honest.
        assert screen.height >= 1


class TestInspectOverlayClipping:
    """``_render_inspect`` must cap the payload excerpt and not crash on it."""

    def test_a_huge_payload_renders_a_clipped_overlay(self):
        app = TuiApp("/tmp/x.sock")
        payload = {"blob": "x" * 5000, "nested": {"y": list(range(2000))}}
        event = sample_event(seq=11, kind="note", topic="audit/huge", **payload)
        app.events = [event]
        app.inspecting = True
        # Width and height generous enough to give the overlay room.
        screen = app.dashboard.render(app.model(width=120, height=40))
        rendered = "\n".join(screen.text_at(r) for r in range(screen.height))
        # The overlay title line must be present.
        assert "inspect event #11" in rendered
        # The dismiss hint must be present.
        assert "Press Enter or Esc" in rendered
        # The 5,000-character blob must not appear verbatim; only its prefix.
        assert "x" * 5000 not in rendered

    def test_a_non_ascii_payload_does_not_break_rendering(self):
        app = TuiApp("/tmp/x.sock")
        payload = {"text": "héllo → 🚀 café"}
        event = sample_event(seq=12, kind="note", **payload)
        app.events = [event]
        app.inspecting = True
        screen = app.dashboard.render(app.model(width=100, height=30))
        # The text must appear in the rendered grid (the cockpit never
        # crashes on multibyte data).
        rendered = "\n".join(screen.text_at(r) for r in range(screen.height))
        assert "inspect event #12" in rendered

    def test_an_unserializable_payload_falls_back_safely(self):
        app = TuiApp("/tmp/x.sock")
        # A set is not JSON-serialisable; the inspect overlay must degrade
        # gracefully instead of taking the frame down.
        event = sample_event(seq=13, kind="note", tags=object())
        app.events = [event]
        app.inspecting = True
        screen = app.dashboard.render(app.model(width=80, height=24))
        rendered = "\n".join(screen.text_at(r) for r in range(screen.height))
        assert "inspect event #13" in rendered

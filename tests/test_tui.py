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
    STYLES,
    Dashboard,
    Screen,
    TuiApp,
    age_of,
    render_diff,
    sanitize,
    summarize_event,
    terminal_session,
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

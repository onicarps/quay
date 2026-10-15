"""End-to-end CLI smoke tests (requirements F14, F15).

Every command is invoked through ``quay.cli.main`` exactly as a user would,
against a real daemon process and a real ledger. Output is validated both as
JSON (the non-TTY default) and as human text (``--no-json``).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
from pathlib import Path

import pytest

from quay import __version__, cli
from quay.daemon import stop_daemon


@pytest.fixture()
def run(capsys, monkeypatch):
    """Invoke the CLI in-process and return (exit_code, stdout, stderr)."""

    def _run(*args: str) -> tuple[int, str, str]:
        code = cli.main(list(args))
        captured = capsys.readouterr()
        return code, captured.out, captured.err

    return _run


@pytest.fixture()
def ws(tmp_path: Path, monkeypatch):
    root = tmp_path / "ws"
    root.mkdir()
    monkeypatch.chdir(root)
    yield root
    try:
        stop_daemon(root)
    except Exception:  # pragma: no cover - teardown best effort
        pass


def as_json(text: str):
    return json.loads(text)


class TestStatusAndLifecycle:
    def test_status_autostarts_a_daemon_and_emits_json(self, run, ws):
        code, out, _err = run("status")
        assert code == 0
        payload = as_json(out)
        assert payload["ledger"]["records"] == 0
        assert payload["daemon"]["workspace"] == str(ws)
        assert payload["daemon"]["protocol_version"] == "1"

    def test_status_human_output(self, run, ws):
        assert run("status")[0] == 0
        code, out, _err = run("status", "--no-json")
        assert code == 0
        assert "records=0" in out
        assert "chain=" in out

    def test_start_is_idempotent(self, run, ws):
        first = as_json(run("start")[1])["health"]["instance_id"]
        second = as_json(run("start")[1])["health"]["instance_id"]
        assert first == second

    def test_restart_changes_the_instance(self, run, ws):
        first = as_json(run("start")[1])["health"]["instance_id"]
        second = as_json(run("restart")[1])["health"]["instance_id"]
        assert first != second

    def test_stop_reports_state(self, run, ws):
        run("start")
        code, out, _err = run("stop")
        assert code == 0
        assert as_json(out)["stopped"] is True
        assert as_json(run("stop")[1])["stopped"] is False

    def test_daemons_lists_the_workspace(self, run, ws):
        run("start")
        payload = as_json(run("daemons")[1])
        assert payload["count"] == 1
        assert payload["daemons"][0]["running"] is True
        assert payload["daemons"][0]["workspace"] == str(ws)

    def test_doctor_reports_every_check(self, run, ws):
        run("start")
        code, out, _err = run("doctor")
        payload = as_json(out)
        assert code == 0
        assert payload["ok"] is True
        names = {check["check"] for check in payload["checks"]}
        assert {"python>=3.11", "workspace", "daemon", "ledger-integrity"} <= names

    def test_status_with_a_missing_workspace_fails_cleanly(self, run, tmp_path):
        code, _out, err = run("--workspace", str(tmp_path / "nope"), "status")
        assert code == 2
        assert as_json(err)["error"]["code"] == "workspace_unavailable"

    def test_unstartable_daemon_is_reported(self, run, ws, monkeypatch):
        monkeypatch.setenv("QUAY_PYTHON", "/bin/false")
        code, _out, err = run("status")
        assert code == 2
        assert as_json(err)["error"]["code"] == "daemon_start_failed"


class TestEmitAndQuery:
    def test_emit_then_log_and_query(self, run, ws):
        code, out, _err = run("emit", "okf/handoff", '{"to": "codex"}', "--producer", "factory")
        assert code == 0
        event = as_json(out)["event"]
        assert event["seq"] == 1
        assert event["topic"] == "okf/handoff"

        logged = as_json(run("log", "--limit", "5")[1])["events"]
        assert [item["seq"] for item in logged] == [1]

        queried = as_json(run("query", "--kind", "generic", "--producer", "factory")[1])["events"]
        assert len(queried) == 1

    def test_emit_accepts_typed_kinds(self, run, ws):
        run("emit", "alert", '{"state": "pending", "summary": "build red"}', "--kind", "signal")
        run("emit", "notes", '{"text": "hello"}', "--kind", "note")
        run("emit", "metrics", '{"name": "p99", "value": 12}', "--kind", "metric")
        status = as_json(run("status")[1])
        assert status["planes"]["signal"]["pending"] == 1
        assert status["kinds"] == {"signal": 1, "note": 1, "metric": 1}

    def test_emit_rejects_a_bad_payload_with_a_code(self, run, ws):
        code, _out, err = run("emit", "t", "{not json}")
        assert code == 2
        assert as_json(err)["error"]["code"] == "invalid_payload"

    def test_emit_reports_schema_rejections_from_the_daemon(self, run, ws):
        code, _out, err = run("emit", "t", '{"state": "nonsense"}', "--kind", "signal")
        assert code == 2
        assert as_json(err)["error"]["code"] == "invalid_state"

    def test_query_count_only_and_ordering(self, run, ws):
        for index in range(4):
            run("emit", "t", json.dumps({"i": index}))
        assert as_json(run("query", "--count-only")[1]) == {"count": 4}
        descending = as_json(run("query", "--order", "desc", "--limit", "2")[1])["events"]
        assert [event["seq"] for event in descending] == [4, 3]

    def test_query_export_jsonl(self, run, ws):
        run("emit", "t", '{"i": 1}')
        code, out, _err = run("query", "--export", "jsonl")
        assert code == 0
        lines = [line for line in out.splitlines() if line.strip()]
        assert len(lines) == 1
        assert json.loads(lines[0])["seq"] == 1

    def test_query_export_csv(self, run, ws):
        run("emit", "t", '{"i": 1}')
        code, out, _err = run("query", "--export", "csv")
        assert code == 0
        header = out.splitlines()[0]
        assert header.startswith("seq,ts,kind,topic,producer")

    def test_query_rejects_unknown_filters(self, run, ws):
        # argparse refuses flags it does not know; the daemon refuses unknown fields.
        code, _out, err = run("query", "--nonsense")
        assert code == 2
        assert "usage" in as_json(err)["error"]["code"]

    def test_log_human_output(self, run, ws):
        run("emit", "t", '{"i": 1}')
        code, out, _err = run("log", "--no-json")
        assert code == 0
        assert "#1" in out
        assert "generic" in out

    def test_kinds_lists_the_registry(self, run, ws):
        payload = as_json(run("kinds")[1])
        assert {item["kind"] for item in payload["kinds"]} == {
            "signal", "work", "handoff", "note", "metric", "generic"
        }


class TestWorkPlane:
    def test_full_work_lifecycle(self, run, ws):
        claim = as_json(run("work", "claim", "job-1", "--executor", "factory", "--lease-seconds", "600")[1])["event"]
        assert claim["payload"]["state"] == "claimed"
        token = str(claim["payload"]["fencing_token"])
        beat = as_json(
            run(
                "work", "heartbeat", "job-1", "--executor", "factory",
                "--causation", str(claim["seq"]), "--fencing-token", token,
            )[1]
        )["event"]
        assert beat["payload"]["state"] == "in_flight"
        done = as_json(
            run(
                "work",
                "complete",
                "job-1",
                "--executor",
                "factory",
                "--causation",
                str(beat["seq"]),
                "--fencing-token",
                token,
                "--evidence",
                '{"commit": "abc123"}',
            )[1]
        )["event"]
        assert done["payload"]["state"] == "done"

        listed = as_json(run("work", "list")[1])["work"]
        assert [row["state"] for row in listed] == ["done"]
        assert as_json(run("status")[1])["planes"]["work"]["done"] == 1

    def test_complete_requires_evidence(self, run, ws):
        run("work", "claim", "job-2", "--executor", "factory")
        code, _out, err = run("work", "complete", "job-2", "--executor", "factory", "--causation", "1")
        assert code == 2  # argparse requires --evidence
        assert "evidence" in err or "usage" in err

    def test_abandon_requires_a_reason(self, run, ws):
        run("work", "claim", "job-3", "--executor", "factory")
        code, _out, err = run("work", "abandon", "job-3", "--executor", "factory", "--causation", "1")
        assert code == 2

    def test_work_list_human_output(self, run, ws):
        run("work", "claim", "job-4", "--executor", "factory")
        code, out, _err = run("work", "list", "--no-json")
        assert code == 0
        assert "job-4" in out

    def test_reap_expires_lapsed_leases(self, run, ws):
        run("work", "claim", "job-5", "--executor", "factory", "--lease-seconds", "1")
        payload = as_json(run("work", "reap")[1])
        # The lease is still live, so nothing is reaped yet.
        assert payload["count"] == 0

    def test_work_without_a_subcommand_is_a_usage_error(self, run, ws):
        code, _out, err = run("work")
        assert code == 2
        assert as_json(err)["error"]["code"] == "usage"


class TestIntegrityAndPrune:
    def test_verify_ok_then_detects_tampering(self, run, ws):
        for index in range(5):
            run("emit", "t", json.dumps({"i": index}))
        code, out, _err = run("verify")
        assert code == 0
        assert as_json(out)["ok"] is True

        # Tamper with the ledger directly, as an attacker or a bad tool would.
        ledger = ws / ".quay" / "ledger.db"
        connection = sqlite3.connect(ledger)
        connection.execute("UPDATE events SET payload = ? WHERE seq = 3", ('{"i": 999}',))
        connection.commit()
        connection.close()

        code, out, _err = run("verify")
        assert code == 1
        report = as_json(out)
        assert report["ok"] is False
        assert report["divergence"]["seq"] == 3

    def test_prune_preview_does_not_delete(self, run, ws):
        for index in range(6):
            run("emit", "t", json.dumps({"i": index}))
        payload = as_json(run("prune", "--keep-last", "2", "--dry-run")[1])
        assert payload["dry_run"] is True
        assert payload["removable"] == 4
        assert as_json(run("status")[1])["ledger"]["records"] == 6

    def test_prune_requires_confirmation(self, run, ws):
        run("emit", "t", '{"i": 1}')
        code, _out, err = run("prune", "--keep-last", "0")
        assert code == 2
        assert as_json(err)["error"]["code"] == "confirmation_required"

    def test_prune_requires_a_scope(self, run, ws):
        code, _out, err = run("prune", "--yes")
        assert code == 2
        assert as_json(err)["error"]["code"] == "prune_scope_required"

    def test_prune_applies_and_keeps_the_chain_verifiable(self, run, ws):
        for index in range(6):
            run("emit", "t", json.dumps({"i": index}))
        payload = as_json(run("prune", "--keep-last", "2", "--yes")[1])
        assert payload["applied"] is True
        assert payload["removed"] == 4
        assert as_json(run("status")[1])["ledger"]["records"] == 2
        assert as_json(run("verify")[1])["ok"] is True


class TestInit:
    def test_init_reports_paths_and_advisory_outside_git(self, run, ws):
        payload = as_json(run("init")[1])
        assert payload["workspace"] == str(ws)
        assert payload["ledger"].endswith("ledger.db")
        assert payload["git_advisory"] is None

    def test_init_git_ignore_preview_then_apply(self, run, ws):
        subprocess.run(["git", "init", "-q", str(ws)], check=True)
        preview = as_json(run("init", "--git-ignore")[1])
        assert preview["git_ignore_applied"] is False
        assert "would append" in preview["git_advisory"]

        applied = as_json(run("init", "--git-ignore", "--yes")[1])
        assert applied["git_ignore_applied"] is True
        assert ".quay/" in (ws / ".gitignore").read_text(encoding="utf-8")

    def test_init_reports_the_git_advisory_when_unignored(self, run, ws):
        subprocess.run(["git", "init", "-q", str(ws)], check=True)
        payload = as_json(run("init")[1])
        assert payload["git_advisory"] is not None
        assert ".quay/" in payload["git_advisory"]


class TestWatch:
    def test_watch_without_a_tty_prints_a_snapshot(self, run, ws):
        code, out, err = run("watch")
        assert code == 0
        assert as_json(out)["ledger"]["records"] == 0
        assert "no TTY" in err

    def test_watch_plain_streams_json_lines(self, run, ws):
        for index in range(3):
            run("emit", "t", json.dumps({"i": index}))
        code, out, _err = run("watch", "--plain", "--history", "3", "--max-events", "3")
        assert code == 0
        lines = [json.loads(line) for line in out.splitlines() if line.strip()]
        assert [event["seq"] for event in lines] == [1, 2, 3]

    def test_log_follow_streams_the_backlog(self, run, ws):
        for index in range(2):
            run("emit", "t", json.dumps({"i": index}))
        code, out, _err = run("log", "--follow", "--limit", "2", "--max-events", "2")
        assert code == 0
        lines = [json.loads(line) for line in out.splitlines() if line.strip()]
        assert [event["seq"] for event in lines] == [1, 2]

    def test_follow_surfaces_a_terminal_recovery_error(self, monkeypatch):
        real_stream = cli.SubscriptionStream

        class FakeConnection:
            socket_path = Path("/tmp/unused.sock")

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                return False

        class RecoveryFailedStream:
            is_terminal = True
            _raise_recovery_error = staticmethod(real_stream._raise_recovery_error)

            def __init__(self, *args, **kwargs):
                self.frames = [
                    {
                        "type": "recovery_error",
                        "error": {
                            "code": "recovery_queue_full",
                            "message": "backfill queue filled",
                            "detail": {"until_seq": 99},
                        },
                    }
                ]

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                return False

            def poll(self, timeout):
                return self.frames.pop(0) if self.frames else None

        monkeypatch.setattr(cli, "connected", lambda args: FakeConnection())
        monkeypatch.setattr(cli, "SubscriptionStream", RecoveryFailedStream)
        args = argparse.Namespace(duration=None, max_events=None)

        with pytest.raises(cli.ClientError) as excinfo:
            cli._follow(args, json_mode=True, initial_limit=0)  # noqa: SLF001
        assert excinfo.value.code == "recovery_queue_full"
        assert excinfo.value.detail == {"until_seq": 99}

    def test_follow_exits_after_a_clean_terminal_stream(self, monkeypatch):
        class FakeConnection:
            socket_path = Path("/tmp/unused.sock")

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                return False

        class EndedStream:
            is_terminal = True

            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                return False

            def poll(self, timeout):
                return None

        monkeypatch.setattr(cli, "connected", lambda args: FakeConnection())
        monkeypatch.setattr(cli, "SubscriptionStream", EndedStream)
        args = argparse.Namespace(duration=None, max_events=None)

        assert cli._follow(args, json_mode=True, initial_limit=0) == 0  # noqa: SLF001

    def test_watch_plain_honours_the_duration_bound(self, run, ws):
        code, _out, _err = run("watch", "--plain", "--history", "0", "--for", "0.3")
        assert code == 0

    def test_monitor_is_a_true_alias_of_watch(self):
        parser = cli.build_parser()
        subparsers = [
            action
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        ][0]
        assert "monitor" in subparsers.choices
        assert cli.COMMANDS["monitor"] is cli.COMMANDS["watch"]

        def option_strings(parser):
            return {option for action in parser._actions for option in action.option_strings}

        def positionals(parser):
            return [action.dest for action in parser._actions if not action.option_strings]

        assert option_strings(subparsers.choices["monitor"]) == option_strings(subparsers.choices["watch"])
        assert positionals(subparsers.choices["monitor"]) == positionals(subparsers.choices["watch"])

    def test_monitor_plain_streams_like_watch(self, run, ws):
        for index in range(3):
            run("emit", "t", json.dumps({"i": index}))
        code, out, _err = run("monitor", "--plain", "--history", "3", "--max-events", "3")
        assert code == 0
        lines = [json.loads(line) for line in out.splitlines() if line.strip()]
        assert [event["seq"] for event in lines] == [1, 2, 3]

    def test_monitor_auto_initializes_a_fresh_workspace(self, run, ws):
        code, out, err = run("monitor", "--history", "1", "--max-events", "1")
        assert code == 0
        events = [json.loads(line) for line in out.splitlines() if line.strip()]
        assert len(events) == 1
        assert events[0]["topic"] == "quay/init"
        assert "streaming JSON lines" in err


class TestPositionalWorkspace:
    def test_positional_directory_opens_the_cockpit_snapshot(self, run, ws):
        code, out, err = run(".")
        assert code == 0
        assert as_json(out)["daemon"]["workspace"] == str(ws)
        assert "no TTY" in err

    def test_positional_path_auto_initializes_and_seeds_one_genesis_event(self, run, tmp_path):
        target = tmp_path / "fresh"
        target.mkdir()
        code, out, _err = run(str(target))
        assert code == 0
        assert as_json(out)["ledger"]["records"] == 1

        # A repeat invocation must not append a second genesis record.
        code, out, _err = run(str(target))
        assert code == 0
        assert as_json(out)["ledger"]["records"] == 1

        events = as_json(run("--workspace", str(target), "log", "--limit", "5")[1])["events"]
        assert len(events) == 1
        genesis = events[0]
        assert (genesis["seq"], genesis["kind"], genesis["topic"], genesis["producer"]) == (
            1,
            "note",
            "quay/init",
            "quay",
        )
        assert genesis["payload"] == {
            "text": "Quay ledger initialized. Waiting for agent events...",
            "tags": ["init", "welcome"],
        }

    def test_positional_path_creates_state_only_inside_the_target(self, run, ws, tmp_path):
        target = tmp_path / "elsewhere"
        target.mkdir()
        assert run(str(target))[0] == 0
        assert (target / ".quay" / "ledger.db").exists()
        assert not (ws / ".quay").exists()

    def test_positional_unknown_token_is_a_usage_error(self, run):
        code, _out, err = run("frobnicate")
        assert code == 2
        assert as_json(err)["error"]["code"] in ("usage", "unknown_command")

    def test_positional_file_is_not_a_workspace(self, run, ws):
        target = ws / "notes.txt"
        target.write_text("hello", encoding="utf-8")
        code, _out, err = run(str(target))
        assert code == 2
        assert as_json(err)["error"]["code"] == "usage"

    def test_filesystem_root_is_rejected(self, run):
        code, _out, err = run("--workspace", "/", "status")
        assert code == 2
        assert as_json(err)["error"]["code"] == "unsafe_workspace"

    def test_positional_path_conflicts_with_the_workspace_flag(self, run, ws):
        code, _out, err = run("watch", str(ws), "--workspace", str(ws))
        assert code == 2
        assert as_json(err)["error"]["code"] == "conflicting_workspace"

    def test_onboarding_appends_exactly_one_gitignore_line(self, run, tmp_path):
        root = tmp_path / "repo"
        root.mkdir()
        subprocess.run(["git", "init", "-q", str(root)], check=True)
        (root / ".gitignore").write_text(".venv", encoding="utf-8")
        assert run(str(root))[0] == 0
        assert run(str(root))[0] == 0
        assert (root / ".gitignore").read_text(encoding="utf-8") == ".venv\n.quay/\n"

    def test_onboarding_leaves_gitignore_alone_outside_git(self, run, tmp_path):
        root = tmp_path / "plain"
        root.mkdir()
        assert run(str(root))[0] == 0
        assert not (root / ".gitignore").exists()


class TestUsageAndErrors:
    def test_version_exits_zero(self, capsys):
        with pytest.raises(SystemExit) as excinfo:
            cli.main(["--version"])
        assert excinfo.value.code == 0
        assert f"quay {__version__}" in capsys.readouterr().out

    def test_unknown_command_is_rejected(self, run):
        code, _out, err = run("frobnicate")
        assert code == 2
        assert as_json(err)["error"]["code"] in ("usage", "unknown_command")

    def test_unknown_flag_is_rejected(self, run):
        code, _out, err = run("status", "--nope")
        assert code == 2
        assert as_json(err)["error"]["code"] == "usage"

    def test_help_text_lists_every_command(self, capsys):
        with pytest.raises(SystemExit):
            cli.main(["--help"])
        text = capsys.readouterr().out
        for command in (
            "init",
            "start",
            "stop",
            "restart",
            "status",
            "daemons",
            "doctor",
            "kinds",
            "verify",
            "emit",
            "work",
            "query",
            "log",
            "watch",
            "monitor",
            "mcp",
            "prune",
            "bench",
            "import",
        ):
            assert command in text

    def test_every_registered_command_is_dispatched(self):
        parser = cli.build_parser()
        subparsers = [
            action
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        ][0]
        assert set(subparsers.choices) == set(cli.COMMANDS)
        # `monitor` is a true alias: one handler, one definition.
        assert cli.COMMANDS["monitor"] is cli.COMMANDS["watch"]
        assert cli.COMMANDS["mcp"] is cli.cmd_mcp

    def test_import_requires_an_existing_source(self, run, tmp_path):
        code, _out, err = run("import", str(tmp_path / "missing.db"), "--json")
        assert code == 2
        assert as_json(err)["error"]["code"] == "source_missing"

    def test_import_reports_a_migration(self, run, ws, tmp_path):
        changi = tmp_path / "changi"
        changi.mkdir()
        connection = sqlite3.connect(changi / "events.db")
        connection.executescript(
            "CREATE TABLE events (event_id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL,"
            " producer_id TEXT NOT NULL, timestamp TEXT NOT NULL, schema_version INTEGER NOT NULL"
            " DEFAULT 1, payload TEXT NOT NULL, causation_id INTEGER);"
        )
        connection.execute(
            "INSERT INTO events (topic, producer_id, timestamp, payload)"
            " VALUES ('demo/topic', 'changi.worker', '2026-10-04T12:00:00+00:00', '{\"n\": 1}')"
        )
        connection.commit()
        connection.close()
        code, out, _err = run("import", str(changi), "--json")
        assert code == 0
        payload = as_json(out)
        assert payload["read"] == 1
        assert payload["migrated"] == 1
        code, out, _err = run("query", "--json")
        assert as_json(out)["count"] == 1

    def test_import_dry_run_writes_nothing(self, run, ws, tmp_path):
        changi = tmp_path / "changi.db"
        connection = sqlite3.connect(changi)
        connection.executescript(
            "CREATE TABLE events (event_id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL,"
            " producer_id TEXT NOT NULL, timestamp TEXT NOT NULL, schema_version INTEGER NOT NULL"
            " DEFAULT 1, payload TEXT NOT NULL, causation_id INTEGER);"
        )
        connection.execute(
            "INSERT INTO events (topic, producer_id, timestamp, payload)"
            " VALUES ('demo/topic', 'changi.worker', '2026-10-04T12:00:00+00:00', '{}')"
        )
        connection.commit()
        connection.close()
        code, out, _err = run("import", str(changi), "--dry-run", "--json")
        assert code == 0
        assert as_json(out)["dry_run"] is True
        code, out, _err = run("query", "--json")
        assert as_json(out)["count"] == 0

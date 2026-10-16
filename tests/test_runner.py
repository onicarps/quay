"""Autonomous runner engine tests: config parsing, turn execution, acknowledgement."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from quay.bridge import WakeBridge
from quay.cursors import CursorStore
from quay.runner import Runner, RunnerConfigError, load_runner_config
from quay.workspace import paths_for


def toml_string(value: str) -> str:
    """Quote ``value`` as a TOML basic string with the required escapes."""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def write_config(workspace: Path, body: str) -> Path:
    target = workspace / "okf" / "harness" / "runners.toml"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body, encoding="utf-8")
    return target


def write_adapter(workspace: Path, name: str, source: str) -> Path:
    """Materialize a standalone adapter script the runner can spawn.

    Adapters are external programs, so the tests exercise real subprocess
    execution without fighting nested shell/TOML quoting.
    """
    target = workspace / name
    target.write_text(source, encoding="utf-8")
    target.chmod(0o755)
    return target


def simple_config(workspace: Path, *, command: str, **overrides: object) -> Path:
    fields: dict[str, object] = {
        "enabled": True,
        "producer": "factory",
        "accept_to": ["factory"],
        "adapter": "command",
        "adapter_command": command,
        "turn_timeout_seconds": 30,
        "max_turns_per_chain": 10,
    }
    fields.update(overrides)
    lines = []
    for key, value in fields.items():
        if isinstance(value, bool):
            lines.append(f"{key} = {'true' if value else 'false'}")
        elif isinstance(value, (int, float)):
            lines.append(f"{key} = {value}")
        elif isinstance(value, list):
            quoted = ", ".join(toml_string(str(item)) for item in value)
            lines.append(f"{key} = [{quoted}]")
        else:
            lines.append(f"{key} = {toml_string(str(value))}")
    return write_config(workspace, "[runner.factory]\n" + "\n".join(lines) + "\n")


def handoff(
    client,
    *,
    recipient: str = "factory",
    sender: str = "agy",
    summary: str = "go",
    **extra,
):
    return client.emit(
        topic=f"swarm/{recipient}/handoff",
        producer=sender,
        kind="handoff",
        payload={"from": sender, "to": recipient, "summary": summary},
        **extra,
    )


def make_runner(workspace: Path, config, *, max_retries: int = 3) -> Runner:
    bridge = WakeBridge(
        workspace,
        runner=config.name,
        wake_dir=config.wake_dir,
        max_retries=max_retries,
        ack_timeout_seconds=config.turn_timeout_seconds,
    )
    return Runner(workspace, config, bridge=bridge)


@pytest.fixture()
def ok_script(workspace: Path) -> Path:
    return write_adapter(
        workspace,
        "ok_adapter.py",
        "import os\n"
        "print('QUAY_EVENT_SEQ=' + os.environ['QUAY_EVENT_SEQ'])\n"
        "print('QUAY_RUNNER=' + os.environ['QUAY_RUNNER'])\n"
        "print('QUAY_RUN_DIR=' + os.environ['QUAY_RUN_DIR'])\n"
        "print('QUAY_TOPIC=' + os.environ['QUAY_TOPIC'])\n",
    )


@pytest.fixture()
def env_script(workspace: Path) -> Path:
    return write_adapter(
        workspace,
        "env_adapter.py",
        "import json, os\n"
        "target = os.path.join(os.environ['QUAY_RUN_DIR'], 'env.json')\n"
        "with open(target, 'w', encoding='utf-8') as handle:\n"
        "    json.dump({key: value for key, value in os.environ.items() "
        "if key.startswith('QUAY_')}, handle)\n",
    )


def adapter_command(script: Path) -> str:
    return f"{sys.executable} {script}"


class TestConfigParsing:
    def test_defaults_are_applied(self, workspace: Path):
        write_config(workspace, '[runner.factory]\nadapter_command = "echo ok"\n')
        runners = load_runner_config(workspace)
        config = runners["factory"]
        assert config.enabled is True
        assert config.producer == "factory"
        assert config.accept_to == ("factory",)
        assert config.wake_dir == workspace / ".quay" / "wake" / "factory"
        assert config.runs_dir == workspace / ".quay" / "runs" / "factory"
        assert config.ack_mode == "done-file"
        assert config.adapter == "command"
        assert config.adapter_command == "echo ok"
        assert config.turn_timeout_seconds == 1800.0
        assert config.max_turns_per_chain == 10

    def test_explicit_values_win(self, workspace: Path):
        write_config(
            workspace,
            "\n".join(
                [
                    "[runner.builder]",
                    "enabled = false",
                    'producer = "codex"',
                    'accept_to = ["codex", "agy"]',
                    'intake = "spool"',
                    "allow_broadcast = true",
                    "max_event_age_hours = 12",
                    'wake_dir = "custom/wake"',
                    'runs_dir = "custom/runs"',
                    'ack_mode = "done-file"',
                    'adapter = "command"',
                    'adapter_command = "echo hi"',
                    "turn_timeout_seconds = 5",
                    "max_turns_per_chain = 3",
                    "",
                ]
            ),
        )
        runners = load_runner_config(workspace)
        config = runners["builder"]
        assert config.enabled is False
        assert config.producer == "codex"
        assert config.accept_to == ("codex", "agy")
        assert config.allow_broadcast is True
        assert config.max_event_age_hours == 12.0
        assert config.wake_dir == workspace / "custom" / "wake"
        assert config.runs_dir == workspace / "custom" / "runs" / "builder"
        assert config.turn_timeout_seconds == 5.0
        assert config.max_turns_per_chain == 3

    def test_absolute_paths_are_kept(self, workspace: Path, tmp_path: Path):
        absolute = tmp_path / "runs"
        write_config(
            workspace,
            f'[runner.factory]\nadapter_command = "echo ok"\nruns_dir = "{absolute}"\n',
        )
        config = load_runner_config(workspace)["factory"]
        assert config.runs_dir == absolute / "factory"

    def test_missing_file_is_reported(self, workspace: Path):
        with pytest.raises(RunnerConfigError) as failure:
            load_runner_config(workspace)
        assert failure.value.code == "runner_config_missing"

    def test_no_runner_tables_is_reported(self, workspace: Path):
        write_config(workspace, "schema_version = 1\n")
        with pytest.raises(RunnerConfigError) as failure:
            load_runner_config(workspace)
        assert failure.value.code == "invalid_runner_config"

    def test_bad_adapter_command_is_reported(self, workspace: Path):
        write_config(workspace, "[runner.factory]\nadapter_command = 5\n")
        with pytest.raises(RunnerConfigError) as failure:
            load_runner_config(workspace)
        assert failure.value.code == "invalid_runner_config"

    def test_unsupported_ack_mode_is_reported(self, workspace: Path):
        write_config(
            workspace,
            '[runner.factory]\nadapter_command = "echo ok"\nack_mode = "webhook"\n',
        )
        with pytest.raises(RunnerConfigError) as failure:
            load_runner_config(workspace)
        assert failure.value.code == "unsupported_ack_mode"

    def test_scaffold_example_table_parses(self, workspace: Path):
        write_config(workspace, '[runner.example]\nadapter_command = "false"\n')
        config = load_runner_config(workspace)["example"]
        assert config.adapter_command == "false"

    def test_packaged_template_parses(self, workspace: Path):
        template = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "quay"
            / "templates"
            / "okf"
            / "okf"
            / "harness"
            / "runners.toml"
        )
        target = write_config(workspace, template.read_text(encoding="utf-8"))
        assert target.exists()
        config = load_runner_config(workspace)["example"]
        assert config.enabled is False
        assert config.adapter_command == "false"
        assert config.runs_dir == workspace / ".quay" / "runs" / "example"


class TestSuccessfulTurn:
    def test_command_adapter_runs_acknowledges_and_advances(
        self, workspace: Path, server, client, ok_script: Path
    ):
        simple_config(workspace, command=adapter_command(ok_script))
        config = load_runner_config(workspace)["factory"]
        event = handoff(client)
        with make_runner(workspace, config) as runner:
            cycle = runner.poll_once()
            run_dir = Path(cycle["result"]["run_dir"])
            log = (run_dir / "run.log").read_text(encoding="utf-8")
            ack = json.loads(runner.bridge._ack_path(event["seq"]).read_text())  # noqa: SLF001

        assert cycle["action"] == "completed"
        assert cycle["exit_code"] == 0
        assert f"QUAY_EVENT_SEQ={event['seq']}" in log
        assert "QUAY_RUNNER=factory" in log
        assert f"QUAY_RUN_DIR={run_dir}" in log
        assert "QUAY_TOPIC=swarm/factory/handoff" in log

        result = json.loads((run_dir / "result.json").read_text(encoding="utf-8"))
        assert result["status"] == "completed"
        assert result["exit_code"] == 0
        assert result["seq"] == event["seq"]
        assert os.stat(run_dir).st_mode & 0o777 == 0o700

        assert ack["seq"] == event["seq"]
        assert ack["summary"].startswith("RUNNER_ACK")

        companions = client.query(
            kind="handoff", producer="factory", causation_id=event["seq"], order="asc"
        )
        assert len(companions) == 1
        payload = companions[0]["payload"]
        assert payload["summary"].startswith("RUNNER_ACK")
        assert payload["action"]["type"] == "runner_ack"
        assert companions[0]["topic"] == "swarm/agy/handoff"

        cursor = CursorStore(paths_for(workspace).root).get("factory")
        assert cursor["last_acked_seq"] == event["seq"]
        assert cursor["in_flight"] is None

    def test_companion_uses_configured_producer_when_different_from_name(
        self, workspace: Path, server, client, ok_script: Path
    ):
        write_config(
            workspace,
            "\n".join(
                [
                    "[runner.factory_worker]",
                    "enabled = true",
                    'producer = "factory"',
                    'accept_to = ["factory"]',
                    'adapter = "command"',
                    f'adapter_command = "{adapter_command(ok_script)}"',
                    "turn_timeout_seconds = 30",
                    "max_turns_per_chain = 10",
                    "",
                ]
            ),
        )
        config = load_runner_config(workspace)["factory_worker"]
        event = handoff(client, recipient="factory_worker")
        with make_runner(workspace, config) as runner:
            cycle = runner.poll_once()

        assert cycle["action"] == "completed"
        companions = client.query(
            kind="handoff", producer="factory", causation_id=event["seq"], order="asc"
        )
        assert len(companions) == 1
        assert companions[0]["producer"] == "factory"
        assert companions[0]["payload"]["from"] == "factory"

    def test_adapter_receives_the_documented_environment(
        self, workspace: Path, server, client, env_script: Path
    ):
        simple_config(workspace, command=adapter_command(env_script))
        config = load_runner_config(workspace)["factory"]
        event = handoff(client)
        with make_runner(workspace, config) as runner:
            cycle = runner.poll_once()
            run_dir = Path(cycle["result"]["run_dir"])

        env = json.loads((run_dir / "env.json").read_text(encoding="utf-8"))
        assert env["QUAY_WORKSPACE"] == str(paths_for(workspace).workspace)
        assert env["QUAY_RUNNER"] == "factory"
        assert env["QUAY_EVENT_SEQ"] == str(event["seq"])
        assert env["QUAY_PRODUCER"] == "factory"
        assert env["QUAY_TOPIC"] == "swarm/factory/handoff"
        assert env["QUAY_SENDER"] == "agy"
        assert env["QUAY_RUN_DIR"] == str(run_dir)

    def test_second_cycle_delivers_next_handoff(
        self, workspace: Path, server, client, ok_script: Path
    ):
        simple_config(workspace, command=adapter_command(ok_script))
        config = load_runner_config(workspace)["factory"]
        first = handoff(client, summary="first")
        second = handoff(client, summary="second")
        with make_runner(workspace, config) as runner:
            runner.poll_once()
            second_cycle = runner.poll_once()

        assert second_cycle["seq"] == second["seq"]
        assert second_cycle["action"] == "completed"
        assert CursorStore(paths_for(workspace).root).get("factory")["last_acked_seq"] == second["seq"]
        assert not runner.bridge._wake_path(first["seq"]).exists()  # noqa: SLF001


class TestFailureHandling:
    def test_nonzero_exit_emits_runner_error(self, workspace: Path, server, client):
        simple_config(
            workspace,
            command='sh -c "echo boom >&2; exit 7"',
        )
        config = load_runner_config(workspace)["factory"]
        event = handoff(client)
        with make_runner(workspace, config) as runner:
            cycle = runner.poll_once()
            run_dir = Path(cycle["result"]["run_dir"])

        assert cycle["action"] == "error"
        assert cycle["exit_code"] == 7
        assert "boom" in (run_dir / "run.log").read_text(encoding="utf-8")

        failures = client.query(
            kind="handoff", producer="factory", causation_id=event["seq"], order="asc"
        )
        assert len(failures) == 1
        assert failures[0]["payload"]["summary"].startswith("RUNNER_ERROR")
        assert failures[0]["payload"]["action"]["type"] == "runner_error"

    def test_timeout_kills_the_adapter(self, workspace: Path, server, client):
        simple_config(workspace, command="sleep 30", turn_timeout_seconds=0.3)
        config = load_runner_config(workspace)["factory"]
        event = handoff(client)
        with make_runner(workspace, config) as runner:
            cycle = runner.poll_once()
            run_dir = Path(cycle["result"]["run_dir"])

        assert cycle["action"] == "timeout"
        assert cycle["result"]["status"] == "timeout"
        assert "process group killed" in (run_dir / "run.log").read_text(encoding="utf-8")
        errors = client.query(
            kind="handoff", producer="factory", causation_id=event["seq"], order="asc"
        )
        assert errors[0]["payload"]["summary"].startswith("RUNNER_ERROR")
        assert errors[0]["payload"]["action"]["reason"] == "TIMEOUT"

    def test_missing_adapter_is_recorded(self, workspace: Path, server, client):
        simple_config(workspace, command="definitely-not-a-real-binary-xyz")
        config = load_runner_config(workspace)["factory"]
        event = handoff(client)
        with make_runner(workspace, config) as runner:
            cycle = runner.poll_once()
            run_dir = Path(cycle["result"]["run_dir"])

        assert cycle["result"]["status"] == "error"
        assert cycle["exit_code"] not in (0, None)
        assert "not found" in (run_dir / "run.log").read_text(encoding="utf-8")
        errors = client.query(
            kind="handoff", producer="factory", causation_id=event["seq"], order="asc"
        )
        assert errors[0]["payload"]["action"]["type"] == "runner_error"

    def test_adapter_start_failure_is_recorded(
        self, workspace: Path, server, client, monkeypatch: pytest.MonkeyPatch
    ):
        simple_config(workspace, command="echo ok")
        config = load_runner_config(workspace)["factory"]
        event = handoff(client)

        def refuse(self, *_args, **_kwargs):
            raise OSError("exec refused")

        monkeypatch.setattr(Runner, "_start_process", refuse)
        with make_runner(workspace, config) as runner:
            cycle = runner.poll_once()

        assert cycle["result"]["status"] == "error"
        assert cycle["result"]["error"].startswith("adapter_start_failed")
        errors = client.query(
            kind="handoff", producer="factory", causation_id=event["seq"], order="asc"
        )
        assert errors[0]["payload"]["action"]["reason"] == "ADAPTER_START_FAILED"


class TestTurnLimit:
    def test_chain_depth_beyond_limit_is_rejected(self, workspace: Path, server, client):
        simple_config(workspace, command="echo should-not-run", max_turns_per_chain=2)
        config = load_runner_config(workspace)["factory"]
        first = handoff(client, summary="hop-1")
        second = handoff(
            client,
            summary="hop-2",
            causation_id=first["seq"],
            root_causation_id=first["seq"],
        )
        third = handoff(
            client,
            summary="hop-3",
            causation_id=second["seq"],
            root_causation_id=first["seq"],
        )
        # Point the durable cursor at the deep event so the rejected turn is the
        # one under test, rather than the earlier in-limit hops.
        state_root = paths_for(workspace).root
        key = f"quay:{server.store.meta('ledger_uuid')}:{third['seq']}"
        with Runner(workspace, config) as runner:
            runner.bridge.cursors.begin_delivery("factory", third["seq"], key)
            runner.bridge._write_wake(  # noqa: SLF001 - simulate a delivered wake
                third, key, 1
            )
            cycle = runner.process_wake(third["seq"])
            run_dir = Path(cycle["result"]["run_dir"])
            turn_log = (run_dir / "run.log").read_text(encoding="utf-8")

        assert cycle["action"] == "rejected"
        assert cycle["result"]["status"] == "rejected"
        assert cycle["result"]["error"] == "turn_limit_exceeded"
        assert "turn-limit exceeded" in turn_log
        assert "should-not-run" not in turn_log

        rejections = client.query(
            kind="handoff", producer="factory", causation_id=third["seq"], order="asc"
        )
        assert len(rejections) == 1
        assert rejections[0]["payload"]["action"]["reason"] == "TURN_LIMIT_EXCEEDED"
        # An error acknowledgement is redelivered by the bridge, so the cursor
        # stays in flight on the rejected event rather than advancing.
        cursor = CursorStore(state_root).get("factory")
        assert cursor["last_acked_seq"] == 0
        assert cursor["in_flight"]["seq"] == third["seq"]

    def test_chain_within_limit_runs(self, workspace: Path, server, client, ok_script: Path):
        simple_config(workspace, command=adapter_command(ok_script), max_turns_per_chain=3)
        config = load_runner_config(workspace)["factory"]
        handoff(client, summary="hop-1")
        with make_runner(workspace, config) as runner:
            cycle = runner.poll_once()
        assert cycle["action"] == "completed"


class TestRunLoop:
    def test_runner_run_once_returns_after_one_cycle(
        self, workspace: Path, server, client, ok_script: Path
    ):
        simple_config(workspace, command=adapter_command(ok_script))
        config = load_runner_config(workspace)["factory"]
        event = handoff(client)
        with make_runner(workspace, config) as runner:
            result = runner.run(once=True)

        assert result["bridge"] == "delivered"
        assert result["seq"] == event["seq"]
        assert result["action"] == "completed"

    def test_empty_cycle_is_idle(self, workspace: Path, server, client, ok_script: Path):
        simple_config(workspace, command=adapter_command(ok_script))
        config = load_runner_config(workspace)["factory"]
        with make_runner(workspace, config) as runner:
            result = runner.poll_once()

        assert result["action"] == "idle"
        assert result["cursor"]["last_acked_seq"] == 0

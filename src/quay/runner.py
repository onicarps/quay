"""Autonomous runner engine: drive local adapters from the wake bridge.

``WakeBridge`` guarantees lossless, one-outstanding delivery of handoffs to a
named consumer, and an :class:`~quay.runner.Runner` turns each delivered wake
into one bounded adapter turn: read the wake, enforce the causal chain turn
limit, spawn the adapter command under a private run directory, capture the
transcript, and emit a companion ``RUNNER_ACK`` / ``RUNNER_ERROR`` handoff that
lets the bridge advance (or retry) the cursor.

The engine therefore needs no AgentBus supervisor daemon: ``quay run`` observes
``okf/harness/runners.toml``, executes adapter commands, writes execution logs,
and acknowledges each turn on its own.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import subprocess
import time
import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, TextIO

from .bridge import WakeBridge
from .store import StoreError, utc_now
from .workspace import LedgerPaths, paths_for

#: Tracked configuration reviewed through Git, relative to the workspace root.
CONFIG_RELATIVE_PATH = Path("okf") / "harness" / "runners.toml"

#: Bound on the causal walk so a malformed ledger cannot spin the runner.
_MAX_CHAIN_HOPS = 10_000

__all__ = [
    "CONFIG_RELATIVE_PATH",
    "Runner",
    "RunnerConfig",
    "RunnerConfigError",
    "load_runner_config",
    "resolve_config_path",
]


class RunnerConfigError(StoreError):
    """Raised when ``okf/harness/runners.toml`` is missing or malformed."""


@dataclass(frozen=True)
class RunnerConfig:
    """One ``[runner.<name>]`` table resolved to absolute paths."""

    name: str
    enabled: bool
    producer: str
    accept_to: tuple[str, ...]
    intake: str
    allow_broadcast: bool
    max_event_age_hours: float | None
    wake_dir: Path
    runs_dir: Path
    ack_mode: str
    adapter: str
    adapter_command: str
    turn_timeout_seconds: float
    max_turns_per_chain: int

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe description for CLI output and logs."""
        return {
            "name": self.name,
            "enabled": self.enabled,
            "producer": self.producer,
            "accept_to": list(self.accept_to),
            "intake": self.intake,
            "wake_dir": str(self.wake_dir),
            "runs_dir": str(self.runs_dir),
            "ack_mode": self.ack_mode,
            "adapter": self.adapter,
            "adapter_command": self.adapter_command,
            "turn_timeout_seconds": self.turn_timeout_seconds,
            "max_turns_per_chain": self.max_turns_per_chain,
        }


def resolve_config_path(
    workspace: str | os.PathLike[str], config: str | os.PathLike[str] | None = None
) -> Path:
    """Return the runners configuration path, relative paths rooted at ``workspace``."""
    if config is None:
        return Path(workspace) / CONFIG_RELATIVE_PATH
    candidate = Path(config).expanduser()
    if not candidate.is_absolute():
        candidate = Path(workspace) / candidate
    return candidate


def _require_bool(value: Any, *, field: str, name: str) -> bool:
    if not isinstance(value, bool):
        raise RunnerConfigError(
            "invalid_runner_config", f"runner '{name}' field '{field}' must be a boolean"
        )
    return value


def _require_text(value: Any, *, field: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RunnerConfigError(
            "invalid_runner_config", f"runner '{name}' field '{field}' must be a non-empty string"
        )
    return value


def _optional_text(value: Any, *, field: str, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise RunnerConfigError(
            "invalid_runner_config", f"runner '{name}' field '{field}' must be a non-empty string"
        )
    return value


def _require_float(value: Any, *, field: str, name: str, minimum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RunnerConfigError(
            "invalid_runner_config", f"runner '{name}' field '{field}' must be a number"
        )
    number = float(value)
    if number < minimum:
        raise RunnerConfigError(
            "invalid_runner_config", f"runner '{name}' field '{field}' must be >= {minimum}"
        )
    return number


def _require_positive_int(value: Any, *, field: str, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise RunnerConfigError(
            "invalid_runner_config", f"runner '{name}' field '{field}' must be a positive integer"
        )
    return value


def _resolve_under(root: Path, value: str | None, default: Path) -> Path:
    """Resolve a config path, keeping relative values under the workspace root."""
    if value is None:
        return default
    candidate = Path(value).expanduser()
    return candidate if candidate.is_absolute() else root / candidate


def _parse_runner(root: Path, name: str, table: Any) -> RunnerConfig:
    if not isinstance(table, Mapping):
        raise RunnerConfigError(
            "invalid_runner_config", f"runner '{name}' must be a TOML table"
        )
    enabled = _require_bool(table.get("enabled", True), field="enabled", name=name)
    producer = _require_text(table.get("producer", name), field="producer", name=name)
    raw_accept = table.get("accept_to", [name])
    if not isinstance(raw_accept, list) or not raw_accept:
        raise RunnerConfigError(
            "invalid_runner_config",
            f"runner '{name}' field 'accept_to' must be a non-empty list of strings",
        )
    accept_to: list[str] = []
    for entry in raw_accept:
        accept_to.append(_require_text(entry, field="accept_to", name=name))
    intake = _require_text(table.get("intake", "spool"), field="intake", name=name)
    allow_broadcast = _require_bool(
        table.get("allow_broadcast", False), field="allow_broadcast", name=name
    )
    raw_age = table.get("max_event_age_hours")
    max_event_age: float | None = None
    if raw_age is not None:
        max_event_age = _require_float(
            raw_age, field="max_event_age_hours", name=name, minimum=0.0
        )
    wake_default = root / ".quay" / "wake" / name
    wake_dir = _resolve_under(
        root,
        _optional_text(table.get("wake_dir"), field="wake_dir", name=name),
        wake_default,
    )
    # ``runs_dir`` is a runs root; the runner name partitions it so two runners
    # sharing a directory never collide on a sequence number.
    runs_default = root / ".quay" / "runs"
    runs_root = _resolve_under(
        root,
        _optional_text(table.get("runs_dir"), field="runs_dir", name=name),
        runs_default,
    )
    ack_mode = _require_text(table.get("ack_mode", "done-file"), field="ack_mode", name=name)
    if ack_mode != "done-file":
        raise RunnerConfigError(
            "unsupported_ack_mode",
            f"runner '{name}' ack_mode {ack_mode!r} is not supported; use 'done-file'",
        )
    adapter = _require_text(table.get("adapter", "command"), field="adapter", name=name)
    adapter_command = _require_text(
        table.get("adapter_command", ""),
        field="adapter_command",
        name=name,
    )
    turn_timeout = _require_float(
        table.get("turn_timeout_seconds", 1800.0),
        field="turn_timeout_seconds",
        name=name,
        minimum=0.001,
    )
    max_turns = _require_positive_int(
        table.get("max_turns_per_chain", 10), field="max_turns_per_chain", name=name
    )
    return RunnerConfig(
        name=name,
        enabled=enabled,
        producer=producer,
        accept_to=tuple(accept_to),
        intake=intake,
        allow_broadcast=allow_broadcast,
        max_event_age_hours=max_event_age,
        wake_dir=wake_dir,
        runs_dir=runs_root / name,
        ack_mode=ack_mode,
        adapter=adapter,
        adapter_command=adapter_command,
        turn_timeout_seconds=turn_timeout,
        max_turns_per_chain=max_turns,
    )


def load_runner_config(
    workspace: str | os.PathLike[str],
    *,
    config: str | os.PathLike[str] | None = None,
) -> dict[str, RunnerConfig]:
    """Parse ``runners.toml`` into ordered ``RunnerConfig`` records.

    The returned dictionary preserves file order so ``quay run`` can default to
    the first enabled runner. Unknown keys are ignored for forward compatibility.
    """
    root = paths_for(workspace).workspace
    path = resolve_config_path(root, config)
    if not path.exists():
        raise RunnerConfigError(
            "runner_config_missing",
            f"no runners configuration at {path}; expected [runner.<name>] tables",
        )
    if path.is_symlink() or not path.is_file():
        raise RunnerConfigError(
            "invalid_runner_config", f"refusing unsafe runners configuration: {path}"
        )
    try:
        with path.open("rb") as stream:
            document = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise RunnerConfigError(
            "invalid_runner_config", f"cannot parse {path}: {exc}"
        ) from exc
    table = document.get("runner")
    if not isinstance(table, Mapping) or not table:
        raise RunnerConfigError(
            "invalid_runner_config",
            f"{path} must define at least one [runner.<name>] table",
        )
    runners: dict[str, RunnerConfig] = {}
    for name, entry in table.items():
        runners[name] = _parse_runner(root, name, entry)
    return runners


def _read_wake(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise StoreError("unsafe_bridge_path", f"refusing unsafe wake file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StoreError("invalid_wake_file", f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise StoreError("invalid_wake_file", "wake file must contain an object")
    event = value.get("event")
    if not isinstance(event, dict) or "seq" not in event:
        raise StoreError("invalid_wake_file", f"wake file {path} is missing an event body")
    return value


def _payload(event: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = event.get("payload")
    return raw if isinstance(raw, Mapping) else {}


def _chain_depth(event: Mapping[str, Any], store: Any) -> int:
    """Return the 1-based turn number of ``event`` inside its causal chain."""
    depth = 1
    hops = 0
    causation = event.get("causation_id")
    seen: set[int] = {int(event["seq"])}
    while isinstance(causation, int) and not isinstance(causation, bool):
        if causation in seen or hops >= _MAX_CHAIN_HOPS:
            return depth + 1
        seen.add(causation)
        hops += 1
        depth += 1
        parent = store.get(causation)
        if parent is None:
            break
        causation = parent.get("causation_id")
    return depth


def _write_private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise StoreError("unsafe_run_path", f"refusing unsafe run directory: {path}")
    with contextlib.suppress(OSError):
        os.chmod(path, 0o700)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    _write_private_directory(path.parent)
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    temporary = path.with_name(f".{path.name}.tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", closefd=True) as stream:
            stream.write(payload)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(temporary)
        raise
    os.replace(temporary, path)


class Runner:
    """Drive one named runner's adapter turns through the wake bridge."""

    def __init__(
        self,
        workspace: str | os.PathLike[str],
        config: RunnerConfig,
        *,
        bridge: WakeBridge | None = None,
        poll_interval: float = 1.0,
    ) -> None:
        if poll_interval <= 0:
            raise StoreError("invalid_poll_interval", "poll interval must be positive")
        if bridge is not None and bridge.paths.workspace != _workspace_root(workspace):
            raise StoreError(
                "runner_bridge_workspace",
                "the injected bridge does not belong to this workspace",
            )
        self.config = config
        self.poll_interval = float(poll_interval)
        self.paths: LedgerPaths = paths_for(workspace, create=False)
        if bridge is None:
            bridge = WakeBridge(
                self.paths.workspace,
                runner=config.name,
                wake_dir=config.wake_dir,
                ack_timeout_seconds=config.turn_timeout_seconds,
            )
        self.bridge = bridge
        self.runs_root = config.runs_dir
        _write_private_directory(self.runs_root)
        self.runs_root = self.runs_root.resolve()

    def close(self) -> None:
        self.bridge.close()

    def __enter__(self) -> "Runner":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------------ turns

    def _run_directory(self, seq: int) -> Path:
        target = self.runs_root / str(seq)
        _write_private_directory(target)
        resolved = target.resolve()
        if resolved.parent != self.runs_root:
            raise StoreError("unsafe_run_path", f"refusing out-of-root run directory: {resolved}")
        return resolved

    def _adapter_env(
        self, event: Mapping[str, Any], seq: int, run_dir: Path
    ) -> dict[str, str]:
        payload = _payload(event)
        env = dict(os.environ)
        env.update(
            {
                "QUAY_WORKSPACE": str(self.paths.workspace),
                "QUAY_RUNNER": self.config.name,
                "QUAY_EVENT_SEQ": str(seq),
                "QUAY_PRODUCER": self.config.producer,
                "QUAY_TOPIC": str(event.get("topic", "")),
                "QUAY_SENDER": str(payload.get("from") or "swarm"),
                "QUAY_RUN_DIR": str(run_dir),
            }
        )
        return env

    def _start_process(
        self, command: str, log: TextIO, env: Mapping[str, str]
    ) -> "subprocess.Popen[str]":
        """Launch one adapter process group with the transcript attached to stdout."""
        return subprocess.Popen(  # noqa: S602 - operator-configured adapter command
            f"exec {command}",
            shell=True,
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=subprocess.STDOUT,
            cwd=str(self.paths.workspace),
            env=dict(env),
            text=True,
            start_new_session=True,
        )

    def _spawn_adapter(
        self,
        event: Mapping[str, Any],
        seq: int,
        run_dir: Path,
        log: Path,
        wake: Mapping[str, Any],
    ) -> tuple[int, bool]:
        """Run the adapter under one bounded turn; return ``(exit_code, timed_out)``."""
        env = self._adapter_env(event, seq, run_dir)
        with log.open("w", encoding="utf-8") as stream:
            stream.write(
                f"[quay] runner={self.config.name} seq={seq} "
                f"adapter={self.config.adapter!r} command={self.config.adapter_command!r} "
                f"started={utc_now()}\n"
            )
            stream.flush()
            # The adapter command is operator-authored and runs through the shell
            # so quoting, pipes, and redirection behave as written. ``exec`` keeps
            # the group leader as the adapter itself, so a timeout can kill it whole.
            process = self._start_process(self.config.adapter_command, stream, env)
            timed_out = False
            try:
                process.communicate(
                    input=json.dumps(dict(wake), sort_keys=True) + "\n",
                    timeout=self.config.turn_timeout_seconds,
                )
            except subprocess.TimeoutExpired:
                timed_out = True
                # Reap the killed group before closing the transcript, because
                # ``communicate`` cannot drain a descriptor it no longer owns.
                _terminate(process)
                stream.flush()
                stream.close()
            exit_code = int(process.returncode if process.returncode is not None else -1)
            if not stream.closed:
                stream.write(
                    f"[quay] exit_code={exit_code} timed_out={timed_out} finished={utc_now()}\n"
                )
                stream.flush()
        if timed_out:
            self._append_log(
                log,
                f"[quay] timeout: adapter exceeded "
                f"{self.config.turn_timeout_seconds:g}s; process group killed\n",
            )
        return exit_code, timed_out

    def _emit_companion(
        self,
        seq: int,
        root: int,
        sender: str,
        *,
        status: str,
        summary: str,
        reason: str | None = None,
        exit_code: int | None = None,
    ) -> dict[str, Any]:
        action: dict[str, Any] = {"type": "runner_ack" if status == "completed" else "runner_error"}
        if reason is not None:
            action["reason"] = reason
        if exit_code is not None:
            action["exit_code"] = exit_code
        return self.bridge.client.emit(
            topic=f"swarm/{sender}/handoff",
            producer=self.config.producer,
            kind="handoff",
            causation_id=seq,
            root_causation_id=root,
            correlation_id=f"runner-{self.config.name}-{seq}",
            payload={
                "from": self.config.producer,
                "to": sender,
                "summary": summary,
                "action": action,
            },
        )

    def _write_ack(self, seq: int, summary: str) -> None:
        target = self.bridge._ack_path(seq)  # noqa: SLF001 - shared acknowledgement contract
        _atomic_json(target, {"seq": seq, "summary": summary})

    def _finish(
        self,
        seq: int,
        run_dir: Path,
        *,
        status: str,
        exit_code: int | None,
        duration: float,
        error: str | None = None,
    ) -> dict[str, Any]:
        finished_ms = time.time()
        result: dict[str, Any] = {
            "seq": seq,
            "runner": self.config.name,
            "status": status,
            "exit_code": exit_code,
            "duration_seconds": round(duration, 6),
            "run_dir": str(run_dir),
            "started_at": _iso_from_epoch(finished_ms - duration),
            "finished_at": utc_now(),
        }
        if error is not None:
            result["error"] = error
        _atomic_json(run_dir / "result.json", result)
        return result

    def _fail_turn(
        self,
        seq: int,
        run_dir: Path,
        *,
        started: float,
        root: int,
        sender: str,
        status: str,
        summary: str,
        error: str,
        reason: str,
        exit_code: int | None = None,
    ) -> dict[str, Any]:
        """Record a non-adapter failure, notify the sender, and release the wake."""
        result = self._finish(
            seq,
            run_dir,
            status=status,
            exit_code=exit_code,
            duration=time.monotonic() - started,
            error=error,
        )
        self._emit_companion(
            seq,
            root,
            sender,
            status="error",
            summary=summary,
            reason=reason,
            exit_code=exit_code,
        )
        self._write_ack(seq, summary)
        self.bridge.poll_once()
        return {
            "seq": seq,
            "action": status,
            "exit_code": exit_code,
            "cursor": self._cursor_snapshot(),
            "result": result,
        }

    def process_wake(self, seq: int) -> dict[str, Any]:
        """Execute one adapter turn for the wake currently spooled at ``seq``."""
        started = time.monotonic()
        wake = _read_wake(self.bridge._wake_path(seq))  # noqa: SLF001 - shared wake contract
        event = wake["event"]
        root = int(event.get("root_causation_id") or seq)
        sender = str(_payload(event).get("from") or "swarm")
        run_dir = self._run_directory(seq)
        self._write_run_metadata(run_dir, wake)
        log = run_dir / "run.log"
        log.touch(mode=0o600, exist_ok=True)

        depth = _chain_depth(event, self.bridge.store)
        if depth > self.config.max_turns_per_chain:
            message = (
                f"{self.config.name} refused Quay event #{seq}: causal chain depth "
                f"{depth} exceeds max_turns_per_chain={self.config.max_turns_per_chain}"
            )
            self._append_log(log, f"[quay] turn-limit exceeded: {message}\n")
            return self._fail_turn(
                seq,
                run_dir,
                started=started,
                root=root,
                sender=sender,
                status="rejected",
                summary=f"RUNNER_ERROR: {message}",
                error="turn_limit_exceeded",
                reason="TURN_LIMIT_EXCEEDED",
            )

        try:
            exit_code, timed_out = self._spawn_adapter(event, seq, run_dir, log, wake)
        except OSError as exc:
            self._append_log(log, f"[quay] adapter failed to start: {exc}\n")
            return self._fail_turn(
                seq,
                run_dir,
                started=started,
                root=root,
                sender=sender,
                status="error",
                summary=f"RUNNER_ERROR: {self.config.name} adapter start failed event #{seq}",
                error=f"adapter_start_failed: {exc}",
                reason="ADAPTER_START_FAILED",
            )

        duration = time.monotonic() - started
        if timed_out:
            status = "timeout"
            summary = (
                f"RUNNER_ERROR: {self.config.name} timeout after "
                f"{self.config.turn_timeout_seconds:g}s event #{seq}"
            )
            reason = "TIMEOUT"
        elif exit_code == 0:
            status = "completed"
            summary = f"RUNNER_ACK: {self.config.name} completed event #{seq}"
            reason = None
        else:
            status = "error"
            summary = f"RUNNER_ERROR: {self.config.name} exit={exit_code} event #{seq}"
            reason = "ADAPTER_FAILED"
        result = self._finish(
            seq, run_dir, status=status, exit_code=exit_code, duration=duration
        )
        self._emit_companion(
            seq,
            root,
            sender,
            status="completed" if status == "completed" else "error",
            summary=summary,
            reason=reason,
            exit_code=exit_code,
        )
        self._write_ack(seq, summary)
        self.bridge.poll_once()
        return {
            "seq": seq,
            "action": status,
            "exit_code": exit_code,
            "cursor": self._cursor_snapshot(),
            "result": result,
        }

    def _write_run_metadata(self, run_dir: Path, wake: Mapping[str, Any]) -> None:
        _atomic_json(
            run_dir / "wake.json",
            {
                "runner": self.config.name,
                "seq": wake.get("event_id"),
                "attempt": wake.get("attempt"),
                "idempotency_key": wake.get("idempotency_key"),
                "delivered_at": wake.get("delivered_at"),
            },
        )

    def _append_log(self, log: Path, text: str) -> None:
        try:
            with log.open("a", encoding="utf-8") as stream:
                stream.write(text)
        except OSError:
            pass

    def _cursor_snapshot(self) -> dict[str, Any]:
        cursor = self.bridge.cursors.get(self.config.name)
        return {
            "last_acked_seq": int(cursor["last_acked_seq"]),
            "in_flight": cursor["in_flight"],
        }

    # ------------------------------------------------------------------ cycles

    def poll_once(self) -> dict[str, Any]:
        """Advance the wake bridge once and execute a turn when a wake is spooled."""
        outcome = self.bridge.poll_once()
        action = str(outcome.get("action"))
        seq = outcome.get("seq")
        if action in ("delivered", "restored", "redelivered") and isinstance(seq, int):
            turn = self.process_wake(seq)
            return {"bridge": action, **turn}
        return {"bridge": action, "seq": seq, "action": action, "cursor": self._cursor_snapshot()}

    def run(self, *, once: bool = False) -> dict[str, Any]:
        """Run one cycle (``once``) or poll until interrupted."""
        result = self.poll_once()
        if once:
            return result
        try:
            while True:
                time.sleep(self.poll_interval)
                result = self.poll_once()
        except KeyboardInterrupt:  # pragma: no cover - interactive stop
            return result


def _workspace_root(workspace: str | os.PathLike[str]) -> Path:
    return paths_for(workspace).workspace


def _iso_from_epoch(seconds: float) -> str:
    return (
        datetime.fromtimestamp(seconds, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _terminate(process: "subprocess.Popen[str]") -> None:
    """Kill the timed-out adapter process group, then reap it."""
    with contextlib.suppress(OSError):
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    try:
        process.wait(timeout=5.0)
    except subprocess.TimeoutExpired:  # pragma: no cover - defensive
        with contextlib.suppress(OSError):
            process.kill()
        process.wait(timeout=5.0)

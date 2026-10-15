"""Focused client terminal-state and recovery-error regressions."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from quay.client import ClientError, SubscriptionStream


def test_terminal_poll_respects_timeout_after_end_marker_is_consumed(tmp_path: Path):
    live = SubscriptionStream(tmp_path / "unused.sock")
    live._finish()  # noqa: SLF001 - deterministic terminal-state seam

    assert live.is_terminal is True
    assert live.poll(0.0) is None  # consume the queued end marker
    started = time.monotonic()
    assert live.poll(0.05) is None
    assert time.monotonic() - started >= 0.04


def test_terminal_indefinite_poll_returns_after_end_marker_is_consumed(tmp_path: Path):
    live = SubscriptionStream(tmp_path / "unused.sock")
    live._finish()  # noqa: SLF001 - deterministic terminal-state seam

    assert live.poll(0.0) is None
    started = time.monotonic()
    assert live.poll(None) is None
    assert time.monotonic() - started < 0.1


def test_wait_for_raises_the_underlying_recovery_error_immediately(tmp_path: Path):
    live = SubscriptionStream(tmp_path / "unused.sock")
    live._finish(  # noqa: SLF001 - deterministic terminal-state seam
        {
            "type": "recovery_error",
            "error": {
                "code": "recovery_stalled",
                "message": "backfill made no progress",
                "detail": {"cursor": 7, "until_seq": 9},
            },
        }
    )

    started = time.monotonic()
    with pytest.raises(ClientError) as excinfo:
        live.wait_for(timeout=1.0)
    assert time.monotonic() - started < 0.2
    assert excinfo.value.code == "recovery_stalled"
    assert excinfo.value.detail == {"cursor": 7, "until_seq": 9}

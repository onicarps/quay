"""Quay: a local-first streaming event ledger and terminal cockpit.

Quay records typed events durably in a single SQLite file, streams them to
live subscribers over a framed Unix-socket protocol, and renders them in an
incremental terminal UI.

The package has no runtime third-party dependencies.
"""

from __future__ import annotations

__all__ = ["__version__", "PROTOCOL_VERSION", "LEDGER_FORMAT_VERSION"]

__version__ = "0.3.0"

#: Wire protocol version. Bumped only on an incompatible frame change.
PROTOCOL_VERSION = "1"

#: On-disk ledger format version stored in the ``meta`` table.
LEDGER_FORMAT_VERSION = 1

# Quay architecture

Quay is a local-first event ledger for coding agents: an append-only SQLite
ledger with a typed envelope, a daemon that speaks a framed socket protocol,
and a CLI plus TUI on top. Everything is Python standard library only.

The design rule that shapes every decision below: **the ledger is the source of
truth, and every derived view is rebuildable from it.** No hidden state lives
only in the daemon, and no operation is correct only because a process happened
to be running.

```
┌──────────────────────────────────────────────────────────────────────────┐
│  clients                                                                 │
│  quay CLI      quay TUI      your agent       any AF_UNIX socket client   │
└───────┬──────────────┬──────────────┬────────────────────────────────────┘
        │              │              │   4-byte length + JSON frame
        └──────────────┴──────────────┘
                       │
              ┌────────▼─────────┐
              │  quayd (daemon)  │  accept loop
              │                  │  one reader + one writer thread per client
              │  server.py       │  subscriptions, bounded outbox, heartbeat
              └────────┬─────────┘
                       │ in-process call
              ┌────────▼─────────┐
              │  LedgerStore     │  store.py
              │                  │  append / query / verify / prune
              └────────┬─────────┘
                       │ one SQLite connection, one lock
              ┌────────▼─────────┐
              │ .quay/ledger.db  │  WAL, synchronous=FULL by default
              │  events          │  append-only, SHA-256 chained
              │  counters        │  projections, rebuilt from events
              │  signals works   │
              │  meta            │
              └──────────────────┘
```

## Modules

| Module | Responsibility |
| --- | --- |
| `canonical.py` | Deterministic JSON encoding and the SHA-256 hash chain |
| `schemas.py` | Envelope and payload contracts, state machines, `prepare_event` |
| `store.py` | The ledger: append, query, projections, leases, verify, prune |
| `scaffold.py` | Conflict-safe packaged OKF templates and Git ignore discipline |
| `distill.py` | Deterministic acknowledged-handoff materialization and high-water state |
| `protocol.py` | Frame encoding/decoding and request validation |
| `server.py` | The daemon: connections, subscriptions, backpressure, ops |
| `client.py` | Blocking client and `SubscriptionStream` |
| `daemon.py` | Lifecycle: ensure, spawn, stop, reclaim, health |
| `workspace.py` | Paths, owned-file safety, process identity, registry |
| `cli.py` | 17 subcommands, JSON by default when piped |
| `tui.py` | ANSI dashboard with incremental screen diffs |
| `bench.py` | Benchmarks with reimplemented incumbent baselines |
| `migrate.py` | Changi ledger import |

## Data model

Every event is one row and one JSON object with a fixed envelope:

| Field | Meaning |
| --- | --- |
| `seq` | Monotonic sequence number, assigned by the ledger |
| `kind` | `signal`, `work`, `note`, `metric`, or `generic` |
| `topic` | Routing label, glob-matchable (`build/queue`, `agent/*/log`) |
| `producer` | Who wrote it |
| `ts` | ISO-8601 UTC timestamp with milliseconds |
| `causation_id` | The `seq` this event is a consequence of |
| `correlation_id` | Groups a causal chain without implying order |
| `payload` | Kind-specific body, validated at the boundary |
| `prev_hash`, `hash` | SHA-256 chain over the canonical body |

`seq` is never reused and never renumbered. `hash` is
`sha256(prev_hash || canonical_body)`, where `canonical_body` is a single-pass
encoding of the envelope plus the already-encoded payload text, so a payload is
serialized exactly once per write.

### Projections

`counters`, `signals`, and `works` are projections: they exist for O(1) status
and for the lease queries, and they are derived state. `rebuild_projections()`
recomputes all of them from `events` inside one transaction, and `prune()`
rebuilds them from the retained suffix. If a projection is ever wrong, the
ledger still tells the truth and the projection can be reconstructed.

`status()` reads the projections plus a few aggregates, so its cost does not
grow with ledger size. The benchmark suite measures that directly against a
reimplemented full-table scan.

### State machines

Plane records follow explicit state machines, enforced on every write:

* **signal**: `pending → claimed → cleared`, with `expired` reachable from
  `pending` and `claimed`. A chain starts at `pending`.
* **work**: `claimed → in_flight → done`, with `failed`, `abandoned`, and
  `expired` as terminals. A chain starts at `claimed`.

The rules exist because a ledger full of impossible transitions is not
auditable. `verify` checks the hash chain; the write path checks the state
machine, so a bad transition is refused rather than stored and regretted.

## Concurrency model

The daemon has no global request lock. Each connection gets a reader thread and
a writer thread, and all ledger writes serialize inside the store.

* **Accept loop** (`_accept_loop`): accepts and hands off. Never blocks on I/O.
* **Reader thread** per connection: reads frames, dispatches ops, enqueues
  responses.
* **Writer thread** per connection: drains a bounded outbox.
* **Maintenance loop**: heartbeats, lease reaping, idle shutdown checks.
* **Watchdog**: whoever requests a stop, exactly one thread tears the daemon
  down.

Responses are never dropped: a client that asked gets an answer. Server pushes
are droppable. When a subscriber's outbox is full the frame is discarded, a
counter is incremented, and the subscriber receives one `lagged` frame with the
number dropped and the last sequence number seen. A client that stops reading
therefore cannot grow daemon memory without bound, and it is told exactly how
much it missed instead of silently receiving a gap.

### Teardown ordering

Teardown is ordered so that observable signals are honest:

1. close the listener and stop accepting;
2. broadcast `shutdown`, drain writers;
3. join the accept and maintenance threads;
4. close the store;
5. remove the pid file, then unregister the workspace, **then** remove the
   socket.

The socket is removed last because it is how every other process decides the
daemon is gone. By the time the socket disappears, the registry and pid file
are already clean, so "socket gone" is a reliable signal. Start-up is the
mirror image: identity is published *before* the socket is bound, so a client
that can connect always finds the daemon in the registry.

## Durability and integrity

* WAL journal mode; `synchronous=FULL` by default (`NORMAL` is available when a
  caller prefers throughput to power-loss durability).
* Writes are batched in one transaction; a batch either commits whole or not at
  all.
* `verify()` recomputes the chain over retained events and reports the first
  divergence, plus any missing sequence numbers. It records the verified
  sequence in `meta`, so a later `verify` knows where it started.
* `prune()` writes a checkpoint and deletes only the prefix before it, keeping
  the suffix and the chain intact. It supports `--dry-run` and prints what it
  would remove. It refuses to run without an explicit scope.

## Files on disk

```
<workspace>/.quay/
  ledger.db        the append-only ledger (WAL + shm files alongside)
  quayd.sock       control socket, mode 0600
  quayd.pid        instance id, pid, and process start identity
  owner            marks the state directory as Quay's
```

`workspace.py` refuses to touch files it does not own: every unlink goes
through `unlink_owned`, which verifies ownership first. Nothing outside
`<workspace>/.quay/` is ever written, and Quay never edits `.gitignore` (it
reports a Git advisory instead).

The registry lives under `$XDG_CONFIG_HOME/quay/workspaces.json` and is written
atomically under an `flock`, so `quay daemons` can see every workspace on the
machine without scanning the filesystem.

## Failure handling

| Situation | Behaviour |
| --- | --- |
| No daemon running | `ensure_daemon` spawns one, serialized by an `flock` so two clients cannot race |
| Daemon record exists but process is dead | stale state is reclaimed |
| Process is alive but not answering | Quay refuses to signal it and reports the conflict |
| Client dies mid-request | reader thread sees EOF, connection is closed, ledger is untouched |
| Subscriber falls behind | `lagged` frame with the exact drop count |
| Malformed frame | structured error reply, then a graceful close so the reply flushes |
| Ledger tampered with | `verify` reports the first divergence and fails the command |

## Benchmarks

`quay bench --scale quick` (or `full`) measures four things and prints the
profile it used, so numbers are never quoted without their workload. Two of the
four compare against a reimplemented baseline rather than an assertion: the
single-append write pattern Changi uses, and the full-table scan status
projection replaces.

Measured on the reference machine (Linux 6.6, WSL2, Python 3.12.3), quick
profile: 2,000 appends, 10,000 rows for status/query, 300 events fanned out to
4 subscribers.

| Benchmark | Result |
| --- | --- |
| Append, batched (500/txn) | 3,008 events/s |
| Append, one txn per event (baseline) | 893 events/s |
| `status()` at 10,000 rows | 0.24 ms |
| Full-table scan projection at 10,000 rows (baseline) | 49.5 ms |
| Query, topic glob / producer | 2.7 ms / 2.7 ms |
| Query, payload text | 11.2 ms |
| Push fan-out, p50 / p99 | 0.0 ms / 8.4 ms |
| Fan-out end-to-end (includes the durable write), p50 / p99 | 10.7 ms / 22.2 ms |
| Fan-out deliveries | 1,200 of 1,200, 0 dropped |

Two honest notes. First, the fan-out benchmark runs four collector threads that
poll concurrently, so its emit round trip (10.5 ms p50) is higher than an idle
daemon's (~2.9 ms p50) — the contention is real and is part of what the number
describes. Second, the `status()` growth factor is reported but should be read
as "flat within noise": the half-size run measured slower than the full-size
run, which is measurement noise, not a trend. The meaningful comparison is
against the scan baseline, which grows with the table.

## Testing

`pytest` runs 381 tests with no skips and no mocks of storage or transport:
real SQLite files, real AF_UNIX sockets, real daemon processes, real
multi-process concurrency. The suite includes tamper detection, crash and
recovery, backpressure, multi-process chain integrity, live TUI loops, and
end-to-end CLI runs. See `README.md` for how to run it.

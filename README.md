# Quay

A local-first event ledger for coding agents. Append-only SQLite, a typed
envelope with real state machines, a daemon that speaks a framed socket
protocol, a CLI, and a live terminal cockpit. Python standard library only.

Quay is a competitor to [Changi](../changi/): it keeps the "one small ledger in
your workspace" idea and fixes the parts that make Changi hard to run for long
periods — quadratic status reads, no crash recovery story, no backpressure, no
way to tell whether your history is intact, and no upgrade path.

- **Integrity you can check.** Every event is SHA-256 chained. `quay verify`
  recomputes the whole chain and names the first divergence.
- **Status that does not get slower.** Plane counts are maintained
  transactionally, so `quay status` is O(1) at 10 rows or 10 million.
- **A daemon that survives clients.** Per-connection reader/writer threads, a
  bounded outbox, and a `lagged` frame that tells a slow subscriber exactly how
  much it missed.
- **Real state machines.** Signals and work records follow explicit transitions,
  enforced on write, so the ledger cannot fill with impossible history.
- **An upgrade path.** `quay import` reads an existing Changi ledger, maps it
  onto Quay's envelope, and reports every adjustment it made.

Status: complete and tested. 381 tests, no skips, no mocks of storage or
transport. See `COMPETITIVE_ANALYSIS.md` for the requirements this implements
and `docs/ARCHITECTURE.md` for the design.

## Install

Quay runs on Linux and macOS. Requires Python 3.11 or newer (standard library only; zero external Python dependencies).

### Via NPM (Global Terminal Binary)

If you have Node.js 18+ and Python 3.11+:

```bash
npm install --global @agentbus/quay
quay --version
quayd --version
```

### Via Pip (PyPI Package)

```bash
pip install okf-quay
quay --version
quayd --version
```

### From Local Checkout

```bash
cd projects/quay
python3 -m pip install .        # or: pip install --user .
quay --version
quayd --version
```

To run from a checkout without installing:

```bash
export PYTHONPATH="$PWD/src"
python3 -m quay.cli --help
```

## Quickstart

```bash
cd ~/my-project
quay init                       # create .quay/ and check the Git advisory

quay emit build/log '{"text": "compiled ok", "tags": ["build"]}' --kind note
quay emit build/alert '{"summary": "disk almost full"}' --kind signal
quay log --limit 5
quay status
```

Payloads are validated per kind. A `note` carries `text` (plus optional
`tags`), a `signal` carries `summary` and a state, a `metric` carries `name` and
a numeric `value`, and `generic` accepts any JSON object. `quay kinds` prints
the exact contract for every kind.

Start work, keep it alive, finish it with evidence:

```bash
quay work claim job-42 --executor builder --lease-seconds 120
quay work heartbeat job-42 --executor builder --lease-seconds 120
quay work complete job-42 --executor builder --evidence '{"artifact": "dist/app.tar.gz"}'
quay work list --state done
```

Watch the ledger live in the cockpit, or stream JSON lines:

```bash
quay watch
quay watch --plain --kind signal
```

Verify and search:

```bash
quay verify
quay query --topic 'build/*' --kind signal --limit 10
quay query --text "almost full" --count-only
quay query --kind work --state done --export csv
```

Everything prints JSON automatically when piped, so no flag is needed in a
pipeline:

```bash
quay status | jq '.planes'
```

## CLI reference

Common flags on every command: `--workspace PATH` (default: the current
directory), `--json`, `--no-json`, `--timeout SECONDS`. Output is JSON when
stdout is not a terminal; `--no-json` forces the human rendering.

| Command | Purpose |
| --- | --- |
| `quay init [--git-ignore --yes]` | Create `.quay/`, report a Git advisory, optionally add the ignore line |
| `quay start [--idle-seconds N]` | Start this workspace's daemon (`0` disables idle shutdown) |
| `quay stop [--all --yes]` | Stop this daemon, or every registered daemon |
| `quay restart` | Stop and start |
| `quay status` | Daemon health plus ledger and plane counts |
| `quay daemons` | Every registered workspace daemon on this machine |
| `quay doctor` | Install, socket, registry, and ledger integrity checks |
| `quay kinds` | The kind contracts: states and required fields |
| `quay verify [--no-record]` | Recompute the hash chain |
| `quay emit TOPIC [PAYLOAD] [--kind K] [--producer P] [--causation SEQ] [--correlation ID]` | Append one event |
| `quay work claim\|heartbeat\|complete\|abandon KEY --executor E […]` | Drive the work plane |
| `quay work list [--state S] [--executor E]` / `quay work reap` | Inspect work and expire lapsed leases |
| `quay query [filters]` | Search with kind, topic glob, producer, state, correlation, causation, seq range, time range, and text filters; `--count-only`, `--export jsonl\|csv`, `--limit`, `--offset`, `--order` |
| `quay log [--limit N] [--follow] [--for S] [--max-events N]` | Recent events, optionally streaming |
| `quay watch [--kind K] [--plain] [--history N] [--for S] [--max-events N]` | Live TUI cockpit, or JSON lines with `--plain` |
| `quay prune (--keep-last N \| --before TS) [--dry-run] [--yes]` | Delete an old prefix after a checkpoint |
| `quay bench [--scale quick\|full] [--out FILE]` | Run the benchmark suite |
| `quay import SOURCE [--dry-run] [--max-warnings N]` | Import a Changi ledger (see `docs/MIGRATION.md`) |

Exit codes: `0` success, `1` a check failed (for example `verify`), `2` usage or
validation error, `3` the daemon is unreachable.

`QUAY_PRODUCER` sets the default producer for `quay emit`.

## Terminal cockpit

`quay watch` renders a full-screen dashboard: daemon health, ledger counters,
plane counts, a live event stream, and a filter bar. Keys:

| Key | Action |
| --- | --- |
| `q` | quit |
| `p` | pause or resume the stream |
| `v` | run verification and show the result |
| `r` | reap expired leases |
| `k` | cycle the kind filter |
| `/` | edit the topic filter |
| `?` | help overlay |

The dashboard redraws with minimal ANSI diffs rather than repainting the whole
screen, so it stays usable over a slow terminal. `quay watch --plain` prints
JSON lines instead, which is what you want in a pipe or a log.

## Python API

```python
from quay.client import QuayClient, SubscriptionStream
from quay.daemon import ensure_daemon

paths, _health = ensure_daemon(".")          # start a daemon if needed

with QuayClient(paths.socket) as client:
    event = client.request("emit", {
        "topic": "agent/build",
        "producer": "my-agent",
        "payload": {"step": "compile"},
        "kind": "note",
    })["event"]
    print(event["seq"], event["hash"][:12])

    client.request("work_claim", {"key": "job-7", "executor": "my-agent", "lease_seconds": 60})
    client.request("work_complete", {"key": "job-7", "executor": "my-agent",
                                     "evidence": {"exit_status": 0}})
```

Live subscription with backfill:

```python
stream = SubscriptionStream(paths.socket, filters={"topic": "agent/*"}, replay=50)
stream.open()
for event in stream.events(timeout=5.0):
    print(event["seq"], event["topic"])
```

Using the store directly, with no daemon:

```python
from quay.store import LedgerStore

with LedgerStore(".quay/ledger.db") as store:
    store.append(topic="t", producer="p", payload={"hello": "world"})
    print(store.status()["planes"])
    print(store.verify()["ok"])
```

## Changi migration

```bash
quay import /path/to/.changi --dry-run     # report only
quay import /path/to/.changi               # import
quay verify
```

Changi's flat payload-with-extensions table is mapped onto Quay's typed
envelope: signals and work receipts become plane records, everything else stays
generic with its payload intact, and causal links are remapped to new sequence
numbers. Where Changi stored a terminal record with no history, Quay writes a
clearly marked reconstructed root rather than inventing a plausible past. Every
adjustment lands in the report. Full details: `docs/MIGRATION.md`.

## Benchmarks

`quay bench --scale quick` prints the workload profile with the numbers, so
results are never quoted without their context. Two of the four benchmarks
compare against a reimplemented baseline: the one-transaction-per-event write
pattern Changi uses, and the full-table scan that a naive status implementation
performs.

Reference machine: Linux 6.6 (WSL2), Python 3.12.3. Quick profile: 2,000
appends, 10,000 rows, 300 events fanned out to 4 subscribers.

| Measurement | Quay | Baseline |
| --- | --- | --- |
| Append throughput (batched, 500/txn) | 3,008 events/s | 893 events/s single-append |
| `status()` at 10,000 rows | 0.24 ms | 49.5 ms full-table scan |
| Query, topic glob / producer filter | 2.7 ms / 2.7 ms | – |
| Query, payload text | 11.2 ms | – |
| Push fan-out p50 / p99 | 0.0 ms / 8.4 ms | – |
| Fan-out end-to-end p50 / p99 | 10.7 ms / 22.2 ms | – |
| Fan-out deliveries | 1,200 of 1,200, 0 dropped | – |

Reading these honestly:

- `status()` cost does not grow with ledger size; the scan baseline does. The
  reported growth factor between the half-size and full-size runs is noise
  (the smaller run measured slower), not a trend.
- The fan-out benchmark runs four concurrent collector threads, so its emit
  round trip (10.5 ms p50) is higher than an idle daemon's (~2.9 ms p50). That
  contention is part of what the number measures, and it is why the benchmark
  reports push latency and end-to-end latency separately.
- The durable write dominates end-to-end latency. Batched writes amortize it.

Run the full profile with `quay bench --scale full --out bench.json`.

## Demo

```bash
./run_demo.sh          # unattended end-to-end walkthrough, exits 0
```

The demo creates a scratch workspace in a temporary directory, starts a daemon,
writes notes, signals, and work records, exports JSON and CSV, streams live
events, runs verification, tampers with the ledger to prove verification fails,
imports a synthetic Changi ledger, runs the benchmark suite, and cleans up. It
prints each step and never touches your real workspace or registry.

## Testing

```bash
python3 -m pip install pytest
python3 -m pytest -q              # 381 tests, ~100s
python3 -m pytest -q tests/test_store.py tests/test_server.py
```

The suite uses real SQLite files, real AF_UNIX sockets, real daemon processes,
and real multi-process writes. Nothing mocks storage or transport. Coverage
includes: canonical encoding and hash chaining, schema validation and state
machines, tamper detection, crash and recovery, work leases and expiry,
backpressure and `lagged` accounting, daemon lifecycle and stale-state reclaim,
live TUI rendering and key handling, every CLI command, the benchmark suite, and
Changi import fidelity.

## Documentation

| Document | Contents |
| --- | --- |
| `COMPETITIVE_ANALYSIS.md` | The target, the weaknesses addressed, requirements F1–F20 / N1–N7, and the milestone gates |
| `docs/ARCHITECTURE.md` | Modules, data model, concurrency, durability, teardown ordering, benchmark method |
| `docs/PROTOCOL.md` | Framing, frame shapes, every operation, filters, backpressure, error codes |
| `docs/MIGRATION.md` | Changi import mapping, reconstructed roots, report format, limitations |

## Design limits

These are deliberate, and each one is documented rather than hidden:

- **One writer per workspace.** SQLite gives one write transaction at a time.
  Batched writes make that a throughput question, not a correctness one.
- **No authentication.** The socket is `0600` and local-only. Multi-user
  isolation is the filesystem's job, not Quay's.
- **No replication or federation.** This is a local ledger, not a cluster.
- **Payload text search scans.** Topic, producer, and sequence filters use
  indexes; `--text` reads payloads by design.
- **Imports are not deduplicated.** Import into a fresh workspace, or expect
  skip warnings for plane records that already reached a terminal state.

## Layout

```
projects/quay/
  pyproject.toml            packaging (hatchling, no runtime deps)
  README.md
  COMPETITIVE_ANALYSIS.md
  docs/                     architecture, protocol, migration
  run_demo.sh               unattended end-to-end demo
  src/quay/                 canonical, schemas, store, protocol, server,
                            client, daemon, workspace, cli, tui, bench, migrate
  tests/                    pytest suite (381 tests)
```

## License

MIT. See `LICENSE`.

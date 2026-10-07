# Migrating from Changi

Quay can import an existing Changi ledger, so switching does not mean abandoning
history.

```bash
# See what would happen, without writing anything
quay import /path/to/workspace/.changi/events.db --dry-run

# Do it
quay import /path/to/workspace/.changi
quay verify
```

`import` accepts either the `events.db` file or the `.changi` directory that
contains it. The import runs inside the daemon, so there is exactly one writer
for the workspace.

## What the import does

Changi stores one flat table where plane semantics are smuggled inside
`payload.changi`. Quay has a typed envelope, so the importer maps rather than
copies:

| Changi | Quay |
| --- | --- |
| `payload.changi.kind = "signal"` | `kind: "signal"`, `state` and `summary` lifted out of the payload |
| valid `payload.work_receipt` | `kind: "work"` with `key`, `executor`, `state`, and `evidence` |
| anything else | `kind: "generic"`, payload preserved verbatim |
| `event_id` | a new `seq`; the old id is kept as `payload.changi_event_id` on generic events |
| `causation_id` | remapped to the new `seq` when the link is still valid |
| `timestamp` | carried over as `ts` |
| `producer_id` | normalized to Quay's producer syntax when it is not already valid |

State mapping for work receipts:

| Changi `exit_status` | Quay `state` |
| --- | --- |
| `0` | `done` |
| any other integer | `failed` |
| absent | `claimed` |

## Reconstructed roots

This is the one place the importer writes something that was not in the source,
and it says so every time.

Changi records a work receipt or a signal state change without requiring the
earlier steps to exist. Quay's state machines require a chain to start at a
root (`pending` for signals, `claimed` for work). When the importer meets a
standalone mid-chain or terminal record, it writes a root first, then links the
real record to it. The root is marked in plain sight:

* its `summary` or `title` reads
  `changi import: reconstructed <state> root for event <n>`;
* it shares `correlation_id = changi-import-<n>` with the record it anchors, so
  `quay query --correlation-id changi-import-42` shows the pair;
* the report counts it under `synthesized` and warns per event.

Nothing else is invented. If a record cannot be represented at all, it is
skipped with a reason rather than silently altered.

## The report

`quay import --json` prints:

```json
{
  "source": "/path/to/.changi/events.db",
  "read": 1200,
  "migrated": 1198,
  "skipped": 2,
  "synthesized": 37,
  "kinds": {"generic": 900, "signal": 180, "work": 118},
  "warnings": ["event 12: producer 'Worker #1!' normalized to 'Worker--1'"],
  "duration_ms": 412.7,
  "dry_run": false
}
```

Human output prints the same totals with the first `--max-warnings` (default 20)
warnings expanded.

## Rules the importer follows

1. **Never drop an event silently.** Every skip and every adjustment appears in
   `warnings`.
2. **Never invent a transition the target would reject.** A broken link is
   dropped, or a root is reconstructed, before the record is skipped.
3. **Never fabricate a link.** A `causation_id` pointing at an event that was
   not migrated is reported and dropped.
4. **Preserve order.** Events are written in `event_id` order, in batches of
   500. If a batch fails, it is retried row by row so one bad row cannot sink
   the rest.
5. **Be honest about repetition.** Importing the same ledger twice into the same
   workspace is not deduplicated. Plane records that already reached a terminal
   state are refused by the state machines and reported as skips. Import into a
   fresh workspace, or expect those warnings.

## What is not migrated

| Changi feature | Status |
| --- | --- |
| Event history | Imported |
| Causal links | Imported when they survive the mapping |
| Signals and work receipts | Mapped to the signal and work planes |
| File locks | No equivalent; use `quay work` leases instead |
| Changi's CLI scripts and `changid` flags | No equivalent; see the CLI reference in `README.md` |
| Schema versions above the one Quay reads | Imported as-is; `schema_version` is not reinterpreted |

The source ledger is opened read-only and is never modified. Keep it until you
have verified the import.

## Verifying the result

```bash
quay verify                 # recomputes the whole hash chain
quay status                 # plane counts
quay query --kind work --state done --limit 5
quay query --text "some payload text" --limit 5
```

`quay verify` covers imported events exactly like native ones: they are hashed
and chained when written, so a tampered import fails verification.

## Uninstalling Changi

Quay does not remove Changi, rewrite its files, or edit your shell profile.
When you are satisfied with the import, delete the `.changi` directory yourself.

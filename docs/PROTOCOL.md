# Quay wire protocol, version 1

The control socket is a Unix domain socket (`AF_UNIX`, `SOCK_STREAM`) at
`<workspace>/.quay/quayd.sock`, mode `0600`. Clients are ordinary processes;
nothing about the protocol depends on Quay's own client library.

## Framing

Every frame is a 4-byte unsigned big-endian length followed by exactly that many
bytes of UTF-8 JSON:

```
 0        4                                    4 + length
 +--------+------------------------------------+
 | length | JSON body                          |
 +--------+------------------------------------+
```

* The length is validated before the body is read, so a peer can never make the
  other side allocate more than `MAX_FRAME_BYTES` (4 MiB).
* A frame shorter than 2 bytes (`{}`) is a protocol error.
* A body that is not valid JSON, or not a JSON object, is a protocol error.
* There is no compression, no chunking, and no pipelining requirement. Requests
  on one connection are answered in order.

Payloads are capped far below the frame ceiling by the schema layer
(256 KiB per payload), so the 4 MiB headroom exists for batched writes.

## Frame shapes

### Request

```json
{"v": "1", "type": "request", "id": "a3f1", "op": "emit", "body": {"topic": "build/log"}}
```

`id` is an opaque string chosen by the client; the matching response carries it
back. `v` must equal the protocol version. `body` is optional and defaults to
`{}`.

### Response

```json
{"v": "1", "type": "response", "id": "a3f1", "ok": true, "result": {"event": {"seq": 42}}}
{"v": "1", "type": "response", "id": "a3f1", "ok": false,
 "error": {"code": "invalid_payload", "message": "…", "detail": {"field": "topic"}}}
```

Exactly one response per request. Errors always carry a stable machine-readable
`code`; `detail` is optional and free-form.

### Server pushes

```json
{"v": "1", "type": "event",     "sub": 3, "event": {"seq": 43, "kind": "signal"}}
{"v": "1", "type": "lagged",    "sub": 3, "dropped": 12, "last_seq": 480}
{"v": "1", "type": "heartbeat", "ts": "2026-10-04T12:00:00.000Z"}
{"v": "1", "type": "shutdown",  "reason": "stop"}
```

`sub` is the subscription id returned by `subscribe`. Pushes arrive on the same
connection as the responses; clients must dispatch on `type`, not on arrival
order.

## Operations

Read operations may be called by any client. Write operations mutate the ledger
and broadcast to subscribers.

### Read

| Op | Body | Result |
| --- | --- | --- |
| `ping` | – | `{"pong": true}` |
| `health` | – | instance id, pid, uptime, protocol version |
| `status` | – | ledger summary and plane counts (O(1)) |
| `stats` | – | connections, emissions, pushes, drops |
| `query` | `kind`, `topic`, `producer`, `state`, `correlation_id`, `causation_id`, `since_seq`, `until_seq`, `since`, `until`, `text`, `limit`, `offset`, `order` | `{"events": [...], "count": n}` |
| `log` | `limit`, `order` | most recent events |
| `since` | `seq`, `limit` | events after a sequence number |
| `get` | `seq` | one event or `null` |
| `kinds` | – | the kind contracts (states, required fields) |
| `verify` | `record` | chain verification report |
| `work_list` | `state`, `key`, `limit` | work records |
| `prune_preview` | `before`, `keep_last` | what a prune would remove |
| `subscribe` | `filters`, `replay` | `{"sub": n, "replayed": m}` |
| `unsubscribe` | `sub` | `{"sub": n, "removed": true}` |
| `stop` | `reason` | `{"stopping": true}` |

### Write

| Op | Body | Result |
| --- | --- | --- |
| `emit` | `topic`, `producer`, `payload`, `kind`, `causation_id`, `correlation_id`, `ts` | `{"event": {...}}` |
| `emit_batch` | `events` (array) | `{"events": [...], "count": n}` |
| `genesis` | – | `{"created": bool, "event": {...} or null}` |
| `work_claim` | `key`, `executor`, `title`, `lease_seconds`, `topic`, `correlation_id` | `{"event": {...}}` |
| `work_heartbeat` | same | `{"event": {...}}` |
| `work_complete` | `key`, `executor`, `evidence` | `{"event": {...}}` |
| `work_abandon` | `key`, `executor`, `reason` | `{"event": {...}}` |
| `work_reap` | `now` | `{"reaped": [...], "count": n}` |
| `prune` | `before`, `keep_last`, `dry_run` | prune report |
| `import` | `source`, `dry_run` | `{"report": {...}}` |

`genesis` appends the single `quay/init` note only when the ledger is empty.
Repeat or concurrent calls report `created: false` and never write a second
record, so onboarding is idempotent.

`import` is deliberately not broadcast: a migration can write hundreds of
thousands of events, and replaying them to live subscribers would turn a bulk
load into a fan-out storm. Subscribers can backfill with `since`.

## Subscription filters

A subscription carries any subset of:

| Filter | Match |
| --- | --- |
| `kind` | Exact kind |
| `topic` | Glob (`fnmatch`), e.g. `build/*` |
| `producer` | Exact producer |
| `state` | `payload.state` for plane records |
| `correlation_id` | Exact correlation id |
| `since_seq` | Only events after this sequence number |

`replay` asks for up to *n* already-stored matching events before live pushes
begin. They are delivered as ordinary `event` frames, so a client needs no
special case for history.

## Backpressure

Responses always block their own connection until queued. Pushes are droppable:

* Each connection has a bounded outbox and a capped kernel send buffer.
* When the outbox is full, the frame is discarded and counted.
* The subscriber then receives one `lagged` frame naming the number dropped and
  the last sequence number it can rely on.

A slow client is therefore told precisely what it missed rather than being
silently given a gap, and the daemon's memory does not grow with its backlog.

## Errors

Common codes, all stable:

| Code | Meaning |
| --- | --- |
| `usage` | Unknown op or malformed request envelope |
| `unknown_op` | The op name is not in the operation table |
| `invalid_payload` | Schema validation failed; `detail.field` names the field |
| `invalid_transition` | The plane state machine refused the change |
| `unknown_causation` | `causation_id` does not name a stored event |
| `work_parent_mismatch` | A work record's parent belongs to a different key |
| `unknown_subscription` | `unsubscribe` named a subscription this connection does not own |
| `ledger_locked` | SQLite is busy past the busy timeout |
| `closed` | The ledger is shutting down |

## Writing a client without the library

```python
import json, socket, struct

HEADER = struct.Struct("!I")

def call(sock, op, body=None, request_id="1"):
    frame = json.dumps({"v": "1", "type": "request", "id": request_id,
                        "op": op, "body": body or {}}).encode()
    sock.sendall(HEADER.pack(len(frame)) + frame)
    (length,) = HEADER.unpack(read_exactly(sock, 4))
    return json.loads(read_exactly(sock, length))
```

`read_exactly` must loop until it has the requested number of bytes; a single
`recv` is not enough. The reference implementation is `src/quay/protocol.py`.

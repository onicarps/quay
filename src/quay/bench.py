"""Benchmarks with honest baselines.

Every benchmark reports the number it measured *and*, where it matters, the
cost of the approach it replaces. Two baselines are deliberately included:

* **single-append** — one transaction per event, which is how Changi writes
  (``with connection:`` around a single INSERT). Comparing batched appends to
  single appends shows what batching actually buys rather than quoting a
  throughput number in isolation.
* **scan projection** — the O(rows) ``ledger_status()`` walk Changi performs on
  every ``status`` call, reimplemented here so the constant-cost projection can
  be compared against the linear one on the same machine and ledger.

Latency percentiles come from real round trips: the fan-out benchmark records
the moment each event was acknowledged and measures arrival time at each
subscriber.
"""

from __future__ import annotations

import contextlib
import statistics
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from .client import QuayClient, SubscriptionStream
from .server import QuayServer
from .store import LedgerStore

#: Scale profiles keep the default run short enough to sit in a test suite.
SCALES: dict[str, dict[str, int]] = {
    "quick": {"append": 2_000, "batch": 500, "scan": 5_000, "query": 10_000, "fanout_events": 300, "fanout_subscribers": 4},
    "full": {"append": 20_000, "batch": 5_000, "scan": 50_000, "query": 100_000, "fanout_events": 2_000, "fanout_subscribers": 8},
}

__all__ = ["run_all", "SCALES", "bench_append", "bench_status", "bench_query", "bench_fanout", "percentiles"]


def percentiles(samples: Sequence[float]) -> dict[str, float]:
    """Return p50/p90/p99/max in milliseconds for a list of seconds."""
    if not samples:
        return {"p50_ms": 0.0, "p90_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0}
    ordered = sorted(samples)

    def pick(fraction: float) -> float:
        index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
        return ordered[index] * 1000.0

    return {
        "p50_ms": round(pick(0.50), 3),
        "p90_ms": round(pick(0.90), 3),
        "p99_ms": round(pick(0.99), 3),
        "max_ms": round(ordered[-1] * 1000.0, 3),
    }


@dataclass
class Result:
    name: str
    metrics: dict[str, Any] = field(default_factory=dict)
    baseline: dict[str, Any] | None = None
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": self.name, "metrics": self.metrics}
        if self.baseline:
            payload["baseline"] = self.baseline
        if self.notes:
            payload["notes"] = self.notes
        return payload


def _ratio(new: float, old: float) -> float | None:
    if old <= 0:
        return None
    return round(new / old, 2)


# ------------------------------------------------------------------- benchmarks


def bench_append(path: Path, count: int, *, batch_size: int = 500) -> Result:
    """Batched append throughput vs one-transaction-per-event appends."""
    batch_store = LedgerStore(path, synchronous="NORMAL")
    try:
        started = time.perf_counter()
        written = 0
        while written < count:
            size = min(batch_size, count - written)
            batch_store.append_batch(
                [
                    {"topic": "bench/append", "producer": "bench", "payload": {"i": written + offset}}
                    for offset in range(size)
                ]
            )
            written += size
        batched_seconds = time.perf_counter() - started
        assert batch_store.count() == count
    finally:
        batch_store.close()

    single_path = path.with_name("single-append.db")
    single_store = LedgerStore(single_path, synchronous="NORMAL")
    try:
        started = time.perf_counter()
        for index in range(count):
            single_store.append(topic="bench/single", producer="bench", payload={"i": index})
        single_seconds = time.perf_counter() - started
    finally:
        single_store.close()
        with contextlib.suppress(OSError):
            for suffix in ("", "-wal", "-shm"):
                Path(f"{single_path}{suffix}").unlink(missing_ok=True)

    batched_rate = count / batched_seconds
    single_rate = count / single_seconds
    return Result(
        name="append_throughput",
        metrics={
            "events": count,
            "batch_size": batch_size,
            "batched_events_per_second": round(batched_rate, 1),
            "batched_seconds": round(batched_seconds, 3),
        },
        baseline={
            "name": "single-append (one transaction per event, Changi style)",
            "events_per_second": round(single_rate, 1),
            "seconds": round(single_seconds, 3),
        },
        notes=f"batched is {_ratio(batched_rate, single_rate)}x the single-append rate",
    )


def _scan_projection(store: LedgerStore) -> dict[str, int]:
    """Reimplement Changi's O(rows) status walk for an honest comparison."""
    counts: dict[str, int] = {}
    connection = store._conn  # noqa: SLF001 - the benchmark intentionally reads raw rows
    for _event_id, _causation_id, payload_text in connection.execute(
        "SELECT seq, causation_id, payload FROM events ORDER BY seq"
    ):
        counts["records"] = counts.get("records", 0) + 1
        if '"state"' in payload_text:
            counts["with_state"] = counts.get("with_state", 0) + 1
    return counts


def bench_status(path: Path, events: int) -> Result:
    """Constant-cost projection reads vs a full-table scan projection."""
    store = LedgerStore(path, synchronous="NORMAL")
    try:
        written = 0
        while written < events:
            size = min(2_000, events - written)
            store.append_batch(
                [
                    {"topic": "bench/status", "producer": "bench", "payload": {"i": written + offset}}
                    for offset in range(size)
                ]
            )
            written += size

        samples_small: list[float] = []
        for _ in range(5):
            started = time.perf_counter()
            store.status()
            samples_small.append(time.perf_counter() - started)

        written = 0
        while written < events:
            size = min(2_000, events - written)
            store.append_batch(
                [
                    {"topic": "bench/status", "producer": "bench", "payload": {"i": events + written + offset}}
                    for offset in range(size)
                ]
            )
            written += size

        samples_large: list[float] = []
        for _ in range(5):
            started = time.perf_counter()
            store.status()
            samples_large.append(time.perf_counter() - started)

        scan_samples: list[float] = []
        scan_size = min(events * 2, 100_000)
        for _ in range(3):
            started = time.perf_counter()
            _scan_projection(store)
            scan_samples.append(time.perf_counter() - started)

        small_ms = statistics.median(samples_small) * 1000
        large_ms = statistics.median(samples_large) * 1000
        scan_ms = statistics.median(scan_samples) * 1000
        growth = _ratio(large_ms, small_ms)
        return Result(
            name="status_latency",
            metrics={
                "events": events * 2,
                "status_ms_at_half": round(small_ms, 4),
                "status_ms_at_full": round(large_ms, 4),
                "growth_factor_doubling": growth,
            },
            baseline={
                "name": f"full-table scan projection over {scan_size} rows",
                "scan_ms": round(scan_ms, 4),
            },
            notes=f"constant-cost status is {_ratio(scan_ms, large_ms)}x faster than the scan baseline",
        )
    finally:
        store.close()


def bench_query(path: Path, events: int) -> Result:
    """Filtered query latency against an indexed ledger."""
    store = LedgerStore(path, synchronous="NORMAL")
    try:
        written = 0
        while written < events:
            size = min(2_000, events - written)
            store.append_batch(
                [
                    {
                        "topic": "bench/query" if (written + offset) % 3 else "bench/other",
                        "producer": "bench" if (written + offset) % 5 else "other",
                        "payload": {"i": written + offset},
                    }
                    for offset in range(size)
                ]
            )
            written += size

        timings: dict[str, list[float]] = {"topic_glob": [], "producer": [], "text": []}
        for _ in range(5):
            started = time.perf_counter()
            store.query(topic="bench/*", limit=100)
            timings["topic_glob"].append(time.perf_counter() - started)

            started = time.perf_counter()
            store.query(producer="bench", limit=100)
            timings["producer"].append(time.perf_counter() - started)

            started = time.perf_counter()
            store.query(text='"i":500', limit=100)
            timings["text"].append(time.perf_counter() - started)

        metrics: dict[str, Any] = {"events": events}
        for name, samples in timings.items():
            metrics[f"{name}_ms"] = round(statistics.median(samples) * 1000, 4)
        return Result(
            name="query_latency",
            metrics=metrics,
            notes="topic and producer filters use indexes; the text filter scans payloads by design",
        )
    finally:
        store.close()


def bench_fanout(workspace: Path, *, subscribers: int, events: int) -> Result:
    """Push latency to N live subscribers, drained concurrently with the writes.

    Each subscriber is drained by its own thread while events are appended, so
    the reported latency is delivery latency, not queueing time accumulated
    after the fact.
    """
    server = QuayServer(
        workspace,
        idle_seconds=0,
        heartbeat_seconds=30.0,
        subscriber_queue_size=4096,
        synchronous="NORMAL",
        reap_interval_seconds=3600,
    )
    server.start()
    streams: list[SubscriptionStream] = []
    collectors: list[threading.Thread] = []
    received: list[list[tuple[int, float]]] = []
    stop_collectors = threading.Event()
    try:
        for index in range(subscribers):
            stream = SubscriptionStream(server.socket_path, filters={}, timeout=10.0).open()
            streams.append(stream)
            bucket: list[tuple[int, float]] = []
            received.append(bucket)
            thread = threading.Thread(
                target=_collect_events,
                args=(stream, bucket, stop_collectors, events),
                name=f"bench-collector-{index}",
                daemon=True,
            )
            collectors.append(thread)
            thread.start()

        emit_times: dict[int, tuple[float, float]] = {}
        with QuayClient(server.socket_path, timeout=15.0) as client:
            for index in range(events):
                started = time.perf_counter()
                event = client.emit(topic="bench/fanout", producer="bench", payload={"i": index})
                emit_times[event["seq"]] = (started, time.perf_counter())

        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline and any(len(bucket) < events for bucket in received):
            time.sleep(0.05)

        push_latencies: list[float] = []
        end_to_end: list[float] = []
        for bucket in received:
            for seq, arrived_at in bucket:
                stamps = emit_times.get(seq)
                if stamps is None:
                    continue
                issued_at, acknowledged_at = stamps
                push_latencies.append(max(0.0, arrived_at - acknowledged_at))
                end_to_end.append(arrived_at - issued_at)

        emit_roundtrips = [
            acknowledged - issued for issued, acknowledged in emit_times.values()
        ]
        stats = server.stats()
        return Result(
            name="push_fanout",
            metrics={
                "subscribers": subscribers,
                "events": events,
                "deliveries_expected": subscribers * events,
                "deliveries_observed": len(push_latencies),
                "emit_roundtrip_p50_ms": round(statistics.median(emit_roundtrips) * 1000, 3),
                "push_p50_ms": percentiles(push_latencies)["p50_ms"],
                "push_p90_ms": percentiles(push_latencies)["p90_ms"],
                "push_p99_ms": percentiles(push_latencies)["p99_ms"],
                "end_to_end_p50_ms": percentiles(end_to_end)["p50_ms"],
                "end_to_end_p99_ms": percentiles(end_to_end)["p99_ms"],
            },
            notes=(
                "push_* measures arrival after the write was acknowledged; "
                "end_to_end_* includes the durable write. "
                f"server counters: delivered={stats['pushes_delivered']} dropped={stats['pushes_dropped']}"
            ),
        )
    finally:
        stop_collectors.set()
        for stream in streams:
            stream.close()
        for thread in collectors:
            thread.join(timeout=5.0)
        server.request_stop("bench")
        server.close()


def _collect_events(
    stream: SubscriptionStream,
    bucket: list[tuple[int, float]],
    stop: threading.Event,
    target: int,
) -> None:
    """Drain one subscriber stream, recording arrival times."""
    while not stop.is_set() and len(bucket) < target:
        frame = stream.poll(0.2)
        if frame is None:
            continue
        if frame.get("type") != "event":
            continue
        bucket.append((int(frame["event"]["seq"]), time.perf_counter()))


# -------------------------------------------------------------------------- run


def run_all(*, scale: str = "quick") -> dict[str, Any]:
    """Run every benchmark in an isolated temporary workspace.

    The caller's real ledger is never opened or modified: storage benchmarks
    use throwaway databases and the fan-out benchmark starts a daemon inside a
    temporary workspace.
    """
    if scale not in SCALES:
        raise ValueError(f"unknown scale {scale!r}; expected one of {sorted(SCALES)}")
    profile = SCALES[scale]

    with tempfile.TemporaryDirectory(prefix="quay-bench-") as temporary:
        root = Path(temporary)
        fanout_workspace = root / "fanout-ws"
        fanout_workspace.mkdir()
        results: list[Result] = [
            bench_append(root / "append.db", profile["append"], batch_size=profile["batch"]),
            bench_status(root / "status.db", profile["scan"]),
            bench_query(root / "query.db", profile["query"]),
            bench_fanout(
                fanout_workspace,
                subscribers=profile["fanout_subscribers"],
                events=profile["fanout_events"],
            ),
        ]

    return {
        "scale": scale,
        "profile": profile,
        "results": [result.to_dict() for result in results],
        "host": {"python": sys.version.split()[0], "platform": sys.platform},
    }


def format_report(report: dict[str, Any]) -> str:
    """Render a benchmark report as aligned text for a terminal."""
    lines = [f"scale: {report['scale']}  ({report.get('host', {}).get('platform', '?')})"]
    for entry in report["results"]:
        lines.append(f"\n{entry['name']}")
        for key, value in entry["metrics"].items():
            lines.append(f"  {key:<28s} {value}")
        baseline = entry.get("baseline")
        if baseline:
            lines.append(f"  baseline: {baseline.get('name')}")
            for key, value in baseline.items():
                if key != "name":
                    lines.append(f"    {key:<26s} {value}")
        if entry.get("notes"):
            lines.append(f"  note: {entry['notes']}")
    return "\n".join(lines)

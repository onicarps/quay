"""Benchmark suite tests (requirement F19).

Benchmarks assert relationships (batched faster than single-append, constant
cost faster than a scan) rather than absolute numbers, so they stay meaningful
on different machines.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from quay.bench import (
    SCALES,
    bench_append,
    bench_fanout,
    bench_query,
    bench_status,
    format_report,
    percentiles,
    run_all,
)


class TestPercentiles:
    def test_empty_sample(self):
        assert percentiles([]) == {"p50_ms": 0.0, "p90_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0}

    def test_single_sample(self):
        stats = percentiles([0.010])
        assert stats["p50_ms"] == 10.0
        assert stats["max_ms"] == 10.0

    def test_ordering(self):
        stats = percentiles([0.001 * index for index in range(1, 101)])
        assert stats["p50_ms"] <= stats["p90_ms"] <= stats["p99_ms"] <= stats["max_ms"]
        assert stats["max_ms"] == pytest.approx(100.0, rel=0.01)


class TestAppendBenchmark:
    def test_batched_appends_beat_single_appends(self, tmp_path: Path):
        result = bench_append(tmp_path / "append.db", 200, batch_size=50)
        metrics = result.metrics
        baseline = result.baseline
        assert metrics["events"] == 200
        assert metrics["batched_events_per_second"] > 0
        assert baseline is not None
        assert baseline["events_per_second"] > 0
        assert metrics["batched_events_per_second"] > baseline["events_per_second"]

    def test_reports_a_ratio_in_notes(self, tmp_path: Path):
        result = bench_append(tmp_path / "append.db", 100, batch_size=25)
        assert "x the single-append rate" in result.notes

    def test_cleans_up_its_baseline_database(self, tmp_path: Path):
        bench_append(tmp_path / "append.db", 50, batch_size=25)
        assert not (tmp_path / "single-append.db").exists()


class TestStatusBenchmark:
    def test_constant_cost_status_beats_a_scan(self, tmp_path: Path):
        result = bench_status(tmp_path / "status.db", 1_000)
        metrics = result.metrics
        baseline = result.baseline
        assert metrics["events"] == 2_000
        assert metrics["status_ms_at_full"] >= 0
        assert baseline is not None
        assert metrics["status_ms_at_full"] < baseline["scan_ms"], (
            "the projection read must be cheaper than a full-table scan"
        )

    def test_reports_growth_across_a_doubling(self, tmp_path: Path):
        result = bench_status(tmp_path / "status.db", 500)
        growth = result.metrics["growth_factor_doubling"]
        assert growth is not None
        assert growth < 10.0, f"status cost grew {growth}x when the ledger doubled"


class TestQueryBenchmark:
    def test_reports_each_filter_kind(self, tmp_path: Path):
        result = bench_query(tmp_path / "query.db", 1_000)
        for key in ("topic_glob_ms", "producer_ms", "text_ms"):
            assert key in result.metrics
            assert result.metrics[key] >= 0
        assert result.metrics["events"] == 1_000

    def test_indexed_filters_are_faster_than_a_text_scan(self, tmp_path: Path):
        result = bench_query(tmp_path / "query.db", 2_000)
        assert result.metrics["producer_ms"] <= max(result.metrics["text_ms"], 0.001) * 5 + 1


class TestFanoutBenchmark:
    def test_every_subscriber_receives_every_event(self, workspace: Path):
        result = bench_fanout(workspace, subscribers=3, events=20)
        metrics = result.metrics
        assert metrics["deliveries_expected"] == 60
        assert metrics["deliveries_observed"] == 60
        assert metrics["push_p50_ms"] >= 0
        assert metrics["push_p99_ms"] >= metrics["push_p50_ms"]
        assert metrics["end_to_end_p99_ms"] >= metrics["end_to_end_p50_ms"]

    def test_separates_write_latency_from_push_latency(self, workspace: Path):
        result = bench_fanout(workspace, subscribers=1, events=10)
        assert result.metrics["emit_roundtrip_p50_ms"] > 0
        assert "push_*" in result.notes
        assert "end_to_end_*" in result.notes

    def test_reports_server_counters_in_notes(self, workspace: Path):
        result = bench_fanout(workspace, subscribers=1, events=5)
        assert "delivered=" in result.notes
        assert "dropped=" in result.notes


class TestRunAll:
    def test_orchestrates_every_benchmark(self, tmp_path: Path):
        SCALES["_test"] = {
            "append": 100,
            "batch": 25,
            "scan": 300,
            "query": 300,
            "fanout_events": 10,
            "fanout_subscribers": 2,
        }
        try:
            report = run_all(scale="_test")
        finally:
            SCALES.pop("_test", None)
        names = [entry["name"] for entry in report["results"]]
        assert names == ["append_throughput", "status_latency", "query_latency", "push_fanout"]
        assert report["scale"] == "_test"
        assert "python" in report["host"]
        for entry in report["results"]:
            assert entry["metrics"]

    def test_rejects_an_unknown_scale(self):
        with pytest.raises(ValueError):
            run_all(scale="enormous")

    def test_report_is_json_serializable_and_formats(self, tmp_path: Path):
        import json

        SCALES["_json"] = {
            "append": 20,
            "batch": 10,
            "scan": 40,
            "query": 40,
            "fanout_events": 3,
            "fanout_subscribers": 1,
        }
        try:
            report = run_all(scale="_json")
        finally:
            SCALES.pop("_json", None)
        text = format_report(report)
        assert "append_throughput" in text
        assert "baseline" in text
        assert json.loads(json.dumps(report)) == report

    def test_never_touches_the_callers_ledger(self, workspace: Path):
        SCALES["_isolated"] = {
            "append": 10,
            "batch": 5,
            "scan": 20,
            "query": 20,
            "fanout_events": 2,
            "fanout_subscribers": 1,
        }
        try:
            run_all(scale="_isolated")
        finally:
            SCALES.pop("_isolated", None)
        assert not (workspace / ".quay").exists(), "bench must use its own temporary workspace"

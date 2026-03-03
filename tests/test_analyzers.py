"""Tests for analyzer modules using sample range data."""

import json

import pytest

from crdb_analyzer.analyzers.base import BaseAnalyzer
from crdb_analyzer.analyzers.data_skew import DataSkewAnalyzer
from crdb_analyzer.analyzers.hot_nodes import HotNodesAnalyzer
from crdb_analyzer.analyzers.hot_ranges import HotRangesAnalyzer
from crdb_analyzer.analyzers.table_stats import TableStatsAnalyzer


class TestBaseAnalyzer:
    def test_extract_qps(self, sample_range_data):
        r = sample_range_data["ranges"]["3"]
        assert BaseAnalyzer._extract_qps(r) == 5000.0

    def test_extract_wps(self, sample_range_data):
        r = sample_range_data["ranges"]["1"]
        assert BaseAnalyzer._extract_wps(r) == 300.1

    def test_extract_live_count(self, sample_range_data):
        r = sample_range_data["ranges"]["1"]
        assert BaseAnalyzer._extract_live_count(r) == 50000

    def test_extract_leaseholder(self, sample_range_data):
        r = sample_range_data["ranges"]["2"]
        assert BaseAnalyzer._extract_leaseholder(r) == 2

    def test_extract_nodes(self, sample_range_data):
        r = sample_range_data["ranges"]["1"]
        assert BaseAnalyzer._extract_nodes(r) == [1, 2]

    def test_extract_start_key(self, sample_range_data):
        r = sample_range_data["ranges"]["3"]
        assert BaseAnalyzer._extract_start_key(r) == "/Table/66/1"

    def test_extract_handles_missing_data(self):
        r = {"nodes": []}
        assert BaseAnalyzer._extract_qps(r) == 0.0
        assert BaseAnalyzer._extract_leaseholder(r) is None
        assert BaseAnalyzer._extract_nodes(r) == []


class TestHotRangesAnalyzer:
    def test_from_file(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))

        analyzer = HotRangesAnalyzer()
        results = analyzer.analyze(limit=10, ranges_file=str(f))

        assert results["title"].startswith("Top 10")
        assert results["source"] == "api"
        assert len(results["rows"]) == 4
        assert results["rows"][0]["range_id"] == 3
        assert results["rows"][0]["qps"] == 5000.0

    def test_sort_by_wps(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))

        analyzer = HotRangesAnalyzer()
        results = analyzer.analyze(limit=10, sort_by="wps", ranges_file=str(f))
        assert results["rows"][0]["wps"] == 2000.0

    def test_no_source_raises(self):
        analyzer = HotRangesAnalyzer()
        with pytest.raises(RuntimeError, match="No data source"):
            analyzer.analyze()


class TestHotNodesAnalyzer:
    def test_from_file(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))

        analyzer = HotNodesAnalyzer()
        results = analyzer.analyze(ranges_file=str(f))

        assert results["source"] == "api"
        assert len(results["rows"]) == 3
        assert results["rows"][0]["node_id"] == 3
        assert results["summary"]["node_count"] == 3


class TestDataSkewAnalyzer:
    def test_from_file(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))

        analyzer = DataSkewAnalyzer()
        results = analyzer.analyze(limit=10, ranges_file=str(f))

        assert results["rows"][0]["range_id"] == 1
        assert results["rows"][0]["live_count"] == 50000


class TestTableStatsAnalyzer:
    def test_from_file(self, sample_range_data, tmp_path):
        f = tmp_path / "ranges.json"
        f.write_text(json.dumps(sample_range_data))

        analyzer = TableStatsAnalyzer()
        results = analyzer.analyze(ranges_file=str(f))

        assert results["source"] == "api"
        table_ids = [r["table_id"] for r in results["rows"]]
        assert "55" in table_ids
        assert "66" in table_ids
        assert "system" in table_ids

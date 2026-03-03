"""Tests for output formatters."""

import json

from crdb_analyzer.formatters.output import format_results

SAMPLE_RESULTS = {
    "title": "Test Results",
    "source": "test",
    "headers": ["id", "name", "value"],
    "rows": [
        {"id": 1, "name": "alpha", "value": 100},
        {"id": 2, "name": "beta", "value": 200},
    ],
    "summary": {"total": 2},
}


class TestFormatResults:
    def test_table_format(self):
        out = format_results(SAMPLE_RESULTS, "table")
        assert "Test Results" in out
        assert "alpha" in out
        assert "beta" in out
        assert "total: 2" in out

    def test_json_format(self):
        out = format_results(SAMPLE_RESULTS, "json")
        parsed = json.loads(out)
        assert parsed["title"] == "Test Results"
        assert len(parsed["rows"]) == 2

    def test_csv_format(self):
        out = format_results(SAMPLE_RESULTS, "csv")
        lines = [line.strip() for line in out.strip().split("\n")]
        assert lines[0] == "id,name,value"
        assert lines[1] == "1,alpha,100"

    def test_empty_rows(self):
        empty = {"title": "Empty", "rows": [], "headers": []}
        out = format_results(empty, "table")
        assert "(no data)" in out

    def test_sections_format(self):
        sectioned = {
            "title": "Multi-section",
            "source": "test",
            "sections": [
                {"title": "Section A", "headers": ["x"], "rows": [{"x": 1}]},
                {"title": "Section B", "headers": ["y"], "rows": [{"y": 2}]},
            ],
            "summary": {},
        }
        out = format_results(sectioned, "table")
        assert "Section A" in out
        assert "Section B" in out

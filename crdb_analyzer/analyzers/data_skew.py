"""Data skew analyzer - identifies ranges with disproportionate data volume."""

import json
from pathlib import Path
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer


class DataSkewAnalyzer(BaseAnalyzer):
    def analyze(
        self,
        limit: int = 50,
        ranges_file: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if ranges_file:
            return self._analyze_from_file(ranges_file, limit)
        if self.sql:
            return self._analyze_from_sql(limit)
        if self.http:
            return self._analyze_from_api(limit)
        msg = "No data source available."
        raise RuntimeError(msg)

    def _analyze_from_sql(self, limit: int) -> dict[str, Any]:
        assert self.sql is not None
        rows = self.sql.get_ranges(limit=limit, order_by="range_size DESC")
        table_map = self.sql.get_table_id_map()

        enriched = []
        for r in rows:
            tid = self.sql.parse_table_id(r.get("start_pretty", ""))
            tinfo = table_map.get(tid, {}) if tid else {}
            enriched.append({
                **r,
                "table_name": tinfo.get("name", ""),
                "database_name": tinfo.get("database_name", ""),
            })

        return {
            "title": f"Top {limit} Largest Ranges (Data Skew)",
            "source": "sql",
            "headers": [
                "range_id", "database_name", "table_name",
                "lease_holder", "range_size_mb", "start_pretty",
            ],
            "rows": enriched,
            "summary": self._summarize_sql(enriched),
        }

    def _analyze_from_api(self, limit: int) -> dict[str, Any]:
        assert self.http is not None
        return self._process_raw(self.http.get_ranges(), limit)

    def _analyze_from_file(self, path: str, limit: int) -> dict[str, Any]:
        data = json.loads(Path(path).read_text())
        return self._process_raw(data, limit)

    def _process_raw(self, data: dict[str, Any], limit: int) -> dict[str, Any]:
        ranges = data.get("ranges", {})
        range_list = list(ranges.values()) if isinstance(ranges, dict) else ranges

        sorted_ranges = sorted(range_list, key=self._extract_live_count, reverse=True)[:limit]

        rows = [
            {
                "range_id": r.get("rangeId"),
                "live_count": self._extract_live_count(r),
                "qps": round(self._extract_qps(r), 2),
                "leaseholder": self._extract_leaseholder(r),
                "nodes": self._extract_nodes(r),
                "start_key": self._extract_start_key(r),
                "end_key": self._extract_end_key(r),
            }
            for r in sorted_ranges
        ]

        return {
            "title": f"Top {limit} Largest Ranges (Data Skew)",
            "source": "api",
            "headers": [
                "range_id", "live_count", "qps", "leaseholder", "nodes", "start_key", "end_key",
            ],
            "rows": rows,
            "summary": {},
        }

    @staticmethod
    def _summarize_sql(rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {}
        sizes = [float(r.get("range_size_mb", 0) or 0) for r in rows]
        return {
            "total_ranges_shown": len(rows),
            "largest_range_mb": round(max(sizes), 2) if sizes else 0,
            "avg_range_mb": round(sum(sizes) / len(sizes), 2) if sizes else 0,
        }

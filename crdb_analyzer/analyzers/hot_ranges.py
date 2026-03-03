"""Hot ranges analyzer - identifies ranges with the highest size (v25/v26 SQL)
or highest QPS (HTTP API / file)."""

import json
from pathlib import Path
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer


class HotRangesAnalyzer(BaseAnalyzer):
    def analyze(
        self,
        limit: int = 50,
        sort_by: str = "qps",
        ranges_file: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if ranges_file:
            return self._analyze_from_file(ranges_file, limit, sort_by)
        if self.sql:
            return self._analyze_from_sql(limit)
        if self.http:
            return self._analyze_from_api(limit, sort_by)
        msg = "No data source available. Provide sql_client, http_client, or ranges_file."
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
                "table_id": tid,
                "table_name": tinfo.get("name", ""),
                "database_name": tinfo.get("database_name", ""),
            })

        return {
            "title": f"Top {limit} Ranges by Size",
            "source": "sql",
            "headers": [
                "range_id", "database_name", "table_name", "lease_holder",
                "replicas", "range_size_mb", "start_pretty",
            ],
            "rows": enriched,
            "summary": self._summarize_sql(enriched),
        }

    def _analyze_from_api(self, limit: int, sort_by: str) -> dict[str, Any]:
        assert self.http is not None
        data = self.http.get_ranges()
        return self._process_raw_ranges(data, limit, sort_by)

    def _analyze_from_file(self, path: str, limit: int, sort_by: str) -> dict[str, Any]:
        data = (
            self.http.load_ranges_from_file(path)
            if self.http
            else json.loads(Path(path).read_text())
        )
        return self._process_raw_ranges(data, limit, sort_by)

    def _process_raw_ranges(
        self, data: dict[str, Any], limit: int, sort_by: str
    ) -> dict[str, Any]:
        ranges = data.get("ranges", {})
        range_list = list(ranges.values()) if isinstance(ranges, dict) else ranges

        key_fn = self._extract_wps if sort_by == "wps" else self._extract_qps
        sorted_ranges = sorted(range_list, key=key_fn, reverse=True)[:limit]

        rows = [
            {
                "range_id": r.get("rangeId"),
                "qps": round(self._extract_qps(r), 2),
                "wps": round(self._extract_wps(r), 2),
                "leaseholder": self._extract_leaseholder(r),
                "nodes": self._extract_nodes(r),
                "start_key": self._extract_start_key(r),
                "end_key": self._extract_end_key(r),
            }
            for r in sorted_ranges
        ]

        sort_label = "writes/s" if sort_by == "wps" else "queries/s"
        return {
            "title": f"Top {limit} Hot Ranges (by {sort_label})",
            "source": "api",
            "headers": ["range_id", "qps", "wps", "leaseholder", "nodes", "start_key", "end_key"],
            "rows": rows,
            "summary": self._summarize_api(rows),
        }

    @staticmethod
    def _summarize_sql(rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {}
        total_mb = sum(float(r.get("range_size_mb", 0) or 0) for r in rows)
        return {
            "total_ranges_shown": len(rows),
            "total_size_mb": round(total_mb, 2),
            "top_range_id": rows[0].get("range_id") if rows else None,
        }

    @staticmethod
    def _summarize_api(rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {}
        total_qps = sum(r.get("qps", 0) for r in rows)
        total_wps = sum(r.get("wps", 0) for r in rows)
        return {
            "total_ranges_shown": len(rows),
            "total_qps": round(total_qps, 2),
            "total_wps": round(total_wps, 2),
            "top_range_id": rows[0].get("range_id") if rows else None,
        }

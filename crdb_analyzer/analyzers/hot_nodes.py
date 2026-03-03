"""Hot nodes analyzer - aggregates range data per node to find overloaded nodes."""

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer


class HotNodesAnalyzer(BaseAnalyzer):
    def analyze(
        self,
        ranges_file: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if ranges_file:
            return self._analyze_from_file(ranges_file)
        if self.sql:
            return self._analyze_from_sql()
        if self.http:
            return self._analyze_from_api()
        msg = "No data source available."
        raise RuntimeError(msg)

    def _analyze_from_sql(self) -> dict[str, Any]:
        assert self.sql is not None
        rows = self.sql.get_node_stats()
        return {
            "title": "Node Load Distribution",
            "source": "sql",
            "headers": [
                "node_id", "lease_count", "total_range_size_mb",
                "avg_range_size_mb", "max_range_size_mb",
            ],
            "rows": rows,
            "summary": self._summarize_sql(rows),
        }

    def _analyze_from_api(self) -> dict[str, Any]:
        assert self.http is not None
        return self._process_raw(self.http.get_ranges())

    def _analyze_from_file(self, path: str) -> dict[str, Any]:
        data = json.loads(Path(path).read_text())
        return self._process_raw(data)

    def _process_raw(self, data: dict[str, Any]) -> dict[str, Any]:
        ranges = data.get("ranges", {})
        range_list = list(ranges.values()) if isinstance(ranges, dict) else ranges

        qps_per_node: dict[int, float] = defaultdict(float)
        wps_per_node: dict[int, float] = defaultdict(float)
        lease_count: dict[int, int] = defaultdict(int)

        for r in range_list:
            qps = self._extract_qps(r)
            wps = self._extract_wps(r)
            lh = self._extract_leaseholder(r)
            if lh is not None:
                qps_per_node[lh] += qps
                wps_per_node[lh] += wps
                lease_count[lh] += 1

        rows = [
            {
                "node_id": node_id,
                "lease_count": lease_count[node_id],
                "total_qps": round(qps_per_node[node_id], 2),
                "total_wps": round(wps_per_node[node_id], 2),
            }
            for node_id in sorted(
                qps_per_node, key=lambda n: qps_per_node[n], reverse=True
            )
        ]

        return {
            "title": "Node Load Distribution",
            "source": "api",
            "headers": ["node_id", "lease_count", "total_qps", "total_wps"],
            "rows": rows,
            "summary": self._summarize_api(rows),
        }

    @staticmethod
    def _summarize_sql(rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {}
        total_mb = sum(float(r.get("total_range_size_mb", 0) or 0) for r in rows)
        max_node = rows[0]
        return {
            "node_count": len(rows),
            "cluster_total_size_mb": round(total_mb, 2),
            "hottest_node": max_node.get("node_id"),
            "hottest_node_size_mb": round(float(max_node.get("total_range_size_mb", 0) or 0), 2),
        }

    @staticmethod
    def _summarize_api(rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {}
        total_qps = sum(r.get("total_qps", 0) for r in rows)
        max_node = rows[0]
        return {
            "node_count": len(rows),
            "cluster_total_qps": round(total_qps, 2),
            "hottest_node": max_node.get("node_id"),
            "hottest_node_qps": max_node.get("total_qps", 0),
        }

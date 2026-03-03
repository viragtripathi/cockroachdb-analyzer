"""Table stats analyzer - per-table breakdown of range distribution and size."""

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

TABLE_ID_RE = re.compile(r"/Table/([0-9]+)/")


class TableStatsAnalyzer(BaseAnalyzer):
    def analyze(
        self,
        database: str | None = None,
        table: str | None = None,
        ranges_file: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if ranges_file:
            return self._analyze_from_file(ranges_file)
        if self.sql:
            return self._analyze_from_sql(database, table)
        if self.http:
            return self._analyze_from_api()
        msg = "No data source available."
        raise RuntimeError(msg)

    def _analyze_from_sql(
        self, database: str | None, table: str | None
    ) -> dict[str, Any]:
        assert self.sql is not None
        table_map = self.sql.get_table_id_map()

        if database and table:
            target_ids = [
                tid
                for tid, info in table_map.items()
                if info.get("database_name") == database and info.get("name") == table
            ]
            rows = self.sql.get_ranges_for_table(target_ids[0]) if target_ids else []
            title = f"Ranges for {database}.{table}"
        else:
            rows = self.sql.get_all_ranges()
            title = "All Tables by Total Range Size"

        per_table: dict[str, dict[str, float]] = defaultdict(
            lambda: {"range_count": 0, "total_size": 0.0}
        )
        for r in rows:
            sp = r.get("start_pretty", "")
            m = TABLE_ID_RE.match(sp)
            if m:
                tid = int(m.group(1))
                tinfo = table_map.get(tid, {})
                key = f"{tinfo.get('database_name', '?')}.{tinfo.get('name', f'table_{tid}')}"
            else:
                key = "system"
            per_table[key]["range_count"] += 1
            per_table[key]["total_size"] += float(r.get("range_size", 0))

        if database:
            per_table = {
                k: v for k, v in per_table.items() if k.startswith(f"{database}.")
            }

        summary_rows = [
            {
                "table": tbl,
                "range_count": int(stats["range_count"]),
                "total_size_mb": round(stats["total_size"] / 1024 / 1024, 2),
            }
            for tbl, stats in sorted(
                per_table.items(), key=lambda x: x[1]["total_size"], reverse=True
            )
        ]

        return {
            "title": title,
            "source": "sql",
            "headers": ["table", "range_count", "total_size_mb"],
            "rows": summary_rows,
            "summary": {"tables_found": len(summary_rows)},
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

        per_table: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "range_count": 0,
                "total_qps": 0.0,
                "total_wps": 0.0,
                "leases_per_node": defaultdict(int),
            }
        )

        for r in range_list:
            start_key = self._extract_start_key(r)
            match = TABLE_ID_RE.match(start_key)
            table_id = match.group(1) if match else "system"
            qps = self._extract_qps(r)
            wps = self._extract_wps(r)
            lh = self._extract_leaseholder(r)

            per_table[table_id]["range_count"] += 1
            per_table[table_id]["total_qps"] += qps
            per_table[table_id]["total_wps"] += wps
            if lh is not None:
                per_table[table_id]["leases_per_node"][lh] += 1

        rows = [
            {
                "table_id": table_id,
                "range_count": stats["range_count"],
                "total_qps": round(stats["total_qps"], 2),
                "total_wps": round(stats["total_wps"], 2),
                "lease_distribution": dict(stats["leases_per_node"]),
            }
            for table_id, stats in sorted(
                per_table.items(), key=lambda x: x[1]["total_qps"], reverse=True
            )
        ]

        return {
            "title": "Per-Table Range Statistics",
            "source": "api",
            "headers": [
                "table_id", "range_count", "total_qps", "total_wps", "lease_distribution",
            ],
            "rows": rows,
            "summary": {"tables_found": len(rows)},
        }

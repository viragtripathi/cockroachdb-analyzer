"""Replica and lease balance analyzer - checks distribution across nodes."""

import re
from collections import defaultdict
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

TABLE_KEY_RE = re.compile(r"^/Table/(\d+)/")


class LeaseBalanceAnalyzer(BaseAnalyzer):
    def analyze(self, **kwargs: Any) -> dict[str, Any]:
        if not self.sql:
            msg = "Lease balance analysis requires a SQL connection."
            raise RuntimeError(msg)
        return self._analyze()

    def _analyze(self) -> dict[str, Any]:
        assert self.sql is not None
        ranges = self.sql.get_all_ranges()
        table_map = self.sql.get_table_id_map()

        replicas_per_node: dict[int, int] = defaultdict(int)
        leases_per_node: dict[int, int] = defaultdict(int)
        size_per_node: dict[int, float] = defaultdict(float)
        per_table_leases: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))

        for r in ranges:
            lh = r.get("lease_holder")
            if lh is not None:
                leases_per_node[lh] += 1
                size_per_node[lh] += float(r.get("range_size", 0))
            for nid in r.get("replicas", []):
                replicas_per_node[nid] += 1
            m = TABLE_KEY_RE.match(r.get("start_pretty", ""))
            if m and lh is not None:
                tid = int(m.group(1))
                tinfo = table_map.get(tid, {})
                tname = tinfo.get("name", f"table_{tid}")
                per_table_leases[tname][lh] += 1

        all_nodes = sorted(set(replicas_per_node) | set(leases_per_node))
        node_rows = [
            {
                "node_id": nid,
                "replicas": replicas_per_node.get(nid, 0),
                "leases": leases_per_node.get(nid, 0),
                "size_mb": round(size_per_node.get(nid, 0) / 1024 / 1024, 2),
                "lease_pct": (
                    round(leases_per_node.get(nid, 0) / max(len(ranges), 1) * 100, 1)
                ),
            }
            for nid in all_nodes
        ]

        imbalance_rows = []
        for tname, node_leases in sorted(
            per_table_leases.items(), key=lambda x: max(x[1].values(), default=0), reverse=True
        ):
            counts = list(node_leases.values())
            if not counts:
                continue
            spread = max(counts) - min(counts)
            if spread > 0:
                imbalance_rows.append({
                    "table": tname,
                    "lease_distribution": dict(node_leases),
                    "max_leases": max(counts),
                    "min_leases": min(counts),
                    "spread": spread,
                })

        lease_values = list(leases_per_node.values())
        spread = max(lease_values, default=0) - min(lease_values, default=0)
        ideal = len(ranges) / max(len(all_nodes), 1)

        return {
            "title": "Replica & Lease Balance",
            "source": "sql",
            "sections": [
                {
                    "title": "Per-Node Distribution",
                    "headers": ["node_id", "replicas", "leases", "size_mb", "lease_pct"],
                    "rows": node_rows,
                },
                {
                    "title": "Top Imbalanced Tables (by lease spread across nodes)",
                    "headers": [
                        "table", "lease_distribution", "max_leases",
                        "min_leases", "spread",
                    ],
                    "rows": imbalance_rows[:20],
                },
            ],
            "summary": {
                "total_ranges": len(ranges),
                "node_count": len(all_nodes),
                "ideal_leases_per_node": round(ideal, 1),
                "actual_lease_spread": spread,
            },
        }

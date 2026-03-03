"""Node hotspot analyzer - diagnose why a specific node is running hot."""

import logging
import re
from collections import defaultdict
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

logger = logging.getLogger(__name__)

TABLE_KEY_RE = re.compile(r"^/Table/(\d+)/")


class NodeHotspotAnalyzer(BaseAnalyzer):
    """Deep-dive analysis of a single node to explain hotspot causes."""

    def analyze(self, node_id: int = 1, limit: int = 50, **kwargs: Any) -> dict[str, Any]:
        if not self.sql:
            msg = "Node hotspot analysis requires a SQL connection."
            raise RuntimeError(msg)
        return self._analyze(node_id, limit)

    def _analyze(self, node_id: int, limit: int) -> dict[str, Any]:
        assert self.sql is not None
        all_ranges = self.sql.get_all_ranges()
        table_map = self.sql.get_table_id_map()

        node_summary = self._node_vs_cluster(all_ranges, node_id)
        table_breakdown = self._tables_on_node(
            all_ranges, table_map, node_id, limit,
        )
        top_ranges = self._top_ranges_on_node(
            all_ranges, table_map, node_id, limit,
        )
        zone_info = self._get_zone_configs()
        store_comparison = self._get_store_comparison()
        lease_prefs = self._get_lease_preferences(node_id)

        sections = [
            {
                "title": f"Node {node_id} vs Cluster Average",
                "headers": list(node_summary[0].keys()) if node_summary else [],
                "rows": node_summary,
            },
            {
                "title": f"Top Tables by Lease Count on Node {node_id}",
                "headers": list(table_breakdown[0].keys()) if table_breakdown else [],
                "rows": table_breakdown,
            },
            {
                "title": f"Largest Ranges on Node {node_id}",
                "headers": list(top_ranges[0].keys()) if top_ranges else [],
                "rows": top_ranges,
            },
            {
                "title": "Store Capacity Across All Nodes",
                "headers": list(store_comparison[0].keys()) if store_comparison else [],
                "rows": store_comparison,
            },
        ]

        if zone_info:
            sections.append({
                "title": "Zone Configs with Lease Preferences or Constraints",
                "headers": list(zone_info[0].keys()),
                "rows": zone_info,
            })

        if lease_prefs:
            sections.append({
                "title": f"Zone Configs Potentially Pinning to Node {node_id}",
                "headers": list(lease_prefs[0].keys()),
                "rows": lease_prefs,
            })

        total_leases = sum(
            1 for r in all_ranges if r.get("lease_holder") == node_id
        )
        total_ranges_cluster = len(all_ranges)
        node_count = len({r.get("lease_holder") for r in all_ranges})
        ideal = total_ranges_cluster / max(node_count, 1)

        return {
            "title": f"Node {node_id} Hotspot Analysis",
            "source": "sql",
            "sections": sections,
            "summary": {
                "target_node": node_id,
                "leases_on_node": total_leases,
                "total_ranges": total_ranges_cluster,
                "ideal_per_node": round(ideal, 1),
                "excess_leases": total_leases - round(ideal),
                "node_count": node_count,
                "pct_of_cluster": round(
                    total_leases / max(total_ranges_cluster, 1) * 100, 1,
                ),
            },
        }

    def _node_vs_cluster(
        self, ranges: list[dict[str, Any]], node_id: int,
    ) -> list[dict[str, Any]]:
        """Compare target node metrics against cluster averages."""
        per_node: dict[int, dict[str, float]] = defaultdict(
            lambda: {"leases": 0, "replicas": 0, "size_bytes": 0},
        )
        for r in ranges:
            lh = r.get("lease_holder")
            if lh is not None:
                per_node[lh]["leases"] += 1
                per_node[lh]["size_bytes"] += float(r.get("range_size", 0))
            for nid in r.get("replicas", []):
                per_node[nid]["replicas"] += 1

        if not per_node:
            return []

        n = len(per_node)
        avg_leases = sum(v["leases"] for v in per_node.values()) / n
        avg_replicas = sum(v["replicas"] for v in per_node.values()) / n
        avg_size = sum(v["size_bytes"] for v in per_node.values()) / n

        target = per_node.get(
            node_id, {"leases": 0, "replicas": 0, "size_bytes": 0},
        )

        def _pct(val: float, avg: float) -> str:
            p = round((val - avg) / max(avg, 1) * 100, 1)
            return f"+{p}%" if p >= 0 else f"{p}%"

        nk = f"node_{node_id}"
        return [
            {
                "metric": "Leases",
                nk: int(target["leases"]),
                "cluster_avg": round(avg_leases, 1),
                "delta": round(target["leases"] - avg_leases, 1),
                "delta_pct": _pct(target["leases"], avg_leases),
            },
            {
                "metric": "Replicas",
                nk: int(target["replicas"]),
                "cluster_avg": round(avg_replicas, 1),
                "delta": round(target["replicas"] - avg_replicas, 1),
                "delta_pct": _pct(target["replicas"], avg_replicas),
            },
            {
                "metric": "Data Size (MB)",
                nk: round(target["size_bytes"] / 1024 / 1024, 2),
                "cluster_avg": round(avg_size / 1024 / 1024, 2),
                "delta": round(
                    (target["size_bytes"] - avg_size) / 1024 / 1024, 2,
                ),
                "delta_pct": _pct(target["size_bytes"], avg_size),
            },
        ]

    def _tables_on_node(
        self,
        ranges: list[dict[str, Any]],
        table_map: dict[int, dict[str, str]],
        node_id: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Break down which tables have the most leases on this node."""
        table_leases: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"leases_on_node": 0, "leases_total": 0, "size_on_node": 0},
        )

        for r in ranges:
            sp = r.get("start_pretty", "")
            m = TABLE_KEY_RE.match(sp)
            if not m:
                continue
            tid = int(m.group(1))
            tinfo = table_map.get(tid, {})
            tname = tinfo.get("name", f"<id:{tid}>")
            db = tinfo.get("database_name", "")
            key = f"{db}.{tname}" if db else tname

            lh = r.get("lease_holder")
            table_leases[key]["leases_total"] += 1
            if lh == node_id:
                table_leases[key]["leases_on_node"] += 1
                table_leases[key]["size_on_node"] += float(
                    r.get("range_size", 0),
                )

        rows = []
        for tname, stats in sorted(
            table_leases.items(),
            key=lambda x: x[1]["leases_on_node"],
            reverse=True,
        ):
            if stats["leases_on_node"] == 0:
                continue
            pct = round(
                stats["leases_on_node"] / max(stats["leases_total"], 1) * 100, 1,
            )
            rows.append({
                "table": tname,
                "leases_on_node": stats["leases_on_node"],
                "leases_total": stats["leases_total"],
                "pct_on_node": f"{pct}%",
                "size_on_node_mb": round(
                    stats["size_on_node"] / 1024 / 1024, 2,
                ),
            })

        return rows[:limit]

    def _top_ranges_on_node(
        self,
        ranges: list[dict[str, Any]],
        table_map: dict[int, dict[str, str]],
        node_id: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Find the largest ranges whose lease is on this node."""
        node_ranges = [
            r for r in ranges if r.get("lease_holder") == node_id
        ]
        node_ranges.sort(key=lambda r: float(r.get("range_size", 0)), reverse=True)

        rows = []
        for r in node_ranges[:limit]:
            sp = r.get("start_pretty", "")
            m = TABLE_KEY_RE.match(sp)
            tid = int(m.group(1)) if m else None
            tinfo = table_map.get(tid, {}) if tid else {}
            rows.append({
                "range_id": r.get("range_id"),
                "table": tinfo.get("name", sp[:40]),
                "database": tinfo.get("database_name", ""),
                "size_mb": round(
                    float(r.get("range_size", 0)) / 1024 / 1024, 2,
                ),
                "replicas": r.get("replicas", []),
                "start_key": sp[:60],
            })
        return rows

    def _get_store_comparison(self) -> list[dict[str, Any]]:
        """Compare store capacity/usage across all nodes."""
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    node_id,
                    range_count,
                    lease_count,
                    round(capacity::numeric / 1024 / 1024 / 1024, 2) AS capacity_gb,
                    round(used::numeric / 1024 / 1024 / 1024, 2) AS used_gb,
                    round(available::numeric / 1024 / 1024 / 1024, 2) AS available_gb,
                    round(
                        used::numeric / NULLIF(capacity, 0)::numeric * 100, 1
                    ) AS used_pct
                FROM crdb_internal.kv_store_status
                ORDER BY node_id
                """
            )
        except Exception:
            logger.warning("kv_store_status query failed", exc_info=True)
            return []

    def _get_zone_configs(self) -> list[dict[str, Any]]:
        """Find zone configs that have lease preferences or constraints."""
        assert self.sql is not None
        try:
            rows = self.sql.execute(
                """
                SELECT
                    target,
                    raw_config_sql
                FROM crdb_internal.zones
                WHERE raw_config_sql LIKE '%lease_preferences%'
                   OR raw_config_sql LIKE '%constraints%'
                   OR raw_config_sql LIKE '%num_replicas%'
                ORDER BY target
                """
            )
            return [
                {"target": r["target"], "config": r["raw_config_sql"]}
                for r in rows
            ]
        except Exception:
            logger.warning("zones query failed", exc_info=True)
            return []

    def _get_lease_preferences(self, node_id: int) -> list[dict[str, Any]]:
        """Check if any zone configs might pin leases to this node."""
        assert self.sql is not None
        try:
            node_locality = self.sql.execute(
                "SELECT locality FROM crdb_internal.gossip_nodes "
                "WHERE node_id = %s",
                (node_id,),
            )
            if not node_locality:
                return []
            locality = str(node_locality[0].get("locality", ""))
            if not locality:
                return []

            zones = self.sql.execute(
                """
                SELECT target, raw_config_sql
                FROM crdb_internal.zones
                WHERE raw_config_sql LIKE '%lease_preferences%'
                """
            )
            matching = []
            for z in zones:
                config = str(z.get("raw_config_sql", ""))
                locality_parts = locality.split(",")
                for part in locality_parts:
                    kv = part.strip()
                    if kv and kv in config:
                        matching.append({
                            "target": z["target"],
                            "config": config,
                            "matched_locality": kv,
                        })
                        break
            return matching
        except Exception:
            logger.warning("lease preferences query failed", exc_info=True)
            return []

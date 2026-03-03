"""Cluster health analyzer - capacity, liveness, version skew."""

import logging
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

logger = logging.getLogger(__name__)


class ClusterHealthAnalyzer(BaseAnalyzer):
    def analyze(self, **kwargs: Any) -> dict[str, Any]:
        if not self.sql:
            msg = "Cluster health analysis requires a SQL connection."
            raise RuntimeError(msg)
        return self._analyze()

    def _analyze(self) -> dict[str, Any]:
        assert self.sql is not None
        nodes = self._get_node_status()
        store = self._get_store_status()
        version_info = self._get_version_info()
        capacity = self._get_capacity()

        return {
            "title": "Cluster Health Overview",
            "source": "sql",
            "sections": [
                {
                    "title": "Node Liveness",
                    "headers": list(nodes[0].keys()) if nodes else [],
                    "rows": nodes,
                },
                {
                    "title": "Store Status",
                    "headers": list(store[0].keys()) if store else [],
                    "rows": store,
                },
                {
                    "title": "Capacity Summary",
                    "headers": list(capacity[0].keys()) if capacity else [],
                    "rows": capacity,
                },
            ],
            "summary": {
                "node_count": len(nodes),
                "versions": list({n.get("build_tag", "?") for n in nodes}),
                "version_skew": len({n.get("build_tag", "?") for n in nodes}) > 1,
                **version_info,
            },
        }

    def _get_node_status(self) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    node_id,
                    address,
                    build_tag,
                    started_at,
                    is_live,
                    locality
                FROM crdb_internal.gossip_nodes
                ORDER BY node_id
                """
            )
        except Exception:
            logger.warning("gossip_nodes query failed", exc_info=True)
            return []

    def _get_store_status(self) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    node_id,
                    store_id,
                    capacity,
                    available,
                    used,
                    range_count,
                    lease_count
                FROM crdb_internal.kv_store_status
                ORDER BY node_id
                """
            )
        except Exception:
            logger.warning("kv_store_status query failed", exc_info=True)
            return []

    def _get_version_info(self) -> dict[str, Any]:
        assert self.sql is not None
        try:
            rows = self.sql.execute(
                "SELECT value FROM crdb_internal.node_build_info "
                "WHERE field = 'Version' LIMIT 1"
            )
            version = rows[0]["value"] if rows else "unknown"
            return {"crdb_version": version}
        except Exception:
            return {"crdb_version": "unknown"}

    def _get_capacity(self) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    node_id,
                    round(capacity::numeric / 1024 / 1024 / 1024, 2) AS capacity_gb,
                    round(available::numeric / 1024 / 1024 / 1024, 2) AS available_gb,
                    round(used::numeric / 1024 / 1024 / 1024, 2) AS used_gb,
                    round(used::numeric / NULLIF(capacity, 0)::numeric * 100, 1) AS used_pct,
                    range_count,
                    lease_count
                FROM crdb_internal.kv_store_status
                ORDER BY node_id
                """
            )
        except Exception:
            logger.warning("capacity query failed", exc_info=True)
            return []

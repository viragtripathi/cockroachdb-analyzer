"""Index usage analyzer - identifies unused and hot indexes."""

import logging
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

logger = logging.getLogger(__name__)


class IndexUsageAnalyzer(BaseAnalyzer):
    def analyze(self, limit: int = 50, **kwargs: Any) -> dict[str, Any]:
        if not self.sql:
            msg = "Index usage analysis requires a SQL connection."
            raise RuntimeError(msg)
        return self._analyze(limit)

    def _analyze(self, limit: int) -> dict[str, Any]:
        assert self.sql is not None
        unused = self._get_unused_indexes(limit)
        hot = self._get_hot_indexes(limit)

        return {
            "title": "Index Usage Analysis",
            "source": "sql",
            "sections": [
                {
                    "title": f"Unused Indexes (0 reads since stats reset) - top {limit}",
                    "headers": list(unused[0].keys()) if unused else [],
                    "rows": unused,
                },
                {
                    "title": f"Hottest Indexes by Total Reads - top {limit}",
                    "headers": list(hot[0].keys()) if hot else [],
                    "rows": hot,
                },
            ],
            "summary": {
                "unused_indexes": len(unused),
                "hot_indexes_shown": len(hot),
            },
        }

    def _get_unused_indexes(self, limit: int) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    ti.descriptor_name AS table_name,
                    ti.index_name,
                    ti.index_type,
                    ti.is_unique,
                    s.total_reads,
                    s.last_read
                FROM crdb_internal.index_usage_statistics s
                JOIN crdb_internal.table_indexes ti
                    ON s.table_id = ti.descriptor_id AND s.index_id = ti.index_id
                WHERE s.total_reads = 0
                    AND ti.index_type != 'primary'
                ORDER BY ti.descriptor_name, ti.index_name
                LIMIT %s
                """,
                (limit,),
            )
        except Exception:
            logger.warning("index_usage_statistics query failed", exc_info=True)
            return []

    def _get_hot_indexes(self, limit: int) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    ti.descriptor_name AS table_name,
                    ti.index_name,
                    ti.index_type,
                    s.total_reads,
                    s.last_read
                FROM crdb_internal.index_usage_statistics s
                JOIN crdb_internal.table_indexes ti
                    ON s.table_id = ti.descriptor_id AND s.index_id = ti.index_id
                WHERE s.total_reads > 0
                ORDER BY s.total_reads DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception:
            logger.warning("hot indexes query failed", exc_info=True)
            return []

"""Statement fingerprint analyzer - slow queries and execution patterns."""

import logging
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

logger = logging.getLogger(__name__)


class StmtFingerprintAnalyzer(BaseAnalyzer):
    def analyze(self, limit: int = 50, **kwargs: Any) -> dict[str, Any]:
        if not self.sql:
            msg = "Statement fingerprint analysis requires a SQL connection."
            raise RuntimeError(msg)
        return self._analyze(limit)

    def _analyze(self, limit: int) -> dict[str, Any]:
        assert self.sql is not None
        by_latency = self._top_by_latency(limit)
        by_exec = self._top_by_exec_count(limit)
        by_rows = self._top_by_rows_read(limit)

        return {
            "title": "Statement Fingerprint Analysis",
            "source": "sql",
            "sections": [
                {
                    "title": f"Slowest Statements by Mean Latency - top {limit}",
                    "headers": list(by_latency[0].keys()) if by_latency else [],
                    "rows": by_latency,
                },
                {
                    "title": f"Most Executed Statements - top {limit}",
                    "headers": list(by_exec[0].keys()) if by_exec else [],
                    "rows": by_exec,
                },
                {
                    "title": f"Highest Rows Read per Execution - top {limit}",
                    "headers": list(by_rows[0].keys()) if by_rows else [],
                    "rows": by_rows,
                },
            ],
            "summary": {
                "slow_stmts": len(by_latency),
                "high_exec_stmts": len(by_exec),
                "high_rows_stmts": len(by_rows),
            },
        }

    def _top_by_latency(self, limit: int) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    fingerprint_id,
                    metadata ->> 'query' AS query,
                    (statistics -> 'statistics' -> 'cnt')::int AS exec_count,
                    round((statistics -> 'statistics' -> 'runLat' ->> 'mean')::numeric, 6)
                        AS mean_latency_s,
                    round((statistics -> 'statistics' -> 'rowsRead' ->> 'mean')::numeric, 1)
                        AS mean_rows_read,
                    round((statistics -> 'statistics' -> 'bytesRead' ->> 'mean')::numeric, 0)
                        AS mean_bytes_read,
                    aggregated_ts
                FROM crdb_internal.statement_statistics
                WHERE (statistics -> 'statistics' -> 'runLat' ->> 'mean')::float > 0
                ORDER BY (statistics -> 'statistics' -> 'runLat' ->> 'mean')::float DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception:
            logger.warning("latency query failed", exc_info=True)
            return []

    def _top_by_exec_count(self, limit: int) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    fingerprint_id,
                    metadata ->> 'query' AS query,
                    (statistics -> 'statistics' -> 'cnt')::int AS exec_count,
                    round((statistics -> 'statistics' -> 'runLat' ->> 'mean')::numeric, 6)
                        AS mean_latency_s
                FROM crdb_internal.statement_statistics
                ORDER BY (statistics -> 'statistics' -> 'cnt')::int DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception:
            logger.warning("exec count query failed", exc_info=True)
            return []

    def _top_by_rows_read(self, limit: int) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    fingerprint_id,
                    metadata ->> 'query' AS query,
                    (statistics -> 'statistics' -> 'cnt')::int AS exec_count,
                    round((statistics -> 'statistics' -> 'rowsRead' ->> 'mean')::numeric, 1)
                        AS mean_rows_read,
                    round((statistics -> 'statistics' -> 'runLat' ->> 'mean')::numeric, 6)
                        AS mean_latency_s
                FROM crdb_internal.statement_statistics
                WHERE (statistics -> 'statistics' -> 'rowsRead' ->> 'mean')::float > 0
                ORDER BY (statistics -> 'statistics' -> 'rowsRead' ->> 'mean')::float DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception:
            logger.warning("rows read query failed", exc_info=True)
            return []

"""Statement errors analyzer - failing queries, retries, and per-node errors."""

import logging
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

logger = logging.getLogger(__name__)


class StmtErrorsAnalyzer(BaseAnalyzer):
    """Analyze statement failures, retry errors, and contention
    from crdb_internal.statement_statistics."""

    def analyze(self, limit: int = 20, **kwargs: Any) -> dict[str, Any]:
        if not self.sql:
            msg = "Statement errors analysis requires a SQL connection."
            raise RuntimeError(msg)
        since = str(kwargs.get("since", "1h"))
        return self._analyze(limit, since)

    def _analyze(self, limit: int, since: str) -> dict[str, Any]:
        assert self.sql is not None
        top_failures = self._top_failures(limit, since)
        retry_errors = self._retry_errors(limit, since)
        per_node = self._per_node_failures(limit)
        contention_stmts = self._contention_failures(limit, since)

        total_failures = sum(
            int(r.get("failure_count", 0)) for r in top_failures
        )
        total_retries = sum(
            int(r.get("max_retries", 0)) for r in retry_errors
        )

        sections = [
            {
                "title": f"Top Failing Statements (last {since})",
                "headers": (
                    list(top_failures[0].keys()) if top_failures else []
                ),
                "rows": top_failures,
            },
            {
                "title": f"Statements with Retry Errors (last {since})",
                "headers": (
                    list(retry_errors[0].keys()) if retry_errors else []
                ),
                "rows": retry_errors,
            },
            {
                "title": "Failures by Node",
                "headers": list(per_node[0].keys()) if per_node else [],
                "rows": per_node,
            },
            {
                "title": (
                    f"Failing Statements with High Contention "
                    f"(last {since})"
                ),
                "headers": (
                    list(contention_stmts[0].keys())
                    if contention_stmts else []
                ),
                "rows": contention_stmts,
            },
        ]

        has_issues = bool(top_failures or retry_errors)
        verdict = (
            f"{total_failures} statement failures detected"
            if has_issues else "No statement failures"
        )

        return {
            "title": "Statement Errors",
            "source": "sql",
            "sections": sections,
            "summary": {
                "verdict": verdict,
                "total_failing_fingerprints": len(top_failures),
                "total_failure_count": total_failures,
                "fingerprints_with_retries": len(retry_errors),
                "max_retries_seen": total_retries,
                "nodes_with_failures": len(per_node),
            },
        }

    # ------------------------------------------------------------------
    # Data collection
    # ------------------------------------------------------------------

    def _top_failures(
        self, limit: int, since: str,
    ) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    fingerprint_id,
                    metadata ->> 'query' AS query,
                    metadata ->> 'db' AS database,
                    (statistics -> 'statistics' ->> 'failureCount')::INT
                        AS failure_count,
                    (statistics -> 'statistics' ->> 'cnt')::INT
                        AS total_count,
                    round(
                        (statistics -> 'statistics' ->> 'failureCount')
                            ::NUMERIC
                        / NULLIF(
                            (statistics -> 'statistics' ->> 'cnt')::NUMERIC,
                            0
                        ) * 100, 1
                    ) AS failure_pct,
                    aggregated_ts
                FROM crdb_internal.statement_statistics
                WHERE (statistics -> 'statistics' ->> 'failureCount')::INT > 0
                  AND aggregated_ts > now() - %s::INTERVAL
                ORDER BY failure_count DESC
                LIMIT %s
                """,
                (since, limit),
            )
        except Exception:
            logger.warning("top failures query failed", exc_info=True)
            return []

    def _retry_errors(
        self, limit: int, since: str,
    ) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    fingerprint_id,
                    metadata ->> 'query' AS query,
                    metadata ->> 'db' AS database,
                    (statistics -> 'statistics' ->> 'cnt')::INT
                        AS total_count,
                    round(
                        (statistics -> 'statistics'
                            -> 'numRows' ->> 'mean')::NUMERIC, 1
                    ) AS avg_rows,
                    round(
                        (statistics -> 'statistics'
                            -> 'retries' ->> 'mean')::NUMERIC, 2
                    ) AS avg_retries,
                    (statistics -> 'statistics' ->> 'maxRetries')::INT
                        AS max_retries
                FROM crdb_internal.statement_statistics
                WHERE (statistics -> 'statistics' ->> 'maxRetries')::INT > 0
                  AND aggregated_ts > now() - %s::INTERVAL
                ORDER BY max_retries DESC
                LIMIT %s
                """,
                (since, limit),
            )
        except Exception:
            logger.warning("retry errors query failed", exc_info=True)
            return []

    def _per_node_failures(self, limit: int) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    node_id,
                    count(DISTINCT statement_id) AS failing_stmts,
                    sum(failure_count)::INT AS total_failures,
                    sum(count)::INT AS total_executions,
                    round(
                        sum(failure_count)::NUMERIC
                        / NULLIF(sum(count)::NUMERIC, 0) * 100, 2
                    ) AS failure_pct
                FROM crdb_internal.node_statement_statistics
                WHERE failure_count > 0
                GROUP BY node_id
                ORDER BY total_failures DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception as exc:
            if "virtual cluster" in str(exc):
                logger.info(
                    "node_statement_statistics not available "
                    "(virtual cluster)",
                )
            else:
                logger.warning(
                    "per-node failures query failed", exc_info=True,
                )
            return []

    def _contention_failures(
        self, limit: int, since: str,
    ) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    fingerprint_id,
                    metadata ->> 'query' AS query,
                    (statistics -> 'statistics' ->> 'failureCount')::INT
                        AS failure_count,
                    (statistics -> 'statistics' ->> 'cnt')::INT
                        AS total_count,
                    round(
                        (statistics -> 'execution_statistics'
                            -> 'contentionTime' ->> 'mean')::NUMERIC, 4
                    ) AS contention_mean_sec,
                    round(
                        (statistics -> 'statistics'
                            -> 'runLat' ->> 'mean')::NUMERIC, 4
                    ) AS mean_latency_sec
                FROM crdb_internal.statement_statistics
                WHERE (statistics -> 'statistics' ->> 'failureCount')::INT > 0
                  AND (statistics -> 'execution_statistics'
                      -> 'contentionTime' ->> 'mean')::FLOAT > 0
                  AND aggregated_ts > now() - %s::INTERVAL
                ORDER BY contention_mean_sec DESC
                LIMIT %s
                """,
                (since, limit),
            )
        except Exception:
            logger.warning(
                "contention failures query failed", exc_info=True,
            )
            return []

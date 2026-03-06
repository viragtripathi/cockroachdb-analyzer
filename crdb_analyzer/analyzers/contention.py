"""Contention analyzer - comprehensive lock contention analysis.

Approximates cr.node.sql.distsql.contended_queries.count using SQL
tables and provides actionable detail on which tables, indexes, and
statements are involved so application logic can be fixed.
"""

import logging
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

logger = logging.getLogger(__name__)

_VIRTUAL_CLUSTER_HINT = "unsupported within a virtual cluster"


def _is_virtual_cluster_error(exc: Exception) -> bool:
    return _VIRTUAL_CLUSTER_HINT in str(exc)


class ContentionAnalyzer(BaseAnalyzer):
    def analyze(self, limit: int = 20, **kwargs: Any) -> dict[str, Any]:
        if not self.sql:
            msg = "Contention analysis requires a SQL connection."
            raise RuntimeError(msg)
        since = str(kwargs.get("since", "1h"))
        return self._analyze(limit, since)

    def _analyze(self, limit: int, since: str) -> dict[str, Any]:
        assert self.sql is not None

        contended_stmts = self._top_contended_statements(limit, since)
        contended_tables = self._top_contended_tables(limit)
        contended_indexes = self._top_contended_indexes(limit)
        events_by_table = self._contention_events_by_table(limit)
        recent_events = self._recent_contention_events(limit, since)
        contended_summary = self._contended_queries_summary(since)

        total_events = sum(
            int(r.get("events", 0)) for r in events_by_table
        )
        total_contended = int(
            contended_summary[0].get("contended_stmts", 0)
        ) if contended_summary else 0

        severity = "NONE"
        if total_contended > 100 or total_events > 10000:
            severity = "HIGH"
        elif total_contended > 10 or total_events > 1000:
            severity = "MODERATE"
        elif total_contended > 0 or total_events > 0:
            severity = "LOW"

        verdict = (
            f"{severity} CONTENTION: {total_contended} contended "
            f"statement fingerprints, {total_events} contention events"
            if severity != "NONE"
            else "No contention detected"
        )

        sections = [
            {
                "title": f"Contended Queries Summary (last {since})",
                "headers": (
                    list(contended_summary[0].keys())
                    if contended_summary else []
                ),
                "rows": contended_summary,
            },
            {
                "title": (
                    f"Top Contended Statements (last {since})"
                ),
                "headers": (
                    list(contended_stmts[0].keys())
                    if contended_stmts else []
                ),
                "rows": contended_stmts,
            },
            {
                "title": "Top Contended Tables (cumulative)",
                "headers": (
                    list(contended_tables[0].keys())
                    if contended_tables else []
                ),
                "rows": contended_tables,
            },
            {
                "title": "Top Contended Indexes (cumulative)",
                "headers": (
                    list(contended_indexes[0].keys())
                    if contended_indexes else []
                ),
                "rows": contended_indexes,
            },
            {
                "title": "Contention Events by Table/Index (cumulative)",
                "headers": (
                    list(events_by_table[0].keys())
                    if events_by_table else []
                ),
                "rows": events_by_table,
            },
            {
                "title": (
                    f"Recent Contention Events with "
                    f"Waiting Query (last {since})"
                ),
                "headers": (
                    list(recent_events[0].keys())
                    if recent_events else []
                ),
                "rows": recent_events,
            },
        ]

        return {
            "title": "Contention Analysis",
            "source": "sql",
            "sections": sections,
            "summary": {
                "verdict": verdict,
                "severity": severity,
                "contended_stmt_fingerprints": total_contended,
                "total_contention_events": total_events,
                "tables_with_contention": len(contended_tables),
                "indexes_with_contention": len(contended_indexes),
            },
        }

    # ------------------------------------------------------------------
    # Data collection
    # ------------------------------------------------------------------

    def _contended_queries_summary(
        self, since: str,
    ) -> list[dict[str, Any]]:
        """Approximate cr.node.sql.distsql.contended_queries.count
        by counting distinct statement fingerprints that experienced
        contention in statement_statistics."""
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    count(*) AS contended_stmts,
                    round(
                        sum(
                            (statistics -> 'execution_statistics'
                                -> 'contentionTime' ->> 'mean')::NUMERIC
                            * (statistics -> 'statistics'
                                ->> 'cnt')::NUMERIC
                        ), 2
                    ) AS total_contention_sec,
                    round(
                        avg(
                            (statistics -> 'execution_statistics'
                                -> 'contentionTime' ->> 'mean')::NUMERIC
                        ), 4
                    ) AS avg_contention_sec_per_stmt,
                    sum(
                        (statistics -> 'statistics' ->> 'cnt')::INT
                    ) AS total_executions
                FROM crdb_internal.statement_statistics
                WHERE (statistics -> 'execution_statistics'
                    -> 'contentionTime' ->> 'mean')::FLOAT > 0
                  AND aggregated_ts > now() - %s::INTERVAL
                """,
                (since,),
            )
        except Exception:
            logger.warning(
                "contended queries summary failed", exc_info=True,
            )
            return []

    def _top_contended_statements(
        self, limit: int, since: str,
    ) -> list[dict[str, Any]]:
        """Statements with highest mean contention time."""
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
                        (statistics -> 'execution_statistics'
                            -> 'contentionTime' ->> 'mean')::NUMERIC, 4
                    ) AS mean_contention_sec,
                    round(
                        (statistics -> 'statistics'
                            -> 'runLat' ->> 'mean')::NUMERIC, 4
                    ) AS mean_latency_sec,
                    round(
                        (statistics -> 'execution_statistics'
                            -> 'contentionTime' ->> 'mean')::NUMERIC
                        / NULLIF(
                            (statistics -> 'statistics'
                                -> 'runLat' ->> 'mean')::NUMERIC, 0
                        ) * 100, 1
                    ) AS contention_pct_of_latency,
                    round(
                        (statistics -> 'execution_statistics'
                            -> 'contentionTime' ->> 'mean')::NUMERIC
                        * (statistics -> 'statistics'
                            ->> 'cnt')::NUMERIC, 2
                    ) AS total_contention_sec
                FROM crdb_internal.statement_statistics
                WHERE (statistics -> 'execution_statistics'
                    -> 'contentionTime' ->> 'mean')::FLOAT > 0
                  AND aggregated_ts > now() - %s::INTERVAL
                ORDER BY mean_contention_sec DESC
                LIMIT %s
                """,
                (since, limit),
            )
        except Exception:
            logger.warning(
                "top contended statements query failed", exc_info=True,
            )
            return []

    def _top_contended_tables(
        self, limit: int,
    ) -> list[dict[str, Any]]:
        """Tables with the most cumulative contention events."""
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    database_name,
                    schema_name,
                    table_name,
                    num_contention_events
                FROM crdb_internal.cluster_contended_tables
                ORDER BY num_contention_events DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception:
            logger.warning(
                "contended tables query failed", exc_info=True,
            )
            return []

    def _top_contended_indexes(
        self, limit: int,
    ) -> list[dict[str, Any]]:
        """Indexes with the most cumulative contention events."""
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    database_name,
                    schema_name,
                    table_name,
                    index_name,
                    num_contention_events
                FROM crdb_internal.cluster_contended_indexes
                ORDER BY num_contention_events DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception:
            logger.warning(
                "contended indexes query failed", exc_info=True,
            )
            return []

    def _contention_events_by_table(
        self, limit: int,
    ) -> list[dict[str, Any]]:
        """Contention events aggregated by table/index from
        transaction_contention_events with duration stats."""
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    database_name,
                    table_name,
                    index_name,
                    contention_type,
                    count(*) AS events,
                    sum(contention_duration) AS total_duration,
                    avg(contention_duration) AS avg_duration,
                    max(contention_duration) AS max_duration,
                    min(collection_ts) AS earliest,
                    max(collection_ts) AS latest
                FROM crdb_internal.transaction_contention_events
                GROUP BY
                    database_name, table_name,
                    index_name, contention_type
                ORDER BY events DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception:
            logger.warning(
                "contention events by table query failed",
                exc_info=True,
            )
            return []

    def _recent_contention_events(
        self, limit: int, since: str,
    ) -> list[dict[str, Any]]:
        """Recent contention events joined with the waiting statement
        fingerprint so the actual query is visible."""
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    ce.collection_ts,
                    ce.database_name,
                    ce.table_name,
                    ce.index_name,
                    ce.contention_type,
                    ce.contention_duration,
                    ts.metadata ->> 'query' AS waiting_query
                FROM crdb_internal.transaction_contention_events ce
                LEFT JOIN crdb_internal.statement_statistics ts
                  ON ce.waiting_stmt_fingerprint_id = ts.fingerprint_id
                  AND ts.aggregated_ts > now() - %s::INTERVAL
                WHERE ce.collection_ts > now() - %s::INTERVAL
                ORDER BY ce.contention_duration DESC
                LIMIT %s
                """,
                (since, since, limit),
            )
        except Exception:
            logger.warning(
                "recent contention events query failed", exc_info=True,
            )
            return []

"""Contention analyzer - detects lock contention and transaction conflicts."""

import logging
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

logger = logging.getLogger(__name__)


class ContentionAnalyzer(BaseAnalyzer):
    def analyze(self, limit: int = 50, **kwargs: Any) -> dict[str, Any]:
        if not self.sql:
            msg = "Contention analysis requires a SQL connection."
            raise RuntimeError(msg)
        return self._analyze_from_sql(limit)

    def _analyze_from_sql(self, limit: int) -> dict[str, Any]:
        assert self.sql is not None
        contention_rows = self.sql.get_contention_events(limit)
        stmt_rows = self.sql.get_statement_stats(limit)

        return {
            "title": "Contention Analysis",
            "source": "sql",
            "sections": [
                {
                    "title": "Top Contention Events",
                    "headers": list(contention_rows[0].keys()) if contention_rows else [],
                    "rows": contention_rows,
                },
                {
                    "title": "Slowest Statements",
                    "headers": list(stmt_rows[0].keys()) if stmt_rows else [],
                    "rows": stmt_rows,
                },
            ],
            "summary": {
                "contention_events": len(contention_rows),
                "slow_statements": len(stmt_rows),
            },
        }

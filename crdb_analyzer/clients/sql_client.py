"""SQL client for querying CockroachDB using psycopg 3 with retry and reconnect.

Compatible with CockroachDB v25.x and v26.x. In these versions,
crdb_internal.ranges has a simplified schema (no database_name, table_name,
queries_per_second, writes_per_second). Table mapping is done by parsing
start_pretty keys and joining with crdb_internal.tables.
"""

import contextlib
import logging
import re
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from crdb_analyzer.config import CRDBConfig
from crdb_analyzer.retry import retry_with_backoff

logger = logging.getLogger(__name__)

TABLE_KEY_RE = re.compile(r"^/Table/(\d+)/")

_ALLOWED_ORDER_COLUMNS = frozenset({
    "range_id", "range_size", "lease_holder", "start_pretty", "end_pretty",
})
_ALLOWED_DIRECTIONS = frozenset({"ASC", "DESC"})


class CRDBSqlClient:
    """SQL client with automatic retry and reconnect for CockroachDB."""

    def __init__(
        self,
        config: CRDBConfig,
        *,
        retry_max_attempts: int = 5,
        retry_initial_backoff: float = 0.1,
        retry_max_backoff: float = 10.0,
    ) -> None:
        self._config = config
        self._conn: psycopg.Connection[dict[str, Any]] | None = None
        self._crdb_version: str | None = None
        self.retry_max_attempts = retry_max_attempts
        self.retry_initial_backoff = retry_initial_backoff
        self.retry_max_backoff = retry_max_backoff

    def connect(self) -> None:
        safe_url = self._config.sql_url.split("@")[-1] if "@" in self._config.sql_url else "***"
        logger.debug("Connecting to: %s", safe_url)
        self._conn = psycopg.connect(
            self._config.sql_url,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=self._config.timeout,
        )
        self._detect_version()

    def _detect_version(self) -> None:
        rows = self._execute_raw("SELECT version()")
        if rows:
            self._crdb_version = str(rows[0].get("version", ""))
            logger.info("Connected to %s", self._crdb_version)

    def _ensure_connected(self) -> psycopg.Connection[dict[str, Any]]:
        if self._conn is None or self._conn.closed:
            self.connect()
        if self._conn is None:
            msg = "Failed to establish database connection"
            raise RuntimeError(msg)
        return self._conn

    def _reconnect_if_needed(self) -> psycopg.Connection[dict[str, Any]]:
        """Reconnect on stale/broken connections."""
        conn = self._ensure_connected()
        try:
            conn.execute("SELECT 1")
        except Exception:
            logger.warning("Connection lost, reconnecting...")
            with contextlib.suppress(Exception):
                conn.close()
            self._conn = None
            conn = self._ensure_connected()
        return conn

    @contextmanager
    def cursor(self) -> Generator[psycopg.Cursor[dict[str, Any]], None, None]:
        conn = self._reconnect_if_needed()
        cur = conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def _execute_raw(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> list[dict[str, Any]]:
        conn = self._ensure_connected()
        cur = conn.execute(query, params)
        if cur.description:
            return [dict(row) for row in cur.fetchall()]
        return []

    def execute(
        self, query: str, params: tuple[Any, ...] | None = None
    ) -> list[dict[str, Any]]:
        """Execute query with retry on transient CockroachDB errors."""

        @retry_with_backoff(
            max_retries=self.retry_max_attempts,
            initial_backoff=self.retry_initial_backoff,
            max_backoff=self.retry_max_backoff,
        )
        def _do_execute() -> list[dict[str, Any]]:
            conn = self._reconnect_if_needed()
            cur = conn.execute(query, params)
            if cur.description:
                return [dict(row) for row in cur.fetchall()]
            return []

        result: list[dict[str, Any]] = _do_execute()
        return result

    @property
    def crdb_version(self) -> str:
        if self._crdb_version is None:
            self._detect_version()
        return self._crdb_version or ""

    # ------------------------------------------------------------------
    # Queries compatible with CockroachDB v25.x / v26.x
    # crdb_internal.ranges columns: range_id, start_key, start_pretty,
    # end_key, end_pretty, replicas, replica_localities, voting_replicas,
    # non_voting_replicas, learner_replicas, split_enforced_until,
    # lease_holder, range_size, errors
    # ------------------------------------------------------------------

    def get_ranges(
        self, limit: int = 50, order_by: str = "range_size DESC"
    ) -> list[dict[str, Any]]:
        safe_order = self._validate_order_by(order_by)
        return self.execute(
            f"""
            SELECT
                range_id,
                start_pretty,
                end_pretty,
                lease_holder,
                replicas,
                voting_replicas,
                non_voting_replicas,
                range_size,
                range_size / 1024 / 1024 AS range_size_mb
            FROM crdb_internal.ranges
            ORDER BY {safe_order}
            LIMIT %s
            """,
            (limit,),
        )

    @staticmethod
    def _validate_order_by(order_by: str) -> str:
        """Validate and sanitise ORDER BY to prevent SQL injection."""
        parts = order_by.strip().split()
        col = parts[0].lower() if parts else "range_size"
        direction = parts[1].upper() if len(parts) > 1 else "DESC"
        if col not in _ALLOWED_ORDER_COLUMNS:
            msg = f"Invalid order column: {col!r}"
            raise ValueError(msg)
        if direction not in _ALLOWED_DIRECTIONS:
            msg = f"Invalid sort direction: {direction!r}"
            raise ValueError(msg)
        return f"{col} {direction}"

    def get_all_ranges(self) -> list[dict[str, Any]]:
        return self.execute(
            """
            SELECT
                range_id,
                start_pretty,
                end_pretty,
                lease_holder,
                replicas,
                range_size
            FROM crdb_internal.ranges
            """
        )

    def get_table_id_map(self) -> dict[int, dict[str, str]]:
        """Build table_id -> {name, database_name, schema_name} map."""
        rows = self.execute(
            """
            SELECT
                table_id,
                name,
                parent_id,
                database_name,
                schema_name
            FROM crdb_internal.tables
            WHERE drop_time IS NULL
            """
        )
        return {
            int(r["table_id"]): {
                "name": r.get("name", ""),
                "database_name": r.get("database_name", ""),
                "schema_name": r.get("schema_name", ""),
            }
            for r in rows
        }

    def get_node_stats(self) -> list[dict[str, Any]]:
        return self.execute(
            """
            SELECT
                lease_holder AS node_id,
                count(*) AS lease_count,
                sum(range_size) / 1024 / 1024 AS total_range_size_mb,
                avg(range_size) / 1024 / 1024 AS avg_range_size_mb,
                max(range_size) / 1024 / 1024 AS max_range_size_mb
            FROM crdb_internal.ranges
            GROUP BY lease_holder
            ORDER BY total_range_size_mb DESC
            """
        )

    def get_ranges_for_table(self, table_id: int) -> list[dict[str, Any]]:
        pattern = f"/Table/{table_id}/%"
        return self.execute(
            """
            SELECT
                range_id,
                start_pretty,
                end_pretty,
                lease_holder,
                replicas,
                range_size,
                range_size / 1024 / 1024 AS range_size_mb
            FROM crdb_internal.ranges
            WHERE start_pretty LIKE %s
            ORDER BY range_size DESC
            """,
            (pattern,),
        )

    def get_range_details(self, range_ids: list[int]) -> list[dict[str, Any]]:
        return self.execute(
            """
            SELECT
                range_id,
                start_pretty,
                end_pretty,
                replicas,
                voting_replicas,
                non_voting_replicas,
                learner_replicas,
                lease_holder,
                range_size,
                range_size / 1024 / 1024 AS range_size_mb,
                split_enforced_until
            FROM crdb_internal.ranges
            WHERE range_id = ANY(%s)
            ORDER BY range_size DESC
            """,
            (range_ids,),
        )

    def get_cluster_settings(self) -> list[dict[str, Any]]:
        return self.execute("SHOW ALL CLUSTER SETTINGS")

    @staticmethod
    def parse_table_id(start_pretty: str) -> int | None:
        """Extract table ID from a range start_pretty key like /Table/106/1."""
        m = TABLE_KEY_RE.match(start_pretty)
        return int(m.group(1)) if m else None

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "CRDBSqlClient":
        self.connect()
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

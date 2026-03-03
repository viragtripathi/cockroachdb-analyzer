"""CockroachDB-backed snapshot storage for centralized historical analysis."""

import json
import uuid
from datetime import datetime
from typing import Any

from crdb_analyzer.clients.sql_client import CRDBSqlClient
from crdb_analyzer.storage.base import SnapshotStore

_SCHEMA = """
CREATE TABLE IF NOT EXISTS crdb_analyzer.snapshots (
    snapshot_id   STRING PRIMARY KEY,
    snapshot_type STRING NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata      JSONB NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS crdb_analyzer.snapshot_rows (
    id          INT8 DEFAULT unique_rowid() PRIMARY KEY,
    snapshot_id STRING NOT NULL REFERENCES crdb_analyzer.snapshots(snapshot_id),
    row_data    JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snapshots_type_ts
    ON crdb_analyzer.snapshots(snapshot_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rows_snapshot
    ON crdb_analyzer.snapshot_rows(snapshot_id);
"""


class CRDBSnapshotStore(SnapshotStore):
    def __init__(self, sql_client: CRDBSqlClient) -> None:
        self._sql = sql_client
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        self._sql.execute("CREATE SCHEMA IF NOT EXISTS crdb_analyzer")
        for stmt in _SCHEMA.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                self._sql.execute(stmt)

    def save_snapshot(
        self,
        snapshot_type: str,
        data: list[dict[str, Any]],
        metadata: dict[str, Any] | None = None,
    ) -> str:
        sid = uuid.uuid4().hex[:12]
        meta = json.dumps(metadata or {}, default=str)
        self._sql.execute(
            "INSERT INTO crdb_analyzer.snapshots (snapshot_id, snapshot_type, metadata) "
            "VALUES (%s, %s, %s::jsonb)",
            (sid, snapshot_type, meta),
        )
        for row in data:
            self._sql.execute(
                "INSERT INTO crdb_analyzer.snapshot_rows (snapshot_id, row_data) "
                "VALUES (%s, %s::jsonb)",
                (sid, json.dumps(row, default=str)),
            )
        return sid

    def list_snapshots(
        self,
        snapshot_type: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if snapshot_type:
            clauses.append("snapshot_type = %s")
            params.append(snapshot_type)
        if since:
            clauses.append("created_at >= %s")
            params.append(since)
        if until:
            clauses.append("created_at <= %s")
            params.append(until)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._sql.execute(
            f"SELECT snapshot_id, snapshot_type, created_at::text, metadata "
            f"FROM crdb_analyzer.snapshots {where} ORDER BY created_at DESC LIMIT %s",
            (*params, limit),
        )

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        rows = self._sql.execute(
            "SELECT snapshot_id, snapshot_type, created_at::text, metadata "
            "FROM crdb_analyzer.snapshots WHERE snapshot_id = %s",
            (snapshot_id,),
        )
        return rows[0] if rows else None

    def get_snapshot_data(self, snapshot_id: str) -> list[dict[str, Any]]:
        rows = self._sql.execute(
            "SELECT row_data FROM crdb_analyzer.snapshot_rows "
            "WHERE snapshot_id = %s ORDER BY id",
            (snapshot_id,),
        )
        return [
            r["row_data"] if isinstance(r["row_data"], dict)
            else json.loads(r["row_data"])
            for r in rows
        ]

    def delete_snapshot(self, snapshot_id: str) -> None:
        """Delete a snapshot and its row data."""
        self._sql.execute(
            "DELETE FROM crdb_analyzer.snapshot_rows "
            "WHERE snapshot_id = %s", (snapshot_id,),
        )
        self._sql.execute(
            "DELETE FROM crdb_analyzer.snapshots "
            "WHERE snapshot_id = %s", (snapshot_id,),
        )

    def close(self) -> None:
        """Close the underlying SQL client connection."""
        self._sql.close()

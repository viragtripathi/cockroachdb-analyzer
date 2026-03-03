"""SQLite-backed snapshot storage for local historical analysis."""

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from crdb_analyzer.storage.base import SnapshotStore

_DEFAULT_DB = Path.home() / ".crdb-analyzer" / "snapshots.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id   TEXT PRIMARY KEY,
    snapshot_type TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    metadata      TEXT NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS snapshot_rows (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id),
    row_data    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snapshots_type_ts
    ON snapshots(snapshot_type, created_at);
CREATE INDEX IF NOT EXISTS idx_rows_snapshot
    ON snapshot_rows(snapshot_id);
"""


class SQLiteSnapshotStore(SnapshotStore):
    def __init__(self, db_path: str | Path | None = None) -> None:
        self._path = Path(db_path) if db_path else _DEFAULT_DB
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path))
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)

    def save_snapshot(
        self,
        snapshot_type: str,
        data: list[dict[str, Any]],
        metadata: dict[str, Any] | None = None,
    ) -> str:
        sid = uuid.uuid4().hex[:12]
        now = datetime.now(tz=timezone.utc).isoformat()
        meta = json.dumps(metadata or {}, default=str)
        self._conn.execute(
            "INSERT INTO snapshots "
            "(snapshot_id, snapshot_type, created_at, metadata) VALUES (?,?,?,?)",
            (sid, snapshot_type, now, meta),
        )
        self._conn.executemany(
            "INSERT INTO snapshot_rows (snapshot_id, row_data) VALUES (?,?)",
            [(sid, json.dumps(row, default=str)) for row in data],
        )
        self._conn.commit()
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
            clauses.append("snapshot_type = ?")
            params.append(snapshot_type)
        if since:
            clauses.append("created_at >= ?")
            params.append(since.isoformat())
        if until:
            clauses.append("created_at <= ?")
            params.append(until.isoformat())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self._conn.execute(
            f"SELECT * FROM snapshots {where} ORDER BY created_at DESC LIMIT ?",
            [*params, limit],
        ).fetchall()
        return [
            {
                "snapshot_id": r["snapshot_id"],
                "snapshot_type": r["snapshot_type"],
                "created_at": r["created_at"],
                "metadata": json.loads(r["metadata"]),
            }
            for r in rows
        ]

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
        ).fetchone()
        if not row:
            return None
        return {
            "snapshot_id": row["snapshot_id"],
            "snapshot_type": row["snapshot_type"],
            "created_at": row["created_at"],
            "metadata": json.loads(row["metadata"]),
        }

    def get_snapshot_data(self, snapshot_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT row_data FROM snapshot_rows WHERE snapshot_id = ? ORDER BY id",
            (snapshot_id,),
        ).fetchall()
        return [json.loads(r["row_data"]) for r in rows]

    def delete_snapshot(self, snapshot_id: str) -> None:
        """Delete a snapshot and its row data."""
        self._conn.execute(
            "DELETE FROM snapshot_rows WHERE snapshot_id = ?", (snapshot_id,),
        )
        self._conn.execute(
            "DELETE FROM snapshots WHERE snapshot_id = ?", (snapshot_id,),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

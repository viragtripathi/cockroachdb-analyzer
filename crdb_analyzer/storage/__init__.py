"""Snapshot storage backends for historical analysis."""

from crdb_analyzer.storage.base import SnapshotStore
from crdb_analyzer.storage.sqlite_store import SQLiteSnapshotStore

__all__ = ["SQLiteSnapshotStore", "SnapshotStore"]

"""Tests for snapshot storage."""

from crdb_analyzer.storage.sqlite_store import SQLiteSnapshotStore


class TestSQLiteSnapshotStore:
    def test_save_and_retrieve(self, tmp_path):
        store = SQLiteSnapshotStore(tmp_path / "test.db")
        data = [{"range_id": 1, "size_mb": 100}, {"range_id": 2, "size_mb": 200}]
        sid = store.save_snapshot("hot-ranges", data, {"title": "test"})

        assert len(sid) == 12

        snaps = store.list_snapshots()
        assert len(snaps) == 1
        assert snaps[0]["snapshot_type"] == "hot-ranges"
        assert snaps[0]["snapshot_id"] == sid

        meta = store.get_snapshot(sid)
        assert meta is not None
        assert meta["metadata"]["title"] == "test"

        rows = store.get_snapshot_data(sid)
        assert len(rows) == 2
        assert rows[0]["range_id"] == 1

        store.close()

    def test_list_filter_by_type(self, tmp_path):
        store = SQLiteSnapshotStore(tmp_path / "test.db")
        store.save_snapshot("hot-ranges", [{"a": 1}])
        store.save_snapshot("hot-nodes", [{"b": 2}])
        store.save_snapshot("hot-ranges", [{"c": 3}])

        assert len(store.list_snapshots(snapshot_type="hot-ranges")) == 2
        assert len(store.list_snapshots(snapshot_type="hot-nodes")) == 1

        store.close()

    def test_get_nonexistent(self, tmp_path):
        store = SQLiteSnapshotStore(tmp_path / "test.db")
        assert store.get_snapshot("nonexistent") is None
        assert store.get_snapshot_data("nonexistent") == []
        store.close()

    def test_multiple_snapshots_ordering(self, tmp_path):
        store = SQLiteSnapshotStore(tmp_path / "test.db")
        sid1 = store.save_snapshot("test", [{"v": 1}])
        sid2 = store.save_snapshot("test", [{"v": 2}])

        snaps = store.list_snapshots()
        assert snaps[0]["snapshot_id"] == sid2
        assert snaps[1]["snapshot_id"] == sid1

        store.close()

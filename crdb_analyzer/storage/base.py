"""Abstract base for snapshot storage backends."""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any


class SnapshotStore(ABC):
    @abstractmethod
    def save_snapshot(
        self, snapshot_type: str, data: list[dict[str, Any]], metadata: dict[str, Any] | None = None
    ) -> str:
        """Save a snapshot. Returns snapshot_id."""
        ...

    @abstractmethod
    def list_snapshots(
        self,
        snapshot_type: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List available snapshots."""
        ...

    @abstractmethod
    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        """Retrieve a snapshot by ID."""
        ...

    @abstractmethod
    def get_snapshot_data(self, snapshot_id: str) -> list[dict[str, Any]]:
        """Retrieve the row data for a snapshot."""
        ...

    @abstractmethod
    def delete_snapshot(self, snapshot_id: str) -> None:
        """Delete a snapshot and its row data."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Release resources held by the store."""
        ...

"""Base analyzer class providing common interface and utilities."""

from abc import ABC, abstractmethod
from typing import Any

from crdb_analyzer.clients.http_client import CRDBHttpClient
from crdb_analyzer.clients.sql_client import CRDBSqlClient


class BaseAnalyzer(ABC):
    """Base class for all analyzers.

    Analyzers can work with either an SQL client (preferred) or HTTP client,
    or both. They can also process pre-loaded data from files.
    """

    def __init__(
        self,
        sql_client: CRDBSqlClient | None = None,
        http_client: CRDBHttpClient | None = None,
    ) -> None:
        self.sql = sql_client
        self.http = http_client

    @abstractmethod
    def analyze(self, **kwargs: Any) -> dict[str, Any]:
        """Run analysis and return structured results.

        Returns a dict with at least:
          - "title": human-readable title
          - "headers": list of column headers
          - "rows": list of row dicts
          - "summary": optional summary dict
        """
        ...

    @staticmethod
    def _extract_qps(range_info: dict[str, Any]) -> float:
        for node in range_info.get("nodes", []):
            node_id = node.get("nodeId")
            try:
                lease_node = node["range"]["state"]["state"]["lease"]["replica"]["nodeId"]
                if node_id == lease_node:
                    return float(node["range"]["stats"].get("queriesPerSecond", 0))
            except (KeyError, TypeError):
                continue
        return 0.0

    @staticmethod
    def _extract_wps(range_info: dict[str, Any]) -> float:
        for node in range_info.get("nodes", []):
            node_id = node.get("nodeId")
            try:
                lease_node = node["range"]["state"]["state"]["lease"]["replica"]["nodeId"]
                if node_id == lease_node:
                    return float(node["range"]["stats"].get("writesPerSecond", 0))
            except (KeyError, TypeError):
                continue
        return 0.0

    @staticmethod
    def _extract_live_count(range_info: dict[str, Any]) -> int:
        for node in range_info.get("nodes", []):
            node_id = node.get("nodeId")
            try:
                lease_node = node["range"]["state"]["state"]["lease"]["replica"]["nodeId"]
                if node_id == lease_node:
                    return int(
                        node["range"]["state"]["state"]["stats"].get("liveCount", 0)
                    )
            except (KeyError, TypeError):
                continue
        return 0

    @staticmethod
    def _extract_leaseholder(range_info: dict[str, Any]) -> int | None:
        try:
            value: int = range_info["nodes"][0]["range"]["state"]["state"]["lease"][
                "replica"
            ]["nodeId"]
            return value
        except (KeyError, TypeError, IndexError):
            return None

    @staticmethod
    def _extract_nodes(range_info: dict[str, Any]) -> list[int]:
        return [n.get("nodeId") for n in range_info.get("nodes", [])]

    @staticmethod
    def _extract_start_key(range_info: dict[str, Any]) -> str:
        try:
            value: str = range_info["nodes"][0]["range"]["span"]["startKey"]
            return value
        except (KeyError, TypeError, IndexError):
            return ""

    @staticmethod
    def _extract_end_key(range_info: dict[str, Any]) -> str:
        try:
            value: str = range_info["nodes"][0]["range"]["span"]["endKey"]
            return value
        except (KeyError, TypeError, IndexError):
            return ""

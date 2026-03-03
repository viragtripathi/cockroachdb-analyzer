"""HTTP client for CockroachDB admin API endpoints.

Fetches data from endpoints like /_status/ranges, /_status/nodes,
/_admin/v1/health, etc.
"""

import json
import logging
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from crdb_analyzer.config import CRDBConfig

logger = logging.getLogger(__name__)


class CRDBHttpClient:
    """HTTP client for querying CockroachDB admin API endpoints.

    Uses the admin UI's HTTP API to fetch range, node, and health data.
    Supports TLS client certificates and basic authentication.
    """

    def __init__(self, config: CRDBConfig) -> None:
        if not config.admin_url:
            msg = "admin_url must be configured to use the HTTP client"
            raise ValueError(msg)
        self._config = config
        self._session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        self._session.headers.update(config.http_headers)
        if config.ca_cert:
            self._session.verify = config.ca_cert
        if config.client_cert and config.client_key:
            self._session.cert = (config.client_cert, config.client_key)
        if config.admin_user and config.admin_password:
            self._authenticate(config.admin_user, config.admin_password)

    def _authenticate(self, username: str, password: str) -> None:
        """Authenticate via CockroachDB's session login endpoint.

        CockroachDB's DB Console uses session cookies, not basic auth.
        POST to /api/v2/login/ (v22.2+) or /_admin/v1/login (older).
        Self-hosted clusters support this; CockroachDB Cloud does not.
        """
        base = self._config.admin_url.rstrip("/")
        login_endpoints = [
            f"{base}/api/v2/login/",
            f"{base}/_admin/v1/login",
        ]
        last_err: Exception | None = None
        for url in login_endpoints:
            try:
                logger.debug("Authenticating via %s", url)
                resp = self._session.post(
                    url,
                    json={"username": username, "password": password},
                    timeout=self._config.timeout,
                )
                if resp.status_code == 200:
                    logger.info("Authenticated successfully via %s", url)
                    return
                last_err = Exception(
                    f"{resp.status_code} from {url}: {resp.text[:200]}"
                )
            except requests.RequestException as exc:
                last_err = exc
                logger.debug("Login endpoint %s failed: %s", url, exc)
                continue

        is_cloud = "cockroachlabs.cloud" in base
        hint = (
            "\n\nCockroachDB Cloud does not support admin API authentication."
            "\nUse --sql-url instead (all analyzers work over SQL):"
            "\n  crdb-analyzer --sql-url \"postgresql://myuser:changeme@host:26257/db\" hot-ranges"
            if is_cloud
            else "\n\nCheck your --admin-user and --admin-password values."
        )
        msg = f"Admin UI authentication failed: {last_err}{hint}"
        raise RuntimeError(msg)

    @property
    def base_url(self) -> str:
        return self._config.admin_url.rstrip("/")

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = f"{self.base_url}{path}"
        logger.debug("GET %s", url)
        resp = self._session.get(url, params=params, timeout=self._config.timeout)
        resp.raise_for_status()
        return resp.json()

    def get_ranges(self) -> dict[str, Any]:
        """Fetch all range data from /_status/ranges."""
        result: dict[str, Any] = self._get("/_status/ranges")
        return result

    def get_nodes(self) -> dict[str, Any]:
        """Fetch node status from /_status/nodes."""
        result: dict[str, Any] = self._get("/_status/nodes")
        return result

    def get_node_ranges(self, node_id: int) -> dict[str, Any]:
        """Fetch ranges for a specific node."""
        result: dict[str, Any] = self._get(f"/_status/ranges/{node_id}")
        return result

    def get_health(self) -> dict[str, Any]:
        """Check cluster health via /_admin/v1/health."""
        result: dict[str, Any] = self._get("/_admin/v1/health")
        return result

    def get_hot_ranges(self, node_id: int | None = None) -> dict[str, Any]:
        """Fetch hot ranges report from /_status/hotranges."""
        path = f"/_status/hotranges/{node_id}" if node_id else "/_status/hotranges"
        result: dict[str, Any] = self._get(path)
        return result

    def load_ranges_from_file(self, path: str | Path) -> dict[str, Any]:
        """Load ranges data from a previously saved JSON file."""
        try:
            result: dict[str, Any] = json.loads(Path(path).read_text())
        except (OSError, json.JSONDecodeError) as exc:
            msg = f"Failed to load ranges from {path}: {exc}"
            raise ValueError(msg) from exc
        return result

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "CRDBHttpClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

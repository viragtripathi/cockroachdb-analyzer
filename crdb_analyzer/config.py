"""Connection configuration for CockroachDB clusters.

Supports local (insecure) and cloud (TLS) deployments via:
- CLI flags (--sql-url, --admin-url)
- Environment variables (CRDB_SQL_URL, CRDB_ADMIN_URL)
- YAML config file (~/.crdb-analyzer.yaml or ./crdb-analyzer.yaml)
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class CRDBConfig:
    """Connection configuration for CockroachDB clusters."""

    sql_url: str = ""
    admin_url: str = ""
    admin_user: str | None = None
    admin_password: str | None = field(default=None, repr=False)
    ca_cert: str | None = None
    client_cert: str | None = None
    client_key: str | None = field(default=None, repr=False)
    timeout: int = 30
    http_headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "CRDBConfig":
        return cls(
            sql_url=os.environ.get("CRDB_SQL_URL", ""),
            admin_url=os.environ.get("CRDB_ADMIN_URL", ""),
            admin_user=os.environ.get("CRDB_ADMIN_USER"),
            admin_password=os.environ.get("CRDB_ADMIN_PASSWORD"),
            ca_cert=os.environ.get("CRDB_CA_CERT"),
            client_cert=os.environ.get("CRDB_CLIENT_CERT"),
            client_key=os.environ.get("CRDB_CLIENT_KEY"),
        )

    @classmethod
    def from_file(cls, path: str | Path) -> "CRDBConfig":
        """Load configuration from a YAML file."""
        try:
            data = yaml.safe_load(Path(path).read_text()) or {}
        except (OSError, yaml.YAMLError) as exc:
            msg = f"Failed to load config from {path}: {exc}"
            raise ValueError(msg) from exc
        if not isinstance(data, dict):
            msg = f"Config file {path} must contain a YAML mapping"
            raise ValueError(msg)
        return cls(
            sql_url=data.get("sql_url", ""),
            admin_url=data.get("admin_url", ""),
            admin_user=data.get("admin_user"),
            admin_password=data.get("admin_password"),
            ca_cert=data.get("ca_cert"),
            client_cert=data.get("client_cert"),
            client_key=data.get("client_key"),
            timeout=data.get("timeout", 30),
        )

    @classmethod
    def resolve(
        cls,
        sql_url: str | None = None,
        admin_url: str | None = None,
        config_file: str | None = None,
    ) -> "CRDBConfig":
        """Resolve config with precedence: CLI flags > env vars > config file."""
        config = cls()

        for candidate in [
            Path.home() / ".crdb-analyzer.yaml",
            Path("crdb-analyzer.yaml"),
        ]:
            if candidate.exists():
                config = cls.from_file(candidate)
                break

        if config_file:
            cfg_path = Path(config_file)
            if not cfg_path.exists():
                msg = f"Config file not found: {cfg_path}"
                raise FileNotFoundError(msg)
            config = cls.from_file(cfg_path)

        env_config = cls.from_env()
        if env_config.sql_url:
            config.sql_url = env_config.sql_url
        if env_config.admin_url:
            config.admin_url = env_config.admin_url
        if env_config.admin_user:
            config.admin_user = env_config.admin_user
        if env_config.admin_password:
            config.admin_password = env_config.admin_password
        if env_config.ca_cert:
            config.ca_cert = env_config.ca_cert
        if env_config.client_cert:
            config.client_cert = env_config.client_cert
        if env_config.client_key:
            config.client_key = env_config.client_key

        if sql_url:
            config.sql_url = sql_url
        if admin_url:
            config.admin_url = admin_url

        return config

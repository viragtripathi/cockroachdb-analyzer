"""Tests for configuration resolution."""


import yaml

from crdb_analyzer.config import CRDBConfig


class TestCRDBConfig:
    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("CRDB_SQL_URL", "postgresql://root@localhost:26257/defaultdb")
        monkeypatch.setenv("CRDB_ADMIN_URL", "http://localhost:8080")
        config = CRDBConfig.from_env()
        assert config.sql_url == "postgresql://root@localhost:26257/defaultdb"
        assert config.admin_url == "http://localhost:8080"

    def test_from_file(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(yaml.dump({
            "sql_url": "postgresql://user@remote:26257/mydb",
            "admin_url": "https://remote:8080",
            "timeout": 60,
        }))
        config = CRDBConfig.from_file(str(cfg_file))
        assert config.sql_url == "postgresql://user@remote:26257/mydb"
        assert config.timeout == 60

    def test_resolve_precedence(self, monkeypatch, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(yaml.dump({"sql_url": "from-file", "admin_url": "from-file"}))
        monkeypatch.setenv("CRDB_SQL_URL", "from-env")
        config = CRDBConfig.resolve(
            sql_url="from-cli",
            config_file=str(cfg_file),
        )
        assert config.sql_url == "from-cli"
        assert config.admin_url == "from-file"

    def test_defaults(self):
        config = CRDBConfig()
        assert config.sql_url == ""
        assert config.admin_url == ""
        assert config.timeout == 30

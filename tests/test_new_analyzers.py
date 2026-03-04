"""Tests for the newer analyzers that require SQL (mocked)."""

from unittest.mock import MagicMock

from crdb_analyzer.analyzers.cluster_health import ClusterHealthAnalyzer
from crdb_analyzer.analyzers.contention import ContentionAnalyzer
from crdb_analyzer.analyzers.index_usage import IndexUsageAnalyzer
from crdb_analyzer.analyzers.job_status import JobStatusAnalyzer
from crdb_analyzer.analyzers.lease_balance import LeaseBalanceAnalyzer
from crdb_analyzer.analyzers.node_hotspot import NodeHotspotAnalyzer
from crdb_analyzer.analyzers.rebalance_status import RebalanceStatusAnalyzer
from crdb_analyzer.analyzers.stmt_fingerprints import StmtFingerprintAnalyzer


def _make_sql_client(**overrides):
    sql = MagicMock()
    sql.execute.return_value = overrides.get("execute_result", [])
    sql.get_all_ranges.return_value = overrides.get("ranges", [])
    sql.get_table_id_map.return_value = overrides.get("table_map", {})
    return sql


class TestIndexUsageAnalyzer:
    def test_analyze_returns_sections(self):
        sql = _make_sql_client(execute_result=[
            {"table_name": "users", "index_name": "idx_email", "index_type": "secondary",
             "is_unique": False, "total_reads": 0, "last_read": None},
        ])
        analyzer = IndexUsageAnalyzer(sql_client=sql)
        result = analyzer.analyze(limit=10)
        assert result["title"] == "Index Usage Analysis"
        assert len(result["sections"]) == 2
        assert "unused_indexes" in result["summary"]

    def test_requires_sql(self):
        analyzer = IndexUsageAnalyzer()
        try:
            analyzer.analyze()
            raise AssertionError("Expected RuntimeError")
        except RuntimeError:
            pass


class TestLeaseBalanceAnalyzer:
    def test_analyze_returns_distribution(self):
        sql = _make_sql_client(
            ranges=[
                {"range_id": 1, "start_pretty": "/Table/100/1", "end_pretty": "/Table/100/2",
                 "lease_holder": 1, "replicas": [1, 2, 3], "range_size": 1048576},
                {"range_id": 2, "start_pretty": "/Table/100/2", "end_pretty": "/Table/100/3",
                 "lease_holder": 2, "replicas": [1, 2, 3], "range_size": 2097152},
                {"range_id": 3, "start_pretty": "/Table/200/1", "end_pretty": "/Table/200/2",
                 "lease_holder": 1, "replicas": [1, 2], "range_size": 524288},
            ],
            table_map={
                100: {"name": "orders", "database_name": "shop", "schema_name": "public"},
                200: {"name": "users", "database_name": "shop", "schema_name": "public"},
            },
        )
        analyzer = LeaseBalanceAnalyzer(sql_client=sql)
        result = analyzer.analyze()
        assert result["title"] == "Replica & Lease Balance"
        assert len(result["sections"]) == 2
        assert result["summary"]["total_ranges"] == 3
        assert result["summary"]["node_count"] == 3

    def test_requires_sql(self):
        analyzer = LeaseBalanceAnalyzer()
        try:
            analyzer.analyze()
            raise AssertionError("Expected RuntimeError")
        except RuntimeError:
            pass


class TestStmtFingerprintAnalyzer:
    def test_analyze_returns_sections(self):
        sql = _make_sql_client(execute_result=[
            {"fingerprint_id": "abc", "query": "SELECT 1",
             "exec_count": 100, "mean_latency_s": 0.001},
        ])
        analyzer = StmtFingerprintAnalyzer(sql_client=sql)
        result = analyzer.analyze(limit=5)
        assert result["title"] == "Statement Fingerprint Analysis"
        assert len(result["sections"]) == 3

    def test_requires_sql(self):
        analyzer = StmtFingerprintAnalyzer()
        try:
            analyzer.analyze()
            raise AssertionError("Expected RuntimeError")
        except RuntimeError:
            pass


class TestClusterHealthAnalyzer:
    def test_analyze_returns_sections(self):
        sql = _make_sql_client(execute_result=[
            {"node_id": 1, "address": "localhost:26257", "build_tag": "v25.2.0",
             "started_at": "2024-01-01", "is_live": True, "locality": ""},
        ])
        analyzer = ClusterHealthAnalyzer(sql_client=sql)
        result = analyzer.analyze()
        assert result["title"] == "Cluster Health Overview"
        assert len(result["sections"]) == 3
        assert "node_count" in result["summary"]

    def test_requires_sql(self):
        analyzer = ClusterHealthAnalyzer()
        try:
            analyzer.analyze()
            raise AssertionError("Expected RuntimeError")
        except RuntimeError:
            pass


class TestContentionAnalyzer:
    def test_analyze_returns_results(self):
        sql = _make_sql_client(execute_result=[
            {"database_name": "mydb", "table_name": "orders",
             "index_name": "primary", "num_contention_events": 42,
             "cumulative_contention_time": "1.5s"},
        ])
        analyzer = ContentionAnalyzer(sql_client=sql)
        result = analyzer.analyze(limit=10)
        assert "Contention" in result["title"]

    def test_requires_sql(self):
        analyzer = ContentionAnalyzer()
        try:
            analyzer.analyze()
            raise AssertionError("Expected RuntimeError")
        except RuntimeError:
            pass


class TestNodeHotspotAnalyzer:
    def test_analyze_returns_sections(self):
        sql = _make_sql_client(
            ranges=[
                {"range_id": 1, "start_pretty": "/Table/100/1",
                 "end_pretty": "/Table/100/2", "lease_holder": 2,
                 "replicas": [1, 2, 3], "range_size": 10485760},
                {"range_id": 2, "start_pretty": "/Table/100/2",
                 "end_pretty": "/Table/100/3", "lease_holder": 2,
                 "replicas": [1, 2, 3], "range_size": 20971520},
                {"range_id": 3, "start_pretty": "/Table/200/1",
                 "end_pretty": "/Table/200/2", "lease_holder": 1,
                 "replicas": [1, 2], "range_size": 5242880},
            ],
            table_map={
                100: {"name": "orders", "database_name": "shop", "schema_name": "public"},
                200: {"name": "users", "database_name": "shop", "schema_name": "public"},
            },
            execute_result=[],
        )
        analyzer = NodeHotspotAnalyzer(sql_client=sql)
        result = analyzer.analyze(node_id=2, limit=10)
        assert result["title"] == "Node 2 Hotspot Analysis"
        assert len(result["sections"]) >= 4
        assert result["summary"]["target_node"] == 2
        assert result["summary"]["leases_on_node"] == 2

    def test_requires_sql(self):
        analyzer = NodeHotspotAnalyzer()
        try:
            analyzer.analyze(node_id=1)
            raise AssertionError("Expected RuntimeError")
        except RuntimeError:
            pass


class TestRebalanceStatusAnalyzer:
    def test_rebalancing_complete(self):
        sql = _make_sql_client(execute_result=[
            {"zone_id": 0, "sub_zone_id": 0, "under_replicated_ranges": 0,
             "over_replicated_ranges": 0, "unavailable_ranges": 0,
             "total_ranges": 100},
        ])
        analyzer = RebalanceStatusAnalyzer(sql_client=sql)
        result = analyzer.analyze(limit=10)
        assert result["title"] == "Rebalance Status"
        assert len(result["sections"]) == 5
        assert "verdict" in result["summary"]

    def test_rebalancing_in_progress(self):
        sql = _make_sql_client(execute_result=[
            {"zone_id": 0, "sub_zone_id": 0, "under_replicated_ranges": 5,
             "over_replicated_ranges": 2, "unavailable_ranges": 0,
             "total_ranges": 100},
        ])
        analyzer = RebalanceStatusAnalyzer(sql_client=sql)
        result = analyzer.analyze(limit=10)
        assert result["summary"]["verdict"] == "REBALANCING IN PROGRESS"

    def test_requires_sql(self):
        analyzer = RebalanceStatusAnalyzer()
        try:
            analyzer.analyze()
            raise AssertionError("Expected RuntimeError")
        except RuntimeError:
            pass


class TestJobStatusAnalyzer:
    def test_healthy_jobs(self):
        sql = _make_sql_client(execute_result=[])
        analyzer = JobStatusAnalyzer(sql_client=sql)
        result = analyzer.analyze(limit=10)
        assert result["title"] == "Job Status"
        assert result["summary"]["verdict"] == "ALL JOBS HEALTHY"
        assert result["summary"]["stuck_gc_jobs"] == 0

    def test_with_running_jobs(self):
        sql = _make_sql_client(execute_result=[
            {
                "job_id": 105, "job_type": "UPDATE TABLE METADATA CACHE",
                "description": "cache update", "status": "running",
                "running_status": None,
                "created": "2026-01-01T00:00:00Z",
                "modified": "2026-01-01T00:00:00Z",
                "fraction_completed": 0, "coordinator_id": 1,
            },
        ])
        analyzer = JobStatusAnalyzer(sql_client=sql)
        result = analyzer.analyze(limit=10)
        assert result["title"] == "Job Status"
        assert len(result["sections"]) == 5

    def test_requires_sql(self):
        analyzer = JobStatusAnalyzer()
        try:
            analyzer.analyze()
            raise AssertionError("Expected RuntimeError")
        except RuntimeError:
            pass

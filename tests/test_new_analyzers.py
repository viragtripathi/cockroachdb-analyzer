"""Tests for the newer analyzers that require SQL (mocked)."""

from unittest.mock import MagicMock

from crdb_analyzer.analyzers.cluster_health import ClusterHealthAnalyzer
from crdb_analyzer.analyzers.contention import ContentionAnalyzer
from crdb_analyzer.analyzers.index_usage import IndexUsageAnalyzer
from crdb_analyzer.analyzers.job_status import JobStatusAnalyzer
from crdb_analyzer.analyzers.lease_balance import LeaseBalanceAnalyzer
from crdb_analyzer.analyzers.node_hotspot import NodeHotspotAnalyzer
from crdb_analyzer.analyzers.rebalance_status import RebalanceStatusAnalyzer
from crdb_analyzer.analyzers.stmt_errors import StmtErrorsAnalyzer
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
    def test_no_contention(self):
        sql = _make_sql_client(execute_result=[])
        analyzer = ContentionAnalyzer(sql_client=sql)
        result = analyzer.analyze(limit=10)
        assert result["title"] == "Contention Analysis"
        assert result["summary"]["severity"] == "NONE"
        assert result["summary"]["contended_stmt_fingerprints"] == 0
        assert len(result["sections"]) == 6

    def test_with_contention(self):
        analyzer = ContentionAnalyzer(sql_client=MagicMock())
        analyzer.sql.execute.side_effect = [
            # _top_contended_statements
            [{"fingerprint_id": "abc", "query": "SELECT FOR UPDATE",
              "database": "mydb", "total_count": 100,
              "mean_contention_sec": 0.5, "mean_latency_sec": 0.8,
              "contention_pct_of_latency": 62.5,
              "total_contention_sec": 50.0}],
            # _top_contended_tables
            [{"database_name": "mydb", "schema_name": "public",
              "table_name": "orders",
              "num_contention_events": 5000}],
            # _top_contended_indexes
            [{"database_name": "mydb", "schema_name": "public",
              "table_name": "orders", "index_name": "orders_pkey",
              "num_contention_events": 5000}],
            # _contention_events_by_table
            [{"database_name": "mydb", "table_name": "orders",
              "index_name": "orders_pkey", "contention_type": "LOCK_WAIT",
              "events": 5000, "total_duration": "10s",
              "avg_duration": "0.002s", "max_duration": "0.05s",
              "earliest": "2026-01-01", "latest": "2026-01-02"}],
            # _recent_contention_events
            [{"collection_ts": "2026-01-02T00:00:00Z",
              "database_name": "mydb", "table_name": "orders",
              "index_name": "orders_pkey",
              "contention_type": "LOCK_WAIT",
              "contention_duration": "0.05s",
              "waiting_query": "SELECT FOR UPDATE"}],
            # _contended_queries_summary
            [{"contended_stmts": 25, "total_contention_sec": 120.5,
              "avg_contention_sec_per_stmt": 4.82,
              "total_executions": 5000}],
        ]
        result = analyzer.analyze(limit=10, since="1h")
        assert result["summary"]["severity"] == "MODERATE"
        assert result["summary"]["contended_stmt_fingerprints"] == 25
        assert result["summary"]["total_contention_events"] == 5000
        assert result["summary"]["tables_with_contention"] == 1
        assert result["summary"]["indexes_with_contention"] == 1
        assert len(result["sections"]) == 6

    def test_high_severity(self):
        analyzer = ContentionAnalyzer(sql_client=MagicMock())
        analyzer.sql.execute.side_effect = [
            [],  # top stmts
            [],  # tables
            [],  # indexes
            [{"database_name": "db", "table_name": "t",
              "index_name": "pk", "contention_type": "LOCK_WAIT",
              "events": 15000, "total_duration": "30s",
              "avg_duration": "0.002s", "max_duration": "1s",
              "earliest": "2026-01-01", "latest": "2026-01-02"}],
            [],  # recent events
            [{"contended_stmts": 150, "total_contention_sec": 500.0,
              "avg_contention_sec_per_stmt": 3.33,
              "total_executions": 10000}],
        ]
        result = analyzer.analyze(limit=10)
        assert result["summary"]["severity"] == "HIGH"

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
        assert len(result["sections"]) == 6
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

    def test_az_aware_single_node_az_is_ok(self):
        """Node 9 in AZ3 alone with RF=3 across 3 AZs must hold all
        ranges -- the analyzer should report status=OK, not OVER."""
        analyzer = RebalanceStatusAnalyzer(sql_client=MagicMock())
        # With RF=3 across 3 AZs, each AZ gets ~(180*3/3)=180 replicas.
        # AZ1 (2 nodes): 180/2 = 90 per node
        # AZ2 (2 nodes): 180/2 = 90 per node
        # AZ3 (1 node): 180/1 = 180 for node 9
        # So node 9 holding 180 replicas is EXPECTED.
        ranges = []
        for i in range(180):
            # Distribute replicas: 1 per AZ => pick one node from each AZ
            az1_node = 3 if i % 2 == 0 else 8
            az2_node = 4 if i % 2 == 0 else 7
            ranges.append({
                "range_id": i + 1,
                "replicas": [az1_node, az2_node, 9],
                "lease_holder": az1_node,
            })
        analyzer.sql.execute.side_effect = [
            # _get_replication_stats
            [{"zone_id": 0, "sub_zone_id": 0,
              "under_replicated_ranges": 0, "over_replicated_ranges": 0,
              "unavailable_ranges": 0, "total_ranges": 180}],
            # _get_store_balance
            [{"node_id": 3, "store_id": 3, "range_count": 90,
              "lease_count": 45, "capacity": 1e12, "available": 9.9e11,
              "used": 1e10},
             {"node_id": 4, "store_id": 4, "range_count": 90,
              "lease_count": 45, "capacity": 1e12, "available": 9.9e11,
              "used": 1e10},
             {"node_id": 7, "store_id": 7, "range_count": 90,
              "lease_count": 45, "capacity": 1e12, "available": 9.9e11,
              "used": 1e10},
             {"node_id": 8, "store_id": 8, "range_count": 90,
              "lease_count": 45, "capacity": 1e12, "available": 9.9e11,
              "used": 1e10},
             {"node_id": 9, "store_id": 9, "range_count": 180,
              "lease_count": 45, "capacity": 1e12, "available": 9.9e11,
              "used": 1e10}],
            # _get_recent_rangelog
            [],
            # _get_cluster_setting (rebalance rate)
            [{"kv.snapshot_rebalance.max_rate": "32 MiB"}],
            # _get_cluster_setting (split qps)
            [{"kv.range_split.load_qps_threshold": "2500"}],
            # _get_range_max_bytes
            [{"raw_config_sql": "range_max_bytes = 536870912"}],
            # _get_node_localities
            [{"node_id": 3, "locality": "region=azure,zone=az1"},
             {"node_id": 4, "locality": "region=azure,zone=az2"},
             {"node_id": 7, "locality": "region=azure,zone=az2"},
             {"node_id": 8, "locality": "region=azure,zone=az1"},
             {"node_id": 9, "locality": "region=azure,zone=az3"}],
            # _build_az_topology -> no extra query
            # _get_node_range_distribution: _get_valid_replica_counts
            [{"target": "RANGE default",
              "raw_config_sql": "num_replicas = 3"}],
            # _get_node_range_distribution: ranges query
            ranges,
            # _get_node_range_distribution: gossip_nodes
            [{"node_id": 3, "started_at": "2026-01-01"},
             {"node_id": 4, "started_at": "2026-01-01"},
             {"node_id": 7, "started_at": "2026-01-01"},
             {"node_id": 8, "started_at": "2026-01-01"},
             {"node_id": 9, "started_at": "2026-01-01"}],
            # _get_rebalance_direction
            [],
        ]
        result = analyzer.analyze(limit=10)
        # Node 9 should NOT be flagged as OVER
        node_dist = result["sections"][3]["rows"]
        node9 = next(n for n in node_dist if n["node_id"] == 9)
        assert node9["status"] == "OK", (
            f"Node 9 (sole node in AZ3) should be OK, got {node9['status']} "
            f"with expected={node9['expected']}, actual={node9['total_replicas']}"
        )
        assert result["summary"]["verdict"] == "REBALANCING COMPLETE"

    def test_az_topology_section(self):
        """AZ Topology section shows nodes per AZ."""
        analyzer = RebalanceStatusAnalyzer(sql_client=MagicMock())
        localities = {
            1: {"zone": "az1"},
            2: {"zone": "az1"},
            3: {"zone": "az2"},
        }
        store_balance = [
            {"node_id": 1, "store_id": 1, "range_count": 100,
             "lease_count": 30},
            {"node_id": 2, "store_id": 2, "range_count": 100,
             "lease_count": 30},
            {"node_id": 3, "store_id": 3, "range_count": 200,
             "lease_count": 40},
        ]
        topo = analyzer._build_az_topology(localities, store_balance)
        assert len(topo) == 2
        az1 = next(t for t in topo if t["az"] == "az1")
        assert az1["node_count"] == 2
        assert az1["total_ranges"] == 200
        az2 = next(t for t in topo if t["az"] == "az2")
        assert az2["node_count"] == 1
        assert az2["total_ranges"] == 200


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


class TestStmtErrorsAnalyzer:
    def test_no_errors(self):
        sql = _make_sql_client(execute_result=[])
        analyzer = StmtErrorsAnalyzer(sql_client=sql)
        result = analyzer.analyze(limit=10)
        assert result["title"] == "Statement Errors"
        assert result["summary"]["verdict"] == "No statement failures"
        assert result["summary"]["total_failure_count"] == 0
        assert len(result["sections"]) == 4

    def test_with_failures(self):
        sql = _make_sql_client(execute_result=[
            {
                "fingerprint_id": "abc123",
                "query": "SELECT * FROM t",
                "database": "mydb",
                "failure_count": 10,
                "total_count": 100,
                "failure_pct": "10.0",
                "aggregated_ts": "2026-01-01T00:00:00Z",
            },
        ])
        analyzer = StmtErrorsAnalyzer(sql_client=sql)
        result = analyzer.analyze(limit=10, since="6h")
        assert "10 statement failures" in result["summary"]["verdict"]

    def test_requires_sql(self):
        analyzer = StmtErrorsAnalyzer()
        try:
            analyzer.analyze()
            raise AssertionError("Expected RuntimeError")
        except RuntimeError:
            pass

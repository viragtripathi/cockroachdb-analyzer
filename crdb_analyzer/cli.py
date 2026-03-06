"""CLI entry point for CockroachDB Analyzer."""

import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import click

from crdb_analyzer import __version__
from crdb_analyzer.clients.http_client import CRDBHttpClient
from crdb_analyzer.clients.sql_client import CRDBSqlClient
from crdb_analyzer.config import CRDBConfig
from crdb_analyzer.formatters import format_results

logger = logging.getLogger(__name__)


def _build_clients(
    config: CRDBConfig,
) -> tuple[CRDBSqlClient | None, CRDBHttpClient | None]:
    sql_client = None
    http_client = None
    if config.sql_url:
        sql_client = CRDBSqlClient(config)
        sql_client.connect()
    if config.admin_url:
        try:
            http_client = CRDBHttpClient(config)
        except RuntimeError as exc:
            click.echo(f"Error: {exc}", err=True)
            sys.exit(1)
    return sql_client, http_client


def _cleanup(
    sql_client: CRDBSqlClient | None, http_client: CRDBHttpClient | None
) -> None:
    if sql_client:
        sql_client.close()
    if http_client:
        http_client.close()


def _get_store(ctx: click.Context) -> Any:
    store_type = ctx.obj.get("snapshot_store", "sqlite")
    if store_type == "crdb":
        config = ctx.obj["config"]
        from crdb_analyzer.storage.crdb_store import CRDBSnapshotStore

        sql = CRDBSqlClient(config)
        sql.connect()
        return CRDBSnapshotStore(sql)
    from crdb_analyzer.storage.sqlite_store import SQLiteSnapshotStore

    db_path = ctx.obj.get("snapshot_db")
    return SQLiteSnapshotStore(db_path)


@click.group()
@click.option("--sql-url", envvar="CRDB_SQL_URL", help="PostgreSQL connection URL.")
@click.option("--admin-url", envvar="CRDB_ADMIN_URL", help="HTTP admin UI URL.")
@click.option("--admin-user", envvar="CRDB_ADMIN_USER", help="Admin UI username.")
@click.option("--admin-password", envvar="CRDB_ADMIN_PASSWORD", help="Admin UI password.")
@click.option("--config-file", type=click.Path(exists=True), help="YAML config file.")
@click.option(
    "--format", "output_format",
    type=click.Choice(["table", "json", "csv"]), default="table",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging.")
@click.option(
    "--snapshot-store",
    type=click.Choice(["sqlite", "crdb"]), default="sqlite",
    help="Where to store snapshots (default: sqlite).",
)
@click.option("--snapshot-db", type=click.Path(), help="Path to SQLite snapshot DB.")
@click.version_option(version=__version__)
@click.pass_context
def main(
    ctx: click.Context,
    sql_url: str | None,
    admin_url: str | None,
    admin_user: str | None,
    admin_password: str | None,
    config_file: str | None,
    output_format: str,
    verbose: bool,
    snapshot_store: str,
    snapshot_db: str | None,
) -> None:
    """CockroachDB Analyzer - diagnose performance bottlenecks and cluster issues."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    )
    ctx.ensure_object(dict)
    config = CRDBConfig.resolve(
        sql_url=sql_url, admin_url=admin_url, config_file=config_file
    )
    if admin_user:
        config.admin_user = admin_user
    if admin_password:
        config.admin_password = admin_password
    ctx.obj["config"] = config
    ctx.obj["format"] = output_format
    ctx.obj["snapshot_store"] = snapshot_store
    ctx.obj["snapshot_db"] = snapshot_db


# ---------------------------------------------------------------------------
# Existing analyzer commands
# ---------------------------------------------------------------------------


@main.command("hot-ranges")
@click.option("--limit", default=50, help="Number of ranges to show.")
@click.option("--sort-by", type=click.Choice(["qps", "wps"]), default="qps")
@click.option("--from-file", type=click.Path(exists=True))
@click.option("--save", is_flag=True, help="Save results as a snapshot for later comparison.")
@click.pass_context
def hot_ranges(
    ctx: click.Context, limit: int, sort_by: str, from_file: str | None, save: bool
) -> None:
    """Find the hottest ranges by size (SQL) or QPS (API/file)."""
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.hot_ranges import HotRangesAnalyzer

        analyzer = HotRangesAnalyzer(sql_client=sql_client, http_client=http_client)
        results = analyzer.analyze(limit=limit, sort_by=sort_by, ranges_file=from_file)
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "hot-ranges", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("hot-nodes")
@click.option("--from-file", type=click.Path(exists=True))
@click.option("--save", is_flag=True, help="Save results as a snapshot.")
@click.pass_context
def hot_nodes(ctx: click.Context, from_file: str | None, save: bool) -> None:
    """Show load distribution across nodes."""
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.hot_nodes import HotNodesAnalyzer

        analyzer = HotNodesAnalyzer(sql_client=sql_client, http_client=http_client)
        results = analyzer.analyze(ranges_file=from_file)
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "hot-nodes", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("data-skew")
@click.option("--limit", default=50)
@click.option("--from-file", type=click.Path(exists=True))
@click.option("--save", is_flag=True)
@click.pass_context
def data_skew(ctx: click.Context, limit: int, from_file: str | None, save: bool) -> None:
    """Detect data skew by finding the largest ranges."""
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.data_skew import DataSkewAnalyzer

        analyzer = DataSkewAnalyzer(sql_client=sql_client, http_client=http_client)
        results = analyzer.analyze(limit=limit, ranges_file=from_file)
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "data-skew", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("table-stats")
@click.option("--database", help="Filter by database name.")
@click.option("--table", help="Filter by table name (requires --database).")
@click.option("--from-file", type=click.Path(exists=True))
@click.option("--save", is_flag=True)
@click.pass_context
def table_stats(
    ctx: click.Context,
    database: str | None,
    table: str | None,
    from_file: str | None,
    save: bool,
) -> None:
    """Show per-table range distribution and size breakdown."""
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.table_stats import TableStatsAnalyzer

        analyzer = TableStatsAnalyzer(sql_client=sql_client, http_client=http_client)
        results = analyzer.analyze(database=database, table=table, ranges_file=from_file)
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "table-stats", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("contention")
@click.option("--limit", default=20, help="Max rows per section.")
@click.option(
    "--since", default="1h",
    help="Time window for contention stats (e.g. 1h, 6h, 24h).",
)
@click.option("--save", is_flag=True)
@click.pass_context
def contention(
    ctx: click.Context, limit: int, since: str, save: bool,
) -> None:
    """Analyze lock contention: contended queries, tables, indexes,
    and individual contention events with waiting statements."""
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.contention import ContentionAnalyzer

        analyzer = ContentionAnalyzer(
            sql_client=sql_client, http_client=http_client,
        )
        results = analyzer.analyze(limit=limit, since=since)
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "contention", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("range-details")
@click.option(
    "--range-ids", required=True,
    help="Range IDs (comma-separated). Spaces after commas are OK.",
)
@click.argument("extra_ids", nargs=-1)
@click.pass_context
def range_details(
    ctx: click.Context, range_ids: str, extra_ids: tuple[str, ...],
) -> None:
    """Get detailed info about specific ranges.

    Tip: get range IDs from the output of hot-ranges, data-skew, or table-stats.

    All of these work:\n
      --range-ids 16895,18879,14166\n
      --range-ids 16895, 18879\n
      --range-ids "16895, 18879, 14166"
    """
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        if not sql_client:
            click.echo("Error: range-details requires --sql-url.", err=True)
            sys.exit(1)
        ids = _parse_range_ids((range_ids, *extra_ids))
        rows = sql_client.get_range_details(ids)
        table_map = sql_client.get_table_id_map()
        _enrich_with_table_names(rows, table_map)
        id_str = ",".join(str(i) for i in ids)
        results = {
            "title": f"Range Details for IDs: {id_str}",
            "source": "sql",
            "headers": list(rows[0].keys()) if rows else [],
            "rows": rows,
            "summary": {"ranges_found": len(rows)},
        }
        click.echo(format_results(results, ctx.obj["format"]))
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


# ---------------------------------------------------------------------------
# New analyzer commands
# ---------------------------------------------------------------------------


@main.command("index-usage")
@click.option("--limit", default=50)
@click.option("--save", is_flag=True)
@click.pass_context
def index_usage(ctx: click.Context, limit: int, save: bool) -> None:
    """Find unused and hot indexes."""
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.index_usage import IndexUsageAnalyzer

        analyzer = IndexUsageAnalyzer(sql_client=sql_client, http_client=http_client)
        results = analyzer.analyze(limit=limit)
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "index-usage", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("lease-balance")
@click.option("--save", is_flag=True)
@click.pass_context
def lease_balance(ctx: click.Context, save: bool) -> None:
    """Check replica and lease balance across nodes."""
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.lease_balance import LeaseBalanceAnalyzer

        analyzer = LeaseBalanceAnalyzer(sql_client=sql_client, http_client=http_client)
        results = analyzer.analyze()
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "lease-balance", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("stmt-fingerprints")
@click.option("--limit", default=50)
@click.option("--save", is_flag=True)
@click.pass_context
def stmt_fingerprints(ctx: click.Context, limit: int, save: bool) -> None:
    """Analyze statement fingerprints: slow queries, execution patterns."""
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.stmt_fingerprints import StmtFingerprintAnalyzer

        analyzer = StmtFingerprintAnalyzer(sql_client=sql_client, http_client=http_client)
        results = analyzer.analyze(limit=limit)
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "stmt-fingerprints", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("cluster-health")
@click.option("--save", is_flag=True)
@click.pass_context
def cluster_health(ctx: click.Context, save: bool) -> None:
    """Show cluster health: node liveness, capacity, version skew."""
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.cluster_health import ClusterHealthAnalyzer

        analyzer = ClusterHealthAnalyzer(sql_client=sql_client, http_client=http_client)
        results = analyzer.analyze()
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "cluster-health", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("node-hotspot")
@click.option("--node-id", required=True, type=int, help="Node ID to investigate.")
@click.option("--limit", default=20)
@click.option("--save", is_flag=True)
@click.pass_context
def node_hotspot(ctx: click.Context, node_id: int, limit: int, save: bool) -> None:
    """Diagnose why a specific node is running hot.

    Shows per-table lease breakdown, zone config analysis, store
    capacity comparison, and top ranges on the target node.

    Example: crdb-analyzer node-hotspot --node-id 2
    """
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.node_hotspot import NodeHotspotAnalyzer

        analyzer = NodeHotspotAnalyzer(sql_client=sql_client, http_client=http_client)
        results = analyzer.analyze(node_id=node_id, limit=limit)
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "node-hotspot", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("rebalance-status")
@click.option("--limit", default=50, help="Number of rangelog events to check.")
@click.option(
    "--balance-threshold", default=5.0, type=float,
    help="Max allowed range-count spread %% across stores (default: 5.0).",
)
@click.option("--save", is_flag=True)
@click.pass_context
def rebalance_status(
    ctx: click.Context, limit: int, balance_threshold: float, save: bool,
) -> None:
    """Check whether cluster rebalancing is complete.

    Examines replication stats, store balance, and recent rangelog
    activity to determine if rebalancing is still in progress.
    """
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.rebalance_status import RebalanceStatusAnalyzer

        analyzer = RebalanceStatusAnalyzer(sql_client=sql_client, http_client=http_client)
        results = analyzer.analyze(limit=limit, balance_threshold=balance_threshold)
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "rebalance-status", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("job-status")
@click.option("--limit", default=50, help="Max failed/reverting jobs to show.")
@click.option("--save", is_flag=True)
@click.pass_context
def job_status(ctx: click.Context, limit: int, save: bool) -> None:
    """Detect stuck, long-running, or problematic CockroachDB jobs.

    Checks for Schema Change GC backlog, coordinator imbalance,
    failed jobs, and GC TTL configuration.
    """
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.job_status import JobStatusAnalyzer

        analyzer = JobStatusAnalyzer(sql_client=sql_client, http_client=http_client)
        results = analyzer.analyze(limit=limit)
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "job-status", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("stmt-errors")
@click.option("--limit", default=20, help="Max rows per section.")
@click.option(
    "--since", default="1h",
    help="Time window for statement stats (e.g. 1h, 6h, 24h).",
)
@click.option("--save", is_flag=True)
@click.pass_context
def stmt_errors(
    ctx: click.Context, limit: int, since: str, save: bool,
) -> None:
    """Analyze statement failures, retries, and contention errors.

    Shows top failing queries, retry errors, per-node failure
    distribution, and failing statements with high contention.
    """
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        from crdb_analyzer.analyzers.stmt_errors import StmtErrorsAnalyzer

        analyzer = StmtErrorsAnalyzer(
            sql_client=sql_client, http_client=http_client,
        )
        results = analyzer.analyze(limit=limit, since=since)
        click.echo(format_results(results, ctx.obj["format"]))
        if save:
            _save_snapshot(ctx, "stmt-errors", results)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


# ---------------------------------------------------------------------------
# Snapshot / history / compare commands
# ---------------------------------------------------------------------------


@main.command("snapshot")
@click.argument("analysis", type=click.Choice([
    "hot-ranges", "hot-nodes", "data-skew", "table-stats",
    "contention", "index-usage", "lease-balance", "stmt-fingerprints",
    "cluster-health", "node-hotspot", "rebalance-status", "job-status",
    "stmt-errors",
]))
@click.option("--limit", default=50)
@click.pass_context
def snapshot(ctx: click.Context, analysis: str, limit: int) -> None:
    """Capture a point-in-time snapshot of any analysis for later comparison.

    Example: crdb-analyzer snapshot hot-nodes
    """
    config = ctx.obj["config"]
    sql_client, http_client = _build_clients(config)
    try:
        results = _run_analysis(analysis, sql_client, http_client, limit)
        store = _get_store(ctx)
        rows = results.get("rows", [])
        if not rows and (sections := results.get("sections")):
            rows = []
            for s in sections:
                rows.extend(s.get("rows", []))
        sid = store.save_snapshot(analysis, rows, {"title": results.get("title", "")})
        store.close()
        click.echo(f"Snapshot saved: {sid} ({analysis}, {len(rows)} rows)")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        _cleanup(sql_client, http_client)


@main.command("history")
@click.option("--type", "snap_type", help="Filter by analysis type (e.g. hot-ranges).")
@click.option("--since", help="Show snapshots since (e.g. '2h', '1d', '2024-01-01T00:00').")
@click.option("--limit", default=20)
@click.option("--show", help="Show full data for a snapshot ID.")
@click.pass_context
def history(
    ctx: click.Context,
    snap_type: str | None,
    since: str | None,
    limit: int,
    show: str | None,
) -> None:
    """Browse historical snapshots.

    Examples:\n
      crdb-analyzer history                          # list all snapshots\n
      crdb-analyzer history --type hot-nodes         # filter by type\n
      crdb-analyzer history --since 2h               # last 2 hours\n
      crdb-analyzer history --show abc123def456      # view snapshot data
    """
    store = _get_store(ctx)
    try:
        if show:
            data = store.get_snapshot_data(show)
            meta = store.get_snapshot(show)
            if not meta:
                click.echo(f"Snapshot {show} not found.", err=True)
                sys.exit(1)
            results = {
                "title": (
                    f"Snapshot {show} ({meta.get('snapshot_type')})"
                    f" @ {meta.get('created_at')}"
                ),
                "source": "history",
                "headers": list(data[0].keys()) if data else [],
                "rows": data,
                "summary": {"rows": len(data)},
            }
            click.echo(format_results(results, ctx.obj["format"]))
        else:
            since_dt = _parse_since(since) if since else None
            snaps = store.list_snapshots(
                snapshot_type=snap_type, since=since_dt, limit=limit
            )
            results = {
                "title": "Snapshot History",
                "source": "history",
                "headers": ["snapshot_id", "snapshot_type", "created_at"],
                "rows": snaps,
                "summary": {"total": len(snaps)},
            }
            click.echo(format_results(results, ctx.obj["format"]))
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        store.close()


@main.command("compare")
@click.argument("snapshot_a")
@click.argument("snapshot_b")
@click.pass_context
def compare(ctx: click.Context, snapshot_a: str, snapshot_b: str) -> None:
    """Compare two snapshots side by side.

    Example: crdb-analyzer compare abc123 def456
    """
    store = _get_store(ctx)
    try:
        meta_a = store.get_snapshot(snapshot_a)
        meta_b = store.get_snapshot(snapshot_b)
        if not meta_a or not meta_b:
            click.echo("One or both snapshot IDs not found.", err=True)
            sys.exit(1)
        data_a = store.get_snapshot_data(snapshot_a)
        data_b = store.get_snapshot_data(snapshot_b)

        diff_rows = _compute_diff(data_a, data_b)
        results = {
            "title": (
                f"Compare: {snapshot_a} ({meta_a.get('created_at')}) "
                f"vs {snapshot_b} ({meta_b.get('created_at')})"
            ),
            "source": "compare",
            "headers": list(diff_rows[0].keys()) if diff_rows else [],
            "rows": diff_rows,
            "summary": {
                "snapshot_a": f"{snapshot_a} @ {meta_a.get('created_at')}",
                "snapshot_b": f"{snapshot_b} @ {meta_b.get('created_at')}",
                "rows_in_a": len(data_a),
                "rows_in_b": len(data_b),
                "changes": len(diff_rows),
            },
        }
        click.echo(format_results(results, ctx.obj["format"]))
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    finally:
        store.close()


_ALL_ANALYSES = [
    "hot-ranges", "hot-nodes", "data-skew", "table-stats",
    "contention", "index-usage", "lease-balance",
    "stmt-fingerprints", "cluster-health", "node-hotspot",
    "rebalance-status", "job-status", "stmt-errors",
]


@main.command("daemon")
@click.option(
    "--interval", default=900, type=int, show_default=True,
    help="Seconds between snapshot cycles.",
)
@click.option(
    "--analyses", default=",".join(_ALL_ANALYSES), show_default=True,
    help="Comma-separated list of analyses to capture each cycle.",
)
@click.option("--limit", default=50, help="Row limit passed to each analysis.")
@click.option(
    "--retention-days", default=0, type=int,
    help="Auto-delete snapshots older than N days (0 = keep forever).",
)
@click.option(
    "--node-ids",
    help="Comma-separated node IDs for node-hotspot analysis each cycle.",
)
@click.pass_context
def daemon(
    ctx: click.Context,
    interval: int,
    analyses: str,
    limit: int,
    retention_days: int,
    node_ids: str | None,
) -> None:
    """Run continuous snapshots on a schedule.

    Captures the selected analyses every --interval seconds and stores
    them for later review with `history` and `compare`.

    For node-hotspot analysis, pass --node-ids to capture per-node data:

    Examples:\n
      crdb-analyzer daemon\n
      crdb-analyzer daemon --interval 300\n
      crdb-analyzer daemon --analyses hot-ranges,hot-nodes --interval 600\n
      crdb-analyzer daemon --retention-days 7\n
      crdb-analyzer daemon --node-ids 1,2,3
    """
    import signal

    analysis_list = [a.strip() for a in analyses.split(",") if a.strip()]
    invalid = set(analysis_list) - set(_ALL_ANALYSES)
    if invalid:
        click.echo(
            f"Error: unknown analyses: {', '.join(invalid)}\n"
            f"Valid: {', '.join(_ALL_ANALYSES)}",
            err=True,
        )
        sys.exit(1)

    hotspot_node_ids: list[int] = []
    if node_ids:
        hotspot_node_ids = [
            int(x.strip()) for x in node_ids.replace(",", " ").split() if x.strip()
        ]
        if "node-hotspot" not in analysis_list:
            analysis_list.append("node-hotspot")

    config = ctx.obj["config"]
    if not config.sql_url and not config.admin_url:
        click.echo(
            "Error: daemon requires --sql-url or CRDB_SQL_URL.", err=True,
        )
        sys.exit(1)

    click.echo(
        f"Starting daemon: {len(analysis_list)} analyses "
        f"every {interval}s"
        + (f", retaining {retention_days}d" if retention_days else "")
    )
    click.echo(f"  Analyses: {', '.join(analysis_list)}")
    if hotspot_node_ids:
        click.echo(f"  Node hotspot IDs: {hotspot_node_ids}")
    click.echo("  Press Ctrl+C to stop.\n")

    running = True

    def _handle_signal(signum: int, frame: Any) -> None:
        nonlocal running
        running = False
        click.echo("\nShutting down daemon...")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    cycle = 0
    while running:
        cycle += 1
        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        click.echo(f"[{now}] Cycle {cycle}: snapshotting...")

        sql_client, http_client = _build_clients(config)
        store = _get_store(ctx)
        try:
            for analysis in analysis_list:
                if analysis == "node-hotspot" and hotspot_node_ids:
                    _daemon_snapshot_node_hotspot(
                        sql_client, http_client, store,
                        hotspot_node_ids, limit, cycle,
                    )
                    continue
                try:
                    results = _run_analysis(
                        analysis, sql_client, http_client, limit,
                    )
                    rows = results.get("rows", [])
                    if not rows and (sections := results.get("sections")):
                        rows = []
                        for s in sections:
                            rows.extend(s.get("rows", []))
                    sid = store.save_snapshot(
                        analysis, rows,
                        {"title": results.get("title", ""), "cycle": cycle},
                    )
                    click.echo(f"  {analysis}: {len(rows)} rows -> {sid}")
                except Exception as exc:
                    click.echo(f"  {analysis}: ERROR - {exc}", err=True)

            if retention_days > 0:
                _prune_old_snapshots(store, retention_days)
        finally:
            store.close()
            _cleanup(sql_client, http_client)

        if running:
            _interruptible_sleep(interval, lambda: running)


def _daemon_snapshot_node_hotspot(
    sql_client: CRDBSqlClient | None,
    http_client: CRDBHttpClient | None,
    store: Any,
    node_ids: list[int],
    limit: int,
    cycle: int,
) -> None:
    """Run node-hotspot for each node ID and save separate snapshots."""
    from crdb_analyzer.analyzers.node_hotspot import NodeHotspotAnalyzer

    analyzer = NodeHotspotAnalyzer(sql_client=sql_client, http_client=http_client)
    for nid in node_ids:
        try:
            results = analyzer.analyze(node_id=nid, limit=limit)
            rows = results.get("rows", [])
            if not rows and (sections := results.get("sections")):
                rows = []
                for s in sections:
                    rows.extend(s.get("rows", []))
            sid = store.save_snapshot(
                f"node-hotspot-n{nid}", rows,
                {"title": results.get("title", ""), "node_id": nid, "cycle": cycle},
            )
            click.echo(f"  node-hotspot (n{nid}): {len(rows)} rows -> {sid}")
        except Exception as exc:
            click.echo(f"  node-hotspot (n{nid}): ERROR - {exc}", err=True)


def _interruptible_sleep(seconds: int, check_fn: Any) -> None:
    """Sleep in 1-second increments so we can respond to signals."""
    for _ in range(seconds):
        if not check_fn():
            break
        __import__("time").sleep(1)


def _prune_old_snapshots(store: Any, retention_days: int) -> None:
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=retention_days)
    old = store.list_snapshots(until=cutoff, limit=1000)
    if not old:
        return
    for snap in old:
        store.delete_snapshot(snap["snapshot_id"])
    click.echo(f"  Pruned {len(old)} snapshots older than {retention_days}d")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _save_snapshot(ctx: click.Context, snap_type: str, results: dict[str, Any]) -> None:
    store = _get_store(ctx)
    rows = results.get("rows", [])
    if not rows and (sections := results.get("sections")):
        rows = []
        for s in sections:
            rows.extend(s.get("rows", []))
    sid = store.save_snapshot(snap_type, rows, {"title": results.get("title", "")})
    store.close()
    click.echo(f"\nSnapshot saved: {sid}", err=True)


def _run_analysis(
    analysis: str,
    sql_client: CRDBSqlClient | None,
    http_client: CRDBHttpClient | None,
    limit: int,
) -> dict[str, Any]:
    from crdb_analyzer.analyzers.cluster_health import ClusterHealthAnalyzer
    from crdb_analyzer.analyzers.contention import ContentionAnalyzer
    from crdb_analyzer.analyzers.data_skew import DataSkewAnalyzer
    from crdb_analyzer.analyzers.hot_nodes import HotNodesAnalyzer
    from crdb_analyzer.analyzers.hot_ranges import HotRangesAnalyzer
    from crdb_analyzer.analyzers.index_usage import IndexUsageAnalyzer
    from crdb_analyzer.analyzers.job_status import JobStatusAnalyzer
    from crdb_analyzer.analyzers.lease_balance import LeaseBalanceAnalyzer
    from crdb_analyzer.analyzers.node_hotspot import NodeHotspotAnalyzer
    from crdb_analyzer.analyzers.rebalance_status import RebalanceStatusAnalyzer
    from crdb_analyzer.analyzers.stmt_errors import StmtErrorsAnalyzer
    from crdb_analyzer.analyzers.stmt_fingerprints import StmtFingerprintAnalyzer
    from crdb_analyzer.analyzers.table_stats import TableStatsAnalyzer

    analyzers: dict[str, type] = {
        "hot-ranges": HotRangesAnalyzer,
        "hot-nodes": HotNodesAnalyzer,
        "data-skew": DataSkewAnalyzer,
        "table-stats": TableStatsAnalyzer,
        "contention": ContentionAnalyzer,
        "index-usage": IndexUsageAnalyzer,
        "lease-balance": LeaseBalanceAnalyzer,
        "stmt-fingerprints": StmtFingerprintAnalyzer,
        "cluster-health": ClusterHealthAnalyzer,
        "node-hotspot": NodeHotspotAnalyzer,
        "rebalance-status": RebalanceStatusAnalyzer,
        "job-status": JobStatusAnalyzer,
        "stmt-errors": StmtErrorsAnalyzer,
    }
    cls = analyzers[analysis]
    analyzer = cls(sql_client=sql_client, http_client=http_client)
    result: dict[str, Any] = analyzer.analyze(limit=limit)
    return result


def _parse_range_ids(raw: tuple[str, ...]) -> list[int]:
    """Parse range IDs from multiple --range-ids values, handling commas and spaces."""
    ids: list[int] = []
    for token in raw:
        for part in token.replace(",", " ").split():
            part = part.strip()
            if part:
                ids.append(int(part))
    return ids


def _enrich_with_table_names(
    rows: list[dict[str, Any]], table_map: dict[int, dict[str, str]],
) -> None:
    """Add table_name and database columns by parsing table ID from start_pretty."""
    import re

    table_re = re.compile(r"^/Table/(\d+)/")
    for row in rows:
        sp = row.get("start_pretty", "")
        m = table_re.match(sp)
        if m:
            tid = int(m.group(1))
            info = table_map.get(tid, {})
            row["table_name"] = info.get("name", f"<id:{tid}>")
            row["database"] = info.get("database_name", "")
        else:
            row["table_name"] = ""
            row["database"] = ""


def _parse_since(since_str: str) -> datetime:
    """Parse a relative (2h, 1d, 30m) or ISO datetime string."""
    try:
        if since_str.endswith("h"):
            return datetime.now(tz=timezone.utc) - timedelta(hours=int(since_str[:-1]))
        if since_str.endswith("d"):
            return datetime.now(tz=timezone.utc) - timedelta(days=int(since_str[:-1]))
        if since_str.endswith("m"):
            return datetime.now(tz=timezone.utc) - timedelta(minutes=int(since_str[:-1]))
        return datetime.fromisoformat(since_str)
    except (ValueError, OverflowError) as exc:
        msg = f"Invalid --since value: {since_str!r} (use e.g. '2h', '1d', '30m', or ISO format)"
        raise click.BadParameter(msg) from exc


def _compute_diff(
    data_a: list[dict], data_b: list[dict]
) -> list[dict]:
    """Compute differences between two snapshot datasets by matching on common key fields."""
    key_fields = ["range_id", "node_id", "table_name", "table", "table_id", "fingerprint_id"]

    def _find_key(rows: list[dict]) -> str | None:
        if not rows:
            return None
        for k in key_fields:
            if k in rows[0]:
                return k
        return None

    key = _find_key(data_a) or _find_key(data_b)
    if not key:
        return [
            {"status": "info", "detail": f"A has {len(data_a)} rows, B has {len(data_b)} rows"}
        ]

    map_a = {str(r.get(key)): r for r in data_a}
    map_b = {str(r.get(key)): r for r in data_b}
    all_keys = sorted(set(map_a) | set(map_b))

    diff_rows = []
    for k in all_keys:
        row_a = map_a.get(k)
        row_b = map_b.get(k)
        if row_a and not row_b:
            diff_rows.append({key: k, "status": "removed_in_B", **row_a})
        elif row_b and not row_a:
            diff_rows.append({key: k, "status": "added_in_B", **row_b})
        elif row_a and row_b:
            changes = {}
            for field in row_b:
                if field == key:
                    continue
                va, vb = row_a.get(field), row_b.get(field)
                if va != vb:
                    changes[f"{field}_A"] = va
                    changes[f"{field}_B"] = vb
            if changes:
                diff_rows.append({key: k, "status": "changed", **changes})

    return diff_rows


if __name__ == "__main__":
    main()

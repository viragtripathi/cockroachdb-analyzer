# CockroachDB Analyzer

Modular diagnostic and performance analyzer for CockroachDB clusters. Works with CockroachDB via SQL (`crdb_internal` tables) or HTTP admin API, and can also analyze offline JSON range dumps.

## Features

- **Hot Ranges** -- Find the largest or busiest ranges in the cluster
- **Hot Nodes** -- Identify overloaded nodes by lease count and data size
- **Data Skew** -- Detect disproportionately large ranges
- **Table Stats** -- Per-table breakdown of range count and total size
- **Contention** -- Surface lock contention events and slow statements
- **Range Details** -- Inspect specific ranges by ID
- **Index Usage** -- Find unused and hottest indexes
- **Lease Balance** -- Check replica/lease distribution across nodes
- **Statement Fingerprints** -- Identify slowest queries, most executed, highest rows read
- **Cluster Health** -- Node liveness, capacity, version skew detection
- **Snapshot / History / Compare** -- Save point-in-time snapshots and diff them later

All commands support **table**, **JSON**, and **CSV** output formats.

## Requirements

- Python 3.10+
- Network access to a CockroachDB cluster (SQL port 26257 and/or HTTP admin port 8080)

## Installation

This project uses [uv](https://docs.astral.sh/uv/) for fast, reproducible
dependency management.

```bash
# Install uv (if you don't have it)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and install
git clone https://github.com/cockroachdb/cockroachdb-analyzer.git
cd cockroachdb-analyzer
uv sync            # installs runtime deps into .venv
uv sync --dev      # includes dev tools (ruff, mypy, pytest)
```

The `crdb-analyzer` CLI is available inside the managed venv. Run it with:

```bash
uv run crdb-analyzer --help
```

<details>
<summary>Alternative: install with pip</summary>

```bash
pip install -e .
pip install -e ".[dev]"   # with dev tools
```
</details>

## Quick Start

### Set your connection URL

```bash
# Local insecure cluster
export CRDB_SQL_URL="postgresql://root@localhost:26257/defaultdb?sslmode=disable"

# CockroachDB Cloud (TLS)
export CRDB_SQL_URL="postgresql://myuser:changeme@host:26257/mydb?sslmode=verify-full&sslrootcert=/path/to/ca.crt"
```

### Analyzer Commands

```bash
# Top 10 ranges by size
crdb-analyzer hot-ranges --limit 10

# Node load distribution
crdb-analyzer hot-nodes

# Data skew detection
crdb-analyzer data-skew --limit 20

# Per-table range breakdown (filter by database/table)
crdb-analyzer table-stats --database mydb --table users

# Contention and slow statements
crdb-analyzer contention --limit 10

# Unused and hot indexes
crdb-analyzer index-usage --limit 30

# Replica/lease balance across nodes
crdb-analyzer lease-balance

# Slow queries, most executed, highest rows read
crdb-analyzer stmt-fingerprints --limit 20

# Cluster health: node liveness, capacity, version skew
crdb-analyzer cluster-health

# Deep-dive: why is node 2 running hot?
crdb-analyzer node-hotspot --node-id 2
```

### Diagnosing a Hot Node

When you see one node with more leases, replicas, or data than others,
`node-hotspot` explains why:

```bash
crdb-analyzer node-hotspot --node-id 2
```

This shows:
- **Node vs Cluster Average** -- leases, replicas, and data size compared to the mean
- **Top Tables by Lease Count** -- which tables have disproportionately many leases on this node
- **Largest Ranges** -- the biggest ranges whose lease sits on this node
- **Store Capacity** -- disk usage across all nodes to spot hardware asymmetry
- **Zone Configs** -- any lease preferences or constraints that could be pinning to this node
- **Locality Match** -- whether the node's locality matches any zone config lease preferences

### Getting Range IDs for range-details

Range IDs appear in the output of `hot-ranges`, `data-skew`, and `table-stats`. Run one of those first, then use the `range_id` column values:

```bash
# Step 1: find interesting ranges
crdb-analyzer hot-ranges --limit 5
# Output shows range_id column: 16895, 18879, 14166, ...

# Step 2: drill into specific ranges
crdb-analyzer range-details --range-ids 16895,18879,14166
```

### Historical Analysis: Snapshot, History, Compare

Any analysis can be saved as a point-in-time snapshot for later review or comparison.

```bash
# Save a snapshot automatically when running any analyzer
crdb-analyzer hot-nodes --save
crdb-analyzer hot-ranges --limit 20 --save

# Or explicitly capture a snapshot
crdb-analyzer snapshot hot-nodes
crdb-analyzer snapshot cluster-health

# List all snapshots
crdb-analyzer history

# Filter by type or time window
crdb-analyzer history --type hot-nodes
crdb-analyzer history --since 2h
crdb-analyzer history --since 1d

# View a specific snapshot
crdb-analyzer history --show abc123def456

# Compare two snapshots (shows added/removed/changed rows)
crdb-analyzer compare abc123 def456
```

Snapshots are stored in **SQLite** by default (`~/.crdb-analyzer/snapshots.db`). To store them in the CockroachDB cluster instead:

```bash
crdb-analyzer --snapshot-store crdb snapshot hot-nodes
crdb-analyzer --snapshot-store crdb history --type hot-nodes
```

### Continuous Monitoring with the Daemon

The `daemon` command runs snapshot cycles on a schedule so you always have
historical data to look back on. It captures all (or selected) analyses every
N seconds and optionally prunes old snapshots.

```bash
# Default: all 9 analyses every 15 minutes
crdb-analyzer daemon

# Every 5 minutes, only hot-ranges and hot-nodes
crdb-analyzer daemon --interval 300 --analyses hot-ranges,hot-nodes

# Every 10 minutes, keep only the last 7 days of data
crdb-analyzer daemon --interval 600 --retention-days 7

# Track specific nodes for hotspot analysis each cycle
crdb-analyzer daemon --node-ids 1,2,3

# Combine: every 5 min, track nodes 2 and 3, keep 14 days
crdb-analyzer daemon --interval 300 --node-ids 2,3 --retention-days 14

# Store snapshots in CockroachDB instead of local SQLite
crdb-analyzer --snapshot-store crdb daemon --interval 900
```

The daemon prints a summary each cycle and responds to Ctrl+C / SIGTERM for
graceful shutdown.

**Running as a background service (systemd example):**

```ini
# /etc/systemd/system/crdb-analyzer.service
[Unit]
Description=CockroachDB Analyzer Daemon
After=network.target

[Service]
Type=simple
EnvironmentFile=/etc/crdb-analyzer/env
ExecStart=/usr/local/bin/crdb-analyzer daemon --interval 900 --retention-days 30
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

Create the environment file (keep credentials out of unit files):

```bash
# /etc/crdb-analyzer/env
CRDB_SQL_URL=postgresql://myuser:changeme@host:26257/defaultdb?sslmode=verify-full
```

```bash
sudo systemctl enable --now crdb-analyzer
```

**Running with nohup:**

```bash
nohup crdb-analyzer daemon --interval 900 --retention-days 14 > /var/log/crdb-analyzer.log 2>&1 &
```

Once the daemon has been collecting data, you can look back at any point:

```bash
# What did hot-ranges look like 24 hours ago?
crdb-analyzer history --type hot-ranges --since 24h

# View a specific snapshot
crdb-analyzer history --show <snapshot_id>

# Compare morning vs evening
crdb-analyzer compare <morning_id> <evening_id>
```

### Output formats

```bash
crdb-analyzer hot-ranges --limit 5                    # table (default)
crdb-analyzer --format json hot-nodes                  # JSON
crdb-analyzer --format csv data-skew --limit 100 > skew.csv  # CSV
```

### Pass URL directly

```bash
crdb-analyzer --sql-url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" hot-ranges
```

### Analyze offline JSON range dumps

```bash
crdb-analyzer hot-ranges --from-file ranges.json
crdb-analyzer hot-nodes --from-file ranges.json
crdb-analyzer data-skew --from-file ranges.json
```

### HTTP admin API mode

```bash
# Self-hosted (insecure)
crdb-analyzer --admin-url "http://localhost:8080" hot-ranges

# With authentication (CLI flags)
crdb-analyzer \
  --admin-url "https://your-cluster:8080" \
  --admin-user "myuser" \
  --admin-password "changeme" \
  hot-ranges

# With authentication (environment variables)
export CRDB_ADMIN_USER="myuser"
export CRDB_ADMIN_PASSWORD="changeme"
crdb-analyzer --admin-url "https://your-cluster:8080" hot-ranges
```

Credentials can also be set in the YAML config file (see below).

> **Note:** CockroachDB Cloud admin UIs often use session-based auth which
> may not work with basic auth. For Cloud clusters, prefer `--sql-url`
> which all analyzers support.

### YAML config file

Create `~/.crdb-analyzer.yaml` or `./crdb-analyzer.yaml`:

```yaml
sql_url: "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
admin_url: "http://localhost:8080"
admin_user: "myuser"        # optional
admin_password: "changeme"  # optional
ca_cert: "/path/to/ca.crt"  # optional, for TLS
client_cert: "/path/to/client.crt"  # optional
client_key: "/path/to/client.key"   # optional
timeout: 30
```

### Debug logging

```bash
crdb-analyzer -v hot-ranges --limit 5
```

## All Commands

| Command             | Description                                         |
|---------------------|-----------------------------------------------------|
| `hot-ranges`        | Find hottest ranges by size (SQL) or QPS (API/file) |
| `hot-nodes`         | Show load distribution across nodes                 |
| `data-skew`         | Detect data skew by finding the largest ranges      |
| `table-stats`       | Per-table range distribution and size breakdown     |
| `contention`        | Lock contention and slow statements                 |
| `range-details`     | Inspect specific ranges by ID                       |
| `index-usage`       | Unused and hot indexes                              |
| `lease-balance`     | Replica/lease distribution across nodes             |
| `stmt-fingerprints` | Slow queries, most executed, highest rows read      |
| `cluster-health`    | Node liveness, capacity, version skew               |
| `node-hotspot`      | Diagnose why a specific node is running hot         |
| `snapshot`          | Capture point-in-time snapshot of any analysis      |
| `history`           | Browse and view historical snapshots                |
| `compare`           | Diff two snapshots side by side                     |
| `daemon`            | Run continuous snapshots on a schedule              |

## Architecture

```
crdb_analyzer/
├── cli.py                 # Click CLI with 15 commands
├── config.py              # Config resolution (CLI > env > YAML file)
├── retry.py               # CockroachDB-aware retry with exponential backoff
├── clients/
│   ├── http_client.py     # HTTP client for /_status/ranges, /hotranges, etc.
│   └── sql_client.py      # psycopg 3 client with retry and reconnect
├── analyzers/
│   ├── base.py            # Base class with shared range data extraction
│   ├── hot_ranges.py      # Top ranges by size (SQL) or QPS (API)
│   ├── hot_nodes.py       # Per-node load aggregation
│   ├── data_skew.py       # Largest ranges detection
│   ├── table_stats.py     # Per-table/index breakdown
│   ├── contention.py      # Lock contention + slow statements
│   ├── index_usage.py     # Unused and hot indexes
│   ├── lease_balance.py   # Replica/lease distribution
│   ├── stmt_fingerprints.py # Statement fingerprint analysis
│   ├── cluster_health.py  # Cluster health overview
│   └── node_hotspot.py   # Per-node hotspot deep-dive
├── storage/
│   ├── base.py            # Abstract snapshot store interface
│   ├── sqlite_store.py    # SQLite backend (default, local)
│   └── crdb_store.py      # CockroachDB backend (centralized)
└── formatters/
    └── output.py          # Table, JSON, CSV output
```

## Reliability

- **Automatic retry** with exponential backoff and jitter on CockroachDB serialization failures (40001), connection resets, and timeouts
- **Auto-reconnect** on stale or broken SQL connections

## Development

```bash
uv sync --dev

# Run tests
uv run pytest

# Lint
uv run ruff check .

# Type check
uv run mypy crdb_analyzer/

# Run a specific command
uv run crdb-analyzer hot-ranges --limit 5
```

## License

Apache License 2.0 - see [LICENSE](https://github.com/viragtripathi/cockroachdb-analyzer/blob/main/LICENSE) for details.

## Acknowledgements

This project was inspired by Andrew Deally's article
[Troubleshooting Hot Ranges — CockroachDB](https://andrewdeally.medium.com/troubleshooting-hot-ranges-cockroachdb-63cf4bb7eaed).

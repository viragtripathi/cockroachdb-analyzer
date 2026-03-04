"""Rebalance status analyzer - detect whether cluster rebalancing is complete."""

import logging
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

logger = logging.getLogger(__name__)

_BALANCE_THRESHOLD_PCT = 5.0
_RANGELOG_WINDOW_MINUTES = 10
_VIRTUAL_CLUSTER_HINT = "unsupported within a virtual cluster"


def _is_virtual_cluster_error(exc: Exception) -> bool:
    return _VIRTUAL_CLUSTER_HINT in str(exc)


class RebalanceStatusAnalyzer(BaseAnalyzer):
    """Check replication stats, store balance, and rangelog activity
    to determine whether the cluster has finished rebalancing."""

    def analyze(self, limit: int = 50, **kwargs: Any) -> dict[str, Any]:
        if not self.sql:
            msg = "Rebalance status analysis requires a SQL connection."
            raise RuntimeError(msg)
        return self._analyze(limit)

    def _analyze(self, limit: int) -> dict[str, Any]:
        assert self.sql is not None
        is_virtual = False

        repl_stats = self._get_replication_stats()
        store_balance = self._get_store_balance()
        if not store_balance:
            is_virtual = True
        recent_events = self._get_recent_rangelog(limit)
        rebalance_rate = self._get_rebalance_rate_setting()
        node_dist = self._get_node_range_distribution()
        rebalance_direction = self._get_rebalance_direction()

        verdict, reasons = self._compute_verdict(
            repl_stats, store_balance, recent_events,
        )
        if is_virtual:
            reasons.append(
                "Virtual cluster detected: store balance, node "
                "distribution, and rangelog data are not available. "
                "Only replication stats from crdb_internal.ranges "
                "can be checked."
            )

        sections = [
            {
                "title": "Replication Stats",
                "headers": list(repl_stats[0].keys()) if repl_stats else [],
                "rows": repl_stats,
            },
            {
                "title": "Store Balance (range_count & lease_count per store)",
                "headers": (
                    list(store_balance[0].keys()) if store_balance else []
                ),
                "rows": store_balance,
            },
            {
                "title": "Per-Node Range Distribution",
                "headers": (
                    list(node_dist[0].keys()) if node_dist else []
                ),
                "rows": node_dist,
            },
            {
                "title": "Rebalance Direction (last 1 hour add/remove voter)",
                "headers": (
                    list(rebalance_direction[0].keys())
                    if rebalance_direction else []
                ),
                "rows": rebalance_direction,
            },
            {
                "title": f"Recent Rangelog Events (last {limit})",
                "headers": (
                    list(recent_events[0].keys()) if recent_events else []
                ),
                "rows": recent_events,
            },
        ]

        return {
            "title": "Rebalance Status",
            "source": "sql",
            "sections": sections,
            "summary": {
                "verdict": verdict,
                "reasons": reasons,
                "rebalance_rate_setting": rebalance_rate,
            },
        }

    # ------------------------------------------------------------------
    # Data collection
    # ------------------------------------------------------------------

    def _get_replication_stats(self) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            rows = self.sql.execute(
                """
                SELECT
                    zone_id,
                    sub_zone_id,
                    under_replicated_ranges,
                    over_replicated_ranges,
                    unavailable_ranges,
                    total_ranges
                FROM crdb_internal.replication_stats
                """
            )
            if rows:
                return rows
        except Exception:
            logger.debug(
                "replication_stats not available, computing from ranges",
            )
        return self._compute_replication_stats()

    def _get_valid_replica_counts(self) -> set[int]:
        """Collect all configured num_replicas values across zone configs."""
        assert self.sql is not None
        valid: set[int] = set()
        try:
            rows = self.sql.execute(
                "SELECT target, raw_config_sql FROM crdb_internal.zones"
            )
            for row in rows:
                raw = str(row.get("raw_config_sql", "") or "")
                m = re.search(r"num_replicas\s*=\s*(\d+)", raw)
                if m:
                    valid.add(int(m.group(1)))
        except Exception:
            logger.debug("zone config query failed", exc_info=True)
        if not valid:
            valid.add(3)
        return valid

    def _compute_replication_stats(self) -> list[dict[str, Any]]:
        """Compute replication health from crdb_internal.ranges.

        Compares each range's voting_replicas count against ALL
        configured num_replicas values across zone configs.  A range
        is only flagged if its count does not match any configured value.
        """
        assert self.sql is not None
        try:
            valid_counts = self._get_valid_replica_counts()
            min_expected = min(valid_counts)

            ranges = self.sql.execute(
                """
                SELECT range_id, replicas, voting_replicas
                FROM crdb_internal.ranges
                """
            )
            total = len(ranges)
            under = 0
            over = 0
            for r in ranges:
                voting = r.get("voting_replicas", [])
                n = len(voting) if isinstance(voting, list) else 0
                if n in valid_counts:
                    continue
                if n < min_expected:
                    under += 1
                elif n > max(valid_counts):
                    over += 1

            return [{
                "source": "computed from crdb_internal.ranges",
                "configured_num_replicas": sorted(valid_counts),
                "total_ranges": total,
                "under_replicated_ranges": under,
                "over_replicated_ranges": over,
                "unavailable_ranges": 0,
            }]
        except Exception:
            logger.warning("ranges query failed", exc_info=True)
            return []

    def _get_store_balance(self) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    node_id,
                    store_id,
                    range_count,
                    lease_count,
                    round(capacity::numeric / 1024 / 1024 / 1024, 2)
                        AS capacity_gb,
                    round(available::numeric / 1024 / 1024 / 1024, 2)
                        AS available_gb,
                    round(
                        used::numeric / NULLIF(capacity, 0)::numeric * 100, 1
                    ) AS used_pct
                FROM crdb_internal.kv_store_status
                ORDER BY node_id
                """
            )
        except Exception as exc:
            if _is_virtual_cluster_error(exc):
                logger.info(
                    "kv_store_status not available (virtual cluster)"
                )
            else:
                logger.warning("kv_store_status query failed", exc_info=True)
            return []

    def _get_node_range_distribution(self) -> list[dict[str, Any]]:
        """Per-node breakdown of replicas by replication factor.

        Computes how many replicas each node holds for each configured
        RF, along with expected counts, to pinpoint which nodes are
        over/under their fair share.
        """
        assert self.sql is not None
        try:
            valid_counts = self._get_valid_replica_counts()

            ranges = self.sql.execute(
                """
                SELECT range_id, replicas, lease_holder
                FROM crdb_internal.ranges
                """
            )

            node_by_rf: dict[int, dict[int, int]] = defaultdict(
                lambda: defaultdict(int),
            )
            node_total: dict[int, int] = defaultdict(int)
            node_leases: dict[int, int] = defaultdict(int)
            rf_range_count: dict[int, int] = defaultdict(int)

            for r in ranges:
                reps = r.get("replicas", [])
                if not isinstance(reps, list):
                    continue
                rf = len(reps)
                rf_range_count[rf] += 1
                lh = r.get("lease_holder")
                for nid in reps:
                    node_total[nid] += 1
                    node_by_rf[nid][rf] += 1
                if lh:
                    node_leases[lh] += 1

            node_started: dict[int, Any] = {}
            try:
                nodes = self.sql.execute(
                    """
                    SELECT node_id, started_at
                    FROM crdb_internal.gossip_nodes
                    ORDER BY node_id
                    """
                )
                node_started = {
                    n["node_id"]: n["started_at"] for n in nodes
                }
            except Exception as exc:
                if _is_virtual_cluster_error(exc):
                    logger.info(
                        "gossip_nodes not available (virtual cluster)"
                    )
                else:
                    logger.debug("gossip_nodes query failed", exc_info=True)

            all_nodes = sorted(node_total.keys())
            num_nodes = len(all_nodes)
            if num_nodes == 0:
                return []

            result: list[dict[str, Any]] = []
            for nid in all_nodes:
                expected = 0
                rf_detail_parts: list[str] = []
                for rf in sorted(valid_counts):
                    count = rf_range_count.get(rf, 0)
                    exp_per_node = round(count * rf / num_nodes)
                    expected += exp_per_node
                    actual = node_by_rf[nid].get(rf, 0)
                    rf_detail_parts.append(f"RF{rf}:{actual}/{exp_per_node}")

                actual_total = node_total[nid]
                delta = actual_total - expected
                status = "OK"
                if expected > 0:
                    pct_off = abs(delta) / expected * 100
                    if pct_off > _BALANCE_THRESHOLD_PCT:
                        status = "OVER" if delta > 0 else "UNDER"

                result.append({
                    "node_id": nid,
                    "total_replicas": actual_total,
                    "expected": expected,
                    "delta": f"{delta:+d}",
                    "leases": node_leases.get(nid, 0),
                    "rf_breakdown": ", ".join(rf_detail_parts),
                    "started_at": str(node_started.get(nid, "?")),
                    "status": status,
                })

            return result
        except Exception as exc:
            if _is_virtual_cluster_error(exc):
                logger.info(
                    "Node distribution not available (virtual cluster)"
                )
            else:
                logger.warning(
                    "node range distribution query failed", exc_info=True,
                )
            return []

    def _get_rebalance_direction(self) -> list[dict[str, Any]]:
        """Show net add/remove voter events per store in the last hour."""
        assert self.sql is not None
        try:
            rows = self.sql.execute(
                """
                SELECT
                    "storeID",
                    "eventType",
                    count(*) AS cnt
                FROM system.rangelog
                WHERE timestamp > now() - '1 hour'::interval
                  AND "eventType" IN ('add_voter', 'remove_voter')
                GROUP BY "storeID", "eventType"
                ORDER BY "storeID", "eventType"
                """
            )
            store_adds: dict[int, int] = defaultdict(int)
            store_removes: dict[int, int] = defaultdict(int)
            for r in rows:
                sid = int(r["storeID"])
                cnt = int(r["cnt"])
                if r["eventType"] == "add_voter":
                    store_adds[sid] = cnt
                else:
                    store_removes[sid] = cnt

            all_stores = sorted(
                set(store_adds.keys()) | set(store_removes.keys()),
            )
            result: list[dict[str, Any]] = []
            for sid in all_stores:
                adds = store_adds.get(sid, 0)
                removes = store_removes.get(sid, 0)
                net = adds - removes
                direction = (
                    "receiving" if net > 0
                    else "shedding" if net < 0
                    else "neutral"
                )
                result.append({
                    "store_id": sid,
                    "add_voter": adds,
                    "remove_voter": removes,
                    "net": f"{net:+d}",
                    "direction": direction,
                })
            return result
        except Exception as exc:
            if _is_virtual_cluster_error(exc):
                logger.info("rangelog not available (virtual cluster)")
            else:
                logger.warning(
                    "rebalance direction query failed", exc_info=True,
                )
            return []

    def _get_recent_rangelog(self, limit: int) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    timestamp,
                    "rangeID",
                    "storeID",
                    "eventType"
                FROM system.rangelog
                ORDER BY timestamp DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception as exc:
            if _is_virtual_cluster_error(exc):
                logger.info("rangelog not available (virtual cluster)")
            else:
                logger.warning("rangelog query failed", exc_info=True)
            return []

    def _get_rebalance_rate_setting(self) -> str:
        assert self.sql is not None
        try:
            rows = self.sql.execute(
                "SHOW CLUSTER SETTING kv.snapshot_rebalance.max_rate"
            )
            if rows:
                return str(next(iter(rows[0].values())))
        except Exception:
            logger.debug("rebalance rate setting query failed", exc_info=True)
        return "unknown"

    # ------------------------------------------------------------------
    # Verdict logic
    # ------------------------------------------------------------------

    def _compute_verdict(
        self,
        repl_stats: list[dict[str, Any]],
        store_balance: list[dict[str, Any]],
        recent_events: list[dict[str, Any]],
    ) -> tuple[str, list[str]]:
        """Return (verdict, reasons) based on the collected data."""
        reasons: list[str] = []
        all_clear = True

        # 1. Replication stats
        total_under = sum(
            int(r.get("under_replicated_ranges", 0)) for r in repl_stats
        )
        total_over = sum(
            int(r.get("over_replicated_ranges", 0)) for r in repl_stats
        )
        total_unavail = sum(
            int(r.get("unavailable_ranges", 0)) for r in repl_stats
        )

        if total_under > 0:
            all_clear = False
            reasons.append(
                f"{total_under} under-replicated ranges (must be 0)"
            )
        else:
            reasons.append("under_replicated_ranges = 0 (good)")

        if total_over > 0:
            all_clear = False
            reasons.append(
                f"{total_over} over-replicated ranges (must be 0)"
            )
        else:
            reasons.append("over_replicated_ranges = 0 (good)")

        if total_unavail > 0:
            all_clear = False
            reasons.append(
                f"{total_unavail} unavailable ranges (CRITICAL)"
            )
        else:
            reasons.append("unavailable_ranges = 0 (good)")

        # 2. Store balance
        if store_balance:
            range_counts = [
                int(s.get("range_count", 0)) for s in store_balance
            ]
            if range_counts:
                avg_rc = sum(range_counts) / len(range_counts)
                max_rc = max(range_counts)
                min_rc = min(range_counts)
                if avg_rc > 0:
                    spread_pct = (max_rc - min_rc) / avg_rc * 100
                    if spread_pct > _BALANCE_THRESHOLD_PCT:
                        all_clear = False
                        reasons.append(
                            f"Range count spread {spread_pct:.1f}% "
                            f"(max={max_rc}, min={min_rc}, "
                            f"avg={avg_rc:.0f}) "
                            f"exceeds {_BALANCE_THRESHOLD_PCT}% threshold"
                        )
                    else:
                        reasons.append(
                            f"Range count spread {spread_pct:.1f}% "
                            f"within {_BALANCE_THRESHOLD_PCT}% "
                            f"threshold (good)"
                        )

        # 3. Rangelog activity (only within the last N minutes)
        #    Only count remove_voter as a signal of active rebalancing.
        #    add_voter alone (without remove_voter) indicates initial
        #    placement or up-replication, not range movement between
        #    nodes.  split/merge are normal background housekeeping.
        now = datetime.now(tz=timezone.utc)
        recent_adds = 0
        recent_removes = 0
        for e in recent_events:
            etype = str(e.get("eventType", "")).lower()
            if etype not in ("add_voter", "remove_voter"):
                continue
            ts = e.get("timestamp")
            if isinstance(ts, datetime):
                ts_aware = (
                    ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None
                    else ts
                )
                age_min = (now - ts_aware).total_seconds() / 60
                if age_min <= _RANGELOG_WINDOW_MINUTES:
                    if etype == "add_voter":
                        recent_adds += 1
                    else:
                        recent_removes += 1

        if recent_removes > 0:
            all_clear = False
            reasons.append(
                f"{recent_adds} add_voter + {recent_removes} "
                f"remove_voter in the last "
                f"{_RANGELOG_WINDOW_MINUTES} minutes "
                f"(active rebalancing)"
            )
        elif recent_adds > 0:
            reasons.append(
                f"{recent_adds} add_voter (no remove_voter) in the "
                f"last {_RANGELOG_WINDOW_MINUTES} minutes "
                f"(initial placement, not rebalancing)"
            )
        else:
            reasons.append(
                f"No voter changes in the last "
                f"{_RANGELOG_WINDOW_MINUTES} minutes (good)"
            )

        verdict = (
            "REBALANCING COMPLETE" if all_clear
            else "REBALANCING IN PROGRESS"
        )
        return verdict, reasons

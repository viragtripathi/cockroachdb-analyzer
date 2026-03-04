"""Rebalance status analyzer - detect whether cluster rebalancing is complete."""

import logging
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

logger = logging.getLogger(__name__)

_BALANCE_THRESHOLD_PCT = 5.0


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
        repl_stats = self._get_replication_stats()
        store_balance = self._get_store_balance()
        recent_events = self._get_recent_rangelog(limit)
        rebalance_rate = self._get_rebalance_rate_setting()

        verdict, reasons = self._compute_verdict(
            repl_stats, store_balance, recent_events,
        )

        sections = [
            {
                "title": "Replication Stats",
                "headers": list(repl_stats[0].keys()) if repl_stats else [],
                "rows": repl_stats,
            },
            {
                "title": "Store Balance (range_count & lease_count per store)",
                "headers": list(store_balance[0].keys()) if store_balance else [],
                "rows": store_balance,
            },
            {
                "title": f"Recent Rangelog Events (last {limit})",
                "headers": list(recent_events[0].keys()) if recent_events else [],
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

    def _get_replication_stats(self) -> list[dict[str, Any]]:
        assert self.sql is not None
        # Try the dedicated view first (available in some versions)
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

        # Fall back: compute from crdb_internal.ranges
        return self._compute_replication_stats()

    def _compute_replication_stats(self) -> list[dict[str, Any]]:
        """Compute replication health from crdb_internal.ranges.

        Compares actual replica count against the configured
        replication factor (default 3) to find under/over replicated.
        """
        assert self.sql is not None
        try:
            expected_replicas = 3
            try:
                zrows = self.sql.execute(
                    "SHOW ZONE CONFIGURATION FOR RANGE default"
                )
                for zr in zrows:
                    raw = str(zr.get("raw_config_sql", ""))
                    if "num_replicas" in raw:
                        import re
                        m = re.search(r"num_replicas\s*=\s*(\d+)", raw)
                        if m:
                            expected_replicas = int(m.group(1))
            except Exception:
                logger.debug("zone config query failed", exc_info=True)

            ranges = self.sql.execute(
                """
                SELECT
                    range_id,
                    replicas,
                    voting_replicas
                FROM crdb_internal.ranges
                """
            )
            total = len(ranges)
            under = 0
            over = 0
            for r in ranges:
                reps = r.get("replicas", [])
                n = len(reps) if isinstance(reps, list) else 0
                if n < expected_replicas:
                    under += 1
                elif n > expected_replicas:
                    over += 1

            return [{
                "source": "computed from crdb_internal.ranges",
                "expected_replicas": expected_replicas,
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
        except Exception:
            logger.warning("kv_store_status query failed", exc_info=True)
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
        except Exception:
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

    def _compute_verdict(
        self,
        repl_stats: list[dict[str, Any]],
        store_balance: list[dict[str, Any]],
        recent_events: list[dict[str, Any]],
    ) -> tuple[str, list[str]]:
        """Return (verdict, reasons) based on the collected data."""
        reasons: list[str] = []
        all_clear = True

        # Check replication stats
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

        # Check store balance
        if store_balance:
            range_counts = [int(s.get("range_count", 0)) for s in store_balance]
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
                            f"(max={max_rc}, min={min_rc}, avg={avg_rc:.0f}) "
                            f"exceeds {_BALANCE_THRESHOLD_PCT}% threshold"
                        )
                    else:
                        reasons.append(
                            f"Range count spread {spread_pct:.1f}% "
                            f"within {_BALANCE_THRESHOLD_PCT}% threshold (good)"
                        )

        # Check rangelog activity
        rebalance_types = {
            "split", "merge", "add_voter", "remove_voter",
            "add_learner", "remove_learner",
        }
        rebalance_events = [
            e for e in recent_events
            if str(e.get("eventType", "")).lower() in rebalance_types
        ]
        if rebalance_events:
            all_clear = False
            reasons.append(
                f"{len(rebalance_events)} recent rebalance events "
                f"in last {len(recent_events)} rangelog entries "
                f"(still active)"
            )
        else:
            reasons.append("No recent rebalance events in rangelog (good)")

        verdict = (
            "REBALANCING COMPLETE" if all_clear
            else "REBALANCING IN PROGRESS"
        )
        return verdict, reasons

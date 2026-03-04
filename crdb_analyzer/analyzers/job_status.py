"""Job status analyzer - detect stuck, long-running, or problematic jobs."""

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from crdb_analyzer.analyzers.base import BaseAnalyzer

logger = logging.getLogger(__name__)

_LONG_RUNNING_SYSTEM_JOBS = frozenset({
    "AUTO SPAN CONFIG RECONCILIATION",
    "UPDATE TABLE METADATA CACHE",
    "MVCC STATISTICS UPDATE",
    "AUTO UPDATE SQL ACTIVITY",
    "POLL JOBS STATS",
    "KEY VISUALIZER",
    "SQL STATS COMPACTION",
})


class JobStatusAnalyzer(BaseAnalyzer):
    """Analyze CockroachDB internal jobs for stuck GC, long-running,
    and coordinator imbalance issues."""

    def analyze(self, limit: int = 50, **kwargs: Any) -> dict[str, Any]:
        if not self.sql:
            msg = "Job status analysis requires a SQL connection."
            raise RuntimeError(msg)
        return self._analyze(limit)

    def _analyze(self, limit: int) -> dict[str, Any]:
        assert self.sql is not None
        running_jobs = self._get_running_jobs()
        gc_jobs = self._get_gc_jobs()
        failed_jobs = self._get_failed_jobs(limit)
        gc_ttls = self._get_gc_ttls()
        coordinator_dist = self._coordinator_distribution(running_jobs)

        problematic, warnings = self._evaluate(
            running_jobs, gc_jobs, coordinator_dist,
        )

        sections = [
            {
                "title": "Running Jobs Summary",
                "headers": (
                    list(coordinator_dist[0].keys())
                    if coordinator_dist else []
                ),
                "rows": coordinator_dist,
            },
            {
                "title": "Problematic Jobs",
                "headers": (
                    list(problematic[0].keys()) if problematic else []
                ),
                "rows": problematic,
            },
            {
                "title": "Schema Change GC Jobs (waiting for MVCC GC)",
                "headers": list(gc_jobs[0].keys()) if gc_jobs else [],
                "rows": gc_jobs,
            },
            {
                "title": "GC TTL Configuration",
                "headers": list(gc_ttls[0].keys()) if gc_ttls else [],
                "rows": gc_ttls,
            },
            {
                "title": f"Recent Failed/Reverting Jobs (last {limit})",
                "headers": (
                    list(failed_jobs[0].keys()) if failed_jobs else []
                ),
                "rows": failed_jobs,
            },
        ]

        has_issues = bool(problematic or gc_jobs or failed_jobs)
        verdict = "ISSUES DETECTED" if has_issues else "ALL JOBS HEALTHY"

        return {
            "title": "Job Status",
            "source": "sql",
            "sections": sections,
            "summary": {
                "verdict": verdict,
                "total_running": len(running_jobs),
                "stuck_gc_jobs": len(gc_jobs),
                "failed_jobs": len(failed_jobs),
                "warnings": warnings,
            },
        }

    # ------------------------------------------------------------------
    # Data collection
    # ------------------------------------------------------------------

    def _get_running_jobs(self) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    job_id, job_type, description, status,
                    running_status, created, modified,
                    fraction_completed, coordinator_id
                FROM crdb_internal.jobs
                WHERE status = 'running'
                ORDER BY created
                """
            )
        except Exception:
            logger.warning("running jobs query failed", exc_info=True)
            return []

    def _get_gc_jobs(self) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    job_id,
                    description,
                    status,
                    running_status,
                    created,
                    modified,
                    coordinator_id
                FROM crdb_internal.jobs
                WHERE job_type = 'SCHEMA CHANGE GC'
                  AND status = 'running'
                ORDER BY created
                """
            )
        except Exception:
            logger.warning("gc jobs query failed", exc_info=True)
            return []

    def _get_failed_jobs(self, limit: int) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            return self.sql.execute(
                """
                SELECT
                    job_id, job_type, description, status,
                    error, created, finished, coordinator_id
                FROM crdb_internal.jobs
                WHERE status IN ('failed', 'reverting', 'cancel-requested')
                ORDER BY COALESCE(finished, created) DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception:
            logger.warning("failed jobs query failed", exc_info=True)
            return []

    def _get_gc_ttls(self) -> list[dict[str, Any]]:
        assert self.sql is not None
        try:
            import re

            rows = self.sql.execute(
                "SELECT target, raw_config_sql FROM crdb_internal.zones"
            )
            result: list[dict[str, Any]] = []
            for r in rows:
                raw = str(r.get("raw_config_sql", "") or "")
                m = re.search(r"gc\.ttlseconds\s*=\s*(\d+)", raw)
                if m:
                    ttl = int(m.group(1))
                    result.append({
                        "target": r["target"],
                        "gc_ttl_seconds": ttl,
                        "gc_ttl_human": f"{ttl / 3600:.1f}h",
                        "adjust_command": (
                            f"ALTER {r['target']} CONFIGURE ZONE "
                            f"USING gc.ttlseconds = {ttl};"
                        ),
                    })
            return result
        except Exception:
            logger.warning("gc ttl query failed", exc_info=True)
            return []

    # ------------------------------------------------------------------
    # Analysis
    # ------------------------------------------------------------------

    def _coordinator_distribution(
        self, jobs: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        by_node: dict[int, dict[str, int]] = defaultdict(
            lambda: {"total": 0, "gc": 0, "system": 0, "user": 0},
        )
        for j in jobs:
            cid = j.get("coordinator_id")
            if cid is None:
                continue
            cid = int(cid)
            by_node[cid]["total"] += 1
            jtype = str(j.get("job_type", ""))
            if jtype == "SCHEMA CHANGE GC":
                by_node[cid]["gc"] += 1
            elif jtype in _LONG_RUNNING_SYSTEM_JOBS:
                by_node[cid]["system"] += 1
            else:
                by_node[cid]["user"] += 1

        return [
            {
                "coordinator_node": nid,
                "total_jobs": counts["total"],
                "gc_jobs": counts["gc"],
                "system_jobs": counts["system"],
                "user_jobs": counts["user"],
            }
            for nid, counts in sorted(by_node.items())
        ]

    def _evaluate(
        self,
        running: list[dict[str, Any]],
        gc_jobs: list[dict[str, Any]],
        coord_dist: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        problematic: list[dict[str, Any]] = []
        warnings: list[str] = []
        now = datetime.now(tz=timezone.utc)

        for j in running:
            jtype = str(j.get("job_type", ""))
            if jtype in _LONG_RUNNING_SYSTEM_JOBS:
                continue

            created = j.get("created")
            if not isinstance(created, datetime):
                continue
            created_aware = (
                created.replace(tzinfo=timezone.utc)
                if created.tzinfo is None else created
            )
            age_hours = (now - created_aware).total_seconds() / 3600

            issue = None
            if jtype == "SCHEMA CHANGE GC" and age_hours > 6:
                issue = (
                    f"GC job waiting >{age_hours:.0f}h "
                    f"(gc.ttlseconds may be blocking)"
                )
            elif jtype not in _LONG_RUNNING_SYSTEM_JOBS and age_hours > 24:
                issue = f"Job running for {age_hours:.0f}h"

            if issue:
                problematic.append({
                    "job_id": j["job_id"],
                    "job_type": jtype,
                    "description": str(
                        j.get("description", ""),
                    )[:80],
                    "age_hours": f"{age_hours:.1f}",
                    "coordinator_node": j.get("coordinator_id"),
                    "issue": issue,
                })

        # GC accumulation warning
        if gc_jobs:
            gc_tables: dict[str, int] = defaultdict(int)
            for g in gc_jobs:
                desc = str(g.get("description", ""))
                gc_tables[desc] += 1
            for desc, count in gc_tables.items():
                if count > 1:
                    warnings.append(
                        f"{count} pending GC jobs for: "
                        f"{desc[:70]}. Repeated TRUNCATEs create "
                        f"GC backlog. Dead data occupies ranges "
                        f"and inflates node storage until "
                        f"gc.ttlseconds expires."
                    )

        # Coordinator imbalance
        if coord_dist:
            totals = [c["total_jobs"] for c in coord_dist]
            if len(totals) > 1 and max(totals) > 2 * min(totals):
                heavy = max(coord_dist, key=lambda c: c["total_jobs"])
                warnings.append(
                    f"Node {heavy['coordinator_node']} is coordinating "
                    f"{heavy['total_jobs']} jobs "
                    f"({heavy['gc_jobs']} GC). Heavy job coordination "
                    f"adds CPU/IO load to that node."
                )

        return problematic, warnings

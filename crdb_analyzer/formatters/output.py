"""Output formatting for analysis results in table, JSON, and CSV formats."""

import csv
import io
import json
from typing import Any

from tabulate import tabulate


def format_results(results: dict[str, Any], fmt: str = "table") -> str:
    match fmt:
        case "json":
            return _format_json(results)
        case "csv":
            return _format_csv(results)
        case _:
            return _format_table(results)


def _format_table(results: dict[str, Any]) -> str:
    parts: list[str] = []

    title = results.get("title", "Results")
    parts.append(f"\n{'=' * len(title)}")
    parts.append(title)
    parts.append(f"{'=' * len(title)}")

    source = results.get("source", "")
    if source:
        parts.append(f"(source: {source})")

    if sections := results.get("sections"):
        for section in sections:
            parts.append(f"\n--- {section.get('title', '')} ---")
            parts.append(_render_table(section.get("headers", []), section.get("rows", [])))
    else:
        headers = results.get("headers", [])
        rows = results.get("rows", [])
        parts.append(_render_table(headers, rows))

    if summary := results.get("summary"):
        parts.append("\nSummary:")
        for k, v in summary.items():
            if isinstance(v, dict):
                parts.append(f"  {k}:")
                for setting, description in v.items():
                    parts.append(f"    {setting}")
                    parts.append(f"      {description}")
            else:
                parts.append(f"  {k}: {v}")

    return "\n".join(parts)


def _render_table(headers: list[str], rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "  (no data)"
    if headers:
        table_data = [[_truncate(row.get(h, ""), 60) for h in headers] for row in rows]
        return tabulate(table_data, headers=headers, tablefmt="simple", numalign="right")
    header_keys = list(rows[0].keys())
    table_data = [[_truncate(v, 60) for v in row.values()] for row in rows]
    return tabulate(table_data, headers=header_keys, tablefmt="simple", numalign="right")


def _format_json(results: dict[str, Any]) -> str:
    return json.dumps(results, indent=2, default=str)


def _format_csv(results: dict[str, Any]) -> str:
    rows = results.get("rows", [])
    if not rows and (sections := results.get("sections")):
        for section in sections:
            rows.extend(section.get("rows", []))
    if not rows:
        return ""
    buf = io.StringIO()
    headers = results.get("headers") or list(rows[0].keys())
    writer = csv.DictWriter(buf, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


def _truncate(value: Any, max_len: int) -> str:
    s = str(value)
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s

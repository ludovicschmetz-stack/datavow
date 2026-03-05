"""Freshness validation — checks timestamp fields against SLA requirements."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

import duckdb

from datavow.contract import DataContract, FieldType, Severity
from datavow.findings import Finding

_DURATION_PATTERN = re.compile(r"^(\d+)\s*(m|min|h|hour|d|day)s?$", re.IGNORECASE)


def parse_duration(s: str) -> timedelta:
    """Parse a human-readable duration string to timedelta."""
    match = _DURATION_PATTERN.match(s.strip())
    if not match:
        raise ValueError(f"Cannot parse duration: '{s}'. Expected format: 24h, 1d, 30m")
    value = int(match.group(1))
    unit = match.group(2).lower()
    if unit in ("m", "min"):
        return timedelta(minutes=value)
    if unit in ("h", "hour"):
        return timedelta(hours=value)
    if unit in ("d", "day"):
        return timedelta(days=value)
    raise ValueError(f"Unknown duration unit: {unit}")


def validate_freshness(
    con: duckdb.DuckDBPyConnection,
    table: str,
    contract: DataContract,
) -> list[Finding]:
    """Validate freshness for timestamp fields with SLA constraints."""
    findings: list[Finding] = []
    now = datetime.now(timezone.utc)

    checks: list[tuple[str, str, str]] = []

    if contract.sla.freshness:
        ts_fields = [
            f for f in contract.schema_.fields
            if f.type in (FieldType.TIMESTAMP, FieldType.DATE)
        ]
        for f in ts_fields:
            checks.append((f.name, contract.sla.freshness, "sla"))

    for field_name, duration_str, source in checks:
        try:
            max_age = parse_duration(duration_str)
            threshold = now - max_age

            result = con.execute(
                f'SELECT MAX("{field_name}") FROM {table}'
            ).fetchone()[0]

            if result is None:
                findings.append(Finding(
                    rule=f"freshness:{field_name}",
                    category="freshness",
                    severity=Severity.CRITICAL,
                    message=f"Field '{field_name}' has no non-null values — cannot assess freshness",
                    passed=False,
                ))
                continue

            if isinstance(result, datetime):
                latest = result if result.tzinfo else result.replace(tzinfo=timezone.utc)
            else:
                latest = datetime.combine(result, datetime.min.time(), tzinfo=timezone.utc)

            passed = latest >= threshold
            age = now - latest
            age_str = _format_timedelta(age)

            findings.append(Finding(
                rule=f"freshness:{field_name}",
                category="freshness",
                severity=Severity.CRITICAL if source == "sla" else Severity.WARNING,
                message=(
                    f"Field '{field_name}' is fresh (age: {age_str}, max: {duration_str})"
                    if passed
                    else f"Field '{field_name}' is stale (age: {age_str}, max allowed: {duration_str})"
                ),
                passed=passed,
                details=f"latest={latest.isoformat()}, threshold={threshold.isoformat()}",
            ))
        except Exception as e:
            findings.append(Finding(
                rule=f"freshness:{field_name}",
                category="freshness",
                severity=Severity.WARNING,
                message=f"Freshness check error for '{field_name}': {e}",
                passed=False,
                details=str(e),
            ))

    return findings


def _format_timedelta(td: timedelta) -> str:
    total_seconds = int(td.total_seconds())
    if total_seconds < 3600:
        return f"{total_seconds // 60}m"
    if total_seconds < 86400:
        return f"{total_seconds // 3600}h {(total_seconds % 3600) // 60}m"
    days = total_seconds // 86400
    hours = (total_seconds % 86400) // 3600
    return f"{days}d {hours}h"

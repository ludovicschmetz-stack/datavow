"""Quality rules engine — executes contract quality.rules against loaded data."""

from __future__ import annotations

import duckdb

from datavow.contract import DataContract, QualityRule, RuleType
from datavow.findings import Finding


def validate_quality(
    con: duckdb.DuckDBPyConnection,
    table: str,
    contract: DataContract,
) -> list[Finding]:
    """Run all quality rules defined in the contract."""
    findings: list[Finding] = []
    for rule in contract.quality.rules:
        try:
            finding = _execute_rule(con, table, rule)
            findings.append(finding)
        except Exception as e:
            findings.append(Finding(
                rule=rule.name,
                category="quality",
                severity=rule.severity,
                message=f"Rule '{rule.name}' execution error: {e}",
                passed=False,
                details=str(e),
            ))
    return findings


def _execute_rule(
    con: duckdb.DuckDBPyConnection,
    table: str,
    rule: QualityRule,
) -> Finding:
    """Dispatch and execute a single quality rule."""
    dispatch = {
        RuleType.SQL: _rule_sql,
        RuleType.NOT_NULL: _rule_not_null,
        RuleType.UNIQUE: _rule_unique,
        RuleType.ROW_COUNT: _rule_row_count,
        RuleType.RANGE: _rule_range,
        RuleType.ACCEPTED_VALUES: _rule_accepted_values,
        RuleType.REGEX: _rule_regex,
    }
    handler = dispatch.get(rule.type)
    if handler is None:
        return Finding(
            rule=rule.name, category="quality", severity=rule.severity,
            message=f"Unknown rule type: {rule.type}", passed=False,
        )
    return handler(con, table, rule)


def _rule_sql(con, table, rule):
    query = rule.query.replace("{table}", table)
    result = con.execute(query).fetchone()[0]
    threshold = rule.threshold if rule.threshold is not None else 0
    passed = result <= threshold
    return Finding(
        rule=rule.name, category="quality", severity=rule.severity,
        message=(
            f"SQL check '{rule.name}' passed (result={result}, threshold={threshold})"
            if passed
            else f"SQL check '{rule.name}' failed: {result} violations (threshold={threshold})"
        ),
        passed=passed,
        details=f"query_result={result}, threshold={threshold}",
    )


def _rule_not_null(con, table, rule):
    null_count = con.execute(
        f'SELECT COUNT(*) FROM {table} WHERE "{rule.field}" IS NULL'
    ).fetchone()[0]
    return Finding(
        rule=rule.name, category="quality", severity=rule.severity,
        message=(f"Field '{rule.field}' has no nulls" if null_count == 0
                 else f"Field '{rule.field}' has {null_count} null(s)"),
        passed=null_count == 0, details=f"null_count={null_count}",
    )


def _rule_unique(con, table, rule):
    dup_count = con.execute(
        f'SELECT COUNT(*) - COUNT(DISTINCT "{rule.field}") FROM {table}'
    ).fetchone()[0]
    return Finding(
        rule=rule.name, category="quality", severity=rule.severity,
        message=(f"Field '{rule.field}' values are unique" if dup_count == 0
                 else f"Field '{rule.field}' has {dup_count} duplicate(s)"),
        passed=dup_count == 0, details=f"duplicate_count={dup_count}",
    )


def _rule_row_count(con, table, rule):
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    passed = True
    violations = []
    if rule.min is not None and count < rule.min:
        passed = False
        violations.append(f"below minimum {rule.min}")
    if rule.max is not None and count > rule.max:
        passed = False
        violations.append(f"above maximum {rule.max}")
    return Finding(
        rule=rule.name, category="quality", severity=rule.severity,
        message=(f"Row count {count} is within bounds [{rule.min}, {rule.max}]" if passed
                 else f"Row count {count} is {' and '.join(violations)}"),
        passed=passed, details=f"row_count={count}, min={rule.min}, max={rule.max}",
    )


def _rule_range(con, table, rule):
    conditions = []
    if rule.min_value is not None:
        conditions.append(f'"{rule.field}" < {rule.min_value}')
    if rule.max_value is not None:
        conditions.append(f'"{rule.field}" > {rule.max_value}')
    if not conditions:
        return Finding(
            rule=rule.name, category="quality", severity=rule.severity,
            message=f"Range rule '{rule.name}' has no bounds defined", passed=True,
        )
    where = " OR ".join(conditions)
    violations = con.execute(
        f'SELECT COUNT(*) FROM {table} WHERE "{rule.field}" IS NOT NULL AND ({where})'
    ).fetchone()[0]
    return Finding(
        rule=rule.name, category="quality", severity=rule.severity,
        message=(f"Field '{rule.field}' values within range" if violations == 0
                 else f"Field '{rule.field}' has {violations} out-of-range value(s)"),
        passed=violations == 0,
        details=f"violations={violations}, min={rule.min_value}, max={rule.max_value}",
    )


def _rule_accepted_values(con, table, rule):
    values_str = ", ".join(f"'{v}'" for v in rule.values)
    violations = con.execute(
        f"""SELECT COUNT(*) FROM {table}
            WHERE "{rule.field}" IS NOT NULL
            AND "{rule.field}"::VARCHAR NOT IN ({values_str})"""
    ).fetchone()[0]
    return Finding(
        rule=rule.name, category="quality", severity=rule.severity,
        message=(f"Field '{rule.field}' values all in accepted set" if violations == 0
                 else f"Field '{rule.field}' has {violations} value(s) not in accepted set"),
        passed=violations == 0, details=f"violations={violations}, accepted={rule.values}",
    )


def _rule_regex(con, table, rule):
    mismatches = con.execute(
        f"""SELECT COUNT(*) FROM {table}
            WHERE "{rule.field}" IS NOT NULL
            AND NOT regexp_matches("{rule.field}"::VARCHAR, '{rule.pattern}')"""
    ).fetchone()[0]
    return Finding(
        rule=rule.name, category="quality", severity=rule.severity,
        message=(f"Field '{rule.field}' matches pattern" if mismatches == 0
                 else f"Field '{rule.field}' has {mismatches} value(s) not matching pattern"),
        passed=mismatches == 0, details=f"pattern={rule.pattern}, mismatches={mismatches}",
    )

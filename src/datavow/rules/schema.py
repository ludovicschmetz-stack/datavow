"""Schema validation rules — type, required, unique, pattern, range, allowed_values."""

from __future__ import annotations

import duckdb

from datavow.contract import DataContract, FieldSpec, Severity
from datavow.findings import Finding


def validate_schema(
    con: duckdb.DuckDBPyConnection,
    table: str,
    contract: DataContract,
) -> list[Finding]:
    """Validate data schema against contract field definitions."""
    findings: list[Finding] = []
    fields = contract.schema_.fields

    col_info = con.execute(f"DESCRIBE {table}").fetchall()
    actual_cols = {row[0]: row[1].upper() for row in col_info}

    for field_spec in fields:
        findings.extend(_validate_field(con, table, field_spec, actual_cols))

    expected_names = {f.name for f in fields}
    unexpected = set(actual_cols.keys()) - expected_names
    if unexpected:
        findings.append(
            Finding(
                rule="unexpected_columns",
                category="schema",
                severity=Severity.INFO,
                message=f"Unexpected columns found: {', '.join(sorted(unexpected))}",
                passed=True,
                details=f"Contract defines {len(fields)} fields, source has {len(actual_cols)}",
            )
        )

    return findings


def _validate_field(
    con: duckdb.DuckDBPyConnection,
    table: str,
    field_spec: FieldSpec,
    actual_cols: dict[str, str],
) -> list[Finding]:
    """Run all schema checks for a single field."""
    findings: list[Finding] = []
    name = field_spec.name

    if name not in actual_cols:
        severity = Severity.CRITICAL if field_spec.required else Severity.WARNING
        findings.append(
            Finding(
                rule=f"field_exists:{name}",
                category="schema",
                severity=severity,
                message=f"Field '{name}' not found in source",
                passed=False,
            )
        )
        return findings

    findings.append(
        Finding(
            rule=f"field_exists:{name}",
            category="schema",
            severity=Severity.CRITICAL,
            message=f"Field '{name}' exists",
            passed=True,
        )
    )

    actual_type = actual_cols[name]
    expected_types = field_spec.type.duckdb_types
    type_ok = any(t in actual_type for t in expected_types)
    findings.append(
        Finding(
            rule=f"field_type:{name}",
            category="schema",
            severity=Severity.CRITICAL,
            message=(
                f"Field '{name}' type OK ({actual_type})"
                if type_ok
                else f"Field '{name}' type mismatch: expected {field_spec.type.value}, got {actual_type}"
            ),
            passed=type_ok,
        )
    )

    if field_spec.required:
        null_count = con.execute(f'SELECT COUNT(*) FROM {table} WHERE "{name}" IS NULL').fetchone()[
            0
        ]
        findings.append(
            Finding(
                rule=f"required:{name}",
                category="schema",
                severity=Severity.CRITICAL,
                message=(
                    f"Field '{name}' has no nulls"
                    if null_count == 0
                    else f"Field '{name}' has {null_count} null(s) but is required"
                ),
                passed=null_count == 0,
                details=f"null_count={null_count}",
            )
        )

    if field_spec.unique:
        dup_count = con.execute(
            f'SELECT COUNT(*) - COUNT(DISTINCT "{name}") FROM {table}'
        ).fetchone()[0]
        findings.append(
            Finding(
                rule=f"unique:{name}",
                category="schema",
                severity=Severity.CRITICAL,
                message=(
                    f"Field '{name}' values are unique"
                    if dup_count == 0
                    else f"Field '{name}' has {dup_count} duplicate(s)"
                ),
                passed=dup_count == 0,
                details=f"duplicate_count={dup_count}",
            )
        )

    if field_spec.pattern:
        pattern = field_spec.pattern
        mismatch_count = con.execute(
            f"""SELECT COUNT(*) FROM {table}
                WHERE "{name}" IS NOT NULL
                AND NOT regexp_matches("{name}"::VARCHAR, '{pattern}')"""
        ).fetchone()[0]
        findings.append(
            Finding(
                rule=f"pattern:{name}",
                category="schema",
                severity=Severity.WARNING,
                message=(
                    f"Field '{name}' matches pattern"
                    if mismatch_count == 0
                    else f"Field '{name}' has {mismatch_count} value(s) not matching pattern"
                ),
                passed=mismatch_count == 0,
                details=f"pattern={pattern}, mismatches={mismatch_count}",
            )
        )

    if field_spec.min is not None or field_spec.max is not None:
        conditions = []
        if field_spec.min is not None:
            conditions.append(f'"{name}" < {field_spec.min}')
        if field_spec.max is not None:
            conditions.append(f'"{name}" > {field_spec.max}')
        where = " OR ".join(conditions)
        violations = con.execute(
            f'SELECT COUNT(*) FROM {table} WHERE "{name}" IS NOT NULL AND ({where})'
        ).fetchone()[0]
        range_desc = (
            f"[{field_spec.min}, {field_spec.max}]"
            if field_spec.min is not None and field_spec.max is not None
            else f">= {field_spec.min}"
            if field_spec.min is not None
            else f"<= {field_spec.max}"
        )
        findings.append(
            Finding(
                rule=f"range:{name}",
                category="schema",
                severity=Severity.WARNING,
                message=(
                    f"Field '{name}' values within range {range_desc}"
                    if violations == 0
                    else f"Field '{name}' has {violations} value(s) outside range {range_desc}"
                ),
                passed=violations == 0,
                details=f"violations={violations}",
            )
        )

    if field_spec.allowed_values:
        values_str = ", ".join(f"'{v}'" for v in field_spec.allowed_values)
        violations = con.execute(
            f"""SELECT COUNT(*) FROM {table}
                WHERE "{name}" IS NOT NULL
                AND "{name}"::VARCHAR NOT IN ({values_str})"""
        ).fetchone()[0]
        findings.append(
            Finding(
                rule=f"allowed_values:{name}",
                category="schema",
                severity=Severity.WARNING,
                message=(
                    f"Field '{name}' values are all in allowed set"
                    if violations == 0
                    else f"Field '{name}' has {violations} value(s) not in allowed set"
                ),
                passed=violations == 0,
                details=f"allowed={field_spec.allowed_values}, violations={violations}",
            )
        )

    return findings

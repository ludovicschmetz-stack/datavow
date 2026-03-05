#!/usr/bin/env bash
# DataVow — integrate validate command into existing scaffold
# Run from repo root: ~/Documents/Projects/datavow
set -euo pipefail

echo "=== DataVow: integrating validate command ==="

# Verify we're in the right place
if [[ ! -f "src/datavow/cli.py" ]]; then
    echo "ERROR: run this from the datavow repo root (where src/datavow/cli.py exists)"
    exit 1
fi

# Create new directories
mkdir -p src/datavow/rules
mkdir -p src/datavow/connectors
mkdir -p tests/fixtures

echo "[1/4] Creating new modules..."

# ── src/datavow/__init__.py (update) ──
cat > src/datavow/__init__.py << 'PYEOF'
"""DataVow — A solemn vow on your data. From YAML to verdict."""

__version__ = "0.1.0"
PYEOF

# ── src/datavow/contract.py (NEW) ──
cat > src/datavow/contract.py << 'PYEOF'
"""Pydantic models for DataVow contract parsing and validation.

Implements a superset of ODCS v3.1 with pragmatic extensions
(severity, notifications, SLA, PII flags).
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class Severity(str, Enum):
    """Rule severity — determines Vow Score impact and CI behavior."""

    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"

    @property
    def weight(self) -> int:
        return {Severity.CRITICAL: 20, Severity.WARNING: 5, Severity.INFO: 1}[self]


class FieldType(str, Enum):
    """Supported column types for schema validation."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"

    @property
    def duckdb_types(self) -> list[str]:
        """Return acceptable DuckDB type names for this logical type."""
        mapping: dict[FieldType, list[str]] = {
            FieldType.STRING: ["VARCHAR", "TEXT", "STRING"],
            FieldType.INTEGER: ["INTEGER", "BIGINT", "SMALLINT", "TINYINT", "INT", "HUGEINT"],
            FieldType.FLOAT: ["FLOAT", "DOUBLE", "REAL"],
            FieldType.DECIMAL: ["DECIMAL", "NUMERIC", "FLOAT", "DOUBLE", "REAL"],
            FieldType.BOOLEAN: ["BOOLEAN", "BOOL"],
            FieldType.DATE: ["DATE"],
            FieldType.TIMESTAMP: ["TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ",
                                  "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS"],
        }
        return mapping[self]


class FieldSpec(BaseModel):
    """Schema field definition."""

    name: str
    type: FieldType
    required: bool = False
    unique: bool = False
    pii: bool = False
    description: str = ""
    pattern: str | None = None
    min: float | None = Field(None, alias="min")
    max: float | None = Field(None, alias="max")
    allowed_values: list[Any] | None = None

    model_config = {"populate_by_name": True}


class SchemaSpec(BaseModel):
    """Schema definition (table structure)."""

    type: str = "table"
    fields: list[FieldSpec]


class RuleType(str, Enum):
    """Built-in quality rule types."""

    SQL = "sql"
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    ROW_COUNT = "row_count"
    RANGE = "range"
    ACCEPTED_VALUES = "accepted_values"
    REGEX = "regex"


class QualityRule(BaseModel):
    """Quality rule definition."""

    name: str
    type: RuleType
    severity: Severity = Severity.WARNING

    # SQL rule
    query: str | None = None
    threshold: int | None = None

    # Field-level rules
    field: str | None = None

    # Row count
    min: int | None = None
    max: int | None = None

    # Range
    min_value: float | None = None
    max_value: float | None = None

    # Accepted values
    values: list[Any] | None = None

    # Regex
    pattern: str | None = None

    @model_validator(mode="after")
    def validate_rule_fields(self) -> "QualityRule":
        if self.type == RuleType.SQL and not self.query:
            raise ValueError(f"Rule '{self.name}': SQL rules require 'query'")
        if self.type in (RuleType.NOT_NULL, RuleType.UNIQUE, RuleType.RANGE,
                         RuleType.ACCEPTED_VALUES, RuleType.REGEX) and not self.field:
            raise ValueError(f"Rule '{self.name}': {self.type.value} rules require 'field'")
        return self


class QualitySpec(BaseModel):
    """Quality rules block."""

    rules: list[QualityRule] = []


class SLASpec(BaseModel):
    """Service Level Agreement spec."""

    freshness: str | None = None
    completeness: str | None = None
    availability: str | None = None


class NotificationTarget(BaseModel):
    """Notification target."""

    type: str
    channel: str | None = None
    to: str | None = None


class NotificationSpec(BaseModel):
    """Notification configuration."""

    on_failure: list[NotificationTarget] = []
    on_warning: list[NotificationTarget] = []


class ContractMetadata(BaseModel):
    """Contract metadata block."""

    name: str
    version: str = "1.0.0"
    owner: str = ""
    domain: str = ""
    description: str = ""
    tags: list[str] = []


class DataContract(BaseModel):
    """Root DataVow contract model — superset of ODCS v3.1."""

    apiVersion: str = "datavow/v1"
    kind: str = "DataContract"
    metadata: ContractMetadata
    schema_: SchemaSpec = Field(alias="schema")
    quality: QualitySpec = QualitySpec()
    sla: SLASpec = SLASpec()
    notifications: NotificationSpec = NotificationSpec()

    model_config = {"populate_by_name": True}

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, v: str) -> str:
        if v != "DataContract":
            raise ValueError(f"Expected kind 'DataContract', got '{v}'")
        return v

    @classmethod
    def from_yaml(cls, path: str | Path) -> "DataContract":
        """Load and validate a contract from a YAML file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Contract file not found: {path}")
        if not path.suffix in (".yaml", ".yml"):
            raise ValueError(f"Expected .yaml/.yml file, got: {path.suffix}")

        with open(path) as f:
            raw = yaml.safe_load(f)

        if not isinstance(raw, dict):
            raise ValueError(f"Invalid contract format in {path}: expected a YAML mapping")

        return cls.model_validate(raw)
PYEOF

# ── src/datavow/findings.py (NEW) ──
cat > src/datavow/findings.py << 'PYEOF'
"""Validation findings and scoring."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from datavow.contract import Severity


class Verdict(str, Enum):
    """Vow verdict based on score."""

    KEPT = "Vow Kept"
    STRAINED = "Vow Strained"
    BROKEN = "Vow Broken"
    SHATTERED = "Vow Shattered"

    @property
    def emoji(self) -> str:
        return {
            Verdict.KEPT: "✅",
            Verdict.STRAINED: "⚠️",
            Verdict.BROKEN: "🔧",
            Verdict.SHATTERED: "❌",
        }[self]

    @property
    def description(self) -> str:
        return {
            Verdict.KEPT: "fully compliant",
            Verdict.STRAINED: "action needed",
            Verdict.BROKEN: "blocking issues",
            Verdict.SHATTERED: "critical violations",
        }[self]


@dataclass
class Finding:
    """Single validation finding."""

    rule: str
    category: str
    severity: Severity
    message: str
    passed: bool
    details: str = ""

    @property
    def status_icon(self) -> str:
        return "✓" if self.passed else "✗"


@dataclass
class ValidationResult:
    """Aggregated validation result with scoring."""

    contract_name: str
    source_path: str
    findings: list[Finding] = field(default_factory=list)

    def add(self, finding: Finding) -> None:
        self.findings.append(finding)

    @property
    def critical_count(self) -> int:
        return sum(1 for f in self.findings if not f.passed and f.severity == Severity.CRITICAL)

    @property
    def warning_count(self) -> int:
        return sum(1 for f in self.findings if not f.passed and f.severity == Severity.WARNING)

    @property
    def info_count(self) -> int:
        return sum(1 for f in self.findings if not f.passed and f.severity == Severity.INFO)

    @property
    def passed_count(self) -> int:
        return sum(1 for f in self.findings if f.passed)

    @property
    def failed_count(self) -> int:
        return sum(1 for f in self.findings if not f.passed)

    @property
    def score(self) -> int:
        """Vow Score = 100 - (20×CRITICAL + 5×WARNING + 1×INFO), min 0."""
        raw = 100 - (20 * self.critical_count + 5 * self.warning_count + 1 * self.info_count)
        return max(0, raw)

    @property
    def verdict(self) -> Verdict:
        s = self.score
        if s >= 95:
            return Verdict.KEPT
        if s >= 80:
            return Verdict.STRAINED
        if s >= 50:
            return Verdict.BROKEN
        return Verdict.SHATTERED

    @property
    def has_critical_failures(self) -> bool:
        return self.critical_count > 0
PYEOF

# ── src/datavow/validator.py (NEW) ──
cat > src/datavow/validator.py << 'PYEOF'
"""DataVow validator — orchestrates schema, quality, and freshness checks."""

from __future__ import annotations

from pathlib import Path

import duckdb

from datavow.connectors.file import load_file
from datavow.contract import DataContract
from datavow.findings import ValidationResult
from datavow.rules.freshness import validate_freshness
from datavow.rules.quality import validate_quality
from datavow.rules.schema import validate_schema


def validate(
    contract_path: str | Path,
    source_path: str | Path,
) -> ValidationResult:
    """Run full validation: load contract, load data, execute all rules."""
    contract = DataContract.from_yaml(contract_path)
    con = duckdb.connect(":memory:")
    table = load_file(con, source_path)

    result = ValidationResult(
        contract_name=contract.metadata.name,
        source_path=str(source_path),
    )

    for finding in validate_schema(con, table, contract):
        result.add(finding)
    for finding in validate_quality(con, table, contract):
        result.add(finding)
    for finding in validate_freshness(con, table, contract):
        result.add(finding)

    con.close()
    return result
PYEOF

# ── src/datavow/connectors/__init__.py (NEW) ──
cat > src/datavow/connectors/__init__.py << 'PYEOF'
"""DataVow connectors — load data sources into DuckDB."""
PYEOF

# ── src/datavow/connectors/file.py (NEW) ──
cat > src/datavow/connectors/file.py << 'PYEOF'
"""File connector — loads Parquet, CSV, JSON into DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb

TABLE_NAME = "datavow_source"

_SUPPORTED_EXTENSIONS = {".csv", ".parquet", ".json", ".jsonl", ".ndjson", ".tsv"}


def load_file(
    con: duckdb.DuckDBPyConnection,
    source_path: str | Path,
) -> str:
    """Load a file into DuckDB and return the table name."""
    path = Path(source_path)
    if not path.exists():
        raise FileNotFoundError(f"Data source not found: {path}")

    ext = path.suffix.lower()
    if ext not in _SUPPORTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file format: {ext}. "
            f"Supported: {', '.join(sorted(_SUPPORTED_EXTENSIONS))}"
        )

    escaped_path = str(path).replace("'", "''")
    con.execute(f"CREATE OR REPLACE TABLE {TABLE_NAME} AS SELECT * FROM '{escaped_path}'")

    row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    if row_count == 0:
        raise ValueError(f"Source file is empty: {path}")

    return TABLE_NAME
PYEOF

# ── src/datavow/rules/__init__.py (NEW) ──
cat > src/datavow/rules/__init__.py << 'PYEOF'
"""DataVow validation rules — schema, quality, freshness."""
PYEOF

# ── src/datavow/rules/schema.py (NEW) ──
cat > src/datavow/rules/schema.py << 'PYEOF'
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
        findings.append(Finding(
            rule="unexpected_columns",
            category="schema",
            severity=Severity.INFO,
            message=f"Unexpected columns found: {', '.join(sorted(unexpected))}",
            passed=True,
            details=f"Contract defines {len(fields)} fields, source has {len(actual_cols)}",
        ))

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
        findings.append(Finding(
            rule=f"field_exists:{name}",
            category="schema",
            severity=severity,
            message=f"Field '{name}' not found in source",
            passed=False,
        ))
        return findings

    findings.append(Finding(
        rule=f"field_exists:{name}",
        category="schema",
        severity=Severity.CRITICAL,
        message=f"Field '{name}' exists",
        passed=True,
    ))

    actual_type = actual_cols[name]
    expected_types = field_spec.type.duckdb_types
    type_ok = any(t in actual_type for t in expected_types)
    findings.append(Finding(
        rule=f"field_type:{name}",
        category="schema",
        severity=Severity.CRITICAL,
        message=(
            f"Field '{name}' type OK ({actual_type})"
            if type_ok
            else f"Field '{name}' type mismatch: expected {field_spec.type.value}, got {actual_type}"
        ),
        passed=type_ok,
    ))

    if field_spec.required:
        null_count = con.execute(
            f'SELECT COUNT(*) FROM {table} WHERE "{name}" IS NULL'
        ).fetchone()[0]
        findings.append(Finding(
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
        ))

    if field_spec.unique:
        dup_count = con.execute(
            f'SELECT COUNT(*) - COUNT(DISTINCT "{name}") FROM {table}'
        ).fetchone()[0]
        findings.append(Finding(
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
        ))

    if field_spec.pattern:
        pattern = field_spec.pattern
        mismatch_count = con.execute(
            f"""SELECT COUNT(*) FROM {table}
                WHERE "{name}" IS NOT NULL
                AND NOT regexp_matches("{name}"::VARCHAR, '{pattern}')"""
        ).fetchone()[0]
        findings.append(Finding(
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
        ))

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
            else f">= {field_spec.min}" if field_spec.min is not None
            else f"<= {field_spec.max}"
        )
        findings.append(Finding(
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
        ))

    if field_spec.allowed_values:
        values_str = ", ".join(f"'{v}'" for v in field_spec.allowed_values)
        violations = con.execute(
            f"""SELECT COUNT(*) FROM {table}
                WHERE "{name}" IS NOT NULL
                AND "{name}"::VARCHAR NOT IN ({values_str})"""
        ).fetchone()[0]
        findings.append(Finding(
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
        ))

    return findings
PYEOF

# ── src/datavow/rules/quality.py (NEW) ──
cat > src/datavow/rules/quality.py << 'PYEOF'
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
PYEOF

# ── src/datavow/rules/freshness.py (NEW) ──
cat > src/datavow/rules/freshness.py << 'PYEOF'
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
PYEOF

echo "[2/4] Updating CLI (merging init + validate)..."

# ── src/datavow/cli.py (MERGE — keep init, add validate) ──
cat > src/datavow/cli.py << 'PYEOF'
"""DataVow CLI — Typer-based command interface.

Commands:
  init      Scaffold a new datavow project.
  validate  Validate a data source against a contract.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from datavow import __version__
from datavow.findings import Finding, Severity, ValidationResult, Verdict

app = typer.Typer(
    name="datavow",
    help="A solemn vow on your data. From YAML to verdict.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
console = Console()

_SEVERITY_STYLES = {
    Severity.CRITICAL: "bold red",
    Severity.WARNING: "yellow",
    Severity.INFO: "dim",
}

_VERDICT_STYLES = {
    Verdict.KEPT: "bold green",
    Verdict.STRAINED: "bold yellow",
    Verdict.BROKEN: "bold red",
    Verdict.SHATTERED: "bold red on white",
}


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"datavow {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        Optional[bool],
        typer.Option("--version", "-v", help="Show version.", callback=_version_callback,
                     is_eager=True),
    ] = None,
) -> None:
    """DataVow — A solemn vow on your data."""


# ──────────────────────────────────────────────
# datavow init
# ──────────────────────────────────────────────

EXAMPLE_CONTRACT = {
    "contract": "example",
    "description": "Sample data contract",
    "columns": [
        {"name": "id", "type": "integer", "not_null": True},
        {"name": "name", "type": "string", "not_null": True},
        {"name": "created_at", "type": "timestamp"},
    ],
}


@app.command()
def init(
    project_name: str = typer.Argument("my-project", help="Name of the project"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing files"),
) -> None:
    """Scaffold a new datavow project in the current directory."""
    config_path = Path("datavow.yaml")

    if config_path.exists() and not force:
        typer.echo("datavow.yaml already exists. Use --force to overwrite.", err=True)
        raise typer.Exit(code=1)

    config = {
        "project": project_name,
        "connectors": [],
        "rules": [],
    }

    config_path.write_text(yaml.dump(config, sort_keys=False))

    contracts_dir = Path("contracts")
    contracts_dir.mkdir(exist_ok=True)

    example_path = contracts_dir / "example.yaml"
    example_path.write_text(yaml.dump(EXAMPLE_CONTRACT, sort_keys=False))

    typer.echo(f"Initialized datavow project '{project_name}'.")
    typer.echo("Created datavow.yaml and contracts/example.yaml")


# ──────────────────────────────────────────────
# datavow validate
# ──────────────────────────────────────────────

@app.command()
def validate(
    contract: Annotated[
        Path,
        typer.Argument(help="Path to the contract YAML file.", exists=True, readable=True),
    ],
    source: Annotated[
        Path,
        typer.Argument(help="Path to the data source (CSV, Parquet, JSON).", exists=True, readable=True),
    ],
    output: Annotated[
        str,
        typer.Option("--output", "-o", help="Output format: table, json, or summary."),
    ] = "table",
    ci: Annotated[
        bool,
        typer.Option("--ci", help="CI mode: exit code 1 on CRITICAL failures."),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", help="Show all findings including passed checks."),
    ] = False,
) -> None:
    """Validate a data source against a DataVow contract.

    Runs schema, quality, and freshness checks. Outputs findings
    with severity levels and a Vow Score.
    """
    from datavow.validator import validate as run_validate

    try:
        result = run_validate(contract, source)
    except FileNotFoundError as e:
        console.print(f"[bold red]Error:[/] {e}")
        raise typer.Exit(code=2)
    except ValueError as e:
        console.print(f"[bold red]Contract error:[/] {e}")
        raise typer.Exit(code=2)
    except Exception as e:
        console.print(f"[bold red]Validation error:[/] {e}")
        raise typer.Exit(code=2)

    if output == "json":
        _output_json(result)
    elif output == "summary":
        _output_summary(result)
    else:
        _output_table(result, verbose=verbose)

    if ci and result.has_critical_failures:
        raise typer.Exit(code=1)


# ──────────────────────────────────────────────
# Output formatters
# ──────────────────────────────────────────────

def _output_table(result: ValidationResult, verbose: bool = False) -> None:
    console.print()
    console.print(
        f"[bold]DataVow[/] — validating [cyan]{result.contract_name}[/]"
        f" against [cyan]{result.source_path}[/]"
    )
    console.print()

    table = Table(show_header=True, header_style="bold", show_lines=False, pad_edge=True)
    table.add_column("", width=3)
    table.add_column("Rule", min_width=25)
    table.add_column("Category", width=10)
    table.add_column("Severity", width=10)
    table.add_column("Message", min_width=40)

    findings = result.findings if verbose else [f for f in result.findings if not f.passed]

    for f in findings:
        sev_style = _SEVERITY_STYLES.get(f.severity, "")
        icon = "[green]✓[/]" if f.passed else f"[{sev_style}]✗[/]"
        table.add_row(icon, f.rule, f.category, Text(f.severity.value, style=sev_style), f.message)

    if not findings:
        if verbose:
            console.print("[dim]No findings.[/]")
        else:
            console.print("[green]All checks passed — no failures to show.[/]")
            console.print("[dim]Use --verbose to see all findings.[/]")

    if findings:
        console.print(table)
    console.print()
    _print_score_panel(result)


def _print_score_panel(result: ValidationResult) -> None:
    v = result.verdict
    style = _VERDICT_STYLES.get(v, "")

    score_text = Text()
    score_text.append(f"{v.emoji} {v.value}", style=style)
    score_text.append(f" — {v.description}\n\n")
    score_text.append(f"Vow Score: {result.score}/100\n", style="bold")
    score_text.append(
        f"Passed: {result.passed_count}  |  "
        f"Critical: {result.critical_count}  |  "
        f"Warning: {result.warning_count}  |  "
        f"Info: {result.info_count}"
    )

    panel = Panel(
        score_text,
        title="[bold]Vow Score[/]",
        border_style=style.split()[-1] if style else "white",
        width=70,
    )
    console.print(panel)


def _output_summary(result: ValidationResult) -> None:
    v = result.verdict
    console.print(
        f"{v.emoji} {result.contract_name}: {v.value} "
        f"(score={result.score}, C={result.critical_count}, "
        f"W={result.warning_count}, I={result.info_count})"
    )


def _output_json(result: ValidationResult) -> None:
    import json

    data = {
        "contract": result.contract_name,
        "source": result.source_path,
        "score": result.score,
        "verdict": result.verdict.value,
        "counts": {
            "passed": result.passed_count,
            "critical": result.critical_count,
            "warning": result.warning_count,
            "info": result.info_count,
        },
        "findings": [
            {
                "rule": f.rule, "category": f.category, "severity": f.severity.value,
                "passed": f.passed, "message": f.message, "details": f.details,
            }
            for f in result.findings
        ],
    }
    console.print_json(json.dumps(data))


if __name__ == "__main__":
    app()
PYEOF

echo "[3/4] Creating test fixtures..."

# ── tests/fixtures/orders_contract.yaml ──
cat > tests/fixtures/orders_contract.yaml << 'YAMLEOF'
apiVersion: datavow/v1
kind: DataContract
metadata:
  name: orders
  version: 1.0.0
  owner: data-team@company.com
  domain: sales
  description: "Customer orders from the e-commerce platform"
  tags: [pii, financial, critical]

schema:
  type: table
  fields:
    - name: order_id
      type: integer
      required: true
      unique: true
      description: "Unique order identifier"
    - name: customer_email
      type: string
      required: true
      pii: true
      pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
    - name: total_amount
      type: decimal
      required: true
      min: 0
      description: "Order total in EUR"
    - name: status
      type: string
      required: true
      allowed_values: [pending, confirmed, shipped, delivered, cancelled]
    - name: created_at
      type: timestamp
      required: true

quality:
  rules:
    - name: no_negative_totals
      type: sql
      query: "SELECT COUNT(*) FROM {table} WHERE total_amount < 0"
      threshold: 0
      severity: CRITICAL
    - name: email_not_null
      type: not_null
      field: customer_email
      severity: CRITICAL
    - name: daily_volume
      type: row_count
      min: 3
      max: 100000
      severity: WARNING

sla:
  freshness: 24h
  completeness: "99.5%"
YAMLEOF

# ── tests/fixtures/orders_clean.csv ──
cat > tests/fixtures/orders_clean.csv << 'CSVEOF'
order_id,customer_email,total_amount,status,created_at
1,alice@example.com,99.99,confirmed,2026-03-05T10:00:00
2,bob@example.com,149.50,shipped,2026-03-05T11:30:00
3,carol@example.com,29.95,pending,2026-03-05T12:15:00
4,dave@example.com,250.00,delivered,2026-03-04T09:00:00
5,eve@example.com,15.00,cancelled,2026-03-04T14:30:00
CSVEOF

# ── tests/fixtures/orders_dirty.csv ──
cat > tests/fixtures/orders_dirty.csv << 'CSVEOF'
order_id,customer_email,total_amount,status,created_at
1,alice@example.com,99.99,confirmed,2026-03-05T10:00:00
2,,149.50,shipped,2026-03-05T11:30:00
1,bad-email,29.95,pending,2026-03-05T12:15:00
4,dave@example.com,-50.00,invalid_status,2026-03-04T09:00:00
5,eve@example.com,15.00,cancelled,2026-03-04T14:30:00
CSVEOF

echo "[4/4] Creating test file..."

# ── tests/test_validate.py (NEW) ──
cat > tests/test_validate.py << 'PYEOF'
"""Tests for DataVow validate command — contract parsing, validation engine, CLI."""

from pathlib import Path

import pytest

from datavow.contract import DataContract, FieldType, Severity
from datavow.findings import Finding, ValidationResult, Verdict
from datavow.validator import validate

FIXTURES = Path(__file__).parent / "fixtures"
CONTRACT = FIXTURES / "orders_contract.yaml"
CLEAN_DATA = FIXTURES / "orders_clean.csv"
DIRTY_DATA = FIXTURES / "orders_dirty.csv"


class TestContractParsing:
    def test_load_valid_contract(self):
        c = DataContract.from_yaml(CONTRACT)
        assert c.metadata.name == "orders"
        assert c.metadata.domain == "sales"
        assert len(c.schema_.fields) == 5
        assert len(c.quality.rules) == 3

    def test_field_types(self):
        c = DataContract.from_yaml(CONTRACT)
        fields_by_name = {f.name: f for f in c.schema_.fields}
        assert fields_by_name["order_id"].type == FieldType.INTEGER
        assert fields_by_name["customer_email"].type == FieldType.STRING
        assert fields_by_name["total_amount"].type == FieldType.DECIMAL
        assert fields_by_name["created_at"].type == FieldType.TIMESTAMP

    def test_required_fields(self):
        c = DataContract.from_yaml(CONTRACT)
        required = [f.name for f in c.schema_.fields if f.required]
        assert set(required) == {"order_id", "customer_email", "total_amount", "status", "created_at"}

    def test_unique_fields(self):
        c = DataContract.from_yaml(CONTRACT)
        unique = [f.name for f in c.schema_.fields if f.unique]
        assert unique == ["order_id"]

    def test_pii_flag(self):
        c = DataContract.from_yaml(CONTRACT)
        pii_fields = [f.name for f in c.schema_.fields if f.pii]
        assert pii_fields == ["customer_email"]

    def test_quality_rules(self):
        c = DataContract.from_yaml(CONTRACT)
        rules_by_name = {r.name: r for r in c.quality.rules}
        assert rules_by_name["no_negative_totals"].severity == Severity.CRITICAL
        assert rules_by_name["daily_volume"].severity == Severity.WARNING

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            DataContract.from_yaml("/nonexistent.yaml")

    def test_invalid_extension(self):
        with pytest.raises(ValueError, match="Expected .yaml"):
            DataContract.from_yaml(__file__)


class TestValidateClean:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.result = validate(CONTRACT, CLEAN_DATA)

    def test_contract_name(self):
        assert self.result.contract_name == "orders"

    def test_no_critical_failures(self):
        assert self.result.critical_count == 0

    def test_score_high(self):
        assert self.result.score >= 80

    def test_all_schema_checks_pass(self):
        schema_failures = [f for f in self.result.findings if f.category == "schema" and not f.passed]
        assert len(schema_failures) == 0

    def test_all_quality_rules_pass(self):
        quality_failures = [f for f in self.result.findings if f.category == "quality" and not f.passed]
        assert len(quality_failures) == 0

    def test_findings_exist(self):
        assert len(self.result.findings) > 0


class TestValidateDirty:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.result = validate(CONTRACT, DIRTY_DATA)

    def test_has_critical_failures(self):
        assert self.result.has_critical_failures

    def test_score_degraded(self):
        assert self.result.score < 80

    def test_detects_null_email(self):
        nulls = [f for f in self.result.findings if "required:customer_email" in f.rule and not f.passed]
        assert len(nulls) > 0

    def test_detects_duplicate_order_id(self):
        dups = [f for f in self.result.findings if "unique:order_id" in f.rule and not f.passed]
        assert len(dups) > 0

    def test_detects_negative_amount(self):
        negatives = [f for f in self.result.findings if f.rule == "no_negative_totals" and not f.passed]
        assert len(negatives) > 0

    def test_detects_email_pattern_violation(self):
        patterns = [f for f in self.result.findings if "pattern:customer_email" in f.rule and not f.passed]
        assert len(patterns) > 0

    def test_detects_invalid_status(self):
        status_checks = [f for f in self.result.findings if "allowed_values:status" in f.rule and not f.passed]
        assert len(status_checks) > 0

    def test_verdict_broken_or_worse(self):
        assert self.result.verdict in (Verdict.BROKEN, Verdict.SHATTERED)


class TestScoring:
    def test_perfect_score(self):
        r = ValidationResult("test", "test.csv")
        assert r.score == 100
        assert r.verdict == Verdict.KEPT

    def test_one_critical(self):
        r = ValidationResult("test", "test.csv")
        r.add(Finding("r1", "quality", Severity.CRITICAL, "fail", False))
        assert r.score == 80
        assert r.verdict == Verdict.STRAINED

    def test_one_warning(self):
        r = ValidationResult("test", "test.csv")
        r.add(Finding("r1", "quality", Severity.WARNING, "warn", False))
        assert r.score == 95
        assert r.verdict == Verdict.KEPT

    def test_one_info(self):
        r = ValidationResult("test", "test.csv")
        r.add(Finding("r1", "quality", Severity.INFO, "info", False))
        assert r.score == 99
        assert r.verdict == Verdict.KEPT

    def test_score_floors_at_zero(self):
        r = ValidationResult("test", "test.csv")
        for i in range(10):
            r.add(Finding(f"r{i}", "quality", Severity.CRITICAL, "fail", False))
        assert r.score == 0
        assert r.verdict == Verdict.SHATTERED

    def test_passed_findings_dont_affect_score(self):
        r = ValidationResult("test", "test.csv")
        r.add(Finding("r1", "quality", Severity.CRITICAL, "pass", True))
        assert r.score == 100

    def test_mixed_severities(self):
        r = ValidationResult("test", "test.csv")
        r.add(Finding("r1", "quality", Severity.CRITICAL, "fail", False))
        r.add(Finding("r2", "quality", Severity.WARNING, "fail", False))
        r.add(Finding("r3", "quality", Severity.INFO, "fail", False))
        assert r.score == 74
        assert r.verdict == Verdict.BROKEN


class TestCLI:
    def test_validate_clean_exit_zero(self):
        from typer.testing import CliRunner
        from datavow.cli import app
        runner = CliRunner()
        result = runner.invoke(app, ["validate", str(CONTRACT), str(CLEAN_DATA), "--ci"])
        assert result.exit_code == 0

    def test_validate_dirty_ci_exit_one(self):
        from typer.testing import CliRunner
        from datavow.cli import app
        runner = CliRunner()
        result = runner.invoke(app, ["validate", str(CONTRACT), str(DIRTY_DATA), "--ci"])
        assert result.exit_code == 1

    def test_json_output(self):
        from typer.testing import CliRunner
        from datavow.cli import app
        runner = CliRunner()
        result = runner.invoke(app, ["validate", str(CONTRACT), str(CLEAN_DATA), "-o", "json"])
        assert result.exit_code == 0
        assert '"contract"' in result.output
        assert '"score"' in result.output

    def test_summary_output(self):
        from typer.testing import CliRunner
        from datavow.cli import app
        runner = CliRunner()
        result = runner.invoke(app, ["validate", str(CONTRACT), str(DIRTY_DATA), "-o", "summary"])
        assert result.exit_code == 0
        assert "orders" in result.output

    def test_verbose_shows_passed(self):
        from typer.testing import CliRunner
        from datavow.cli import app
        runner = CliRunner()
        result = runner.invoke(app, ["validate", str(CONTRACT), str(CLEAN_DATA), "--verbose"])
        assert result.exit_code == 0
        assert "✓" in result.output
PYEOF

echo "[4/4] Updating pyproject.toml..."

# ── pyproject.toml (MERGE — add deps) ──
cat > pyproject.toml << 'TOMLEOF'
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "datavow"
version = "0.1.0"
description = "A solemn vow on your data. From YAML to verdict."
readme = "README.md"
license = "Apache-2.0"
requires-python = ">=3.12"
authors = [{ name = "Ludovic Schmetz" }]
keywords = ["data-contracts", "data-quality", "validation", "duckdb", "odcs"]
classifiers = [
    "Development Status :: 3 - Alpha",
    "Intended Audience :: Developers",
    "Topic :: Database",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3.12",
]
dependencies = [
    "typer>=0.12",
    "rich>=13.0",
    "pydantic>=2.0",
    "pyyaml>=6.0",
    "duckdb>=1.0",
    "jinja2>=3.1",
]

[project.scripts]
datavow = "datavow.cli:app"

[tool.hatch.build.targets.wheel]
packages = ["src/datavow"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]

[tool.ruff]
target-version = "py312"
line-length = 100

[project.optional-dependencies]
dev = ["pytest>=8.0", "pytest-cov>=5.0", "ruff>=0.4"]
TOMLEOF

echo ""
echo "=== Integration complete ==="
echo ""
echo "New files added:"
echo "  src/datavow/contract.py"
echo "  src/datavow/findings.py"
echo "  src/datavow/validator.py"
echo "  src/datavow/connectors/__init__.py"
echo "  src/datavow/connectors/file.py"
echo "  src/datavow/rules/__init__.py"
echo "  src/datavow/rules/schema.py"
echo "  src/datavow/rules/quality.py"
echo "  src/datavow/rules/freshness.py"
echo "  tests/test_validate.py"
echo "  tests/fixtures/orders_contract.yaml"
echo "  tests/fixtures/orders_clean.csv"
echo "  tests/fixtures/orders_dirty.csv"
echo ""
echo "Updated files:"
echo "  pyproject.toml  (added duckdb, pydantic, rich, jinja2)"
echo "  src/datavow/__init__.py  (added docstring)"
echo "  src/datavow/cli.py  (added validate command)"
echo ""
echo "Next steps:"
echo "  1. uv pip install -e '.[dev]'"
echo "  2. pytest tests/ -v"
echo "  3. datavow validate tests/fixtures/orders_contract.yaml tests/fixtures/orders_clean.csv --verbose"
echo "  4. git add . && git commit -m 'feat: datavow validate command'"

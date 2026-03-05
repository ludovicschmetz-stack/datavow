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
            FieldType.TIMESTAMP: [
                "TIMESTAMP",
                "TIMESTAMP WITH TIME ZONE",
                "TIMESTAMPTZ",
                "TIMESTAMP_S",
                "TIMESTAMP_MS",
                "TIMESTAMP_NS",
            ],
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
        if (
            self.type
            in (
                RuleType.NOT_NULL,
                RuleType.UNIQUE,
                RuleType.RANGE,
                RuleType.ACCEPTED_VALUES,
                RuleType.REGEX,
            )
            and not self.field
        ):
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
        if path.suffix not in (".yaml", ".yml"):
            raise ValueError(f"Expected .yaml/.yml file, got: {path.suffix}")

        with open(path) as f:
            raw = yaml.safe_load(f)

        if not isinstance(raw, dict):
            raise ValueError(f"Invalid contract format in {path}: expected a YAML mapping")

        return cls.model_validate(raw)

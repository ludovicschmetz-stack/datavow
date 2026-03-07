"""dbt connector — reads manifest.json and profiles.yml to extract models and connections.

Supports dbt-core manifest v7+ (dbt 1.5+).
Extracts model schemas, column types, dbt tests → DataVow contract fields.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from datavow.contract import (
    ContractMetadata,
    DataContract,
    FieldSpec,
    FieldType,
    QualityRule,
    QualitySpec,
    RuleType,
    SchemaSpec,
    Severity,
)


# ──────────────────────────────────────────────
# Type mapping: dbt/warehouse types → DataVow FieldType
# ──────────────────────────────────────────────

_DBT_TYPE_MAP: dict[str, FieldType] = {
    # String types
    "text": FieldType.STRING,
    "character varying": FieldType.STRING,
    "varchar": FieldType.STRING,
    "string": FieldType.STRING,
    "char": FieldType.STRING,
    "nvarchar": FieldType.STRING,
    "nchar": FieldType.STRING,
    # Integer types
    "integer": FieldType.INTEGER,
    "int": FieldType.INTEGER,
    "bigint": FieldType.INTEGER,
    "smallint": FieldType.INTEGER,
    "tinyint": FieldType.INTEGER,
    "int64": FieldType.INTEGER,
    "int32": FieldType.INTEGER,
    "number": FieldType.INTEGER,
    # Float types
    "float": FieldType.FLOAT,
    "float64": FieldType.FLOAT,
    "double": FieldType.FLOAT,
    "double precision": FieldType.FLOAT,
    "real": FieldType.FLOAT,
    # Decimal types
    "numeric": FieldType.DECIMAL,
    "decimal": FieldType.DECIMAL,
    # Boolean
    "boolean": FieldType.BOOLEAN,
    "bool": FieldType.BOOLEAN,
    # Date/time
    "date": FieldType.DATE,
    "timestamp": FieldType.TIMESTAMP,
    "timestamp without time zone": FieldType.TIMESTAMP,
    "timestamp with time zone": FieldType.TIMESTAMP,
    "timestamptz": FieldType.TIMESTAMP,
    "datetime": FieldType.TIMESTAMP,
    "timestamp_ntz": FieldType.TIMESTAMP,
    "timestamp_ltz": FieldType.TIMESTAMP,
    "timestamp_tz": FieldType.TIMESTAMP,
}


def _map_dbt_type(dbt_type: str) -> FieldType:
    """Map a dbt/warehouse column type to a DataVow FieldType."""
    normalized = dbt_type.lower().strip()
    # Handle parameterized types: numeric(10,2) → numeric
    base = normalized.split("(")[0].strip()
    return _DBT_TYPE_MAP.get(base, FieldType.STRING)


# ──────────────────────────────────────────────
# Manifest parsing
# ──────────────────────────────────────────────


class DbtModel:
    """Parsed dbt model from manifest.json."""

    def __init__(
        self,
        unique_id: str,
        name: str,
        schema: str,
        database: str | None,
        description: str,
        columns: dict[str, dict[str, Any]],
        tags: list[str],
        meta: dict[str, Any],
        depends_on: list[str],
        tests: list[dict[str, Any]],
    ):
        self.unique_id = unique_id
        self.name = name
        self.schema = schema
        self.database = database
        self.description = description
        self.columns = columns
        self.tags = tags
        self.meta = meta
        self.depends_on = depends_on
        self.tests = tests

    @property
    def fqn_table(self) -> str:
        """Fully qualified table name: schema.name."""
        if self.database:
            return f"{self.database}.{self.schema}.{self.name}"
        return f"{self.schema}.{self.name}"

    @property
    def domain(self) -> str:
        """Extract domain from meta, tags, or schema name."""
        if "domain" in self.meta:
            return self.meta["domain"]
        if "datavow_domain" in self.meta:
            return self.meta["datavow_domain"]
        # Fallback: use schema name as domain
        return self.schema


def parse_manifest(manifest_path: str | Path) -> list[DbtModel]:
    """Parse a dbt manifest.json and return model definitions."""
    path = Path(manifest_path)
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")

    with open(path) as f:
        manifest = json.load(f)

    nodes = manifest.get("nodes", {})
    models: list[DbtModel] = []

    # Collect tests per model for mapping
    test_map: dict[str, list[dict[str, Any]]] = {}
    for node_id, node in nodes.items():
        if node.get("resource_type") == "test":
            # Link test to its parent model(s)
            for dep in node.get("depends_on", {}).get("nodes", []):
                if dep.startswith("model."):
                    test_info = {
                        "name": node.get("test_metadata", {}).get("name", node.get("name", "")),
                        "column": node.get("test_metadata", {})
                        .get("kwargs", {})
                        .get("column_name"),
                        "kwargs": node.get("test_metadata", {}).get("kwargs", {}),
                    }
                    test_map.setdefault(dep, []).append(test_info)

    for node_id, node in nodes.items():
        if node.get("resource_type") != "model":
            continue

        columns = {}
        for col_name, col_info in node.get("columns", {}).items():
            columns[col_name] = {
                "name": col_name,
                "type": col_info.get("data_type", col_info.get("dtype", "string")),
                "description": col_info.get("description", ""),
                "meta": col_info.get("meta", {}),
                "tags": col_info.get("tags", []),
            }

        models.append(
            DbtModel(
                unique_id=node_id,
                name=node.get("name", ""),
                schema=node.get("schema", "public"),
                database=node.get("database"),
                description=node.get("description", ""),
                columns=columns,
                tags=node.get("tags", []),
                meta=node.get("meta", {}),
                depends_on=node.get("depends_on", {}).get("nodes", []),
                tests=test_map.get(node_id, []),
            )
        )

    return models


# ──────────────────────────────────────────────
# Profiles parsing (for database connection)
# ──────────────────────────────────────────────


class DbtConnectionInfo:
    """Database connection info extracted from profiles.yml."""

    def __init__(
        self,
        adapter: str,
        host: str = "localhost",
        port: int = 5432,
        user: str = "",
        password: str = "",
        database: str = "",
        schema: str = "public",
        extra: dict[str, Any] | None = None,
    ):
        self.adapter = adapter
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.extra = extra or {}

    @property
    def duckdb_attach_string(self) -> str | None:
        """Generate DuckDB ATTACH string for supported adapters."""
        if self.adapter == "postgres":
            parts = [f"host={self.host}", f"port={self.port}", f"dbname={self.database}"]
            if self.user:
                parts.append(f"user={self.user}")
            if self.password:
                parts.append(f"password={self.password}")
            return " ".join(parts)
        if self.adapter == "duckdb":
            return self.extra.get("path", ":memory:")
        return None

    @property
    def is_duckdb_attachable(self) -> bool:
        """Whether this adapter can use DuckDB ATTACH (zero extra deps)."""
        return self.adapter in ("postgres", "duckdb")


def parse_profiles(
    profiles_path: str | Path | None = None,
    project_dir: str | Path | None = None,
    profile_name: str | None = None,
    target_name: str | None = None,
) -> DbtConnectionInfo:
    """Parse dbt profiles.yml and return connection info."""
    # Resolve profiles.yml location
    if profiles_path:
        path = Path(profiles_path)
    elif project_dir:
        # Check for profiles.yml in project dir first (dbt 1.5+)
        proj = Path(project_dir) / "profiles.yml"
        if proj.exists():
            path = proj
        else:
            path = Path.home() / ".dbt" / "profiles.yml"
    else:
        path = Path.home() / ".dbt" / "profiles.yml"

    if not path.exists():
        raise FileNotFoundError(f"dbt profiles.yml not found: {path}")

    with open(path) as f:
        profiles = yaml.safe_load(f)

    if not profiles or not isinstance(profiles, dict):
        raise ValueError(f"Invalid profiles.yml: {path}")

    # Resolve profile name from dbt_project.yml if not provided
    if not profile_name and project_dir:
        proj_file = Path(project_dir) / "dbt_project.yml"
        if proj_file.exists():
            with open(proj_file) as f:
                proj = yaml.safe_load(f)
            profile_name = proj.get("profile")

    if not profile_name:
        # Use first profile
        profile_name = next(iter(profiles))

    if profile_name not in profiles:
        raise ValueError(f"Profile '{profile_name}' not found. Available: {list(profiles.keys())}")

    profile = profiles[profile_name]
    target_name = target_name or profile.get("target", "dev")
    outputs = profile.get("outputs", {})

    if target_name not in outputs:
        raise ValueError(
            f"Target '{target_name}' not found in profile '{profile_name}'. "
            f"Available: {list(outputs.keys())}"
        )

    target = outputs[target_name]
    adapter = target.get("type", "unknown")

    return DbtConnectionInfo(
        adapter=adapter,
        host=target.get("host", target.get("server", "localhost")),
        port=int(target.get("port", 5432)),
        user=target.get("user", target.get("username", "")),
        password=target.get("password", target.get("pass", "")),
        database=target.get("database", target.get("dbname", "")),
        schema=target.get("schema", "public"),
        extra=target,
    )


# ──────────────────────────────────────────────
# Contract generation from dbt models
# ──────────────────────────────────────────────


def generate_contract(model: DbtModel, owner: str = "") -> DataContract:
    """Generate a DataVow contract from a dbt model definition."""
    fields: list[FieldSpec] = []
    quality_rules: list[QualityRule] = []

    for col_name, col_info in model.columns.items():
        col_type = _map_dbt_type(col_info.get("type", "string"))
        is_pii = "pii" in col_info.get("tags", []) or col_info.get("meta", {}).get("pii", False)

        fields.append(
            FieldSpec(
                name=col_name,
                type=col_type,
                required=True,  # Default to required, user can relax
                pii=is_pii,
                description=col_info.get("description", ""),
            )
        )

    # Convert dbt tests to DataVow quality rules
    for test in model.tests:
        test_name = test.get("name", "")
        column = test.get("column")

        if test_name == "not_null" and column:
            quality_rules.append(
                QualityRule(
                    name=f"dbt_{test_name}_{column}",
                    type=RuleType.NOT_NULL,
                    field=column,
                    severity=Severity.CRITICAL,
                )
            )
        elif test_name == "unique" and column:
            quality_rules.append(
                QualityRule(
                    name=f"dbt_{test_name}_{column}",
                    type=RuleType.UNIQUE,
                    field=column,
                    severity=Severity.CRITICAL,
                )
            )
        elif test_name == "accepted_values" and column:
            values = test.get("kwargs", {}).get("values", [])
            if values:
                quality_rules.append(
                    QualityRule(
                        name=f"dbt_{test_name}_{column}",
                        type=RuleType.ACCEPTED_VALUES,
                        field=column,
                        values=values,
                        severity=Severity.WARNING,
                    )
                )

    # Determine tags
    tags = list(model.tags)
    if any(f.pii for f in fields):
        tags.append("pii")
    tags = sorted(set(tags))

    return DataContract(
        apiVersion="datavow/v1",
        kind="DataContract",
        metadata=ContractMetadata(
            name=model.name,
            version="1.0.0",
            owner=owner,
            domain=model.domain,
            description=model.description or f"Auto-generated from dbt model {model.unique_id}",
            tags=tags,
        ),
        schema=SchemaSpec(type="table", fields=fields),
        quality=QualitySpec(rules=quality_rules),
    )


def contract_to_yaml(contract: DataContract) -> str:
    """Serialize a DataContract to YAML string."""
    data = contract.model_dump(
        mode="json", by_alias=True, exclude_none=True, exclude_defaults=False
    )

    # Clean up empty blocks
    if not data.get("quality", {}).get("rules"):
        data.pop("quality", None)
    if not any(data.get("sla", {}).values()):
        data.pop("sla", None)
    if not data.get("notifications", {}).get("on_failure"):
        data.pop("notifications", None)

    return yaml.dump(data, sort_keys=False, default_flow_style=False, allow_unicode=True)

"""Tests for dbt connector — manifest parsing and contract generation."""

import json
from pathlib import Path

import pytest
import yaml

from datavow.connectors.dbt import (
    DbtConnectionInfo,
    contract_to_yaml,
    generate_contract,
    parse_manifest,
    parse_profiles,
)
from datavow.contract import FieldType, RuleType, Severity


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────


@pytest.fixture
def sample_manifest(tmp_path: Path) -> Path:
    """Create a minimal dbt manifest.json for testing."""
    manifest = {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12/manifest.json"
        },
        "nodes": {
            "model.my_project.orders": {
                "resource_type": "model",
                "unique_id": "model.my_project.orders",
                "name": "orders",
                "schema": "analytics",
                "database": "warehouse",
                "description": "Customer orders from the e-commerce platform",
                "columns": {
                    "order_id": {
                        "name": "order_id",
                        "data_type": "integer",
                        "description": "Unique order identifier",
                        "meta": {},
                        "tags": [],
                    },
                    "customer_email": {
                        "name": "customer_email",
                        "data_type": "character varying",
                        "description": "Customer email address",
                        "meta": {"pii": True},
                        "tags": ["pii"],
                    },
                    "total_amount": {
                        "name": "total_amount",
                        "data_type": "numeric(10,2)",
                        "description": "Order total in EUR",
                        "meta": {},
                        "tags": [],
                    },
                    "status": {
                        "name": "status",
                        "data_type": "varchar",
                        "description": "Order status",
                        "meta": {},
                        "tags": [],
                    },
                    "created_at": {
                        "name": "created_at",
                        "data_type": "timestamp without time zone",
                        "description": "Order creation timestamp",
                        "meta": {},
                        "tags": [],
                    },
                },
                "tags": ["daily", "critical"],
                "meta": {"domain": "sales"},
                "depends_on": {"nodes": ["source.my_project.raw.raw_orders"]},
            },
            "model.my_project.customers": {
                "resource_type": "model",
                "unique_id": "model.my_project.customers",
                "name": "customers",
                "schema": "analytics",
                "database": "warehouse",
                "description": "Customer dimension table",
                "columns": {
                    "customer_id": {
                        "name": "customer_id",
                        "data_type": "bigint",
                        "description": "Customer ID",
                        "meta": {},
                        "tags": [],
                    },
                    "name": {
                        "name": "name",
                        "data_type": "text",
                        "description": "Customer full name",
                        "meta": {},
                        "tags": [],
                    },
                },
                "tags": [],
                "meta": {},
                "depends_on": {"nodes": []},
            },
            "test.my_project.not_null_orders_order_id.abc123": {
                "resource_type": "test",
                "name": "not_null_orders_order_id",
                "test_metadata": {
                    "name": "not_null",
                    "kwargs": {"column_name": "order_id", "model": "ref('orders')"},
                },
                "depends_on": {"nodes": ["model.my_project.orders"]},
            },
            "test.my_project.unique_orders_order_id.def456": {
                "resource_type": "test",
                "name": "unique_orders_order_id",
                "test_metadata": {
                    "name": "unique",
                    "kwargs": {"column_name": "order_id", "model": "ref('orders')"},
                },
                "depends_on": {"nodes": ["model.my_project.orders"]},
            },
            "test.my_project.accepted_values_orders_status.ghi789": {
                "resource_type": "test",
                "name": "accepted_values_orders_status",
                "test_metadata": {
                    "name": "accepted_values",
                    "kwargs": {
                        "column_name": "status",
                        "values": ["confirmed", "shipped", "delivered", "cancelled"],
                        "model": "ref('orders')",
                    },
                },
                "depends_on": {"nodes": ["model.my_project.orders"]},
            },
        },
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return path


@pytest.fixture
def sample_profiles(tmp_path: Path) -> Path:
    """Create a minimal dbt profiles.yml for testing."""
    profiles = {
        "my_project": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "postgres",
                    "host": "localhost",
                    "port": 5432,
                    "user": "dev_user",
                    "password": "dev_pass",
                    "database": "analytics_dev",
                    "schema": "public",
                },
                "prod": {
                    "type": "postgres",
                    "host": "prod-db.example.com",
                    "port": 5432,
                    "user": "prod_user",
                    "password": "prod_pass",
                    "database": "analytics",
                    "schema": "public",
                },
            },
        },
        "duckdb_project": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "duckdb",
                    "path": "/data/warehouse.duckdb",
                    "schema": "main",
                },
            },
        },
    }
    path = tmp_path / "profiles.yml"
    path.write_text(yaml.dump(profiles))
    return path


# ──────────────────────────────────────────────
# Manifest parsing tests
# ──────────────────────────────────────────────


def test_parse_manifest_basic(sample_manifest):
    models = parse_manifest(sample_manifest)
    assert len(models) == 2

    orders = next(m for m in models if m.name == "orders")
    assert orders.schema == "analytics"
    assert orders.database == "warehouse"
    assert orders.description == "Customer orders from the e-commerce platform"
    assert len(orders.columns) == 5
    assert orders.domain == "sales"
    assert "daily" in orders.tags
    assert "critical" in orders.tags


def test_parse_manifest_columns(sample_manifest):
    models = parse_manifest(sample_manifest)
    orders = next(m for m in models if m.name == "orders")

    assert "order_id" in orders.columns
    assert orders.columns["order_id"]["type"] == "integer"
    assert orders.columns["customer_email"]["type"] == "character varying"
    assert orders.columns["total_amount"]["type"] == "numeric(10,2)"


def test_parse_manifest_tests(sample_manifest):
    models = parse_manifest(sample_manifest)
    orders = next(m for m in models if m.name == "orders")

    assert len(orders.tests) == 3
    test_names = [t["name"] for t in orders.tests]
    assert "not_null" in test_names
    assert "unique" in test_names
    assert "accepted_values" in test_names


def test_parse_manifest_no_tests(sample_manifest):
    models = parse_manifest(sample_manifest)
    customers = next(m for m in models if m.name == "customers")
    assert len(customers.tests) == 0


def test_parse_manifest_fqn(sample_manifest):
    models = parse_manifest(sample_manifest)
    orders = next(m for m in models if m.name == "orders")
    assert orders.fqn_table == "warehouse.analytics.orders"


def test_parse_manifest_not_found():
    with pytest.raises(FileNotFoundError):
        parse_manifest("/nonexistent/manifest.json")


# ──────────────────────────────────────────────
# Contract generation tests
# ──────────────────────────────────────────────


def test_generate_contract_basic(sample_manifest):
    models = parse_manifest(sample_manifest)
    orders = next(m for m in models if m.name == "orders")

    contract = generate_contract(orders, owner="data-team@company.com")

    assert contract.metadata.name == "orders"
    assert contract.metadata.domain == "sales"
    assert contract.metadata.owner == "data-team@company.com"
    assert len(contract.schema_.fields) == 5


def test_generate_contract_field_types(sample_manifest):
    models = parse_manifest(sample_manifest)
    orders = next(m for m in models if m.name == "orders")
    contract = generate_contract(orders)

    fields_by_name = {f.name: f for f in contract.schema_.fields}
    assert fields_by_name["order_id"].type == FieldType.INTEGER
    assert fields_by_name["customer_email"].type == FieldType.STRING
    assert fields_by_name["total_amount"].type == FieldType.DECIMAL
    assert fields_by_name["status"].type == FieldType.STRING
    assert fields_by_name["created_at"].type == FieldType.TIMESTAMP


def test_generate_contract_pii_flags(sample_manifest):
    models = parse_manifest(sample_manifest)
    orders = next(m for m in models if m.name == "orders")
    contract = generate_contract(orders)

    fields_by_name = {f.name: f for f in contract.schema_.fields}
    assert fields_by_name["customer_email"].pii is True
    assert fields_by_name["order_id"].pii is False


def test_generate_contract_dbt_tests_to_rules(sample_manifest):
    models = parse_manifest(sample_manifest)
    orders = next(m for m in models if m.name == "orders")
    contract = generate_contract(orders)

    rules = contract.quality.rules
    assert len(rules) == 3

    rule_names = {r.name for r in rules}
    assert "dbt_not_null_order_id" in rule_names
    assert "dbt_unique_order_id" in rule_names
    assert "dbt_accepted_values_status" in rule_names

    not_null_rule = next(r for r in rules if "not_null" in r.name)
    assert not_null_rule.type == RuleType.NOT_NULL
    assert not_null_rule.field == "order_id"
    assert not_null_rule.severity == Severity.CRITICAL

    accepted = next(r for r in rules if "accepted_values" in r.name)
    assert accepted.type == RuleType.ACCEPTED_VALUES
    assert accepted.values == ["confirmed", "shipped", "delivered", "cancelled"]


def test_generate_contract_tags(sample_manifest):
    models = parse_manifest(sample_manifest)
    orders = next(m for m in models if m.name == "orders")
    contract = generate_contract(orders)

    assert "critical" in contract.metadata.tags
    assert "daily" in contract.metadata.tags
    assert "pii" in contract.metadata.tags


def test_contract_to_yaml_roundtrip(sample_manifest):
    models = parse_manifest(sample_manifest)
    orders = next(m for m in models if m.name == "orders")
    contract = generate_contract(orders)

    yaml_str = contract_to_yaml(contract)
    parsed = yaml.safe_load(yaml_str)

    assert parsed["metadata"]["name"] == "orders"
    assert parsed["metadata"]["domain"] == "sales"
    assert parsed["kind"] == "DataContract"
    assert len(parsed["schema"]["fields"]) == 5


# ──────────────────────────────────────────────
# Profiles parsing tests
# ──────────────────────────────────────────────


def test_parse_profiles_postgres(sample_profiles):
    conn = parse_profiles(
        profiles_path=sample_profiles, profile_name="my_project", target_name="dev"
    )
    assert conn.adapter == "postgres"
    assert conn.host == "localhost"
    assert conn.port == 5432
    assert conn.user == "dev_user"
    assert conn.database == "analytics_dev"
    assert conn.is_duckdb_attachable is True


def test_parse_profiles_duckdb(sample_profiles):
    conn = parse_profiles(profiles_path=sample_profiles, profile_name="duckdb_project")
    assert conn.adapter == "duckdb"
    assert conn.is_duckdb_attachable is True
    assert conn.extra.get("path") == "/data/warehouse.duckdb"


def test_parse_profiles_default_target(sample_profiles):
    conn = parse_profiles(profiles_path=sample_profiles, profile_name="my_project")
    # Should use "dev" as default target
    assert conn.host == "localhost"


def test_parse_profiles_prod_target(sample_profiles):
    conn = parse_profiles(
        profiles_path=sample_profiles, profile_name="my_project", target_name="prod"
    )
    assert conn.host == "prod-db.example.com"
    assert conn.database == "analytics"


def test_parse_profiles_not_found():
    with pytest.raises(FileNotFoundError):
        parse_profiles(profiles_path="/nonexistent/profiles.yml")


def test_parse_profiles_invalid_profile(sample_profiles):
    with pytest.raises(ValueError, match="not found"):
        parse_profiles(profiles_path=sample_profiles, profile_name="nonexistent")


def test_parse_profiles_invalid_target(sample_profiles):
    with pytest.raises(ValueError, match="not found"):
        parse_profiles(
            profiles_path=sample_profiles,
            profile_name="my_project",
            target_name="staging",
        )


def test_duckdb_attach_string_postgres():
    conn = DbtConnectionInfo(
        adapter="postgres",
        host="db.example.com",
        port=5432,
        user="myuser",
        password="mypass",
        database="mydb",
    )
    attach = conn.duckdb_attach_string
    assert "host=db.example.com" in attach
    assert "port=5432" in attach
    assert "dbname=mydb" in attach
    assert "user=myuser" in attach
    assert "password=mypass" in attach


def test_duckdb_attach_string_unsupported():
    conn = DbtConnectionInfo(adapter="snowflake")
    assert conn.duckdb_attach_string is None
    assert conn.is_duckdb_attachable is False

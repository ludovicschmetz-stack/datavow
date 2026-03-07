"""Tests for dbt sync — generating dbt tests from DataVow contracts."""

from pathlib import Path

import pytest
import yaml

from datavow.connectors.dbt_sync import (
    sync_all,
    sync_contract,
    _build_schema_yml,
)
from datavow.contract import DataContract


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────


@pytest.fixture
def sample_contract(tmp_path: Path) -> Path:
    """Create a sample DataVow contract with various rule types."""
    contract = {
        "apiVersion": "datavow/v1",
        "kind": "DataContract",
        "metadata": {
            "name": "orders",
            "version": "1.0.0",
            "owner": "data-team@company.com",
            "domain": "sales",
            "description": "Customer orders",
            "tags": ["critical"],
        },
        "schema": {
            "type": "table",
            "fields": [
                {"name": "order_id", "type": "integer", "required": True, "unique": True},
                {
                    "name": "customer_email",
                    "type": "string",
                    "required": True,
                    "pii": True,
                    "pattern": "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$",
                },
                {"name": "total_amount", "type": "decimal", "required": True, "min": 0},
                {
                    "name": "status",
                    "type": "string",
                    "required": True,
                    "allowed_values": ["confirmed", "shipped", "delivered", "cancelled"],
                },
                {"name": "created_at", "type": "timestamp", "required": True},
            ],
        },
        "quality": {
            "rules": [
                {
                    "name": "no_negative_totals",
                    "type": "sql",
                    "query": "SELECT COUNT(*) FROM {table} WHERE total_amount < 0",
                    "threshold": 0,
                    "severity": "CRITICAL",
                },
                {
                    "name": "email_not_null",
                    "type": "not_null",
                    "field": "customer_email",
                    "severity": "CRITICAL",
                },
                {
                    "name": "unique_orders",
                    "type": "unique",
                    "field": "order_id",
                    "severity": "CRITICAL",
                },
                {
                    "name": "valid_status",
                    "type": "accepted_values",
                    "field": "status",
                    "values": ["confirmed", "shipped", "delivered", "cancelled"],
                    "severity": "WARNING",
                },
                {
                    "name": "daily_volume",
                    "type": "row_count",
                    "min": 1000,
                    "max": 100000,
                    "severity": "WARNING",
                },
                {
                    "name": "amount_range",
                    "type": "range",
                    "field": "total_amount",
                    "min_value": 0,
                    "max_value": 999999,
                    "severity": "WARNING",
                },
                {
                    "name": "email_format",
                    "type": "regex",
                    "field": "customer_email",
                    "pattern": "^[a-zA-Z0-9_.+-]+@",
                    "severity": "WARNING",
                },
            ]
        },
    }
    path = tmp_path / "contracts" / "orders.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(yaml.dump(contract, sort_keys=False))
    return path


@pytest.fixture
def minimal_contract(tmp_path: Path) -> Path:
    """Contract with only generic-type rules (no singular tests needed)."""
    contract = {
        "apiVersion": "datavow/v1",
        "kind": "DataContract",
        "metadata": {"name": "users", "version": "1.0.0", "domain": "auth"},
        "schema": {
            "type": "table",
            "fields": [
                {"name": "user_id", "type": "integer", "required": True, "unique": True},
                {"name": "email", "type": "string", "required": True},
            ],
        },
    }
    path = tmp_path / "contracts" / "users.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.dump(contract, sort_keys=False))
    return path


@pytest.fixture
def dbt_project(tmp_path: Path) -> Path:
    """Create a minimal dbt project structure."""
    project_dir = tmp_path / "dbt_project"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text("name: test_project\nversion: 1.0.0\n")
    (project_dir / "models").mkdir()
    (project_dir / "tests").mkdir()
    return project_dir


# ──────────────────────────────────────────────
# sync_contract tests
# ──────────────────────────────────────────────


class TestSyncContract:
    def test_generates_singular_tests(self, sample_contract, dbt_project):
        result = sync_contract(sample_contract, dbt_project)

        assert len(result.singular_tests) == 4  # sql, row_count, range, regex
        for path in result.singular_tests:
            assert Path(path).exists()
            assert Path(path).suffix == ".sql"

    def test_generates_schema_yml(self, sample_contract, dbt_project):
        result = sync_contract(sample_contract, dbt_project)

        assert result.schema_yml_path is not None
        assert Path(result.schema_yml_path).exists()

        content = yaml.safe_load(Path(result.schema_yml_path).read_text())
        assert content["version"] == 2
        assert len(content["models"]) == 1
        assert content["models"][0]["name"] == "orders"

    def test_generic_test_count(self, sample_contract, dbt_project):
        result = sync_contract(sample_contract, dbt_project)

        # Schema fields: 5 required (5 not_null) + 1 unique + 1 allowed_values = 7
        # Quality rules: not_null, unique, accepted_values (3 generic, but deduplicated)
        # not_null on customer_email: already from schema required → deduped
        # unique on order_id: already from schema unique → deduped
        # accepted_values on status: already from schema allowed_values → deduped
        assert result.generic_test_count >= 7

    def test_singular_test_content_sql_rule(self, sample_contract, dbt_project):
        result = sync_contract(sample_contract, dbt_project)

        sql_test = next((p for p in result.singular_tests if "no_negative_totals" in p), None)
        assert sql_test is not None

        content = Path(sql_test).read_text()
        assert "ref('orders')" in content
        assert "total_amount < 0" in content
        assert "Generated by DataVow" in content
        assert "CRITICAL" in content
        assert "tag:datavow" in content or "datavow" in content

    def test_singular_test_content_row_count(self, sample_contract, dbt_project):
        result = sync_contract(sample_contract, dbt_project)

        rc_test = next((p for p in result.singular_tests if "daily_volume" in p), None)
        assert rc_test is not None

        content = Path(rc_test).read_text()
        assert "ref('orders')" in content
        assert "cnt < 1000" in content
        assert "cnt > 100000" in content

    def test_singular_test_content_range(self, sample_contract, dbt_project):
        result = sync_contract(sample_contract, dbt_project)

        range_test = next((p for p in result.singular_tests if "amount_range" in p), None)
        assert range_test is not None

        content = Path(range_test).read_text()
        assert "ref('orders')" in content
        assert "total_amount" in content
        assert "0" in content

    def test_singular_test_content_regex(self, sample_contract, dbt_project):
        result = sync_contract(sample_contract, dbt_project)

        regex_test = next((p for p in result.singular_tests if "email_format" in p), None)
        assert regex_test is not None

        content = Path(regex_test).read_text()
        assert "ref('orders')" in content
        assert "REGEXP_LIKE" in content
        assert "customer_email" in content

    def test_output_directory_structure(self, sample_contract, dbt_project):
        sync_contract(sample_contract, dbt_project)

        datavow_dir = dbt_project / "tests" / "datavow"
        assert datavow_dir.is_dir()

        sql_files = list(datavow_dir.glob("*.sql"))
        yml_files = list(datavow_dir.glob("*.yml"))
        assert len(sql_files) == 4
        assert len(yml_files) == 1

    def test_custom_model_name(self, sample_contract, dbt_project):
        result = sync_contract(sample_contract, dbt_project, model_name="stg_orders")

        sql_test = result.singular_tests[0]
        content = Path(sql_test).read_text()
        assert "ref('stg_orders')" in content

    def test_minimal_contract_no_singular(self, minimal_contract, dbt_project):
        result = sync_contract(minimal_contract, dbt_project)

        assert len(result.singular_tests) == 0
        assert result.generic_test_count >= 2  # required → not_null for 2 fields + unique

    def test_dbt_severity_mapping(self, sample_contract, dbt_project):
        result = sync_contract(sample_contract, dbt_project)

        # CRITICAL → error
        critical_test = next((p for p in result.singular_tests if "no_negative_totals" in p), None)
        assert critical_test
        content = Path(critical_test).read_text()
        assert "severity='error'" in content

        # WARNING → warn
        warning_test = next((p for p in result.singular_tests if "daily_volume" in p), None)
        assert warning_test
        content = Path(warning_test).read_text()
        assert "severity='warn'" in content


# ──────────────────────────────────────────────
# schema.yml generation tests
# ──────────────────────────────────────────────


class TestSchemaYml:
    def test_required_fields_become_not_null(self, sample_contract):
        contract = DataContract.from_yaml(sample_contract)
        schema = _build_schema_yml("orders", contract)

        columns = {c["name"]: c for c in schema["models"][0]["columns"]}
        order_id_tests = columns["order_id"]["data_tests"]
        not_null_tests = [t for t in order_id_tests if isinstance(t, dict) and "not_null" in t]
        assert len(not_null_tests) == 1

    def test_unique_fields_become_unique(self, sample_contract):
        contract = DataContract.from_yaml(sample_contract)
        schema = _build_schema_yml("orders", contract)

        columns = {c["name"]: c for c in schema["models"][0]["columns"]}
        order_id_tests = columns["order_id"]["data_tests"]
        unique_tests = [t for t in order_id_tests if isinstance(t, dict) and "unique" in t]
        assert len(unique_tests) == 1

    def test_allowed_values_become_accepted(self, sample_contract):
        contract = DataContract.from_yaml(sample_contract)
        schema = _build_schema_yml("orders", contract)

        columns = {c["name"]: c for c in schema["models"][0]["columns"]}
        status_tests = columns["status"]["data_tests"]
        av_tests = [t for t in status_tests if isinstance(t, dict) and "accepted_values" in t]
        assert len(av_tests) == 1
        assert "confirmed" in av_tests[0]["accepted_values"]["arguments"]["values"]

    def test_datavow_tags_on_all_tests(self, sample_contract):
        contract = DataContract.from_yaml(sample_contract)
        schema = _build_schema_yml("orders", contract)

        for col in schema["models"][0]["columns"]:
            for test in col.get("data_tests", []):
                if isinstance(test, dict):
                    for test_config in test.values():
                        if isinstance(test_config, dict):
                            assert "datavow" in test_config.get("tags", [])

    def test_no_duplicate_generic_tests(self, sample_contract):
        """Quality rules should not duplicate schema-level generic tests."""
        contract = DataContract.from_yaml(sample_contract)
        schema = _build_schema_yml("orders", contract)

        columns = {c["name"]: c for c in schema["models"][0]["columns"]}

        # customer_email: required (schema) + not_null (quality rule) → only 1 not_null
        email_tests = columns["customer_email"]["data_tests"]
        not_null_tests = [t for t in email_tests if isinstance(t, dict) and "not_null" in t]
        assert len(not_null_tests) == 1

        # order_id: unique (schema) + unique (quality rule) → only 1 unique
        order_tests = columns["order_id"]["data_tests"]
        unique_tests = [t for t in order_tests if isinstance(t, dict) and "unique" in t]
        assert len(unique_tests) == 1


# ──────────────────────────────────────────────
# sync_all tests
# ──────────────────────────────────────────────


class TestSyncAll:
    def test_syncs_multiple_contracts(self, sample_contract, minimal_contract, dbt_project):
        results = sync_all(
            contracts_dir=sample_contract.parent,
            dbt_project_dir=dbt_project,
        )

        assert len(results) == 2
        names = {r.contract_name for r in results}
        assert "orders" in names
        assert "users" in names

    def test_clean_removes_existing(self, sample_contract, dbt_project):
        # First sync
        sync_contract(sample_contract, dbt_project)
        datavow_dir = dbt_project / "tests" / "datavow"
        assert len(list(datavow_dir.iterdir())) > 0

        # Create a stale file
        stale = datavow_dir / "stale_test.sql"
        stale.write_text("-- old test")

        # Sync with clean
        sync_all(
            contracts_dir=sample_contract.parent,
            dbt_project_dir=dbt_project,
            clean=True,
        )

        # Stale file should be gone
        assert not stale.exists()
        # But new files should exist
        assert len(list(datavow_dir.glob("*.sql"))) > 0

    def test_empty_contracts_dir(self, tmp_path, dbt_project):
        empty = tmp_path / "empty_contracts"
        empty.mkdir()

        results = sync_all(
            contracts_dir=empty,
            dbt_project_dir=dbt_project,
        )

        assert len(results) == 0

"""Tests for ODCS JSON Schema validation and format adapter."""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest
import yaml

from datavow.odcs import (
    ContractFormat,
    OdcsValidationResult,
    detect_format,
    odcs_to_datavow,
    validate_odcs_schema,
    validate_odcs_schema_dict,
)


VALID_ODCS_CONTRACT = dedent("""\
    kind: DataContract
    apiVersion: v3.1.0
    id: 53581432-6c55-4ba2-a65f-72344a91553a
    version: 1.0.0
    status: active
    name: orders
    domain: sales
    description:
      purpose: Customer orders from the e-commerce platform
    schema:
      - name: orders_table
        physicalType: table
        properties:
          - name: order_id
            logicalType: integer
            required: true
            unique: true
          - name: customer_email
            logicalType: string
            required: true
            classification: confidential
          - name: total_amount
            logicalType: number
            required: true
          - name: created_at
            logicalType: timestamp
""")

VALID_ODCS_WITH_QUALITY = dedent("""\
    kind: DataContract
    apiVersion: v3.1.0
    id: test-quality-001
    version: 1.0.0
    status: active
    name: air_quality
    domain: environment
    schema:
      - name: measurements
        physicalType: table
        properties:
          - name: value
            logicalType: number
            quality:
              - type: sql
                name: no_negative_values
                query: "SELECT COUNT(*) FROM {table} WHERE value < 0"
                mustBe: 0
                severity: error
                dimension: accuracy
          - name: sensor_id
            logicalType: string
            required: true
    slaProperties:
      - property: latency
        value: 24h
      - property: completeness
        value: "99.5%"
""")

DATAVOW_CONTRACT = dedent("""\
    apiVersion: datavow/v1
    kind: DataContract
    metadata:
      name: orders
      version: 1.0.0
      owner: data-team@company.com
      domain: sales
    schema:
      type: table
      fields:
        - name: order_id
          type: integer
          required: true
""")

INVALID_ODCS_MISSING_REQUIRED = dedent("""\
    kind: DataContract
    apiVersion: v3.1.0
    name: orders
    schema:
      - name: table1
        properties:
          - name: col1
            logicalType: string
""")


@pytest.fixture
def tmp_odcs_contract(tmp_path: Path) -> Path:
    p = tmp_path / "odcs_contract.yaml"
    p.write_text(VALID_ODCS_CONTRACT)
    return p


@pytest.fixture
def tmp_odcs_quality_contract(tmp_path: Path) -> Path:
    p = tmp_path / "odcs_quality.yaml"
    p.write_text(VALID_ODCS_WITH_QUALITY)
    return p


@pytest.fixture
def tmp_datavow_contract(tmp_path: Path) -> Path:
    p = tmp_path / "datavow_contract.yaml"
    p.write_text(DATAVOW_CONTRACT)
    return p


@pytest.fixture
def tmp_invalid_odcs(tmp_path: Path) -> Path:
    p = tmp_path / "invalid_odcs.yaml"
    p.write_text(INVALID_ODCS_MISSING_REQUIRED)
    return p


class TestDetectFormat:
    def test_detect_datavow(self):
        raw = yaml.safe_load(DATAVOW_CONTRACT)
        assert detect_format(raw) == ContractFormat.DATAVOW

    def test_detect_odcs(self):
        raw = yaml.safe_load(VALID_ODCS_CONTRACT)
        assert detect_format(raw) == ContractFormat.ODCS

    def test_detect_odcs_with_quality(self):
        raw = yaml.safe_load(VALID_ODCS_WITH_QUALITY)
        assert detect_format(raw) == ContractFormat.ODCS

    def test_detect_unknown(self):
        assert detect_format({"foo": "bar"}) == ContractFormat.UNKNOWN

    def test_detect_datavow_by_metadata(self):
        raw = {"metadata": {"name": "test"}, "schema": {"fields": []}}
        assert detect_format(raw) == ContractFormat.DATAVOW


class TestOdcsSchemaValidation:
    def test_valid_contract(self, tmp_odcs_contract: Path):
        result = validate_odcs_schema(tmp_odcs_contract)
        assert result.valid, f"Expected valid, got errors: {[e.message for e in result.errors]}"
        assert result.error_count == 0

    def test_invalid_missing_required(self, tmp_invalid_odcs: Path):
        result = validate_odcs_schema(tmp_invalid_odcs)
        assert not result.valid
        assert result.error_count > 0
        error_messages = " ".join(e.message for e in result.errors)
        assert "id" in error_messages or "status" in error_messages or "version" in error_messages

    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            validate_odcs_schema("/nonexistent/path.yaml")

    def test_validate_dict(self):
        raw = yaml.safe_load(VALID_ODCS_CONTRACT)
        result = validate_odcs_schema_dict(raw)
        assert result.valid

    def test_summary_valid(self, tmp_odcs_contract: Path):
        result = validate_odcs_schema(tmp_odcs_contract)
        assert "valid" in result.summary()

    def test_summary_invalid(self, tmp_invalid_odcs: Path):
        result = validate_odcs_schema(tmp_invalid_odcs)
        assert "error" in result.summary()


class TestOdcsToDatavow:
    def test_basic_conversion(self):
        raw = yaml.safe_load(VALID_ODCS_CONTRACT)
        result = odcs_to_datavow(raw)
        assert result["apiVersion"] == "datavow/v1"
        assert result["kind"] == "DataContract"
        assert result["metadata"]["name"] == "orders"
        assert result["metadata"]["version"] == "1.0.0"
        assert result["metadata"]["domain"] == "sales"

    def test_fields_converted(self):
        raw = yaml.safe_load(VALID_ODCS_CONTRACT)
        result = odcs_to_datavow(raw)
        fields = result["schema"]["fields"]
        assert len(fields) == 4
        order_id = next(f for f in fields if f["name"] == "order_id")
        assert order_id["type"] == "integer"
        assert order_id["required"] is True
        assert order_id["unique"] is True
        email = next(f for f in fields if f["name"] == "customer_email")
        assert email["type"] == "string"
        assert email.get("pii") is True
        amount = next(f for f in fields if f["name"] == "total_amount")
        assert amount["type"] == "float"

    def test_quality_rules_converted(self):
        raw = yaml.safe_load(VALID_ODCS_WITH_QUALITY)
        result = odcs_to_datavow(raw)
        rules = result.get("quality", {}).get("rules", [])
        assert len(rules) >= 1
        sql_rule = next((r for r in rules if r["type"] == "sql"), None)
        assert sql_rule is not None
        assert sql_rule["severity"] == "CRITICAL"
        assert "value < 0" in sql_rule["query"]

    def test_sla_converted(self):
        raw = yaml.safe_load(VALID_ODCS_WITH_QUALITY)
        result = odcs_to_datavow(raw)
        sla = result.get("sla", {})
        assert sla.get("freshness") == "24h"
        assert sla.get("completeness") == "99.5%"

    def test_description_from_purpose(self):
        raw = yaml.safe_load(VALID_ODCS_CONTRACT)
        result = odcs_to_datavow(raw)
        assert "e-commerce" in result["metadata"]["description"].lower()


class TestAutoDetection:
    def test_loads_datavow_format(self, tmp_datavow_contract: Path):
        from datavow.contract import DataContract
        c = DataContract.from_yaml(tmp_datavow_contract)
        assert c.metadata.name == "orders"
        assert c.metadata.domain == "sales"

    def test_loads_odcs_format(self, tmp_odcs_contract: Path):
        from datavow.contract import DataContract
        c = DataContract.from_yaml(tmp_odcs_contract)
        assert c.metadata.name == "orders"
        assert c.metadata.domain == "sales"
        assert len(c.schema_.fields) == 4

    def test_odcs_preserves_field_types(self, tmp_odcs_contract: Path):
        from datavow.contract import DataContract
        c = DataContract.from_yaml(tmp_odcs_contract)
        field_names = {f.name for f in c.schema_.fields}
        assert "order_id" in field_names
        assert "customer_email" in field_names
        assert "total_amount" in field_names
        assert "created_at" in field_names

    def test_odcs_with_quality_loads(self, tmp_odcs_quality_contract: Path):
        from datavow.contract import DataContract
        c = DataContract.from_yaml(tmp_odcs_quality_contract)
        assert c.metadata.name == "air_quality"
        assert len(c.quality.rules) >= 1

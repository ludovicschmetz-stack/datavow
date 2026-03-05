"""Tests for DataVow define and ci commands."""

import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from datavow.cli import app

FIXTURES = Path(__file__).parent / "fixtures"
CONTRACT = FIXTURES / "orders_contract.yaml"
CUSTOMERS_CONTRACT = FIXTURES / "customers_contract.yaml"
CLEAN_DATA = FIXTURES / "orders_clean.csv"
DIRTY_DATA = FIXTURES / "orders_dirty.csv"

runner = CliRunner()


# ──────────────────────────────────────────────
# datavow define
# ──────────────────────────────────────────────

class TestDefine:
    def test_valid_contract(self):
        result = runner.invoke(app, ["define", str(CONTRACT)])
        assert result.exit_code == 0
        assert "orders" in result.output
        assert "is valid" in result.output

    def test_shows_fields(self):
        result = runner.invoke(app, ["define", str(CONTRACT)])
        assert "order_id" in result.output
        assert "customer_email" in result.output
        assert "total_amount" in result.output

    def test_shows_field_count(self):
        result = runner.invoke(app, ["define", str(CONTRACT)])
        assert "5 fields" in result.output

    def test_shows_required(self):
        result = runner.invoke(app, ["define", str(CONTRACT)])
        assert "required" in result.output

    def test_shows_pii(self):
        result = runner.invoke(app, ["define", str(CONTRACT)])
        assert "pii" in result.output

    def test_shows_quality_rules(self):
        result = runner.invoke(app, ["define", str(CONTRACT)])
        assert "Quality rules" in result.output
        assert "no_negative_totals" in result.output
        assert "CRITICAL" in result.output

    def test_shows_sla(self):
        result = runner.invoke(app, ["define", str(CONTRACT)])
        assert "SLA" in result.output
        assert "24h" in result.output

    def test_shows_domain(self):
        result = runner.invoke(app, ["define", str(CONTRACT)])
        assert "sales" in result.output

    def test_shows_tags(self):
        result = runner.invoke(app, ["define", str(CONTRACT)])
        assert "pii" in result.output
        assert "financial" in result.output

    def test_invalid_contract_exits_2(self, tmp_path):
        bad = tmp_path / "bad.yaml"
        bad.write_text("not: a\nvalid: contract\n")
        result = runner.invoke(app, ["define", str(bad)])
        assert result.exit_code == 2
        assert "error" in result.output.lower()


# ──────────────────────────────────────────────
# datavow ci
# ──────────────────────────────────────────────

@pytest.fixture
def ci_dirs(tmp_path):
    """Set up contracts/ and sources/ directories for CI testing."""
    contracts = tmp_path / "contracts"
    sources = tmp_path / "sources"
    contracts.mkdir()
    sources.mkdir()

    # Copy contracts
    shutil.copy(CONTRACT, contracts / "orders.yaml")
    shutil.copy(CUSTOMERS_CONTRACT, contracts / "customers.yaml")

    # Create matching source files
    shutil.copy(CLEAN_DATA, sources / "orders.csv")

    # Create customers source
    (sources / "customers.csv").write_text(
        "customer_id,email,country\n"
        "1,alice@example.com,FR\n"
        "2,bob@example.com,DE\n"
        "3,carol@example.com,LU\n"
    )

    return contracts, sources


class TestCI:
    def test_all_pass(self, ci_dirs):
        contracts, sources = ci_dirs
        result = runner.invoke(app, ["ci", str(contracts), str(sources)])
        assert result.exit_code == 0
        assert "CI PASSED" in result.output

    def test_shows_each_contract(self, ci_dirs):
        contracts, sources = ci_dirs
        result = runner.invoke(app, ["ci", str(contracts), str(sources)])
        assert "orders" in result.output
        assert "customers" in result.output

    def test_shows_summary(self, ci_dirs):
        contracts, sources = ci_dirs
        result = runner.invoke(app, ["ci", str(contracts), str(sources)])
        assert "2 validated" in result.output
        assert "Average score" in result.output

    def test_critical_failure_exits_1(self, ci_dirs):
        contracts, sources = ci_dirs
        # Replace orders source with dirty data
        shutil.copy(DIRTY_DATA, sources / "orders.csv")
        result = runner.invoke(app, ["ci", str(contracts), str(sources)])
        assert result.exit_code == 1
        assert "CI FAILED" in result.output

    def test_skips_unmatched_contracts(self, ci_dirs):
        contracts, sources = ci_dirs
        # Add a contract with no matching source
        (contracts / "orphan.yaml").write_text(
            "apiVersion: datavow/v1\nkind: DataContract\n"
            "metadata:\n  name: orphan\nschema:\n  fields:\n"
            "    - name: id\n      type: integer\n"
        )
        result = runner.invoke(app, ["ci", str(contracts), str(sources)])
        assert "skipped" in result.output.lower() or "no matching" in result.output.lower()

    def test_empty_contracts_dir(self, tmp_path):
        contracts = tmp_path / "empty_contracts"
        sources = tmp_path / "sources"
        contracts.mkdir()
        sources.mkdir()
        result = runner.invoke(app, ["ci", str(contracts), str(sources)])
        assert result.exit_code == 0
        assert "No .yaml files" in result.output

    def test_fail_on_warning(self, ci_dirs):
        contracts, sources = ci_dirs
        # Clean data passes — should exit 0 even with --fail-on warning
        result = runner.invoke(app, [
            "ci", str(contracts), str(sources), "--fail-on", "warning"
        ])
        assert result.exit_code == 0

    def test_nonexistent_dir_exits_2(self, tmp_path):
        result = runner.invoke(app, [
            "ci", str(tmp_path / "nope"), str(tmp_path / "also_nope")
        ])
        assert result.exit_code == 2

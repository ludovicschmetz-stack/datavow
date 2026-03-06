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
        assert set(required) == {
            "order_id",
            "customer_email",
            "total_amount",
            "status",
            "created_at",
        }

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
        schema_failures = [
            f for f in self.result.findings if f.category == "schema" and not f.passed
        ]
        assert len(schema_failures) == 0

    def test_all_quality_rules_pass(self):
        quality_failures = [
            f for f in self.result.findings if f.category == "quality" and not f.passed
        ]
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
        nulls = [
            f for f in self.result.findings if "required:customer_email" in f.rule and not f.passed
        ]
        assert len(nulls) > 0

    def test_detects_duplicate_order_id(self):
        dups = [f for f in self.result.findings if "unique:order_id" in f.rule and not f.passed]
        assert len(dups) > 0

    def test_detects_negative_amount(self):
        negatives = [
            f for f in self.result.findings if f.rule == "no_negative_totals" and not f.passed
        ]
        assert len(negatives) > 0

    def test_detects_email_pattern_violation(self):
        patterns = [
            f for f in self.result.findings if "pattern:customer_email" in f.rule and not f.passed
        ]
        assert len(patterns) > 0

    def test_detects_invalid_status(self):
        status_checks = [
            f for f in self.result.findings if "allowed_values:status" in f.rule and not f.passed
        ]
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

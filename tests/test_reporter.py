"""Tests for DataVow report command — HTML and Markdown generation."""

from pathlib import Path

import pytest

from datavow.contract import DataContract
from datavow.reporter import generate_html, generate_markdown, write_report
from datavow.validator import validate

FIXTURES = Path(__file__).parent / "fixtures"
CONTRACT = FIXTURES / "orders_contract.yaml"
CLEAN_DATA = FIXTURES / "orders_clean.csv"
DIRTY_DATA = FIXTURES / "orders_dirty.csv"


@pytest.fixture
def clean_context():
    contract = DataContract.from_yaml(CONTRACT)
    result = validate(CONTRACT, CLEAN_DATA)
    return contract, result


@pytest.fixture
def dirty_context():
    contract = DataContract.from_yaml(CONTRACT)
    result = validate(CONTRACT, DIRTY_DATA)
    return contract, result


class TestHTMLReport:
    def test_generates_html(self, clean_context):
        contract, result = clean_context
        html = generate_html(contract, result)
        assert "<!DOCTYPE html>" in html
        assert "</html>" in html

    def test_contains_contract_name(self, clean_context):
        contract, result = clean_context
        html = generate_html(contract, result)
        assert "orders" in html

    def test_contains_verdict(self, clean_context):
        contract, result = clean_context
        html = generate_html(contract, result)
        assert "Vow Kept" in html

    def test_contains_score(self, clean_context):
        contract, result = clean_context
        html = generate_html(contract, result)
        assert str(result.score) in html

    def test_contains_schema_fields(self, clean_context):
        contract, result = clean_context
        html = generate_html(contract, result)
        assert "order_id" in html
        assert "customer_email" in html
        assert "total_amount" in html

    def test_contains_domain(self, clean_context):
        contract, result = clean_context
        html = generate_html(contract, result)
        assert "sales" in html

    def test_contains_pii_tag(self, clean_context):
        contract, result = clean_context
        html = generate_html(contract, result)
        assert "PII" in html

    def test_contains_version(self, clean_context):
        contract, result = clean_context
        html = generate_html(contract, result)
        assert "DataVow v" in html

    def test_dirty_shows_failures(self, dirty_context):
        contract, result = dirty_context
        html = generate_html(contract, result)
        assert "Vow Shattered" in html
        assert "CRITICAL" in html
        assert "Failures" in html

    def test_dirty_shows_findings(self, dirty_context):
        contract, result = dirty_context
        html = generate_html(contract, result)
        assert "no_negative_totals" in html
        assert "unique:order_id" in html


class TestMarkdownReport:
    def test_generates_markdown(self, clean_context):
        contract, result = clean_context
        md = generate_markdown(contract, result)
        assert "# DataVow Report" in md

    def test_contains_verdict(self, clean_context):
        contract, result = clean_context
        md = generate_markdown(contract, result)
        assert "Vow Kept" in md

    def test_contains_schema_table(self, clean_context):
        contract, result = clean_context
        md = generate_markdown(contract, result)
        assert "order_id" in md
        assert "`integer`" in md

    def test_dirty_shows_failures(self, dirty_context):
        contract, result = dirty_context
        md = generate_markdown(contract, result)
        assert "Failures" in md
        assert "CRITICAL" in md


class TestWriteReport:
    def test_write_html(self, tmp_path, clean_context):
        contract, result = clean_context
        out = tmp_path / "report.html"
        path = write_report(contract, result, out, format="html")
        assert path.exists()
        content = path.read_text()
        assert "<!DOCTYPE html>" in content
        assert "orders" in content

    def test_write_markdown(self, tmp_path, clean_context):
        contract, result = clean_context
        out = tmp_path / "report.md"
        path = write_report(contract, result, out, format="md")
        assert path.exists()
        content = path.read_text()
        assert "# DataVow Report" in content

    def test_creates_parent_dirs(self, tmp_path, clean_context):
        contract, result = clean_context
        out = tmp_path / "nested" / "dir" / "report.html"
        path = write_report(contract, result, out, format="html")
        assert path.exists()

    def test_unknown_format_raises(self, tmp_path, clean_context):
        contract, result = clean_context
        with pytest.raises(ValueError, match="Unknown report format"):
            write_report(contract, result, tmp_path / "r.txt", format="pdf")


class TestReportCLI:
    def test_report_html_default(self, tmp_path, monkeypatch):
        from typer.testing import CliRunner
        from datavow.cli import app
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(app, ["report", str(CONTRACT), str(CLEAN_DATA)])
        assert result.exit_code == 0
        assert "Report written to" in result.output
        report_file = tmp_path / "orders-report.html"
        assert report_file.exists()
        content = report_file.read_text()
        assert "<!DOCTYPE html>" in content

    def test_report_markdown(self, tmp_path, monkeypatch):
        from typer.testing import CliRunner
        from datavow.cli import app
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(app, ["report", str(CONTRACT), str(CLEAN_DATA), "-f", "md"])
        assert result.exit_code == 0
        report_file = tmp_path / "orders-report.md"
        assert report_file.exists()

    def test_report_custom_output(self, tmp_path):
        from typer.testing import CliRunner
        from datavow.cli import app
        out = tmp_path / "custom-report.html"
        runner = CliRunner()
        result = runner.invoke(app, ["report", str(CONTRACT), str(DIRTY_DATA), "-o", str(out)])
        assert result.exit_code == 0
        assert out.exists()
        content = out.read_text()
        assert "Vow Shattered" in content

    def test_report_dirty_shows_score(self, tmp_path, monkeypatch):
        from typer.testing import CliRunner
        from datavow.cli import app
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        result = runner.invoke(app, ["report", str(CONTRACT), str(DIRTY_DATA)])
        assert result.exit_code == 0
        assert "Vow Shattered" in result.output

#!/usr/bin/env bash
# DataVow — integrate define + ci commands (Phase 1 completion)
# Run from repo root: ~/Documents/Projects/datavow
set -euo pipefail

echo "=== DataVow: integrating define + ci commands ==="

if [[ ! -f "src/datavow/cli.py" ]]; then
    echo "ERROR: run this from the datavow repo root"
    exit 1
fi

echo "[1/3] Creating test fixture..."
cat > tests/fixtures/customers_contract.yaml << 'YAMLEOF'
apiVersion: datavow/v1
kind: DataContract
metadata:
  name: customers
  version: 1.0.0
  owner: data-team@company.com
  domain: sales
  description: "Customer master data"

schema:
  type: table
  fields:
    - name: customer_id
      type: integer
      required: true
      unique: true
    - name: email
      type: string
      required: true
    - name: country
      type: string
      required: true

quality:
  rules:
    - name: id_not_null
      type: not_null
      field: customer_id
      severity: CRITICAL
YAMLEOF

echo "[2/3] Updating CLI (adding define + ci commands)..."
cat > src/datavow/cli.py << 'PYEOF'
"""DataVow CLI — Typer-based command interface.

Commands:
  init      Scaffold a new datavow project.
  define    Validate contract syntax and show structure summary.
  validate  Validate a data source against a contract.
  report    Generate an HTML or Markdown report from a validation.
  ci        Batch-validate contracts against data sources. Exit 1 on CRITICAL.
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
# datavow define
# ──────────────────────────────────────────────

@app.command()
def define(
    contract: Annotated[
        Path,
        typer.Argument(help="Path to the contract YAML file.", exists=True, readable=True),
    ],
) -> None:
    """Validate a contract's YAML syntax and display its structure.

    Parses the contract without requiring data. Use this to verify
    a contract before committing it to version control.
    """
    from datavow.contract import DataContract

    try:
        c = DataContract.from_yaml(contract)
    except FileNotFoundError as e:
        console.print(f"[bold red]Error:[/] {e}")
        raise typer.Exit(code=2)
    except Exception as e:
        console.print(f"[bold red]Contract error:[/] {e}")
        raise typer.Exit(code=2)

    # Metadata
    console.print()
    console.print(f"[bold green]✓[/] Contract [bold]{c.metadata.name}[/] is valid")
    console.print()

    table = Table(show_header=False, show_edge=False, pad_edge=False, box=None)
    table.add_column("Key", style="dim", width=16)
    table.add_column("Value")
    table.add_row("Name", c.metadata.name)
    table.add_row("Version", c.metadata.version)
    table.add_row("Domain", c.metadata.domain or "—")
    table.add_row("Owner", c.metadata.owner or "—")
    if c.metadata.tags:
        table.add_row("Tags", ", ".join(c.metadata.tags))
    if c.metadata.description:
        table.add_row("Description", c.metadata.description)
    console.print(table)
    console.print()

    # Schema summary
    fields = c.schema_.fields
    required_count = sum(1 for f in fields if f.required)
    pii_count = sum(1 for f in fields if f.pii)
    console.print(
        f"[bold]Schema:[/] {len(fields)} fields "
        f"({required_count} required, {pii_count} PII)"
    )
    for f in fields:
        flags = []
        if f.required:
            flags.append("[green]required[/]")
        if f.unique:
            flags.append("[cyan]unique[/]")
        if f.pii:
            flags.append("[yellow]pii[/]")
        flag_str = f" ({', '.join(flags)})" if flags else ""
        console.print(f"  [dim]•[/] {f.name} [dim]{f.type.value}[/]{flag_str}")

    # Quality rules
    if c.quality.rules:
        console.print()
        console.print(f"[bold]Quality rules:[/] {len(c.quality.rules)}")
        for r in c.quality.rules:
            sev_style = _SEVERITY_STYLES.get(r.severity, "")
            console.print(
                f"  [dim]•[/] {r.name} [{sev_style}]{r.severity.value}[/] ({r.type.value})"
            )

    # SLA
    if c.sla.freshness or c.sla.completeness or c.sla.availability:
        console.print()
        sla_parts = []
        if c.sla.freshness:
            sla_parts.append(f"freshness={c.sla.freshness}")
        if c.sla.completeness:
            sla_parts.append(f"completeness={c.sla.completeness}")
        if c.sla.availability:
            sla_parts.append(f"availability={c.sla.availability}")
        console.print(f"[bold]SLA:[/] {', '.join(sla_parts)}")

    console.print()


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
# datavow report
# ──────────────────────────────────────────────

@app.command()
def report(
    contract: Annotated[
        Path,
        typer.Argument(help="Path to the contract YAML file.", exists=True, readable=True),
    ],
    source: Annotated[
        Path,
        typer.Argument(help="Path to the data source (CSV, Parquet, JSON).", exists=True, readable=True),
    ],
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Output file path. Default: <contract_name>-report.<format>"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Report format: html or markdown (md)."),
    ] = "html",
) -> None:
    """Generate an HTML or Markdown report from a data validation.

    Runs validation then produces a standalone report file that can be
    shared with stakeholders, attached to deliveries, or published.
    """
    from datavow.contract import DataContract
    from datavow.reporter import write_report
    from datavow.validator import validate as run_validate

    fmt = format.lower()
    if fmt not in ("html", "markdown", "md"):
        console.print(f"[bold red]Error:[/] Unknown format '{format}'. Use 'html' or 'markdown'.")
        raise typer.Exit(code=2)

    try:
        parsed_contract = DataContract.from_yaml(contract)
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

    if output is None:
        ext = "md" if fmt in ("markdown", "md") else "html"
        output = Path(f"{parsed_contract.metadata.name}-report.{ext}")

    report_path = write_report(parsed_contract, result, output, format=fmt)

    v = result.verdict
    console.print()
    console.print(
        f"{v.emoji} [bold]{parsed_contract.metadata.name}[/]: "
        f"{v.value} (score={result.score})"
    )
    console.print(f"Report written to [bold cyan]{report_path}[/]")


# ──────────────────────────────────────────────
# datavow ci
# ──────────────────────────────────────────────

_SOURCE_EXTENSIONS = (".csv", ".parquet", ".json", ".jsonl", ".ndjson", ".tsv")


def _find_source(name: str, source_dir: Path) -> Path | None:
    """Find a data source matching a contract name by convention.

    Looks for <source_dir>/<name>.{csv,parquet,json,...}
    """
    for ext in _SOURCE_EXTENSIONS:
        candidate = source_dir / f"{name}{ext}"
        if candidate.exists():
            return candidate
    return None


@app.command(name="ci")
def ci_cmd(
    contracts_dir: Annotated[
        Path,
        typer.Argument(help="Directory containing contract YAML files."),
    ],
    source_dir: Annotated[
        Path,
        typer.Argument(help="Directory containing data sources. Matched by name convention."),
    ],
    fail_on: Annotated[
        str,
        typer.Option("--fail-on", help="Fail on: 'critical' (default) or 'warning'."),
    ] = "critical",
) -> None:
    """Batch-validate all contracts in a directory against matching data sources.

    Matches contracts to sources by name convention:
    contracts/orders.yaml → sources/orders.csv (or .parquet, .json)

    Exits with code 1 if any contract has failures at or above --fail-on level.
    Designed for CI/CD pipelines (GitHub Actions, GitLab CI, etc.).
    """
    from datavow.validator import validate as run_validate

    contracts_path = Path(contracts_dir)
    sources_path = Path(source_dir)

    if not contracts_path.is_dir():
        console.print(f"[bold red]Error:[/] {contracts_path} is not a directory")
        raise typer.Exit(code=2)
    if not sources_path.is_dir():
        console.print(f"[bold red]Error:[/] {sources_path} is not a directory")
        raise typer.Exit(code=2)

    # Find all contract YAML files
    contract_files = sorted(
        p for p in contracts_path.iterdir()
        if p.suffix in (".yaml", ".yml") and p.is_file()
    )

    if not contract_files:
        console.print(f"[bold yellow]Warning:[/] No .yaml files found in {contracts_path}")
        raise typer.Exit(code=0)

    console.print()
    console.print(
        f"[bold]DataVow CI[/] — {len(contract_files)} contract(s) in "
        f"[cyan]{contracts_path}[/] against [cyan]{sources_path}[/]"
    )
    console.print()

    results: list[tuple[str, str, ValidationResult | None]] = []
    has_critical = False
    has_warning = False

    for contract_file in contract_files:
        name = contract_file.stem
        source_file = _find_source(name, sources_path)

        if source_file is None:
            console.print(
                f"  [yellow]⊘[/] {name}: no matching source in {sources_path}"
            )
            results.append((name, "skipped", None))
            continue

        try:
            result = run_validate(contract_file, source_file)
            v = result.verdict
            console.print(
                f"  {v.emoji} {name}: {v.value} (score={result.score})"
            )
            results.append((name, "validated", result))
            if result.has_critical_failures:
                has_critical = True
            if result.warning_count > 0:
                has_warning = True
        except Exception as e:
            console.print(f"  [bold red]✗[/] {name}: error — {e}")
            results.append((name, "error", None))
            has_critical = True

    # Summary
    console.print()
    validated = sum(1 for _, s, _ in results if s == "validated")
    skipped = sum(1 for _, s, _ in results if s == "skipped")
    errors = sum(1 for _, s, _ in results if s == "error")
    total_score = 0
    scored_count = 0
    for _, status, result in results:
        if status == "validated" and result is not None:
            total_score += result.score
            scored_count += 1
    avg_score = total_score // scored_count if scored_count > 0 else 0

    console.print(
        f"[bold]Summary:[/] {validated} validated, {skipped} skipped, "
        f"{errors} error(s). Average score: {avg_score}/100"
    )

    # Exit code
    should_fail = has_critical or (fail_on == "warning" and has_warning)
    if should_fail:
        console.print("[bold red]CI FAILED[/]")
        raise typer.Exit(code=1)
    else:
        console.print("[bold green]CI PASSED[/]")


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

echo "[3/3] Creating tests..."
cat > tests/test_define_ci.py << 'PYEOF'
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
PYEOF

echo ""
echo "=== Phase 1 MVP complete ==="
echo ""
echo "New files:"
echo "  tests/fixtures/customers_contract.yaml"
echo "  tests/test_define_ci.py"
echo ""
echo "Updated files:"
echo "  src/datavow/cli.py  (added define + ci commands)"
echo ""
echo "All 5 commands ready:"
echo "  datavow init        — scaffold project"
echo "  datavow define      — validate contract syntax"
echo "  datavow validate    — validate data vs contract"
echo "  datavow report      — generate HTML/Markdown report"
echo "  datavow ci          — batch validate for CI/CD"
echo ""
echo "Next steps:"
echo "  1. pip install -e '.[dev]'"
echo "  2. pytest tests/ -v    # expected: 77 passed"
echo "  3. datavow define tests/fixtures/orders_contract.yaml"
echo "  4. git add . && git commit -m 'feat: datavow define + ci commands (Phase 1 complete)'"

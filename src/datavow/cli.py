"""DataVow CLI — Typer-based command interface.

Commands:
  init      Scaffold a new datavow project.
  validate  Validate a data source against a contract.
  report    Generate an HTML or Markdown report from a validation.
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

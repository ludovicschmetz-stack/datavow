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
from datavow.findings import Severity, ValidationResult, Verdict

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
        typer.Option(
            "--version", "-v", help="Show version.", callback=_version_callback, is_eager=True
        ),
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
        f"[bold]Schema:[/] {len(fields)} fields ({required_count} required, {pii_count} PII)"
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
        typer.Argument(
            help="Path to the data source (CSV, Parquet, JSON).", exists=True, readable=True
        ),
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
        typer.Argument(
            help="Path to the data source (CSV, Parquet, JSON).", exists=True, readable=True
        ),
    ],
    output: Annotated[
        Path,
        typer.Option(
            "--output", "-o", help="Output file path. Default: <contract_name>-report.<format>"
        ),
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
        f"{v.emoji} [bold]{parsed_contract.metadata.name}[/]: {v.value} (score={result.score})"
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
        p for p in contracts_path.iterdir() if p.suffix in (".yaml", ".yml") and p.is_file()
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
            console.print(f"  [yellow]⊘[/] {name}: no matching source in {sources_path}")
            results.append((name, "skipped", None))
            continue

        try:
            result = run_validate(contract_file, source_file)
            v = result.verdict
            console.print(f"  {v.emoji} {name}: {v.value} (score={result.score})")
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
                "rule": f.rule,
                "category": f.category,
                "severity": f.severity.value,
                "passed": f.passed,
                "message": f.message,
                "details": f.details,
            }
            for f in result.findings
        ],
    }
    console.print_json(json.dumps(data))


# ──────────────────────────────────────────────
# datavow dbt (subcommand group)
# ──────────────────────────────────────────────

dbt_app = typer.Typer(
    name="dbt",
    help="dbt integration — generate contracts from manifest, validate dbt models.",
    no_args_is_help=True,
)
app.add_typer(dbt_app)


@dbt_app.command(name="generate")
def dbt_generate(
    manifest: Annotated[
        Path,
        typer.Option("--manifest", "-m", help="Path to dbt manifest.json."),
    ] = Path("target/manifest.json"),
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Output directory for generated contracts."),
    ] = Path("contracts"),
    owner: Annotated[
        str,
        typer.Option("--owner", help="Default owner for generated contracts."),
    ] = "",
    models: Annotated[
        Optional[str],
        typer.Option("--models", help="Comma-separated model names to generate (default: all)."),
    ] = None,
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", help="Overwrite existing contracts."),
    ] = False,
) -> None:
    """Generate DataVow contracts from a dbt manifest.json.

    Reads model schemas, column types, and dbt tests from the manifest
    and creates YAML contracts ready for validation.
    """
    from datavow.connectors.dbt import contract_to_yaml, parse_manifest

    if not manifest.exists():
        console.print(f"[bold red]Error:[/] Manifest not found: {manifest}")
        console.print("[dim]Run 'dbt compile' or 'dbt run' first to generate the manifest.[/]")
        raise typer.Exit(code=2)

    parsed_models = parse_manifest(manifest)

    if not parsed_models:
        console.print("[bold yellow]Warning:[/] No models found in manifest.")
        raise typer.Exit(code=0)

    # Filter models if specified
    if models:
        model_names = {m.strip() for m in models.split(",")}
        parsed_models = [m for m in parsed_models if m.name in model_names]
        if not parsed_models:
            console.print(f"[bold red]Error:[/] None of the specified models found: {models}")
            raise typer.Exit(code=2)

    output.mkdir(parents=True, exist_ok=True)

    console.print()
    console.print(
        f"[bold]DataVow dbt generate[/] — {len(parsed_models)} model(s) from [cyan]{manifest}[/]"
    )
    console.print()

    generated = 0
    skipped = 0

    for model in parsed_models:
        # Organize by domain
        domain_dir = output / model.domain if model.domain else output
        domain_dir.mkdir(parents=True, exist_ok=True)
        contract_path = domain_dir / f"{model.name}.yaml"

        if contract_path.exists() and not overwrite:
            console.print(f"  [yellow]⊘[/] {model.name}: exists, skipped (use --overwrite)")
            skipped += 1
            continue

        from datavow.connectors.dbt import generate_contract

        contract = generate_contract(model, owner=owner)
        yaml_content = contract_to_yaml(contract)
        contract_path.write_text(yaml_content)

        col_count = len(model.columns)
        rule_count = len(contract.quality.rules)
        console.print(
            f"  [green]✓[/] {model.name}: {col_count} fields, "
            f"{rule_count} rules → [cyan]{contract_path}[/]"
        )
        generated += 1

    console.print()
    console.print(
        f"[bold]Done:[/] {generated} generated, {skipped} skipped. Contracts in [cyan]{output}[/]"
    )
    if generated > 0:
        console.print(
            "[dim]Review generated contracts and adjust required/severity fields as needed.[/]"
        )
    console.print()


@dbt_app.command(name="validate")
def dbt_validate(
    manifest: Annotated[
        Path,
        typer.Option("--manifest", "-m", help="Path to dbt manifest.json."),
    ] = Path("target/manifest.json"),
    contracts_dir: Annotated[
        Path,
        typer.Option("--contracts", "-c", help="Directory containing DataVow contracts."),
    ] = Path("contracts"),
    profiles: Annotated[
        Optional[Path],
        typer.Option("--profiles", help="Path to dbt profiles.yml (default: ~/.dbt/profiles.yml)."),
    ] = None,
    project_dir: Annotated[
        Optional[Path],
        typer.Option("--project-dir", help="dbt project directory (for auto-resolving profiles)."),
    ] = None,
    target: Annotated[
        Optional[str],
        typer.Option("--target", "-t", help="dbt target to use (default: from profiles.yml)."),
    ] = None,
    models: Annotated[
        Optional[str],
        typer.Option("--models", help="Comma-separated model names to validate (default: all)."),
    ] = None,
    fail_on: Annotated[
        str,
        typer.Option("--fail-on", help="Fail on: 'critical' (default) or 'warning'."),
    ] = "critical",
    limit: Annotated[
        Optional[int],
        typer.Option("--limit", help="Limit rows to sample from each table (for large datasets)."),
    ] = None,
) -> None:
    """Validate dbt models against DataVow contracts.

    Reads the manifest to find models, matches them with contracts by name,
    then connects to the warehouse to validate data against each contract.

    Requires database connection via dbt profiles.yml.
    """
    from datavow.connectors.dbt import parse_manifest, parse_profiles
    from datavow.validator import validate_database

    if not manifest.exists():
        console.print(f"[bold red]Error:[/] Manifest not found: {manifest}")
        console.print("[dim]Run 'dbt compile' or 'dbt run' first to generate the manifest.[/]")
        raise typer.Exit(code=2)

    if not contracts_dir.is_dir():
        console.print(f"[bold red]Error:[/] Contracts directory not found: {contracts_dir}")
        raise typer.Exit(code=2)

    # Parse connection info
    try:
        conn_info = parse_profiles(
            profiles_path=profiles,
            project_dir=project_dir or Path.cwd(),
            target_name=target,
        )
    except (FileNotFoundError, ValueError) as e:
        console.print(f"[bold red]Error:[/] {e}")
        raise typer.Exit(code=2)

    if not conn_info.is_duckdb_attachable:
        console.print(
            f"[bold red]Error:[/] Adapter '{conn_info.adapter}' requires extras. "
            f"Install: pip install datavow[{conn_info.adapter}]"
        )
        console.print("[dim]Currently supported without extras: postgres, duckdb[/]")
        raise typer.Exit(code=2)

    # Parse models from manifest
    parsed_models = parse_manifest(manifest)
    if models:
        model_names = {m.strip() for m in models.split(",")}
        parsed_models = [m for m in parsed_models if m.name in model_names]

    if not parsed_models:
        console.print("[bold yellow]Warning:[/] No models found to validate.")
        raise typer.Exit(code=0)

    # Collect all contract files (including subdirectories for domain structure)
    contract_files: dict[str, Path] = {}
    for p in contracts_dir.rglob("*.yaml"):
        contract_files[p.stem] = p
    for p in contracts_dir.rglob("*.yml"):
        if p.stem not in contract_files:
            contract_files[p.stem] = p

    console.print()
    console.print(
        f"[bold]DataVow dbt validate[/] — {len(parsed_models)} model(s), "
        f"{len(contract_files)} contract(s), adapter={conn_info.adapter}"
    )
    console.print()

    has_critical = False
    has_warning = False
    validated = 0
    skipped = 0
    errors = 0
    total_score = 0

    for model in parsed_models:
        contract_path = contract_files.get(model.name)
        if not contract_path:
            console.print(f"  [yellow]⊘[/] {model.name}: no contract found, skipped")
            skipped += 1
            continue

        table_ref = f"{model.schema}.{model.name}"

        try:
            result = validate_database(
                contract_path=contract_path,
                connection_info=conn_info,
                table_ref=table_ref,
                limit=limit,
            )
            v = result.verdict
            console.print(f"  {v.emoji} {model.name}: {v.value} (score={result.score})")
            validated += 1
            total_score += result.score

            if result.has_critical_failures:
                has_critical = True
            if result.warning_count > 0:
                has_warning = True

        except Exception as e:
            console.print(f"  [bold red]✗[/] {model.name}: error — {e}")
            errors += 1
            has_critical = True

    # Summary
    avg_score = total_score // validated if validated > 0 else 0
    console.print()
    console.print(
        f"[bold]Summary:[/] {validated} validated, {skipped} skipped, "
        f"{errors} error(s). Average score: {avg_score}/100"
    )

    should_fail = has_critical or (fail_on == "warning" and has_warning)
    if should_fail:
        console.print("[bold red]CI FAILED[/]")
        raise typer.Exit(code=1)
    else:
        console.print("[bold green]CI PASSED[/]")


if __name__ == "__main__":
    app()

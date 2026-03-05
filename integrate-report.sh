#!/usr/bin/env bash
# DataVow — integrate report command into existing repo
# Run from repo root: ~/Documents/Projects/datavow
set -euo pipefail

echo "=== DataVow: integrating report command ==="

if [[ ! -f "src/datavow/cli.py" ]]; then
    echo "ERROR: run this from the datavow repo root"
    exit 1
fi

echo "[1/4] Creating reporter module..."

mkdir -p src/datavow/templates

# ── src/datavow/reporter.py (NEW) ──
cat > src/datavow/reporter.py << 'PYEOF'
"""DataVow reporter — generates HTML and Markdown reports from validation results.

Uses Jinja2 templates for rendering. Reports are self-contained (no external deps)
and designed to be readable by non-technical stakeholders.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from datavow import __version__
from datavow.contract import DataContract
from datavow.findings import ValidationResult, Verdict

_TEMPLATES_DIR = Path(__file__).parent / "templates"

_VERDICT_CLASS = {
    Verdict.KEPT: "kept",
    Verdict.STRAINED: "strained",
    Verdict.BROKEN: "broken",
    Verdict.SHATTERED: "shattered",
}


def _build_context(
    contract: DataContract,
    result: ValidationResult,
) -> dict:
    """Build the Jinja2 template context from contract + validation result."""
    return {
        "contract": contract,
        "result": result,
        "verdict_class": _VERDICT_CLASS[result.verdict],
        "failed_findings": [f for f in result.findings if not f.passed],
        "passed_findings": [f for f in result.findings if f.passed],
        "version": __version__,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


def _get_env() -> Environment:
    """Create a Jinja2 environment pointing at the templates directory."""
    return Environment(
        loader=FileSystemLoader(str(_TEMPLATES_DIR)),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )


def generate_html(
    contract: DataContract,
    result: ValidationResult,
) -> str:
    """Render the HTML report as a string."""
    env = _get_env()
    template = env.get_template("report.html.j2")
    return template.render(**_build_context(contract, result))


def generate_markdown(
    contract: DataContract,
    result: ValidationResult,
) -> str:
    """Render the Markdown report as a string."""
    env = _get_env()
    template = env.get_template("report.md.j2")
    return template.render(**_build_context(contract, result))


def write_report(
    contract: DataContract,
    result: ValidationResult,
    output_path: str | Path,
    format: str = "html",
) -> Path:
    """Generate and write a report to disk."""
    output_path = Path(output_path)

    if format in ("markdown", "md"):
        content = generate_markdown(contract, result)
    elif format == "html":
        content = generate_html(contract, result)
    else:
        raise ValueError(f"Unknown report format: {format}. Use 'html' or 'markdown'.")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding="utf-8")
    return output_path
PYEOF

echo "[2/4] Creating Jinja2 templates..."

# ── HTML template ──
cat > src/datavow/templates/report.html.j2 << 'J2EOF'
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DataVow Report — {{ contract.metadata.name }}</title>
<style>
  :root {
    --c-bg: #fafafa;
    --c-surface: #ffffff;
    --c-text: #1a1a2e;
    --c-muted: #6b7280;
    --c-border: #e5e7eb;
    --c-brand: #1e3a5f;
    --c-brand-light: #2d5a8e;
    --c-kept: #059669;
    --c-kept-bg: #d1fae5;
    --c-strained: #d97706;
    --c-strained-bg: #fef3c7;
    --c-broken: #dc2626;
    --c-broken-bg: #fee2e2;
    --c-shattered: #991b1b;
    --c-shattered-bg: #fecaca;
    --c-critical: #dc2626;
    --c-warning: #d97706;
    --c-info: #6b7280;
    --c-pass: #059669;
    --font-body: 'Segoe UI', system-ui, -apple-system, sans-serif;
    --font-mono: 'SF Mono', 'Cascadia Code', 'Consolas', monospace;
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    font-family: var(--font-body);
    background: var(--c-bg);
    color: var(--c-text);
    line-height: 1.6;
    font-size: 15px;
  }

  .container { max-width: 900px; margin: 0 auto; padding: 0 24px; }

  header {
    background: linear-gradient(135deg, var(--c-brand) 0%, var(--c-brand-light) 100%);
    color: white;
    padding: 32px 0;
  }
  header .container { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 16px; }
  .brand { font-size: 14px; letter-spacing: 2px; text-transform: uppercase; opacity: 0.8; }
  .brand strong { font-size: 22px; letter-spacing: 0; text-transform: none; opacity: 1; display: block; margin-top: 2px; }
  .verdict-badge {
    display: inline-flex; align-items: center; gap: 10px;
    padding: 10px 20px; border-radius: 8px;
    font-size: 18px; font-weight: 700;
  }
  .verdict-badge .emoji { font-size: 24px; }
  .verdict-kept { background: var(--c-kept-bg); color: var(--c-kept); }
  .verdict-strained { background: var(--c-strained-bg); color: var(--c-strained); }
  .verdict-broken { background: var(--c-broken-bg); color: var(--c-broken); }
  .verdict-shattered { background: var(--c-shattered-bg); color: var(--c-shattered); }

  section { margin-top: 28px; }
  .card {
    background: var(--c-surface);
    border: 1px solid var(--c-border);
    border-radius: 10px;
    padding: 24px;
    margin-bottom: 20px;
  }
  h2 {
    font-size: 16px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 1.5px;
    color: var(--c-brand);
    margin-bottom: 16px;
    padding-bottom: 8px;
    border-bottom: 2px solid var(--c-brand);
  }

  .score-grid {
    display: grid;
    grid-template-columns: 160px 1fr;
    gap: 24px;
    align-items: center;
  }
  .score-circle {
    width: 140px; height: 140px;
    border-radius: 50%;
    display: flex; flex-direction: column;
    align-items: center; justify-content: center;
    border: 6px solid var(--c-border);
  }
  .score-circle.kept { border-color: var(--c-kept); }
  .score-circle.strained { border-color: var(--c-strained); }
  .score-circle.broken { border-color: var(--c-broken); }
  .score-circle.shattered { border-color: var(--c-shattered); }
  .score-number { font-size: 42px; font-weight: 800; line-height: 1; }
  .score-label { font-size: 12px; color: var(--c-muted); text-transform: uppercase; letter-spacing: 1px; margin-top: 2px; }
  .score-number.kept { color: var(--c-kept); }
  .score-number.strained { color: var(--c-strained); }
  .score-number.broken { color: var(--c-broken); }
  .score-number.shattered { color: var(--c-shattered); }

  .counters { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
  .counter {
    text-align: center; padding: 14px 8px;
    border-radius: 8px; background: var(--c-bg);
  }
  .counter-value { font-size: 28px; font-weight: 800; line-height: 1; }
  .counter-label { font-size: 11px; text-transform: uppercase; letter-spacing: 1px; color: var(--c-muted); margin-top: 4px; }
  .counter-value.pass { color: var(--c-pass); }
  .counter-value.critical { color: var(--c-critical); }
  .counter-value.warning { color: var(--c-warning); }
  .counter-value.info { color: var(--c-info); }

  .meta-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px; }
  .meta-item label { display: block; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; color: var(--c-muted); margin-bottom: 2px; }
  .meta-item span { font-weight: 600; }
  .tag {
    display: inline-block; padding: 2px 8px;
    background: var(--c-bg); border: 1px solid var(--c-border);
    border-radius: 4px; font-size: 12px; margin-right: 4px;
    font-family: var(--font-mono);
  }
  .tag-pii { background: #fef3c7; border-color: #f59e0b; color: #92400e; }

  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  th {
    text-align: left; padding: 10px 12px;
    background: var(--c-bg); font-weight: 700;
    font-size: 11px; text-transform: uppercase;
    letter-spacing: 1px; color: var(--c-muted);
    border-bottom: 2px solid var(--c-border);
  }
  td {
    padding: 10px 12px;
    border-bottom: 1px solid var(--c-border);
    vertical-align: top;
  }
  tr:last-child td { border-bottom: none; }
  .type-badge {
    display: inline-block; padding: 2px 8px;
    background: #eff6ff; border: 1px solid #bfdbfe;
    border-radius: 4px; font-family: var(--font-mono);
    font-size: 12px; color: #1e40af;
  }
  .flag { font-size: 13px; }
  .flag-yes { color: var(--c-pass); }
  .flag-no { color: var(--c-border); }

  .finding-group { margin-bottom: 20px; }
  .finding-group-title {
    font-size: 13px; font-weight: 700;
    text-transform: uppercase; letter-spacing: 1px;
    color: var(--c-muted); margin-bottom: 8px;
    padding-left: 4px;
  }
  .finding {
    display: grid;
    grid-template-columns: 28px 80px 1fr;
    gap: 8px;
    padding: 8px 12px;
    border-radius: 6px;
    margin-bottom: 4px;
    align-items: start;
    font-size: 14px;
  }
  .finding:nth-child(even) { background: var(--c-bg); }
  .finding-icon { font-size: 16px; text-align: center; padding-top: 1px; }
  .finding-pass .finding-icon { color: var(--c-pass); }
  .finding-fail .finding-icon { color: var(--c-critical); }
  .sev {
    display: inline-block; padding: 2px 8px;
    border-radius: 4px; font-size: 11px;
    font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.5px; text-align: center;
  }
  .sev-critical { background: #fee2e2; color: var(--c-critical); }
  .sev-warning { background: #fef3c7; color: var(--c-warning); }
  .sev-info { background: #f3f4f6; color: var(--c-info); }
  .finding-details { font-size: 12px; color: var(--c-muted); font-family: var(--font-mono); margin-top: 2px; }

  footer {
    margin-top: 32px; padding: 20px 0;
    border-top: 1px solid var(--c-border);
    text-align: center;
    font-size: 12px; color: var(--c-muted);
  }
  footer strong { color: var(--c-brand); }

  @media print {
    body { font-size: 12px; }
    header { padding: 16px 0; }
    .card { break-inside: avoid; box-shadow: none; border: 1px solid #ddd; }
    .container { max-width: 100%; }
  }
</style>
</head>
<body>

<header>
  <div class="container">
    <div class="brand">
      DATAVOW
      <strong>{{ contract.metadata.name }}</strong>
    </div>
    <div class="verdict-badge verdict-{{ verdict_class }}">
      <span class="emoji">{{ result.verdict.emoji }}</span>
      {{ result.verdict.value }}
    </div>
  </div>
</header>

<div class="container">

  <section>
    <div class="card">
      <h2>Vow Score</h2>
      <div class="score-grid">
        <div class="score-circle {{ verdict_class }}">
          <div class="score-number {{ verdict_class }}">{{ result.score }}</div>
          <div class="score-label">out of 100</div>
        </div>
        <div class="counters">
          <div class="counter">
            <div class="counter-value pass">{{ result.passed_count }}</div>
            <div class="counter-label">Passed</div>
          </div>
          <div class="counter">
            <div class="counter-value critical">{{ result.critical_count }}</div>
            <div class="counter-label">Critical</div>
          </div>
          <div class="counter">
            <div class="counter-value warning">{{ result.warning_count }}</div>
            <div class="counter-label">Warning</div>
          </div>
          <div class="counter">
            <div class="counter-value info">{{ result.info_count }}</div>
            <div class="counter-label">Info</div>
          </div>
        </div>
      </div>
    </div>
  </section>

  <section>
    <div class="card">
      <h2>Contract</h2>
      <div class="meta-grid">
        <div class="meta-item">
          <label>Name</label>
          <span>{{ contract.metadata.name }}</span>
        </div>
        <div class="meta-item">
          <label>Version</label>
          <span>{{ contract.metadata.version }}</span>
        </div>
        <div class="meta-item">
          <label>Owner</label>
          <span>{{ contract.metadata.owner or "—" }}</span>
        </div>
        <div class="meta-item">
          <label>Domain</label>
          <span>{{ contract.metadata.domain or "—" }}</span>
        </div>
      </div>
      {% if contract.metadata.description %}
      <p style="margin-top: 12px; color: var(--c-muted);">{{ contract.metadata.description }}</p>
      {% endif %}
      {% if contract.metadata.tags %}
      <div style="margin-top: 10px;">
        {% for tag in contract.metadata.tags %}
        <span class="tag{% if tag == 'pii' %} tag-pii{% endif %}">{{ tag }}</span>
        {% endfor %}
      </div>
      {% endif %}
      <div style="margin-top: 12px;">
        <div class="meta-item">
          <label>Source</label>
          <span style="font-family: var(--font-mono); font-size: 13px;">{{ result.source_path }}</span>
        </div>
      </div>
    </div>
  </section>

  <section>
    <div class="card">
      <h2>Schema — {{ contract.schema_.fields | length }} fields</h2>
      <table>
        <thead>
          <tr>
            <th>Field</th>
            <th>Type</th>
            <th>Required</th>
            <th>Unique</th>
            <th>PII</th>
            <th>Constraints</th>
          </tr>
        </thead>
        <tbody>
          {% for field in contract.schema_.fields %}
          <tr>
            <td><strong>{{ field.name }}</strong></td>
            <td><span class="type-badge">{{ field.type.value }}</span></td>
            <td class="flag {% if field.required %}flag-yes{% else %}flag-no{% endif %}">{{ "✓" if field.required else "—" }}</td>
            <td class="flag {% if field.unique %}flag-yes{% else %}flag-no{% endif %}">{{ "✓" if field.unique else "—" }}</td>
            <td>{% if field.pii %}<span class="tag tag-pii">PII</span>{% else %}<span class="flag flag-no">—</span>{% endif %}</td>
            <td style="font-size: 12px; color: var(--c-muted);">
              {% set constraints = [] %}
              {% if field.pattern %}{% set _ = constraints.append("pattern: " ~ field.pattern) %}{% endif %}
              {% if field.min is not none %}{% set _ = constraints.append("min: " ~ field.min) %}{% endif %}
              {% if field.max is not none %}{% set _ = constraints.append("max: " ~ field.max) %}{% endif %}
              {% if field.allowed_values %}{% set _ = constraints.append("values: " ~ field.allowed_values | join(", ")) %}{% endif %}
              {{ constraints | join(" · ") if constraints else "—" }}
            </td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </section>

  <section>
    <div class="card">
      <h2>Findings — {{ result.findings | length }} checks</h2>

      {% if failed_findings %}
      <div class="finding-group">
        <div class="finding-group-title">Failures ({{ failed_findings | length }})</div>
        {% for f in failed_findings %}
        <div class="finding finding-fail">
          <div class="finding-icon">✗</div>
          <div><span class="sev sev-{{ f.severity.value | lower }}">{{ f.severity.value }}</span></div>
          <div>
            <strong>{{ f.rule }}</strong> <span style="color: var(--c-muted);">[{{ f.category }}]</span><br>
            {{ f.message }}
            {% if f.details %}<div class="finding-details">{{ f.details }}</div>{% endif %}
          </div>
        </div>
        {% endfor %}
      </div>
      {% endif %}

      {% if passed_findings %}
      <div class="finding-group">
        <div class="finding-group-title">Passed ({{ passed_findings | length }})</div>
        {% for f in passed_findings %}
        <div class="finding finding-pass">
          <div class="finding-icon">✓</div>
          <div><span class="sev sev-{{ f.severity.value | lower }}">{{ f.severity.value }}</span></div>
          <div>
            <strong>{{ f.rule }}</strong> <span style="color: var(--c-muted);">[{{ f.category }}]</span><br>
            {{ f.message }}
          </div>
        </div>
        {% endfor %}
      </div>
      {% endif %}

    </div>
  </section>

</div>

<footer>
  <div class="container">
    Generated by <strong>DataVow v{{ version }}</strong> on {{ timestamp }}<br>
    <em>A solemn vow on your data. From YAML to verdict.</em>
  </div>
</footer>

</body>
</html>
J2EOF

# ── Markdown template ──
cat > src/datavow/templates/report.md.j2 << 'J2EOF'
# DataVow Report — {{ contract.metadata.name }}

{{ result.verdict.emoji }} **{{ result.verdict.value }}** — {{ result.verdict.description }}

---

## Vow Score: {{ result.score }}/100

| Passed | Critical | Warning | Info |
|:------:|:--------:|:-------:|:----:|
| {{ result.passed_count }} | {{ result.critical_count }} | {{ result.warning_count }} | {{ result.info_count }} |

---

## Contract

| Property | Value |
|----------|-------|
| Name | {{ contract.metadata.name }} |
| Version | {{ contract.metadata.version }} |
| Owner | {{ contract.metadata.owner or "—" }} |
| Domain | {{ contract.metadata.domain or "—" }} |
| Source | `{{ result.source_path }}` |
{% if contract.metadata.description %}| Description | {{ contract.metadata.description }} |
{% endif %}{% if contract.metadata.tags %}| Tags | {{ contract.metadata.tags | join(", ") }} |
{% endif %}

---

## Schema — {{ contract.schema_.fields | length }} fields

| Field | Type | Required | Unique | PII |
|-------|------|:--------:|:------:|:---:|
{% for field in contract.schema_.fields %}| {{ field.name }} | `{{ field.type.value }}` | {{ "✓" if field.required else "—" }} | {{ "✓" if field.unique else "—" }} | {{ "⚠ PII" if field.pii else "—" }} |
{% endfor %}

---

## Findings — {{ result.findings | length }} checks

{% if failed_findings %}### Failures ({{ failed_findings | length }})

| Status | Severity | Rule | Category | Message |
|:------:|----------|------|----------|---------|
{% for f in failed_findings %}| ✗ | {{ f.severity.value }} | {{ f.rule }} | {{ f.category }} | {{ f.message }} |
{% endfor %}
{% endif %}

{% if passed_findings %}### Passed ({{ passed_findings | length }})

| Status | Severity | Rule | Category | Message |
|:------:|----------|------|----------|---------|
{% for f in passed_findings %}| ✓ | {{ f.severity.value }} | {{ f.rule }} | {{ f.category }} | {{ f.message }} |
{% endfor %}
{% endif %}

---

*Generated by DataVow v{{ version }} on {{ timestamp }}*
J2EOF

echo "[3/4] Updating CLI..."

# ── Update cli.py — add report command ──
# We need to add the report command. The cleanest way is to replace the full file.
cat > src/datavow/cli.py << 'PYEOF'
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
PYEOF

echo "[4/4] Creating tests..."

# ── tests/test_reporter.py (NEW) ──
cat > tests/test_reporter.py << 'PYEOF'
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
PYEOF

echo ""
echo "=== Integration complete ==="
echo ""
echo "New files:"
echo "  src/datavow/reporter.py"
echo "  src/datavow/templates/report.html.j2"
echo "  src/datavow/templates/report.md.j2"
echo "  tests/test_reporter.py"
echo ""
echo "Updated files:"
echo "  src/datavow/cli.py  (added report command)"
echo ""
echo "Next steps:"
echo "  1. pytest tests/ -v"
echo "  2. datavow report tests/fixtures/orders_contract.yaml tests/fixtures/orders_dirty.csv"
echo "  3. open orders-report.html"
echo "  4. git add . && git commit -m 'feat: datavow report command (HTML + Markdown)'"

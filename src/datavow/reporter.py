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

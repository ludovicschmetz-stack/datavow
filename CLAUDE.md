# DataVow — Development Guide

## Project layout

```
src/datavow/
├── __init__.py          # version
├── cli.py               # Typer CLI (init, define, validate, report, ci)
├── contract.py          # Pydantic models for YAML contract parsing
├── findings.py          # Finding, ValidationResult, Verdict, scoring
├── validator.py         # Orchestrates schema + quality + freshness checks
├── reporter.py          # Jinja2-based HTML/Markdown report generation
├── connectors/
│   └── file.py          # CSV/Parquet/JSON loader via DuckDB
├── rules/
│   ├── schema.py        # Field existence, type, required, unique, pattern, range
│   ├── quality.py       # SQL, not_null, unique, row_count, range, regex rules
│   └── freshness.py     # SLA freshness checks on timestamp fields
└── templates/
    ├── report.html.j2   # Self-contained HTML report template
    └── report.md.j2     # Markdown report template
```

## Commands

```bash
pip install -e '.[dev]'
pytest tests/ -v          # 77 tests expected
ruff check src/ tests/    # lint
ruff format src/ tests/   # format
```

## Conventions

- Python >=3.12, use modern syntax (`X | Y` unions, `match`, `Annotated`)
- CLI commands in `src/datavow/cli.py` using Typer + Rich
- Contract parsing via Pydantic v2 — all validation in models
- Data validation via DuckDB SQL — no pandas/polars
- Tests use pytest; `tmp_path` and `monkeypatch.chdir` for filesystem tests
- Scoring: `100 - (20×CRITICAL + 5×WARNING + 1×INFO)`, floor 0
- Severity: CRITICAL (blocks CI), WARNING (alerts), INFO (logs)
- Contract format: superset of ODCS v3.1

## Architecture decisions

See `datavow-decisions-log.md` in the project instructions for full decision log (D001–D017).

Key decisions:
- D005: DuckDB as validation engine (reads Parquet/CSV/JSON natively)
- D004: ODCS v3.1 superset (compatible but extended)
- D006: Vow Score formula aligned with Olympus
- D007: Apache 2.0 license (max adoption, SaaS monetization later)

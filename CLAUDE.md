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
├── airflow/             # Airflow operator (v0.4.0)
│   ├── __init__.py      # Lazy import — scheduler must NOT load datavow
│   └── operators/
│       ├── __init__.py  # Re-export DataVowOperator
│       └── datavow_operator.py  # Main operator
└── templates/
    ├── report.html.j2   # Self-contained HTML report template
    └── report.md.j2     # Markdown report template
```

## Commands
```bash
pip install -e '.[dev]'
pytest tests/ -v          # 137+ tests expected (6 freshness failures are known — stale CSV data)
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
- macOS: `sed -i ''` (not `sed -i`), `python3` (not `python`)
- Version source of truth: `pyproject.toml` (hatchling reads it). Both `pyproject.toml` AND `src/datavow/__init__.py` must be bumped together, pyproject.toml FIRST. Forgetting causes PyPI 400 Bad Request.
- Publish: GitHub Actions on `release: published` via OIDC Trusted Publisher (not tag push, not API token)

## Architecture decisions

See `datavow-decisions-log.md` in the project instructions for full decision log (D001–D040).

Key decisions:
- D005: DuckDB as validation engine (reads Parquet/CSV/JSON natively)
- D004: ODCS v3.1 superset (compatible but extended)
- D006: Vow Score formula aligned with Olympus
- D007: Apache 2.0 license (max adoption, SaaS monetization later)
- D020: Version sync discipline — pyproject.toml + __init__.py together
- D040: 30-day distribution moratorium overridden for FundsDLT mission (April 1st)

## Airflow Operator (v0.4.0)

### Context
DataVow is deployed alongside Lakecast (declarative lakehouse framework) at FundsDLT starting April 1st. Lakecast ADR-013/014 define a `type: "datavow"` task that runs as a quality gate between bronze and silver layers via KubernetesPodOperator. The DataVow Airflow operator must work BOTH standalone in any DAG AND as the execution target for Lakecast pipelines.

### Architecture decisions
- Library call (not subprocess) — faster, typed XCom, no stdout parsing
- Lazy imports inside `execute()` only — scheduler node must NOT need datavow installed (critical for K8s executor where only the worker pod has it)
- Packaged as `pip install datavow[airflow]` with `apache-airflow>=2.7` optional dependency
- `fail_on` parameter aligned with `datavow_fail_on` from dbt package: strained (score<95), broken (score<80), shattered (score<50)
- Code lives in `src/datavow/airflow/` (same repo, not a separate package)

### Internal API the operator must use
Read these files BEFORE writing the operator to get the real signatures:
- `src/datavow/validator.py` — `validate(contract_path, source_path) -> ValidationResult`
- `src/datavow/findings.py` — `ValidationResult` (`.score`, `.verdict`, `.findings`, `.contract_name`), `Finding` (`.passed`, `.severity`, `.rule`, `.category`, `.message`), `Verdict` enum, `Severity` enum
- `src/datavow/reporter.py` — `generate_report()` — check actual signature

### Operator spec
- Class: `DataVowOperator(BaseOperator)`
- Params: `contract_path`, `data_path`, `on_failure="fail"|"warn"|"skip"`, `fail_on="strained"|"broken"|"shattered"`, `report_format=None`, `report_path=None`
- `template_fields`: contract_path, data_path, report_path
- `on_failure="fail"` → raise AirflowException
- `on_failure="warn"` → log warning, task succeeds
- `on_failure="skip"` → raise AirflowSkipException
- XCom outputs: vow_score, vow_verdict, violations_critical, violations_warning, violations_info, contract_name, report_path

### Test strategy
- Mock Airflow modules in `sys.modules` (airflow not installed in test env)
- Mock `datavow.validator`, `datavow.findings`, `datavow.reporter` in `sys.modules` for isolation
- DO NOT mock the `datavow` package itself — it must remain a real package for Python import traversal to `datavow.airflow.operators`
- 27 tests: TestParams (7), TestVowKept (2), TestFail (5), TestWarn (2), TestSkip (2), TestCounting (2), TestReport (3), TestEdge (4)
```

C'est prêt. Ensuite dans Claude Code, ton prompt est simplement :
```
Implement the DataVow Airflow Operator as described in CLAUDE.md section "Airflow Operator (v0.4.0)". Read validator.py, findings.py, reporter.py and pyproject.toml first. Bump version to 0.4.0. Target 27/27 tests green + ruff clean.
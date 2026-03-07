<div align="center">

# DataVow

**A solemn vow on your data. From YAML to verdict.**

Data contract enforcement for modern data teams.
Define contracts in YAML. Validate anywhere. Block in CI. Report for stakeholders.

[![PyPI](https://img.shields.io/pypi/v/datavow)](https://pypi.org/project/datavow/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://python.org)
[![Tests](https://img.shields.io/badge/tests-117%20passed-brightgreen.svg)](#)
[![GitHub Action](https://img.shields.io/badge/GitHub_Action-Marketplace-blue?logo=github)](https://github.com/marketplace/actions/datavow-data-contract-validation)

</div>

---

## Why DataVow?

89% of data teams report pain points with data modeling and ownership. Data contracts are the answer — but the tooling is fragmented:

- **dbt tests**: SQL-only, no formal contract, no pre-ingestion validation
- **Great Expectations**: verbose Python, steep learning curve
- **Soda**: good YAML checks, but no CI-native workflow or stakeholder reporting
- **ODCS v3.1**: promising standard, but no complete implementation

**DataVow fills the gap**: one tool from contract definition to validation, CI blocking, and human-readable reports. Built on [ODCS v3.1](https://bitol.io/open-data-contract-standard/) and powered by [DuckDB](https://duckdb.org/).

Works with **every warehouse**: Snowflake, BigQuery, Redshift, SQL Server, PostgreSQL, DuckDB, Databricks — via native dbt integration.

## Install

```bash
pip install datavow
```

## Quick start

### Standalone (CSV, Parquet, JSON)

```bash
datavow init my-project
datavow validate contracts/orders.yaml data/orders.csv
datavow report contracts/orders.yaml data/orders.csv
datavow ci contracts/ data/
```

### With dbt (any warehouse)

```bash
# Generate contracts from your dbt models
datavow dbt generate --manifest target/manifest.json

# Sync contracts → dbt-native tests
datavow dbt sync --contracts contracts/

# Run tests in your warehouse
dbt test --select tag:datavow

# Or run the full pipeline in one command
datavow dbt ci --contracts contracts/ --dbt-project .
```

### In GitHub Actions

```yaml
- uses: ludovicschmetz-stack/datavow-action@v1
  with:
    contracts: contracts/
    source: data/
```

## Commands

### Core

| Command | Description |
|---------|-------------|
| `datavow init` | Scaffold a new project with config and example contract |
| `datavow define <contract>` | Validate contract syntax, display structure |
| `datavow validate <contract> <source>` | Validate data against a contract |
| `datavow report <contract> <source>` | Generate HTML or Markdown report |
| `datavow ci <contracts_dir> <sources_dir>` | Batch validate, exit 1 on failures |

### dbt integration

| Command | Description |
|---------|-------------|
| `datavow dbt generate` | Auto-generate contracts from `manifest.json` |
| `datavow dbt validate` | Validate models via direct warehouse connection |
| `datavow dbt sync` | Generate dbt-native tests from contracts |
| `datavow dbt ci` | Full pipeline: sync → dbt test → Vow Score |

## dbt integration

DataVow integrates natively with dbt. Three ways to use it:

### 1. Generate contracts from dbt models

```bash
datavow dbt generate --manifest target/manifest.json --output contracts/
```

Reads your `manifest.json` and creates DataVow contracts with:
- Column names, types, and descriptions from your schema
- `not_null`, `unique`, `accepted_values` tests auto-mapped to quality rules
- PII flags from column meta/tags
- Domain extracted from model meta or schema name

### 2. Sync contracts to dbt tests

```bash
datavow dbt sync --contracts contracts/ --dbt-project .
```

Converts DataVow rules into dbt-native tests:
- **Generic tests** (schema.yml): `not_null`, `unique`, `accepted_values`
- **Singular tests** (SQL files): custom SQL, `row_count`, `range`, `regex`

All generated tests are tagged `datavow`. Run them with:

```bash
dbt test --select tag:datavow
```

This works with **every dbt adapter** — Snowflake, BigQuery, Redshift, SQL Server, PostgreSQL, DuckDB, Databricks.

### 3. On-run-end hook

Install the [datavow-dbt](https://github.com/ludovicschmetz-stack/datavow-dbt) package:

```yaml
# packages.yml
packages:
  - git: "https://github.com/ludovicschmetz-stack/datavow-dbt"
    revision: v1.0.0
```

```yaml
# dbt_project.yml
on-run-end:
  - "{{ datavow.datavow_summary(results) }}"
```

After `dbt build`, you get:

```
╔══════════════════════════════════════════════════╗
║  DataVow — A solemn vow on your data            ║
╠══════════════════════════════════════════════════╣
║  ❌ Vow Shattered — Score: 0/100                ║
║  Passed: 15  Failed: 11  Warned: 2  Total: 28  ║
╚══════════════════════════════════════════════════╝
```

Pipeline blocked on failures. Configure with `datavow_fail_on: 'none'` to allow.

### 4. Full CI pipeline

```bash
datavow dbt ci --contracts contracts/ --dbt-project .
```

One command: syncs contracts, runs `dbt test`, reports Vow Score, exits 1 on failure.

## GitHub Action

Available on the [GitHub Marketplace](https://github.com/marketplace/actions/datavow-data-contract-validation).

```yaml
- uses: ludovicschmetz-stack/datavow-action@v1
  id: datavow
  with:
    contracts: contracts/
    source: data/
    fail-on: critical
    generate-report: "true"
    comment-on-pr: "true"
```

Features: pip caching, HTML report artifacts, PR comments with Vow Score, configurable fail threshold.

## Contract format

DataVow contracts are a superset of [ODCS v3.1](https://bitol.io/open-data-contract-standard/) — compatible but extended with severity, SLA, and PII flags.

```yaml
apiVersion: datavow/v1
kind: DataContract
metadata:
  name: orders
  version: 1.0.0
  owner: data-team@company.com
  domain: sales
  description: "Customer orders from the e-commerce platform"
  tags: [pii, financial, critical]

schema:
  type: table
  fields:
    - name: order_id
      type: integer
      required: true
      unique: true
    - name: customer_email
      type: string
      required: true
      pii: true
      pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
    - name: total_amount
      type: decimal
      required: true
      min: 0
    - name: status
      type: string
      required: true
      allowed_values: [confirmed, shipped, delivered, cancelled]
    - name: created_at
      type: timestamp
      required: true

quality:
  rules:
    - name: no_negative_totals
      type: sql
      query: "SELECT COUNT(*) FROM {table} WHERE total_amount < 0"
      threshold: 0
      severity: CRITICAL
    - name: email_not_null
      type: not_null
      field: customer_email
      severity: CRITICAL
    - name: daily_volume
      type: row_count
      min: 1000
      max: 100000
      severity: WARNING

sla:
  freshness: 24h
  completeness: "99.5%"
```

### Supported quality rule types

| Type | Description | Required fields |
|------|-------------|-----------------|
| `sql` | Custom SQL query returning a count | `query`, `threshold` |
| `not_null` | Field has no nulls | `field` |
| `unique` | Field values are unique | `field` |
| `row_count` | Row count within bounds | `min`, `max` |
| `range` | Field values within bounds | `field`, `min_value`, `max_value` |
| `accepted_values` | Field values in allowed set | `field`, `values` |
| `regex` | Field values match pattern | `field`, `pattern` |

## Vow Score

```
Score = 100 - (20 × CRITICAL + 5 × WARNING + 1 × INFO)

95-100  ✅ Vow Kept       — fully compliant
80-94   ⚠️ Vow Strained   — action needed
50-79   🔧 Vow Broken     — blocking issues
0-49    ❌ Vow Shattered   — critical violations
```

## Data sources

### File-based (via DuckDB)

CSV, Parquet, JSON, JSONL, TSV — zero config, just point to the file.

### Database (via dbt sync)

Any warehouse supported by dbt: Snowflake, BigQuery, Redshift, SQL Server, PostgreSQL, DuckDB, Databricks, Spark, Trino.

DataVow syncs contracts to dbt tests → dbt executes them in your warehouse. No direct database connection needed from DataVow.

### Database (direct connection)

PostgreSQL and DuckDB via `datavow dbt validate --mode direct`. Uses DuckDB ATTACH for zero-dependency connections.

## Data Mesh ready

Contracts are organized by domain. Each contract has a `metadata.domain` field:

```
contracts/
├── sales/
│   ├── orders.yaml
│   └── invoices.yaml
├── logistics/
│   └── shipments.yaml
└── finance/
    └── transactions.yaml
```

## Who is DataVow for?

| Persona | Interface | Usage |
|---------|-----------|-------|
| Data Engineer | CLI + CI/CD | `datavow ci` in the pipeline |
| Analytics Engineer | CLI + dbt | `datavow dbt sync` + `dbt test` |
| Domain Data Owner | YAML contracts | Define and version contracts |
| Data Governance | Reports | Consolidated compliance view |
| Data Analyst | Reports | "Can I trust this table?" |
| Tech Lead | CI gate | No pipeline to prod without a contract |
| Freelance / Consultant | Branded reports | Proof of quality in deliverables |

## Tech stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.12+ |
| CLI | Typer + Rich |
| Contract parsing | Pydantic v2 |
| Data validation | DuckDB |
| Reporting | Jinja2 |
| File formats | CSV, Parquet, JSON, JSONL, TSV |
| dbt integration | manifest.json, profiles.yml, dbt test |
| CI/CD | GitHub Action on Marketplace |

## Development

```bash
git clone https://github.com/ludovicschmetz-stack/datavow.git
cd datavow
uv venv && source .venv/bin/activate
uv pip install -e '.[dev]'
pytest tests/ -v
```

## Roadmap

- [x] **Phase 1 — CLI MVP**: init, define, validate, report, ci
- [x] **Phase 2 — dbt integration**: generate, sync, validate, ci, on-run-end hook
- [x] **Phase 2 — GitHub Action**: Marketplace, PR comments, report artifacts
- [x] **Phase 2 — PyPI**: Trusted Publisher, automated releases
- [ ] **Phase 2 — Notifications**: Slack, Teams, Email
- [ ] **Phase 2 — Airflow**: DataVowValidateOperator
- [ ] **Phase 3 — SaaS**: web dashboard, contract catalogue, role-based access, API

## Pricing

| Tier | Features |
|------|----------|
| **Community** (free, forever) | CLI, all commands, dbt integration, GitHub Action, reports |
| **Team** (coming soon) | Web dashboard, history, alerts, team collaboration |
| **Business** (coming soon) | SSO, audit trail, custom roles, API, unlimited users |

## Ecosystem

| Repo | Description |
|------|-------------|
| [datavow](https://github.com/ludovicschmetz-stack/datavow) | CLI & core engine |
| [datavow-action](https://github.com/ludovicschmetz-stack/datavow-action) | GitHub Action (Marketplace) |
| [datavow-dbt](https://github.com/ludovicschmetz-stack/datavow-dbt) | dbt package (on-run-end hook) |

## License

[Apache 2.0](LICENSE) — free and open source forever. The CLI stays free. Monetization comes from the SaaS (Phase 3).

## Author

Built by [Ludovic Schmetz](https://github.com/ludovicschmetz-stack) — Senior Data Engineer/Architect, Luxembourg. Also the author of [Olympus](https://github.com/ludovicschmetz-stack/olympus).

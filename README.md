<div align="center">

# DataVow

**A solemn vow on your data. From YAML to verdict.**

Data contract enforcement for modern data teams.
Define contracts in YAML. Validate with DuckDB. Block in CI. Report for stakeholders.

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://python.org)
[![Tests](https://img.shields.io/badge/tests-77%20passed-brightgreen.svg)](#)

</div>

---

## Why DataVow?

89% of data teams report pain points with data modeling and ownership. Data contracts are the answer — but the tooling is fragmented:

- **dbt tests**: SQL-only, no formal contract, no pre-ingestion validation
- **Great Expectations**: verbose Python, steep learning curve
- **Soda**: good YAML checks, but no CI-native workflow or stakeholder reporting
- **ODCS v3.1**: promising standard, but no complete implementation

**DataVow fills the gap**: one tool from contract definition to validation, CI blocking, and human-readable reports. Built on [ODCS v3.1](https://bitol.io/open-data-contract-standard/) and powered by [DuckDB](https://duckdb.org/).

## Install

```bash
pip install datavow
```

## Quick start

```bash
# 1. Scaffold a project
datavow init my-project

# 2. Write a contract (or edit the example)
cat contracts/orders.yaml

# 3. Validate data against the contract
datavow validate contracts/orders.yaml data/orders.csv

# 4. Generate a stakeholder report
datavow report contracts/orders.yaml data/orders.csv

# 5. Run in CI — exit 1 on critical failures
datavow ci contracts/ data/
```

## Commands

### `datavow init [project-name]`

Scaffold a new project with a `datavow.yaml` config and a `contracts/` directory.

### `datavow define <contract.yaml>`

Validate a contract's YAML syntax and display its structure — fields, rules, SLA — without needing data.

```
✓ Contract orders is valid

Name              orders
Version           1.0.0
Domain            sales
Owner             data-team@company.com

Schema: 5 fields (5 required, 1 PII)
  • order_id integer (required, unique)
  • customer_email string (required, pii)
  • total_amount decimal (required)
  • status string (required)
  • created_at timestamp (required)

Quality rules: 3
  • no_negative_totals CRITICAL (sql)
  • email_not_null CRITICAL (not_null)
  • daily_volume WARNING (row_count)

SLA: freshness=24h, completeness=99.5%
```

### `datavow validate <contract.yaml> <source>`

Run schema, quality, and freshness checks against a data source (CSV, Parquet, JSON).

```
datavow validate contracts/orders.yaml data/orders.csv --verbose
datavow validate contracts/orders.yaml data/orders.csv --ci        # exit 1 on CRITICAL
datavow validate contracts/orders.yaml data/orders.csv -o json     # JSON output
datavow validate contracts/orders.yaml data/orders.csv -o summary  # one-liner
```

### `datavow report <contract.yaml> <source>`

Generate a self-contained HTML or Markdown report. Share it with stakeholders, attach to deliveries, or publish.

```
datavow report contracts/orders.yaml data/orders.csv                    # HTML (default)
datavow report contracts/orders.yaml data/orders.csv -f md              # Markdown
datavow report contracts/orders.yaml data/orders.csv -o my-report.html  # custom path
```

### `datavow ci <contracts_dir> <sources_dir>`

Batch-validate all contracts against matching data sources. Matches by name convention: `contracts/orders.yaml` → `sources/orders.csv`.

```
datavow ci contracts/ data/                    # exit 1 on CRITICAL
datavow ci contracts/ data/ --fail-on warning  # stricter: fail on WARNING too
```

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
      allowed_values: [pending, confirmed, shipped, delivered, cancelled]
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

### Supported field types

`string`, `integer`, `float`, `decimal`, `boolean`, `date`, `timestamp`

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

### Severity levels

Each rule has a severity: `CRITICAL`, `WARNING`, or `INFO`.

## Vow Score

```
Score = 100 - (20 × CRITICAL + 5 × WARNING + 1 × INFO)

95-100  ✅ Vow Kept       — fully compliant
80-94   ⚠️ Vow Strained   — action needed
50-79   🔧 Vow Broken     — blocking issues
0-49    ❌ Vow Shattered   — critical violations
```

## Data Mesh ready

Contracts are organized by domain. Each contract has a `metadata.domain` field. Structure your repo naturally:

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

## Tech stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.12+ |
| CLI | Typer + Rich |
| Contract parsing | Pydantic v2 |
| Data validation | DuckDB |
| Reporting | Jinja2 |
| Data formats | CSV, Parquet, JSON (via DuckDB) |

## Development

```bash
git clone https://github.com/ludovicschmetz-stack/datavow.git
cd datavow
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
pytest tests/ -v
```

## Roadmap

- [x] **Phase 1 — CLI MVP** (done)
- [ ] **Phase 2 — Integrations**: dbt post-hook, Airflow operator, GitHub Action, PostgreSQL/MySQL via DuckDB, Slack/Teams notifications
- [ ] **Phase 3 — SaaS**: web dashboard, contract catalogue, role-based access, API

## License

[Apache 2.0](LICENSE) — free and open source forever. The CLI stays free. Monetization comes from the SaaS (Phase 3).

## Author

Built by [Ludovic Schmetz](https://github.com/ludovicschmetz-stack) — Senior Data Engineer/Architect, Luxembourg. Also the author of [Olympus](https://github.com/ludovicschmetz-stack/olympus).

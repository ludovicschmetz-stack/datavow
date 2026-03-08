<p align="center">
  <img src="https://datavow.io/img/logo.png" alt="DataVow" width="200">
</p>

<h3 align="center">Trust Your Data. Know Why You Can't.</h3>

<p align="center">
  Open-source data contract enforcement for modern data teams.<br>
  Define contracts in YAML. Sync to dbt. Validate in CI. Block bad data before it reaches production.
</p>

<p align="center">
  <a href="https://pypi.org/project/datavow/"><img src="https://img.shields.io/pypi/v/datavow?color=blue&label=PyPI" alt="PyPI"></a>
  <a href="https://pypi.org/project/datavow/"><img src="https://img.shields.io/pypi/pyversions/datavow" alt="Python"></a>
  <a href="https://github.com/ludovicschmetz-stack/datavow/actions"><img src="https://github.com/ludovicschmetz-stack/datavow/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/ludovicschmetz-stack/datavow/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-green" alt="License"></a>
  <a href="https://github.com/marketplace/actions/datavow-data-contract-validation"><img src="https://img.shields.io/badge/GitHub%20Action-Marketplace-blue?logo=github" alt="GitHub Action"></a>
</p>

---

## The problem

89% of data teams report pain points with data modeling and ownership. Data contracts are the solution — but the tooling is fragmented:

- **dbt tests** → SQL only, no formal contract, no pre-ingestion validation
- **Great Expectations** → verbose Python, steep learning curve, no standard format
- **Soda** → good YAML checks, but no CI/CD gate, no stakeholder reporting, no ODCS
- **Data Contract CLI** → ODCS compatible, but no dbt sync, no scoring, no CI gate

**DataVow covers the full lifecycle: define → sync dbt → validate → block → report.** One tool. One standard.

## Quick start

```bash
pip install datavow

# Initialize a project
datavow init my-project

# Define a contract
datavow define contracts/orders.yaml

# Validate data against contracts
datavow validate contracts/orders.yaml --source data/orders.csv

# Generate an HTML report
datavow report contracts/orders.yaml --source data/orders.csv --format html

# Run in CI mode (exit code 1 on critical violations)
datavow ci contracts/ --source data/
```

## Key features

### YAML-first contracts (ODCS v3.1 native)

Define schemas, quality rules, and SLAs in readable YAML. DataVow supports **both** its own format and native ODCS v3.1 contracts — auto-detected, no config needed.

```yaml
apiVersion: datavow/v1
kind: DataContract
metadata:
  name: orders
  version: 1.0.0
  owner: data-team@company.com
  domain: sales

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

quality:
  rules:
    - name: no_negative_totals
      type: sql
      query: "SELECT COUNT(*) FROM {table} WHERE total_amount < 0"
      threshold: 0
      severity: CRITICAL
```

### `datavow dbt sync` — the killer feature

One command generates dbt-native tests from your contracts. Works on **every** dbt adapter — no connector needed.

```bash
# Generate dbt tests from contracts
datavow dbt sync contracts/ --dbt-project-dir .

# 3 contracts → 28 tests generated (generic + singular)
# All tagged `datavow` for easy filtering
```

### Vow Score — every validation renders a verdict

```
Vow Score = 100 - (20 × CRITICAL + 5 × WARNING + 1 × INFO)

  95-100  ✅ Vow Kept      — fully compliant, ship it
  80-94   ⚠️ Vow Strained  — action needed
  50-79   🔧 Vow Broken    — blocking issues
   0-49   ❌ Vow Shattered  — critical violations
```

### CI pipeline gating

Block bad data automatically. No manual intervention.

**GitHub Action** ([Marketplace](https://github.com/marketplace/actions/datavow-data-contract-validation)):

```yaml
- uses: ludovicschmetz-stack/datavow-action@v1
  with:
    contracts: contracts/
    source: data/
    fail-on: critical
    comment-on-pr: "true"
```

**dbt on-run-end hook** ([datavow-dbt](https://github.com/ludovicschmetz-stack/datavow-dbt)):

```yaml
# dbt_project.yml
on-run-end:
  - "{{ datavow_summary() }}"

vars:
  datavow_fail_on: broken  # block pipeline on Vow Broken or worse
```

### ODCS v3.1 — validate against the official standard

```bash
# Validate a contract against the ODCS v3.1 JSON Schema
datavow odcs check contracts/orders.yaml

# Convert ODCS native → DataVow format
datavow odcs convert contracts/orders-odcs.yaml -o contracts/orders.yaml
```

DataVow bundles the official ODCS v3.1.0 JSON Schema (2928 lines, Draft 2019-09). No other CLI tool does this.

## Full command reference

| Command | Description |
|---|---|
| `datavow init` | Initialize project with config and example contract |
| `datavow define` | Create or edit a data contract interactively |
| `datavow validate` | Validate data against contracts |
| `datavow report` | Generate HTML or Markdown reports |
| `datavow ci` | CI mode — validate + exit code 0/1 |
| `datavow dbt generate` | Auto-generate contracts from dbt manifest |
| `datavow dbt validate` | Validate against dbt warehouse (via profiles.yml) |
| `datavow dbt sync` | Generate dbt tests from contracts |
| `datavow dbt ci` | Full pipeline: sync → dbt test → Vow Score |
| `datavow odcs check` | Validate contract against ODCS v3.1 JSON Schema |
| `datavow odcs convert` | Convert ODCS native → DataVow format |

## Data sources

DataVow validates files and databases via DuckDB:

| Source | How |
|---|---|
| CSV, Parquet, JSON, TSV | Direct file validation |
| PostgreSQL | `datavow validate --source postgresql://...` |
| DuckDB | `datavow validate --source path/to/db.duckdb` |
| Snowflake, BigQuery, Redshift, SQL Server | `pip install datavow[snowflake]` (via DuckDB ATTACH) |

## Built for your whole team

| Persona | Uses | Gets |
|---|---|---|
| **Data Engineer** | `datavow ci` in pipeline | Automated quality gate |
| **Analytics Engineer** | `datavow dbt sync` | One source of truth, zero test duplication |
| **Domain Data Owner** | YAML contracts in git | Versioned, reviewable data agreements |
| **Data Governance** | HTML reports | Conformity view across domains |
| **Tech Lead** | CI gate + Vow Score | No pipeline in prod without a contract |
| **Freelance / Consultant** | `datavow report` | Quality proof attached to every delivery |

## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│  YAML       │    │   DataVow    │    │   Outputs   │
│  Contracts  │───▶│   Engine     │───▶│             │
│  (ODCS/DV)  │    │   (DuckDB)   │    │  ✅ Score   │
└─────────────┘    └──────┬───────┘    │  📊 Report  │
                          │            │  🚦 Exit 1  │
              ┌───────────┼──────┐     └─────────────┘
              ▼           ▼      ▼
          CSV/Parquet  PostgreSQL  dbt
```

## Ecosystem

| Package | Description | Version |
|---|---|---|
| [`datavow`](https://pypi.org/project/datavow/) | CLI — define, validate, report, CI | v0.3.0 |
| [`datavow-action`](https://github.com/marketplace/actions/datavow-data-contract-validation) | GitHub Action — CI gate | v1.0.0 |
| [`datavow-dbt`](https://github.com/ludovicschmetz-stack/datavow-dbt) | dbt package — on-run-end Vow Score | v1.0.0 |

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

```bash
# Development setup
git clone https://github.com/ludovicschmetz-stack/datavow.git
cd datavow
python -m venv .venv && source .venv/bin/activate
uv pip install -e ".[dev]"
pytest  # 137 tests
```

## License

[Apache 2.0](LICENSE) — free forever. Use it, fork it, ship it.

---

<p align="center">
  <a href="https://datavow.io">Website</a> · 
  <a href="https://datavow.io/docs">Documentation</a> · 
  <a href="https://pypi.org/project/datavow/">PyPI</a> · 
  <a href="https://github.com/ludovicschmetz-stack/datavow/issues">Issues</a>
</p>

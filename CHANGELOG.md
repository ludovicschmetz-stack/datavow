# Changelog

All notable changes to DataVow will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-03-07

### Added

- **dbt integration**: `datavow dbt generate` — auto-generate contracts from dbt `manifest.json`
- **dbt integration**: `datavow dbt validate` — validate dbt models against contracts via warehouse connection
- **Database connector**: PostgreSQL support via DuckDB ATTACH (zero extra dependencies)
- **Database connector**: DuckDB file support for dbt-duckdb projects
- **Contract generation**: dbt tests (not_null, unique, accepted_values) auto-mapped to DataVow quality rules
- **Type mapping**: 30+ warehouse column types (PostgreSQL, Snowflake, BigQuery, etc.) mapped to DataVow types
- **Profiles parsing**: reads dbt `profiles.yml` for database connection info
- **Domain extraction**: auto-detects domain from dbt model meta or schema name
- **PII detection**: honors `pii` tag and meta from dbt column definitions
- **Optional extras**: `pip install datavow[snowflake]`, `datavow[bigquery]`, `datavow[redshift]`, `datavow[sqlserver]`, `datavow[all-warehouses]`
- **21 new tests** for dbt connector (manifest parsing, contract generation, profiles, type mapping)

### Changed

- Version bumped to 0.2.0
- `pyproject.toml`: added `dbt` keyword, optional database extras
- Validator refactored to support both file and database sources

## [0.1.0] - 2026-03-06

### Added

- **CLI commands**: `init`, `define`, `validate`, `report`, `ci`
- **Contract format**: YAML-based, ODCS v3.1 superset with metadata, schema, quality rules, SLA
- **Validation engine**: DuckDB-based validation for schema, quality, and freshness checks
- **Rule types**: `not_null`, `unique`, `row_count`, `range`, `pattern`, `sql`, `freshness`
- **Severity levels**: CRITICAL (blocks), WARNING (alerts), INFO (logs)
- **Vow Score**: 0-100 scoring with verdicts (Kept / Strained / Broken / Shattered)
- **Reports**: HTML (self-contained, branded) and Markdown output
- **CI mode**: `datavow ci` with exit code 0/1 for pipeline integration
- **Data sources**: CSV, Parquet, JSON, JSONL, TSV via DuckDB
- **GitHub Actions CI**: automated tests on Python 3.12 and 3.13

[0.1.0]: https://github.com/ludovicschmetz-stack/datavow/releases/tag/v0.1.0

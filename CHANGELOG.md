# Changelog

All notable changes to DataVow will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

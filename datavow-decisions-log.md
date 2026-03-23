# DataVow — Decision Log

## D026: Airflow Operator — DELIVERED

| Field | Value |
|---|---|
| Status | **DELIVERED** |
| Date | 2026-03-23 |
| Version | v0.4.0 |
| Tests | 27 new (164 total) |

DataVowOperator ships as `pip install datavow[airflow]`. Supports `on_failure` modes (fail/warn/skip), `fail_on` thresholds (strained/broken/shattered), lazy imports for K8s executor compatibility, and XCom outputs for downstream tasks.

## D041: Override moratorium D040 for FundsDLT — Airflow operator only

| Field | Value |
|---|---|
| Status | **ACTIVE** |
| Date | 2026-03-23 |
| Scope | Airflow operator (D026) only |

The 30-day distribution moratorium (D040) is overridden **exclusively** for the Airflow operator release (v0.4.0) to support the FundsDLT mission starting April 1st. All other aspects of the moratorium remain active — no other new features or distribution changes are permitted until the moratorium expires.

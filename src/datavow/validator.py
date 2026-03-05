"""DataVow validator — orchestrates schema, quality, and freshness checks."""

from __future__ import annotations

from pathlib import Path

import duckdb

from datavow.connectors.file import load_file
from datavow.contract import DataContract
from datavow.findings import ValidationResult
from datavow.rules.freshness import validate_freshness
from datavow.rules.quality import validate_quality
from datavow.rules.schema import validate_schema


def validate(
    contract_path: str | Path,
    source_path: str | Path,
) -> ValidationResult:
    """Run full validation: load contract, load data, execute all rules."""
    contract = DataContract.from_yaml(contract_path)
    con = duckdb.connect(":memory:")
    table = load_file(con, source_path)

    result = ValidationResult(
        contract_name=contract.metadata.name,
        source_path=str(source_path),
    )

    for finding in validate_schema(con, table, contract):
        result.add(finding)
    for finding in validate_quality(con, table, contract):
        result.add(finding)
    for finding in validate_freshness(con, table, contract):
        result.add(finding)

    con.close()
    return result

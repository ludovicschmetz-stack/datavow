"""File connector — loads Parquet, CSV, JSON into DuckDB."""

from __future__ import annotations

from pathlib import Path

import duckdb

TABLE_NAME = "datavow_source"

_SUPPORTED_EXTENSIONS = {".csv", ".parquet", ".json", ".jsonl", ".ndjson", ".tsv"}


def load_file(
    con: duckdb.DuckDBPyConnection,
    source_path: str | Path,
) -> str:
    """Load a file into DuckDB and return the table name."""
    path = Path(source_path)
    if not path.exists():
        raise FileNotFoundError(f"Data source not found: {path}")

    ext = path.suffix.lower()
    if ext not in _SUPPORTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported file format: {ext}. Supported: {', '.join(sorted(_SUPPORTED_EXTENSIONS))}"
        )

    escaped_path = str(path).replace("'", "''")
    con.execute(f"CREATE OR REPLACE TABLE {TABLE_NAME} AS SELECT * FROM '{escaped_path}'")

    row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    if row_count == 0:
        raise ValueError(f"Source file is empty: {path}")

    return TABLE_NAME

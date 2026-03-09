"""Database connector — loads warehouse tables into DuckDB for validation.

Uses DuckDB ATTACH for PostgreSQL (zero extra deps).
DuckDB adapter connects directly to the .duckdb file.
Other adapters should use 'datavow dbt sync' to validate via dbt.
"""

from __future__ import annotations

import duckdb

from datavow.connectors.dbt import DbtConnectionInfo
from datavow.connectors.file import TABLE_NAME


def load_database_table(
    con: duckdb.DuckDBPyConnection,
    connection_info: DbtConnectionInfo,
    table_ref: str,
    limit: int | None = None,
) -> str:
    """Load a table from a database into DuckDB for validation.

    Args:
        con: DuckDB connection (in-memory).
        connection_info: Database connection details from dbt profiles.yml.
        table_ref: Fully qualified table reference (e.g. "schema.table_name").
        limit: Optional row limit for sampling large tables.

    Returns:
        Local table name in DuckDB (TABLE_NAME constant).
    """
    if connection_info.adapter == "postgres":
        return _load_postgres(con, connection_info, table_ref, limit)
    elif connection_info.adapter == "duckdb":
        return _load_duckdb(con, connection_info, table_ref, limit)
    else:
        raise ValueError(
            f"Adapter '{connection_info.adapter}' not yet supported for direct validation. "
            f"Supported: postgres, duckdb. "
            f"For cloud warehouses, use 'datavow dbt sync' instead — it generates dbt-native tests that run on your existing dbt adapter."
        )


def _load_postgres(
    con: duckdb.DuckDBPyConnection,
    info: DbtConnectionInfo,
    table_ref: str,
    limit: int | None,
) -> str:
    """Load a PostgreSQL table via DuckDB ATTACH (requires duckdb postgres extension)."""
    attach_str = info.duckdb_attach_string
    db_alias = "pg_source"

    # Install and load the postgres extension
    con.execute("INSTALL postgres; LOAD postgres;")

    # Attach the PostgreSQL database
    con.execute(f"ATTACH '{attach_str}' AS {db_alias} (TYPE POSTGRES, READ_ONLY);")

    # Resolve table reference: could be "schema.table" or just "table"
    parts = table_ref.split(".")
    if len(parts) == 3:
        # database.schema.table — skip database since we attached it
        qualified = f"{db_alias}.{parts[1]}.{parts[2]}"
    elif len(parts) == 2:
        qualified = f"{db_alias}.{parts[0]}.{parts[1]}"
    else:
        qualified = f"{db_alias}.{info.schema}.{table_ref}"

    limit_clause = f" LIMIT {limit}" if limit else ""
    con.execute(f"CREATE OR REPLACE TABLE {TABLE_NAME} AS SELECT * FROM {qualified}{limit_clause}")

    row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    if row_count == 0:
        raise ValueError(f"Table '{table_ref}' is empty in PostgreSQL")

    # Detach after copying
    con.execute(f"DETACH {db_alias};")

    return TABLE_NAME


def _load_duckdb(
    con: duckdb.DuckDBPyConnection,
    info: DbtConnectionInfo,
    table_ref: str,
    limit: int | None,
) -> str:
    """Load a table from another DuckDB database file."""
    db_path = info.extra.get("path", info.database)
    if not db_path or db_path == ":memory:":
        raise ValueError(
            "DuckDB adapter requires a file path (not :memory:) for validation. "
            "Set 'path' in your dbt profiles.yml."
        )

    db_alias = "duckdb_source"
    con.execute(f"ATTACH '{db_path}' AS {db_alias} (READ_ONLY);")

    parts = table_ref.split(".")
    if len(parts) == 3:
        qualified = f"{db_alias}.{parts[1]}.{parts[2]}"
    elif len(parts) == 2:
        qualified = f"{db_alias}.{parts[0]}.{parts[1]}"
    else:
        qualified = f"{db_alias}.{info.schema}.{table_ref}"

    limit_clause = f" LIMIT {limit}" if limit else ""
    con.execute(f"CREATE OR REPLACE TABLE {TABLE_NAME} AS SELECT * FROM {qualified}{limit_clause}")

    row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    if row_count == 0:
        raise ValueError(f"Table '{table_ref}' is empty in DuckDB at {db_path}")

    con.execute(f"DETACH {db_alias};")

    return TABLE_NAME

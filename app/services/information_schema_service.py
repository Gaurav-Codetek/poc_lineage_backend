from typing import Any

from app.db.databricks import fetch_all, fetch_one


class EntityNotFoundError(LookupError):
    """Raised when a requested Information Schema entity does not exist."""


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _quote_sql_identifier(value: str) -> str:
    if not value:
        raise ValueError("Catalog name cannot be empty.")
    return f"`{value.replace('`', '``')}`"


def _load_catalog_row(catalog: str) -> dict[str, Any] | None:
    catalog_escaped = _escape_sql_literal(catalog)
    query = f"""
        SELECT
            catalog_name,
            catalog_owner,
            created AS created_at,
            created_by,
            last_altered AS last_altered_at,
            last_altered_by
        FROM system.information_schema.catalogs
        WHERE catalog_name = '{catalog_escaped}'
        LIMIT 1
    """
    return fetch_one(query)


def _load_schema_row(catalog: str, schema: str) -> dict[str, Any] | None:
    catalog_escaped = _escape_sql_literal(catalog)
    schema_escaped = _escape_sql_literal(schema)
    query = f"""
        SELECT
            catalog_name,
            schema_name,
            schema_owner,
            created AS created_at,
            last_altered AS last_altered_at,
            last_altered_by
        FROM system.information_schema.schemata
        WHERE catalog_name = '{catalog_escaped}'
          AND schema_name = '{schema_escaped}'
        LIMIT 1
    """
    return fetch_one(query)


def _load_table_row(catalog: str, schema: str, table: str) -> dict[str, Any] | None:
    catalog_escaped = _escape_sql_literal(catalog)
    schema_escaped = _escape_sql_literal(schema)
    table_escaped = _escape_sql_literal(table)
    query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            comment,
            created AS created_at,
            created_by
        FROM system.information_schema.tables
        WHERE table_catalog = '{catalog_escaped}'
          AND table_schema = '{schema_escaped}'
          AND table_name = '{table_escaped}'
        LIMIT 1
    """
    return fetch_one(query)


def load_catalog_metadata(catalog: str) -> dict[str, Any]:
    catalog_row = _load_catalog_row(catalog)
    if catalog_row is None:
        raise EntityNotFoundError(f"Catalog '{catalog}' not found.")

    catalog_escaped = _escape_sql_literal(catalog)
    schemas_query = f"""
        SELECT
            schema_name,
            schema_owner,
            created AS created_at,
            last_altered AS last_altered_at,
            last_altered_by
        FROM system.information_schema.schemata
        WHERE catalog_name = '{catalog_escaped}'
        ORDER BY schema_name
    """
    schema_rows = fetch_all(schemas_query)

    return {
        "catalog": catalog_row,
        "schema_count": len(schema_rows),
        "schemas": schema_rows,
    }


def load_schema_metadata(catalog: str, schema: str) -> dict[str, Any]:
    schema_row = _load_schema_row(catalog, schema)
    if schema_row is None:
        raise EntityNotFoundError(f"Schema '{catalog}.{schema}' not found.")

    quoted_catalog = _quote_sql_identifier(catalog)
    schema_escaped = _escape_sql_literal(schema)
    tables_query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            table_type,
            table_owner,
            comment,
            created AS created_at,
            created_by
        FROM {quoted_catalog}.information_schema.tables
        WHERE table_schema = '{schema_escaped}'
        ORDER BY table_name
    """
    table_rows = fetch_all(tables_query)

    return {
        "schema": schema_row,
        "table_count": len(table_rows),
        "tables": table_rows,
    }


def load_table_metadata(catalog: str, schema: str, table: str) -> dict[str, Any]:
    table_row = _load_table_row(catalog, schema, table)
    if table_row is None:
        raise EntityNotFoundError(f"Table '{catalog}.{schema}.{table}' not found.")

    quoted_catalog = _quote_sql_identifier(catalog)
    schema_escaped = _escape_sql_literal(schema)
    table_escaped = _escape_sql_literal(table)
    columns_query = f"""
        SELECT
            table_catalog,
            table_schema,
            table_name,
            column_name,
            comment,
            full_data_type,
            data_type
        FROM {quoted_catalog}.information_schema.columns
        WHERE table_schema = '{schema_escaped}'
          AND table_name = '{table_escaped}'
        ORDER BY ordinal_position, column_name
    """
    column_rows = fetch_all(columns_query)

    return {
        "table": table_row,
        "column_count": len(column_rows),
        "columns": column_rows,
    }

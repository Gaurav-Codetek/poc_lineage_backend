from typing import Any

from app.db.databricks import fetch_all

TABLE_SIGNALS_TABLE = "intelliegencedatacatalog.observability_signals.table_observability_signals"
COLUMN_SIGNALS_TABLE = "intelliegencedatacatalog.observability_signals.column_observability_signals"


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _build_is_current_filter(is_current: bool | None) -> str:
    if is_current is None:
        return ""
    return f" AND is_current = {'true' if is_current else 'false'}"


def load_table_health(
    catalog: str, schema: str, table: str, is_current: bool | None = None
) -> list[dict[str, Any]]:
    full_table_name = _escape_sql_literal(f"{catalog}.{schema}.{table}")
    query = f"""
        SELECT *
        FROM {TABLE_SIGNALS_TABLE}
        WHERE full_table_name = '{full_table_name}'
        {_build_is_current_filter(is_current)}
        ORDER BY freshness_timestamp DESC
    """
    return fetch_all(query)


def load_complete_table_health(is_current: bool | None = None) -> list[dict[str, Any]]:
    query = f"""
        SELECT *
        FROM {TABLE_SIGNALS_TABLE}
        WHERE 1 = 1
        {_build_is_current_filter(is_current)}
        ORDER BY freshness_timestamp DESC
    """
    return fetch_all(query)


def load_column_health(
    catalog: str, schema: str, table: str, is_current: bool | None = None
) -> list[dict[str, Any]]:
    full_table_name = _escape_sql_literal(f"{catalog}.{schema}.{table}")
    query = f"""
        SELECT *
        FROM {COLUMN_SIGNALS_TABLE}
        WHERE full_table_name = '{full_table_name}'
        {_build_is_current_filter(is_current)}
    """
    return fetch_all(query)


def load_complete_column_health(is_current: bool | None = None) -> list[dict[str, Any]]:
    query = f"""
        SELECT *
        FROM {COLUMN_SIGNALS_TABLE}
        WHERE 1 = 1
        {_build_is_current_filter(is_current)}
    """
    return fetch_all(query)

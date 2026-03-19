from typing import Any

from app.db.databricks import fetch_all, fetch_one
from app.core.config import CATALOG, databricks_schema


def get_target_table(stats_type: str) -> str:
    return (
        f"{CATALOG}.{databricks_schema}."
        f"complete_{stats_type}_stats"
    )


def fetch_freshness(catalog: str, schema: str, table: str) -> dict[str, Any] | None:
    target_table = get_target_table("freshness")
    query = f"""
        SELECT last_modified_at, latency_hours, current_version, last_documented_at, partition_count
        FROM {target_table}
        WHERE catalog_name = '{catalog}' AND schema_name = '{schema}' AND table_name = '{table}'
        LIMIT 1
    """
    return fetch_one(query)


def fetch_quality(catalog: str, schema: str, table: str) -> list[dict[str, Any]]:
    target_table = get_target_table("quality")
    query = f"""
        SELECT column_name, data_type, null_count, null_percentage, distinct_count, zero_count, total_rows, fingerprint
        FROM {target_table}
        WHERE catalog_name = '{catalog}' AND schema_name = '{schema}' AND table_name = '{table}'
        ORDER BY column_name
    """
    return fetch_all(query)


def fetch_profile(catalog: str, schema: str, table: str) -> list[dict[str, Any]]:
    target_table = get_target_table("profile")
    query = f"""
        SELECT column_name, data_type, row_count, file_size_bytes, distinct_count, mean, min_val, p25, median, p75, max_val
        FROM {target_table}
        WHERE catalog_name = '{catalog}' AND schema_name = '{schema}' AND table_name = '{table}'
        ORDER BY column_name
    """
    return fetch_all(query)

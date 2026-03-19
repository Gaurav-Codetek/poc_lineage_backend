from databricks import sql
from typing import Any
from app.core.config import (
    DATABRICKS_HOST,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_TOKEN
)

class DatabricksQueryError(RuntimeError):
    """Raised when Databricks operations fail."""

def get_db_connection():
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )

def fetch_one(query: str) -> dict[str, Any] | None:
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                row = cursor.fetchone()
                print(row)
                return None if row is None else row.asDict()
    except Exception as exc:
        raise DatabricksQueryError("Databricks query failed") from exc

def fetch_all(query: str) -> list[dict[str, Any]]:
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                print(rows)
                return [row.asDict() for row in rows]
    except Exception as exc:
        raise DatabricksQueryError("Databricks query failed") from exc

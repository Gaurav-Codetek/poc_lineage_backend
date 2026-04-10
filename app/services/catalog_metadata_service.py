import json
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from app.connectors.databricks_connector import DatabricksConnector
from app.core.config import DATABRICKS_CONFIG
from app.extractors.catalog_metadata_extractor import CatalogMetadataExtractor

APP_ROOT = Path(__file__).resolve().parents[1]
CATALOG_HIERARCHY_PATH = APP_ROOT / "data" / "catalog_hierarchy.json"
CATALOG_REFRESH_LOCK = threading.Lock()


def _clean_scalar(value):
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            return value

    return value


def _table_key(catalog_name, schema_name, table_name):
    return f"{catalog_name}.{schema_name}.{table_name}"


def build_catalog_snapshot(tables_df: pd.DataFrame, columns_df: pd.DataFrame) -> dict:
    columns_by_table = {}

    if not columns_df.empty:
        sorted_columns = columns_df.sort_values(
            by=["catalog_name", "schema_name", "table_name", "ordinal_position", "column_name"],
            kind="stable",
        )

        for _, row in sorted_columns.iterrows():
            catalog_name = _clean_scalar(row["catalog_name"])
            schema_name = _clean_scalar(row["schema_name"])
            table_name = _clean_scalar(row["table_name"])
            column_name = _clean_scalar(row["column_name"])

            if not catalog_name or not schema_name or not table_name or not column_name:
                continue

            key = _table_key(catalog_name, schema_name, table_name)
            ordinal_position = _clean_scalar(row["ordinal_position"])
            is_nullable = _clean_scalar(row["is_nullable"])

            columns_by_table.setdefault(key, []).append(
                {
                    "name": column_name,
                    "ordinal_position": int(ordinal_position) if ordinal_position is not None else None,
                    "data_type": _clean_scalar(row["data_type"]),
                    "is_nullable": True if is_nullable == "YES" else False if is_nullable == "NO" else None,
                    "comment": _clean_scalar(row["comment"]),
                }
            )

    hierarchy_map = {}

    if not tables_df.empty:
        sorted_tables = tables_df.sort_values(
            by=["catalog_name", "schema_name", "table_name"],
            kind="stable",
        )

        for _, row in sorted_tables.iterrows():
            catalog_name = _clean_scalar(row["catalog_name"])
            schema_name = _clean_scalar(row["schema_name"])
            table_name = _clean_scalar(row["table_name"])

            if not catalog_name or not schema_name or not table_name:
                continue

            full_name = _table_key(catalog_name, schema_name, table_name)
            schema_tables = hierarchy_map.setdefault(catalog_name, {}).setdefault(schema_name, [])
            schema_tables.append(
                {
                    "name": table_name,
                    "full_name": full_name,
                    "table_type": _clean_scalar(row["table_type"]),
                    "comment": _clean_scalar(row["comment"]),
                    "data_source_format": _clean_scalar(row["data_source_format"]),
                    "storage_path": _clean_scalar(row["storage_path"]),
                    "columns": columns_by_table.get(full_name, []),
                }
            )

    catalogs = []
    schema_count = 0
    table_count = 0
    column_count = 0

    for catalog_name in sorted(hierarchy_map):
        schemas = []
        for schema_name in sorted(hierarchy_map[catalog_name]):
            tables = hierarchy_map[catalog_name][schema_name]
            table_count += len(tables)
            column_count += sum(len(table.get("columns", [])) for table in tables)
            schema_count += 1
            schemas.append(
                {
                    "name": schema_name,
                    "tables": tables,
                }
            )

        catalogs.append(
            {
                "name": catalog_name,
                "schemas": schemas,
            }
        )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "system.information_schema",
        "stats": {
            "catalog_count": len(catalogs),
            "schema_count": schema_count,
            "table_count": table_count,
            "column_count": column_count,
        },
        "catalogs": catalogs,
    }


def write_catalog_snapshot(snapshot: dict, path: Path = CATALOG_HIERARCHY_PATH) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(snapshot, indent=2)

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f"{path.stem}_",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(payload)
        temp_path = Path(handle.name)

    temp_path.replace(path)
    return path


def load_catalog_snapshot(path: Path = CATALOG_HIERARCHY_PATH) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def refresh_catalog_snapshot() -> dict:
    databricks = DatabricksConnector(**DATABRICKS_CONFIG)
    extractor = CatalogMetadataExtractor(databricks)

    tables_df = extractor.get_all_tables()
    columns_df = extractor.get_all_columns()

    snapshot = build_catalog_snapshot(tables_df, columns_df)
    write_catalog_snapshot(snapshot)
    return snapshot


def background_refresh_catalog_snapshot() -> dict | None:
    if CATALOG_REFRESH_LOCK.locked():
        print("Catalog hierarchy refresh already running...")
        return None

    with CATALOG_REFRESH_LOCK:
        print("Starting catalog hierarchy snapshot refresh...")
        snapshot = refresh_catalog_snapshot()
        stats = snapshot.get("stats", {})
        print(
            "Catalog hierarchy snapshot refresh completed. "
            f"catalogs={stats.get('catalog_count', 0)}, "
            f"schemas={stats.get('schema_count', 0)}, "
            f"tables={stats.get('table_count', 0)}, "
            f"columns={stats.get('column_count', 0)}."
        )
        return snapshot

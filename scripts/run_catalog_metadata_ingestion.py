import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.services.catalog_metadata_service import CATALOG_HIERARCHY_PATH, refresh_catalog_snapshot


def run():
    snapshot = refresh_catalog_snapshot()
    stats = snapshot.get("stats", {})

    print(f"Catalog hierarchy snapshot written to {CATALOG_HIERARCHY_PATH}")
    print(
        "Catalogs: {catalog_count}, Schemas: {schema_count}, Tables: {table_count}, Columns: {column_count}".format(
            catalog_count=stats.get("catalog_count", 0),
            schema_count=stats.get("schema_count", 0),
            table_count=stats.get("table_count", 0),
            column_count=stats.get("column_count", 0),
        )
    )


if __name__ == "__main__":
    run()

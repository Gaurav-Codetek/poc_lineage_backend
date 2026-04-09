from fastapi import APIRouter, HTTPException

from app.services.catalog_metadata_service import CATALOG_HIERARCHY_PATH, load_catalog_snapshot

router = APIRouter()


@router.get("/hierarchy")
def get_catalog_hierarchy():
    try:
        return load_catalog_snapshot()
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=(
                "Catalog hierarchy snapshot not found. "
                "Run scripts/run_catalog_metadata_ingestion.py to generate it."
            ),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load catalog hierarchy from {CATALOG_HIERARCHY_PATH.name}: {exc}",
        )

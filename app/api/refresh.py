from fastapi import BackgroundTasks
from fastapi import APIRouter

from app.services.catalog_metadata_service import (
    CATALOG_REFRESH_LOCK,
    background_refresh_catalog_snapshot,
)
from app.services.refresh_service import background_ingestion, sync_cache_refresh

router = APIRouter()

@router.post("/cache")
def refresh_cache():
    """
    Blocking refresh.
    Neo4j → Redis → Memory.
    """
    sync_cache_refresh()
    return {"status": "Cache refreshed successfully"}

@router.post("/ingestion")
def refresh_ingestion(background_tasks: BackgroundTasks):
    """
    Long-running ingestion.
    Runs in background.
    """
    background_tasks.add_task(background_ingestion)
    return {"status": "Background ingestion started"}


@router.post("/catalog-metadata")
def refresh_catalog_metadata(background_tasks: BackgroundTasks):
    """
    Long-running catalog snapshot refresh.
    Runs in background.
    """
    if CATALOG_REFRESH_LOCK.locked():
        return {"status": "Catalog metadata refresh already running"}

    background_tasks.add_task(background_refresh_catalog_snapshot)
    return {"status": "Background catalog metadata refresh started"}

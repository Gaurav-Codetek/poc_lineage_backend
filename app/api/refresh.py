from fastapi import APIRouter
from app.services.refresh_service import sync_cache_refresh
from fastapi import BackgroundTasks
from app.services.refresh_service import background_ingestion

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
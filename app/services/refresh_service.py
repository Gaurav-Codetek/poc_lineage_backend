import threading
from app.services.table_service import preload_graph, load_cache_into_memory
from app.services.ingestion_service import ingest_lineage_from_databricks

# Lock to prevent duplicate refresh
BACKGROUND_LOCK = threading.Lock()


# -----------------------------
# Background refresh
# Databricks → Neo4j
# -----------------------------
def background_ingestion():

    if BACKGROUND_LOCK.locked():
        print("Ingestion already running...")
        return

    with BACKGROUND_LOCK:
        print("Starting Databricks → Neo4j refresh...")
        ingest_lineage_from_databricks()
        sync_cache_refresh()
        print("Databricks → Neo4j refresh completed.")


# -----------------------------
# Blocking refresh
# Neo4j → Redis → Memory
# -----------------------------
def sync_cache_refresh():
    print("Refreshing Redis and memory cache...")
    preload_graph()
    load_cache_into_memory()
    print("Cache refresh completed.")
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import column_lineage, dq, health, lineage_retriever, stats, table_lineage
from app.api import refresh
from app.services.table_service import USE_REDIS_CACHE, load_cache_into_memory, preload_graph


app = FastAPI(title="Enterprise Lineage Backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(table_lineage.router, prefix="/lineage/table")
app.include_router(column_lineage.router, prefix="/lineage/column")
app.include_router(refresh.router, prefix="/refresh")
app.include_router(lineage_retriever.router, prefix="/retriever")
app.include_router(stats.router, prefix="/stats")
app.include_router(health.router, prefix="/health")
app.include_router(dq.router, prefix="/dq")

@app.on_event("startup")
def startup():
    if USE_REDIS_CACHE:
        print("Initializing Redis lineage cache...")
        preload_graph()
        load_cache_into_memory()
    else:
        print("Redis cache warmup skipped. Using direct Neo4j traversal.")


from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.api import catalog_metadata, column_lineage, dq, health, lineage_retriever, stats, table_lineage
from app.api import refresh
from app.services.refresh_service import trigger_startup_cache_warmup
from app.services.table_service import USE_REDIS_CACHE

FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend_static"
ASSETS_DIR = FRONTEND_DIR / "assets"

API_ROUTERS = [
    (table_lineage.router, "/lineage/table"),
    (column_lineage.router, "/lineage/column"),
    (refresh.router, "/refresh"),
    (lineage_retriever.router, "/retriever"),
    (catalog_metadata.router, "/catalog-metadata"),
    (stats.router, "/stats"),
    (health.router, "/health"),
    (dq.router, "/dq"),
    (dq.router, "/dq-checker"),
]

app = FastAPI(
    title="Enterprise Lineage Backend",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for router, prefix in API_ROUTERS:
    app.include_router(router, prefix=f"/api{prefix}")

for router, prefix in API_ROUTERS:
    app.include_router(router, prefix=prefix, include_in_schema=False)

if ASSETS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="frontend-assets")


def _frontend_file_response(path: Path) -> FileResponse:
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail=f"Frontend file not found: {path.name}")
    return FileResponse(path)


@app.get("/", include_in_schema=False)
def serve_frontend_index() -> FileResponse:
    return _frontend_file_response(FRONTEND_DIR / "index.html")


@app.get("/{full_path:path}", include_in_schema=False)
def serve_frontend(full_path: str) -> FileResponse:
    if full_path.startswith("api/") or full_path == "api":
        raise HTTPException(status_code=404, detail="API route not found")

    requested_path = FRONTEND_DIR / full_path
    if requested_path.exists() and requested_path.is_file():
        return FileResponse(requested_path)

    return _frontend_file_response(FRONTEND_DIR / "index.html")

@app.on_event("startup")
def startup():
    if USE_REDIS_CACHE:
        trigger_startup_cache_warmup()
    else:
        print("Redis cache warmup skipped. Using direct Neo4j traversal.")


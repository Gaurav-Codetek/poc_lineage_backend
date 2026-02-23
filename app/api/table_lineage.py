from fastapi import APIRouter
from app.services.table_service import traverse_graph

router = APIRouter()

@router.get("/{table}")
def upstream(table: str):
    return traverse_graph(table, "upstream")

@router.get("/downstream/{table}")
def downstream(table: str):
    return traverse_graph(table, "downstream")
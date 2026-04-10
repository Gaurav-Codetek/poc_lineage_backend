from fastapi import APIRouter, Query
from app.services.column_service import get_column_lineage, get_column_graph

router = APIRouter()


@router.get("/{table}")
def column_lineage(
    table: str,
    max_depth: int = Query(default=20, ge=1, le=50),
):
    return get_column_lineage(table, max_depth=max_depth)


@router.get("/{table}/{column}")
def column_graph(
    table: str,
    column: str,
    max_depth: int = Query(default=20, ge=1, le=50),
):
    return get_column_graph(table, column, max_depth=max_depth)

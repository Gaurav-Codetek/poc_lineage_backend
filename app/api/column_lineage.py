from fastapi import APIRouter
from app.services.column_service import get_column_lineage, get_column_graph

router = APIRouter()

@router.get("/{table}")
def column_lineage(table: str):
    return get_column_lineage(table)

@router.get("/{table}/{column}")
def column_graph(table: str, column: str):
    return get_column_graph(table, column)
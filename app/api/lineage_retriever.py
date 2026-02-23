from fastapi import APIRouter, HTTPException
from app.services.column_service import get_column_lineage, get_column_graph
from app.utils.lineage_retriever import lineage_retriever

router = APIRouter()

@router.get("/lineage/{catalog}.{schema}.{table}")
def lineage_endpoint(catalog: str, schema: str, table: str):
    try:
        lineage_tables = lineage_retriever([f"{catalog}.{schema}.{table}"])
        print(lineage_tables)
        return lineage_tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
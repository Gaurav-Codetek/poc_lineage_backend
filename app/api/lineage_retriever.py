from fastapi import APIRouter, HTTPException, Path
from app.services.column_service import get_column_lineage, get_column_graph
from app.utils.lineage_retriever import lineage_retriever
from app.intelligence.dagbuilder import DAGBuilder
from app.connectors.neo4j_connector import Neo4jConnector
from app.utils.config import NEO4J_CONFIG

router = APIRouter()

neo4j = Neo4jConnector(**NEO4J_CONFIG)
dag_builder = DAGBuilder(neo4j)

@router.get("/lineage/{catalog}.{schema}.{table}")
def lineage_endpoint(catalog: str, schema: str, table: str):
    try:
        lineage_tables = lineage_retriever([f"{catalog}.{schema}.{table}"])
        print(lineage_tables)
        return lineage_tables
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dimensions/{tableName}")
def get_dimensions(
    tableName: str = Path(..., description="Target table")
):
    dimension = dag_builder.get_dimensions(tableName)
    result =[]
    for dim in dimension:
        result.append(dim["dimension"])
    return result

@router.get("/dimensions/detail/{tableName}")
def get_dimensions(
    tableName: str = Path(..., description="Target table")
):
    dimension = dag_builder.get_dimensions(tableName)
    return dimension
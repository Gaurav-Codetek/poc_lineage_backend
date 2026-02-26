from fastapi import APIRouter, HTTPException, Path

from app.connectors.neo4j_connector import Neo4jConnector
from app.core.config import NEO4J_CONFIG
from app.intelligence.dagbuilder import DAGBuilder
from app.retrieval.lineage_retriever import lineage_retriever

router = APIRouter()

neo4j = Neo4jConnector(**NEO4J_CONFIG)
dag_builder = DAGBuilder(neo4j)


@router.get("/lineage/{catalog}.{schema}.{table}")
def lineage_endpoint(catalog: str, schema: str, table: str):
    try:
        return lineage_retriever([f"{catalog}.{schema}.{table}"])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dimensions/{table_name}")
def get_dimensions(table_name: str = Path(..., description="Target table")):
    dimension = dag_builder.get_dimensions(table_name)
    return [dim["dimension"] for dim in dimension]


@router.get("/dimensions/detail/{table_name}")
def get_dimension_details(table_name: str = Path(..., description="Target table")):
    return dag_builder.get_dimensions(table_name)

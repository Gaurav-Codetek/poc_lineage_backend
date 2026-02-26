from neo4j import GraphDatabase
from app.core.config import NEO4J_URI, NEO4J_USER, NEO4J_PASS

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASS),
    max_connection_pool_size=50
)

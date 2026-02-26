from connectors.neo4j_connector import Neo4jConnector
from connectors.databricks_connector import DatabricksConnector
from extractors.metadata_extractor import MetadataExtractor
from ingestion.graph_ingestion import (
    create_constraints,
    ingest_tables,
    ingest_columns,
    ingest_table_lineage
)
from config import CATALOG, DATABRICKS_CONFIG, NEO4J_CONFIG


def run():

    db = DatabricksConnector(**DATABRICKS_CONFIG)
    neo4j = Neo4jConnector(**NEO4J_CONFIG)

    extractor = MetadataExtractor(db)

    print("Extracting metadata...")
    profile = extractor.get_data_profile(CATALOG)
    lineage = extractor.get_table_lineage()

    print("Creating constraints...")
    create_constraints(neo4j)

    print("Ingesting tables...")
    ingest_tables(neo4j, profile)

    print("Ingesting columns...")
    ingest_columns(neo4j, profile)

    print("Ingesting lineage...")
    ingest_table_lineage(neo4j, lineage)

    print("Done")
    


if __name__ == "__main__":
    run()
from app.connectors.databricks_connector import DatabricksConnector
from app.connectors.neo4j_connector import Neo4jConnector
from app.core.config import CATALOG, DATABRICKS_CONFIG, NEO4J_CONFIG
from app.extractors.metadata_extractor import MetadataExtractor
from app.intelligence.fk_detection import FKBatchEngine


def run():
    db = DatabricksConnector(**DATABRICKS_CONFIG)
    neo4j = Neo4jConnector(**NEO4J_CONFIG)
    extractor = MetadataExtractor(db)

    profile = extractor.get_data_profile(CATALOG)
    profile["full_name"] = (
        profile["catalog_name"] + "." + profile["schema_name"] + "." + profile["table_name"]
    )

    fk_engine = FKBatchEngine(neo4j, db, profile)
    fk_engine.run_for_table("intelliegencedatacatalog.silver_pharma_sales.fact_sales")


if __name__ == "__main__":
    run()

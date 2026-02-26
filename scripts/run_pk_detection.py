from app.connectors.databricks_connector import DatabricksConnector
from app.connectors.neo4j_connector import Neo4jConnector
from app.core.config import CATALOG, DATABRICKS_CONFIG, NEO4J_CONFIG
from app.extractors.metadata_extractor import MetadataExtractor
from app.intelligence.pk_detection import PKBatchEngine


def run():
    db = DatabricksConnector(**DATABRICKS_CONFIG)
    neo4j = Neo4jConnector(**NEO4J_CONFIG)
    extractor = MetadataExtractor(db)

    print("Fetching data profile...")
    profile = extractor.get_data_profile(CATALOG)

    pk_engine = PKBatchEngine(neo4j, profile)
    pk_engine.run()

    print("PK detection completed.")


if __name__ == "__main__":
    run()

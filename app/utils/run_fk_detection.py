from connectors.databricks_connector import DatabricksConnector
from connectors.neo4j_connector import Neo4jConnector
from extractors.metadata_extractor import MetadataExtractor
from intelligence.fk_detection import FKBatchEngine
from config import DATABRICKS_CONFIG, NEO4J_CONFIG, CATALOG


def run():

    db = DatabricksConnector(**DATABRICKS_CONFIG)
    neo4j = Neo4jConnector(**NEO4J_CONFIG)

    extractor = MetadataExtractor(db)

    profile = extractor.get_data_profile(CATALOG)
    profile["full_name"] = (
    profile["catalog_name"] + "." +
    profile["schema_name"] + "." +
    profile["table_name"]
    )

    fk_engine = FKBatchEngine(neo4j, db, profile)

    # Example
    fk_engine.run_for_table(
        "intelliegencedatacatalog.silver_pharma_sales.fact_sales"
    )


if __name__ == "__main__":
    run()
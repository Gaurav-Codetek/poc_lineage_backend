from connectors.databricks_connector import DatabricksConnector
from connectors.neo4j_connector import Neo4jConnector
from extractors.metadata_extractor import MetadataExtractor
from intelligence.table_classification import TableClassificationEngine
from config import DATABRICKS_CONFIG, NEO4J_CONFIG, CATALOG


def run():

    db = DatabricksConnector(**DATABRICKS_CONFIG)
    neo4j = Neo4jConnector(**NEO4J_CONFIG)

    extractor = MetadataExtractor(db)

    profile = extractor.get_data_profile(CATALOG)

    engine = TableClassificationEngine(neo4j, profile)
    engine.run()


if __name__ == "__main__":
    run()
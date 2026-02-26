from connectors.neo4j_connector import Neo4jConnector
from lineage.neo4j_lineage import Neo4jLineage
from intelligence.fk_scoring import FKScoringEngine
from config import NEO4J_CONFIG


def run():

    neo4j = Neo4jConnector(**NEO4J_CONFIG)

    lineage = Neo4jLineage(neo4j)

    scoring = FKScoringEngine(neo4j, lineage)

    print("Running FK scoring...")
    scoring.run()

    print("Done.")


if __name__ == "__main__":
    run()
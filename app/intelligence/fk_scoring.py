from app.services.inference import lineage_distance
from rapidfuzz import fuzz


class FKScoringEngine:

    def __init__(self, neo4j, lineage_graph):
        self.neo4j = neo4j
        self.lineage = lineage_graph

    def run(self):

        query = """
        MATCH (src:Column)-[r:FK_RELATION]->(tgt:Column)
        MATCH (src)<-[:HAS_COLUMN]-(fact:Table)
        MATCH (tgt)<-[:HAS_COLUMN]-(dim:Table)
        RETURN src.id AS fk, tgt.id AS pk,
               fact.full_name AS fact,
               dim.full_name AS dim
        """

        rels = self.neo4j.execute_read(query)

        for rel in rels:

            naming_score = fuzz.partial_ratio(
                rel["fk"].split(".")[-1],
                rel["pk"].split(".")[-1]
            ) / 100

            semantic_score = 0.2

            dist = lineage_distance(
                self.lineage,
                rel["dim"],
                rel["fact"]
            )

            lineage_score = 0.1 if dist <= 2 else 0

            confidence = min(
                0.5 + naming_score * 0.2 +
                semantic_score +
                lineage_score,
                0.99
            )

            update = """
            MATCH (src:Column {id:$fk})
            MATCH (tgt:Column {id:$pk})
            MATCH (src)-[r:FK_RELATION]->(tgt)
            SET r.confidence = $confidence,
                r.naming_score = $naming,
                r.semantic_score = $semantic,
                r.lineage_score = $lineage
            """

            self.neo4j.execute_write(update, {
                "fk": rel["fk"],
                "pk": rel["pk"],
                "confidence": confidence,
                "naming": naming_score,
                "semantic": semantic_score,
                "lineage": lineage_score
            })
from app.services.inference import detect_fk_for_target
from app.lineage.neo4j_lineage import Neo4jLineage


class FKBatchEngine:

    def __init__(self, neo4j, connector, profile_df):
        self.neo4j = neo4j
        self.connector = connector
        self.profile = profile_df
        self.lineage = Neo4jLineage(neo4j)

    def run_for_table(self, target_table):

        print(f"Detecting FK for {target_table}")

        # Step 1 — family
        family = self.lineage.get_family(target_table)

        # Step 2 — filter profile
        family_profile = self.profile[
            self.profile["full_name"].isin(family)
        ]

        # Step 3 — fetch PK from graph
        pk_df = self._fetch_pk(family)

        # Step 4 — detect FK
        relationships = detect_fk_for_target(
            self.connector,
            target_table,
            pk_df,
            family_profile
        )

        # Step 5 — store in graph
        self._store_fk(relationships)

    def _fetch_pk(self, family):

        # Step 1 — get PK column IDs from Neo4j
        query = """
        MATCH (c:Column)
        WHERE c.is_primary_key = true
        RETURN c.id AS col_id, c.pk_confidence AS confidence
        """
    
        results = self.neo4j.execute_read(query)
    
        # Step 2 — extract table and column
        pk_columns = []
    
        for r in results:
            parts = r["col_id"].split(".")
    
            table_full = ".".join(parts[:3])
    
            if table_full in family:
                pk_columns.append({
                    "catalog_name": parts[0],
                    "schema_name": parts[1],
                    "table_name": parts[2],
                    "column_name": parts[3]
                })
    
        import pandas as pd
        pk_df = pd.DataFrame(pk_columns)
    
        # Step 3 — join with profile to get full metadata
        if pk_df.empty:
            return pk_df
    
        merged = pd.merge(
            pk_df,
            self.profile,
            on=[
                "catalog_name",
                "schema_name",
                "table_name",
                "column_name"
            ],
            how="left"
        )
    
        return merged

    def _store_fk(self, relationships):

        query = """
        MATCH (src:Column {id:$fk})
        MATCH (tgt:Column {id:$pk})
        MERGE (src)-[r:FK_RELATION]->(tgt)
        SET r.confidence = $confidence,
            r.relationship_type = $type,
            r.source = "rule_based",
            r.last_updated = datetime()
        """

        for rel in relationships:

            fk = f"{rel['target_table']}.{rel['fk_column']}"
            pk = f"{rel['dimension_table']}.{rel['pk_column']}"

            self.neo4j.execute_write(query, {
                "fk": fk,
                "pk": pk,
                "confidence": 0.8,  # temporary
                "type": "semantic"
            })
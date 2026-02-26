from app.services.inference import KeyInferenceEngine


class PKBatchEngine:

    def __init__(self, neo4j, profile_df):
        self.neo4j = neo4j
        self.profile = profile_df

    def run(self):

        print("Running PK detection...")

        pk_engine = KeyInferenceEngine(self.profile)
        pk_df = pk_engine.detect_primary_keys()

        print(f"Detected {len(pk_df)} PK candidates")

        self._store_pk(pk_df)

    def _store_pk(self, pk_df):

        query = """
        MATCH (c:Column {id:$col_id})
        SET c.is_primary_key = true,
            c.pk_confidence = $confidence,
            c.pk_source = "rule_based",
            c.pk_last_updated = datetime()
        """

        for _, row in pk_df.iterrows():

            table = f"{row.catalog_name}.{row.schema_name}.{row.table_name}"
            col_id = f"{table}.{row.column_name}"

            self.neo4j.execute_write(query, {
                "col_id": col_id,
                "confidence": float(row.uniqueness_ratio)
            })
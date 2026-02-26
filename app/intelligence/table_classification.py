from app.services.inference import EnhancedTableClassifier


class TableClassificationEngine:

    def __init__(self, neo4j, profile_df):
        self.neo4j = neo4j
        self.profile = profile_df

    def run(self):

        classifier = EnhancedTableClassifier(
            self.neo4j,
            self.profile
        )

        table_types = classifier.classify()

        query = """
            MATCH (t:Table {full_name:$table})
            SET t.table_type = $type,
                t.fact_score = $score,
                t.numeric_density = $numeric,
                t.numeric_columns = $num_cols,
                t.total_columns = $total,
                t.fk_columns = $fk_cols,
                t.dim_connections = $dims
            """

        for _, row in table_types.iterrows():

            self.neo4j.execute_write(query, {
                "table": row["full_name"],
                "type": row["type"],
                "score": float(row["fact_score"]),
                "numeric": float(row["numeric_density"]),
                "num_cols": int(row["numeric_cols"]),
                "total": int(row["total_cols"]),
                "fk_cols": int(row["fk_columns"]),
                "dims": int(row["dim_connections"])
            })
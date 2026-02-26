def create_constraints(neo4j):
    queries = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Table) REQUIRE t.full_name IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Column) REQUIRE c.id IS UNIQUE"
    ]

    for q in queries:
        neo4j.execute_write(q)

def ingest_tables(neo4j, profile_df):

    for _, row in profile_df.iterrows():

        full_name = f"{row.catalog_name}.{row.schema_name}.{row.table_name}"

        query = """
        MERGE (t:Table {full_name:$full_name})
        SET t.catalog = $catalog,
            t.schema = $schema,
            t.name = $name,
            t.row_count = $row_count
        """

        neo4j.execute_write(query, {
            "full_name": full_name,
            "catalog": row.catalog_name,
            "schema": row.schema_name,
            "name": row.table_name,
            "row_count": row.row_count
        })

def ingest_columns(neo4j, profile_df):

    for _, row in profile_df.iterrows():

        table = f"{row.catalog_name}.{row.schema_name}.{row.table_name}"
        col_id = f"{table}.{row.column_name}"

        query = """
        MATCH (t:Table {full_name:$table})
        MERGE (c:Column {id:$col_id})
        SET c.name = $column,
            c.data_type = $data_type,
            c.distinct_count = $distinct_count,
            c.null_ratio = $null_ratio,
            c.uniqueness_ratio = $uniqueness_ratio
        MERGE (t)-[:HAS_COLUMN]->(c)
        """

        neo4j.execute_write(query, {
            "table": table,
            "col_id": col_id,
            "column": row.column_name,
            "data_type": row.data_type,
            "distinct_count": row.distinct_count,
            "null_ratio": row.null_ratio,
            "uniqueness_ratio": row.uniqueness_ratio
        })

def ingest_table_lineage(neo4j, lineage_df):

    query = """
    MERGE (src:Table {full_name:$src})
    MERGE (tgt:Table {full_name:$tgt})
    MERGE (src)-[:DERIVED_FROM]->(tgt)
    """

    for _, row in lineage_df.iterrows():

        neo4j.execute_write(query, {
            "src": row.source_table_full_name,
            "tgt": row.target_table_full_name
        })
from app.utils.config import DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN
from databricks import sql
from app.db.neo4j import driver

def get_db_connection():
    return sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )


def load_column_lineage_stream(batch_size=5000):

    query = """
    SELECT
    source_table_full_name,
    source_column_name,
    target_table_full_name,
    target_column_name
FROM system.access.column_lineage
WHERE source_table_full_name IS NOT NULL
  AND target_table_full_name IS NOT NULL
  AND source_column_name IS NOT NULL
  AND target_column_name IS NOT NULL
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)

    cypher = """
    UNWIND $rows AS row
    MERGE (s:Column {name: row.src_col, table: row.src_tbl})
    MERGE (t:Column {name: row.tgt_col, table: row.tgt_tbl})
    MERGE (t)-[:MAPS_TO]->(s)
    """

    with driver.session() as session:

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            data = [{
                "src_tbl": r[0],
                "src_col": r[1],
                "tgt_tbl": r[2],
                "tgt_col": r[3],
            } for r in rows]

            session.execute_write(
                lambda tx: tx.run(cypher, rows=data)
            )

    cursor.close()
    conn.close()


def load_table_lineage_stream(batch_size=5000):

    query = """
    SELECT DISTINCT
        source_table_full_name,
        target_table_full_name
    FROM system.access.table_lineage
    WHERE source_table_full_name IS NOT NULL
      AND target_table_full_name IS NOT NULL
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)

    cypher = """
    UNWIND $rows AS row
    MERGE (s:Table {name: row.source})
    MERGE (t:Table {name: row.target})
    MERGE (t)-[:DEPENDS_ON]->(s)
    """

    with driver.session() as session:

        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break

            data = [{"source": r[0], "target": r[1]} for r in rows]

            session.execute_write(
                lambda tx: tx.run(cypher, rows=data)
            )

    cursor.close()
    conn.close()

def ingest_lineage_from_databricks():
    load_table_lineage_stream()
    load_column_lineage_stream()
from app.db.neo4j import driver

def get_column_lineage(table: str):

    query = """
    MATCH (c:Column {table: $table})
    OPTIONAL MATCH (c)-[:MAPS_TO]->(u:Column)
    RETURN
        c.name AS column,
        collect({
            table: u.table,
            column: u.name
        }) AS upstream
    """

    with driver.session() as session:
        result = session.run(query, table=table)
        records = result.data()

    return {
        "table": table,
        "columns": records
    }


def get_column_graph(table: str, column: str):

    query = """
    MATCH path = (c:Column {name: $column, table: $table})-[:MAPS_TO*]->(upstream)
    RETURN path
    """

    nodes = {}
    edges = []

    with driver.session() as session:
        records = list(session.run(query, table=table, column=column))

        for record in records:
            path = record["path"]

            for node in path.nodes:
                key = f"{node['table']}.{node['name']}"

                if key not in nodes:
                    nodes[key] = {
                        "column": node["name"],
                        "table": node["table"]
                    }

            for rel in path.relationships:
                edges.append({
                    "source": f"{rel.start_node['table']}.{rel.start_node['name']}",
                    "target": f"{rel.end_node['table']}.{rel.end_node['name']}"
                })

    return {
        "nodes": list(nodes.values()),
        "edges": edges
    }
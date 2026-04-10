from app.db.neo4j import driver


def _column_key(table: str, column: str) -> str:
    return f"{table}.{column}"


def _normalize_depth(max_depth: int) -> int:
    return max(1, min(int(max_depth), 50))


def _get_table_columns(table: str) -> list[str]:
    query = """
    MATCH (c:Column {table: $table})
    WHERE c.name IS NOT NULL
    RETURN DISTINCT c.name AS column
    ORDER BY column
    """

    with driver.session() as session:
        rows = session.run(query, table=table).data()

    return [row["column"] for row in rows if row.get("column")]


def get_column_graph(table: str, column: str, max_depth: int = 20):
    depth = _normalize_depth(max_depth)

    query = f"""
    MATCH (root:Column {{name: $column, table: $table}})
    OPTIONAL MATCH upstream_path = (root)-[:MAPS_TO*1..{depth}]->(:Column)
    WITH root, collect(DISTINCT upstream_path) AS upstream_paths
    OPTIONAL MATCH downstream_path = (:Column)-[:MAPS_TO*1..{depth}]->(root)
    RETURN root, upstream_paths, collect(DISTINCT downstream_path) AS downstream_paths
    """

    with driver.session() as session:
        record = session.run(query, table=table, column=column).single()

    if record is None or record["root"] is None:
        return {
            "root": None,
            "max_depth": depth,
            "stats": {
                "node_count": 0,
                "edge_count": 0,
                "upstream_node_count": 0,
                "downstream_node_count": 0,
            },
            "graph": {
                "nodes": [],
                "edges": [],
            },
        }

    nodes = {}
    edges = {}
    upstream_node_ids = set()
    downstream_node_ids = set()

    root_node = record["root"]
    root_id = _column_key(root_node["table"], root_node["name"])
    nodes[root_id] = {
        "id": root_id,
        "table": root_node["table"],
        "column": root_node["name"],
        "position": "root",
    }

    def add_node(node, position: str) -> None:
        node_id = _column_key(node["table"], node["name"])
        existing = nodes.get(node_id)

        if existing is None:
            nodes[node_id] = {
                "id": node_id,
                "table": node["table"],
                "column": node["name"],
                "position": position,
            }
            return

        if existing["position"] == "root" or existing["position"] == position:
            return

        existing["position"] = "both"

    def add_path(path, position: str) -> None:
        if path is None:
            return

        for node in path.nodes:
            node_id = _column_key(node["table"], node["name"])
            if node_id != root_id:
                if position == "upstream":
                    upstream_node_ids.add(node_id)
                elif position == "downstream":
                    downstream_node_ids.add(node_id)
            add_node(node, position)

        # MAPS_TO is stored as downstream -> upstream.
        # Emit edges in lineage flow order so upstream nodes point into downstream nodes.
        for rel in path.relationships:
            source_id = _column_key(rel.end_node["table"], rel.end_node["name"])
            target_id = _column_key(rel.start_node["table"], rel.start_node["name"])
            edges[(source_id, target_id)] = {
                "source": source_id,
                "target": target_id,
            }

    for path in record["upstream_paths"]:
        add_path(path, "upstream")

    for path in record["downstream_paths"]:
        add_path(path, "downstream")

    return {
        "root": {
            "id": root_id,
            "table": table,
            "column": column,
        },
        "max_depth": depth,
        "stats": {
            "node_count": len(nodes),
            "edge_count": len(edges),
            "upstream_node_count": len(upstream_node_ids),
            "downstream_node_count": len(downstream_node_ids),
        },
        "graph": {
            "nodes": list(nodes.values()),
            "edges": list(edges.values()),
        },
    }


def get_column_lineage(table: str, max_depth: int = 20):
    columns = _get_table_columns(table)
    depth = _normalize_depth(max_depth)

    if not columns:
        return {
            "table": table,
            "max_depth": depth,
            "stats": {
                "root_column_count": 0,
                "columns_with_upstream": 0,
                "columns_with_downstream": 0,
                "aggregate_node_count": 0,
                "aggregate_edge_count": 0,
                "unique_node_count": 0,
                "unique_edge_count": 0,
            },
            "columns": [],
        }

    flows = []
    unique_node_ids = set()
    unique_edge_keys = set()
    columns_with_upstream = 0
    columns_with_downstream = 0
    aggregate_node_count = 0
    aggregate_edge_count = 0

    for column in columns:
        flow = get_column_graph(table, column, max_depth=depth)
        flows.append(flow)

        stats = flow["stats"]
        aggregate_node_count += stats["node_count"]
        aggregate_edge_count += stats["edge_count"]

        if stats["upstream_node_count"] > 0:
            columns_with_upstream += 1
        if stats["downstream_node_count"] > 0:
            columns_with_downstream += 1

        for node in flow["graph"]["nodes"]:
            unique_node_ids.add(node["id"])

        for edge in flow["graph"]["edges"]:
            unique_edge_keys.add((edge["source"], edge["target"]))

    return {
        "table": table,
        "max_depth": depth,
        "stats": {
            "root_column_count": len(columns),
            "columns_with_upstream": columns_with_upstream,
            "columns_with_downstream": columns_with_downstream,
            "aggregate_node_count": aggregate_node_count,
            "aggregate_edge_count": aggregate_edge_count,
            "unique_node_count": len(unique_node_ids),
            "unique_edge_count": len(unique_edge_keys),
        },
        "columns": flows,
    }

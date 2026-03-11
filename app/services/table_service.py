import os

from app.db.neo4j import driver

from app.cache.table_cache import load_cache, save_cache

UPSTREAM_GRAPH = {}
DOWNSTREAM_GRAPH = {}
UPSTREAM_COUNT = {}

USE_REDIS_CACHE = os.getenv("USE_REDIS_CACHE", "false").lower() == "true"


def preload_graph():

    print("Preloading lineage from Neo4j into Redis...")

    upstream_graph = {}
    downstream_graph = {}
    upstream_count = {}

    query = """
    MATCH (t:Table)-[:DEPENDS_ON]->(u)
    RETURN t.name AS target, collect(u.name) AS upstream
    """

    with driver.session() as session:
        result = session.run(query)

        for row in result:
            target = row["target"]
            upstream = row["upstream"]

            upstream_graph[target] = upstream
            upstream_count[target] = len(upstream)

            for upstream_table in upstream:
                downstream_graph.setdefault(upstream_table, []).append(target)

    save_cache(upstream_graph, downstream_graph, upstream_count)

    print("Redis cache updated successfully.")


def _get_upstream_tables(session, table: str, upstream_cache: dict[str, list[str]]) -> list[str]:
    if table not in upstream_cache:
        row = session.run(
            """
            MATCH (t:Table {name: $table})-[:DEPENDS_ON]->(u:Table)
            RETURN collect(u.name) AS upstream
            """,
            table=table,
        ).single()
        upstream_cache[table] = row["upstream"] if row and row["upstream"] else []
    return upstream_cache[table]


def _get_downstream_tables(session, table: str, downstream_cache: dict[str, list[str]]) -> list[str]:
    if table not in downstream_cache:
        row = session.run(
            """
            MATCH (d:Table)-[:DEPENDS_ON]->(t:Table {name: $table})
            RETURN collect(d.name) AS downstream
            """,
            table=table,
        ).single()
        downstream_cache[table] = row["downstream"] if row and row["downstream"] else []
    return downstream_cache[table]


def _traverse_graph_from_memory(root_table: str, direction: str) -> dict:
    visited = set()
    stack = [root_table]

    nodes = {}
    edges = []

    while stack:
        current = stack.pop()

        if current in visited:
            continue

        visited.add(current)

        downstream_tables = DOWNSTREAM_GRAPH.get(current, [])

        nodes[current] = {
            "id": current,
            "downstream_count": len(downstream_tables),
            "downstream_tables": downstream_tables,
        }

        if direction == "upstream":
            parents = UPSTREAM_GRAPH.get(current, [])

            for parent in parents:
                edges.append(
                    {
                        "source": parent,
                        "target": current,
                        "is_join": len(parents) > 1,
                    }
                )

                if parent not in visited:
                    stack.append(parent)
        else:
            for child in downstream_tables:
                cnt = UPSTREAM_COUNT.get(child, 0)

                edges.append(
                    {
                        "source": current,
                        "target": child,
                        "is_join": cnt > 1,
                    }
                )

                if child not in visited:
                    stack.append(child)

    return {
        "root": root_table,
        "direction": direction,
        "nodes": list(nodes.values()),
        "edges": edges,
    }


def _traverse_graph_from_neo4j(root_table: str, direction: str) -> dict:
    visited = set()
    stack = [root_table]

    nodes = {}
    edges = []

    upstream_cache: dict[str, list[str]] = {}
    downstream_cache: dict[str, list[str]] = {}

    with driver.session() as session:
        while stack:
            current = stack.pop()

            if current in visited:
                continue

            visited.add(current)

            upstream_tables = _get_upstream_tables(session, current, upstream_cache)
            downstream_tables = _get_downstream_tables(session, current, downstream_cache)

            nodes[current] = {
                "id": current,
                "downstream_count": len(downstream_tables),
                "downstream_tables": downstream_tables,
            }

            if direction == "upstream":
                for parent in upstream_tables:
                    edges.append(
                        {
                            "source": parent,
                            "target": current,
                            "is_join": len(upstream_tables) > 1,
                        }
                    )

                    if parent not in visited:
                        stack.append(parent)
            else:
                for child in downstream_tables:
                    child_upstream = _get_upstream_tables(session, child, upstream_cache)
                    edges.append(
                        {
                            "source": current,
                            "target": child,
                            "is_join": len(child_upstream) > 1,
                        }
                    )

                    if child not in visited:
                        stack.append(child)

    return {
        "root": root_table,
        "direction": direction,
        "nodes": list(nodes.values()),
        "edges": edges,
    }


def traverse_graph(root_table: str, direction="upstream"):
    if USE_REDIS_CACHE and UPSTREAM_GRAPH:
        return _traverse_graph_from_memory(root_table, direction)
    return _traverse_graph_from_neo4j(root_table, direction)


def load_cache_into_memory():

    global UPSTREAM_GRAPH, DOWNSTREAM_GRAPH, UPSTREAM_COUNT

    cache = load_cache()

    if cache:
        UPSTREAM_GRAPH = cache["upstream"]
        DOWNSTREAM_GRAPH = cache["downstream"]
        UPSTREAM_COUNT = cache["counts"]

    print("Memory cache loaded.")

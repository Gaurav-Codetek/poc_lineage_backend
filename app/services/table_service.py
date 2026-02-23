from collections import defaultdict
from app.db.neo4j import driver

UPSTREAM_GRAPH = {}
DOWNSTREAM_GRAPH = {}
UPSTREAM_COUNT = {}

from app.db.neo4j import driver
from app.cache.table_cache import save_cache
from app.cache.table_cache import load_cache


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

            for u in upstream:
                downstream_graph.setdefault(u, []).append(target)

    save_cache(upstream_graph, downstream_graph, upstream_count)

    print("Redis cache updated successfully.")

def traverse_graph(root_table: str, direction="upstream"):

    # cache = load_cache()

    # if not cache:
    #     raise Exception("Redis cache not initialized")

    # UPSTREAM_GRAPH = cache["upstream"]
    # DOWNSTREAM_GRAPH = cache["downstream"]
    # UPSTREAM_COUNT = cache["counts"]

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
            "downstream_tables": downstream_tables
        }

        if direction == "upstream":

            parents = UPSTREAM_GRAPH.get(current, [])

            for parent in parents:
                edges.append({
                    "source": parent,
                    "target": current,
                    "is_join": len(parents) > 1
                })

                if parent not in visited:
                    stack.append(parent)

        else:

            for child in downstream_tables:

                cnt = UPSTREAM_COUNT.get(child, 0)

                edges.append({
                    "source": current,
                    "target": child,
                    "is_join": cnt > 1
                })

                if child not in visited:
                    stack.append(child)

    return {
        "root": root_table,
        "direction": direction,
        "nodes": list(nodes.values()),
        "edges": edges
    }


def load_cache_into_memory():

    global UPSTREAM_GRAPH, DOWNSTREAM_GRAPH, UPSTREAM_COUNT

    cache = load_cache()

    if cache:
        UPSTREAM_GRAPH = cache["upstream"]
        DOWNSTREAM_GRAPH = cache["downstream"]
        UPSTREAM_COUNT = cache["counts"]

    print("Memory cache loaded.")
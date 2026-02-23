import networkx as nx
import json


class DatabricksLineageGraph:
    def __init__(self):
        self.graph = nx.DiGraph()

    # ---------------------------
    # 1. Build graph
    # ---------------------------
    def build_from_json(self, lineage_json):

        # 🔥 Step 1: add all nodes first
        for node in lineage_json.get("nodes", []):
            table = node["id"]
            self.graph.add_node(table)
    
        # 🔥 Step 2: add edges
        for edge in lineage_json.get("edges", []):
            source = edge["source"]
            target = edge["target"]
            is_join = edge.get("is_join", False)
    
            self.graph.add_edge(source, target, is_join=is_join)

    # ---------------------------
    # 2. Expand tables using join-aware lineage
    # ---------------------------
    def expand_tables_join_priority(self, tables, hops=2):
        expanded = set(tables)

        for table in tables:
            upstream_paths = nx.single_source_shortest_path(
                self.graph.reverse(), table, cutoff=hops
            )

            for node, path in upstream_paths.items():
                if node == table:
                    continue

                # Check if path contains join edge
                for i in range(len(path) - 1):
                    source = path[i + 1]
                    target = path[i]

                    if self.graph.has_edge(source, target):
                        if self.graph[source][target].get("is_join", False):
                            expanded.add(node)
                            break

        return list(expanded)

    # ---------------------------
    # 3. Build join chains (shortest path)
    # ---------------------------
    def build_join_chains(self, root_table, tables):
        chains = []

        for table in tables:
            if table == root_table:
                continue

            try:
                path = nx.shortest_path(
                    self.graph.to_undirected(),
                    table,
                    root_table
                )
                chains.append(path)

            except nx.NetworkXNoPath:
                continue

        return chains

    # ---------------------------
    # 4. Remove redundant chains
    # ---------------------------
    def filter_redundant_chains(self, chains):
        filtered = []

        for chain in chains:
            is_subset = False

            for other in chains:
                if chain == other:
                    continue

                if len(chain) < len(other) and all(x in other for x in chain):
                    is_subset = True
                    break

            if not is_subset:
                filtered.append(chain)

        return filtered
    
def lineage_retriever(target_tables):
    """
    target_tables: list of tables from semantic search
    """

    # Load lineage
    with open("./app/data/lineage.json", encoding="utf-8") as f:
        lineage_json = json.load(f)

    lg = DatabricksLineageGraph()
    # print(lineage_json)
    lg.build_from_json(lineage_json)

    # 🔥 Step 1: expand lineage for all tables
    expanded_tables = lg.expand_tables_join_priority(target_tables, hops=2)
    print("Expanded tables:", expanded_tables)

    # 🔥 Step 2: build join chains for each root table
    all_chains = []

    for root in target_tables:
        chains = lg.build_join_chains(root, expanded_tables)
        chains = lg.filter_redundant_chains(chains)
        all_chains.extend(chains)

    # 🔥 Step 3: remove duplicate chains
    unique_chains = []
    seen = set()

    for chain in all_chains:
        key = tuple(chain)
        if key not in seen:
            unique_chains.append(chain)
            seen.add(key)

    print("Join chains:", unique_chains)

    # 🔥 Step 4: extract relevant tables
    relevant_tables = set()

    for chain in unique_chains:
        relevant_tables.update(chain)

    relevant_tables = list(relevant_tables)

    print("Relevant tables for SQL generation:", relevant_tables)

    return relevant_tables

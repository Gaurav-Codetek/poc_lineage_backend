class DAGBuilder:

    def __init__(self, neo4j):
        self.neo4j = neo4j

    # ---------- lineage traversal ----------
    def _fetch_lineage(self, table, depth):

        query = f"""
        MATCH (t:Table {{full_name:$table}})

        // upstream
        OPTIONAL MATCH path_up =
            (src)-[:DERIVED_FROM*1..{depth}]->(t)

        WITH t,
             collect(DISTINCT nodes(path_up)) AS up_nodes,
             collect(DISTINCT [rel IN relationships(path_up) |
                {{source: startNode(rel).full_name,
                  target: endNode(rel).full_name}}]) AS up_edges

        // downstream
        OPTIONAL MATCH path_down =
            (t)-[:DERIVED_FROM*1..{depth}]->(dst)

        RETURN
            up_nodes AS upstream_nodes,
            up_edges AS upstream_edges,
            collect(DISTINCT nodes(path_down)) AS downstream_nodes,
            collect(DISTINCT [rel IN relationships(path_down) |
                {{source: startNode(rel).full_name,
                  target: endNode(rel).full_name}}]) AS downstream_edges
        """

        return self.neo4j.execute_read(query, {"table": table})[0]

    # ---------- dimension relationships ----------
    def _fetch_dimensions(self, table):

        query = """
        MATCH (fact:Table {full_name:$table})
              -[:HAS_COLUMN]->(fk)-[r:FK_RELATION]->(pk)
        MATCH (pk)<-[:HAS_COLUMN]-(dim:Table)

        RETURN dim.full_name AS dim,
               collect({
                   fact_column: split(fk.id,'.')[-1],
                   dimension_column: split(pk.id,'.')[-1],
                   confidence: r.confidence
               }) AS relationships
        """

        return self.neo4j.execute_read(query, {"table": table})

    # ---------- downstream impact ----------
    def _fetch_downstream(self, table):

        query = """
        MATCH (t:Table {full_name:$table})
        MATCH (t)-[:DERIVED_FROM*]->(downstream)
        RETURN DISTINCT downstream.full_name AS table
        """

        results = self.neo4j.execute_read(query, {"table": table})

        return [r["table"] for r in results]

    # ---------- public method ----------
    def build(self, table, depth=3):

        lineage = self._fetch_lineage(table, depth)

        nodes = {}
        edges = []

        # upstream nodes
        for group in lineage["upstream_nodes"]:
            for node in group:
                nodes[node["full_name"]] = {
                    "id": node["full_name"],
                    "type": node.get("table_type", "TABLE")
                }

        # downstream nodes
        for group in lineage["downstream_nodes"]:
            for node in group:
                nodes[node["full_name"]] = {
                    "id": node["full_name"],
                    "type": node.get("table_type", "TABLE")
                }

        # edges
        for group in lineage["upstream_edges"] + lineage["downstream_edges"]:
            for rel in group:
                edges.append(rel)
        
        # add target table
        nodes[table] = {"id": table, "type": "FACT"}

        dimensions = self._fetch_dimensions(table)

        downstream = self._fetch_downstream(table)

        return {
            "target_table": table,
            "lineage_depth": depth,
            "dag": {
                "nodes": list(nodes.values()),
                "edges": edges
            },
            "dimensions": dimensions,
            "downstream_impact": downstream
        }
    
    def get_dimensions(self, table):

        query = """
        MATCH (fact:Table {full_name:$table})
              -[:HAS_COLUMN]->(fk)-[r:FK_RELATION]->(pk)
        MATCH (pk)<-[:HAS_COLUMN]-(dim:Table)
    
        RETURN dim.full_name AS dimension,
               collect({
                   fact_column: split(fk.id,'.')[-1],
                   dimension_column: split(pk.id,'.')[-1],
                   confidence: r.confidence
               }) AS relationships
        """
    
        return self.neo4j.execute_read(query, {"table": table})
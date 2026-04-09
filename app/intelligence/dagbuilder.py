class DAGBuilder:

    def __init__(self, neo4j):
        self.neo4j = neo4j

    # ---------------------------------------------------
    # TABLE TYPE
    # ---------------------------------------------------
    def _get_table_type(self, table):
        query = """
        MATCH (t:Table {full_name:$table})
        RETURN t.table_type AS type
        """
        result = self.neo4j.execute_read(query, {"table": table})
        return result[0]["type"] if result and result[0]["type"] else "TABLE"


    # ---------------------------------------------------
    # LINEAGE TRAVERSAL
    # ---------------------------------------------------
    def _fetch_lineage(self, table, depth):

        query = f"""
        MATCH (t:Table {{full_name:$table}})

        OPTIONAL MATCH path_up =
            (src)-[:DERIVED_FROM*1..{depth}]->(t)

        WITH t,
             collect(DISTINCT nodes(path_up)) AS up_nodes,
             collect(DISTINCT [rel IN relationships(path_up) |
                {{source: startNode(rel).full_name,
                  target: endNode(rel).full_name}}]) AS up_edges

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

        result = self.neo4j.execute_read(query, {"table": table})
        return result[0] if result else None


    # ---------------------------------------------------
    # FACT -> DIM RELATIONSHIPS
    # (Flat list format — unchanged)
    # ---------------------------------------------------
    def _fetch_fact_dimensions(self, table):

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


    # ---------------------------------------------------
    # DIM -> RELATED FACTS
    # ---------------------------------------------------
    def _fetch_related_facts(self, table):

        query = """
        MATCH (dim:Table {full_name:$table})
              -[:HAS_COLUMN]->(pk)
              <-[:FK_RELATION]-()
              <-[:HAS_COLUMN]-(fact:Table)
        WHERE fact.table_type = "FACT"
        RETURN DISTINCT fact.full_name AS fact
        """

        results = self.neo4j.execute_read(query, {"table": table})
        return [r["fact"] for r in results]


    # ---------------------------------------------------
    # SMART DIMENSION DISCOVERY
    # RETURNS FLAT LIST (Backward Compatible)
    # ---------------------------------------------------
    def get_dimensions(self, table):

        table_type = self._get_table_type(table)
    
        # ----------------------------
        # CASE 1: FACT → simple return
        # ----------------------------
        if table_type == "FACT":
            return self._fetch_fact_dimensions(table)
    
        # ----------------------------
        # CASE 2: DIM → include fact + full star
        # ----------------------------
        elif table_type == "DIM":
        
            facts = self._fetch_related_facts(table)
            dimension_map = {}
    
            for fact in facts:
            
                # 👉 Add FACT itself into list
                if fact not in dimension_map:
                    dimension_map[fact] = {
                        "dimension": fact,
                        "relationships": []   # fact has no FK relationship here
                    }
    
                dims = self._fetch_fact_dimensions(fact)
    
                for d in dims:
                    dim_name = d["dimension"]
    
                    if dim_name not in dimension_map:
                        dimension_map[dim_name] = {
                            "dimension": dim_name,
                            "relationships": d["relationships"]
                        }
                    else:
                        dimension_map[dim_name]["relationships"].extend(
                            d["relationships"]
                        )
    
            return list(dimension_map.values())
    
        # ----------------------------
        # CASE 3: UNKNOWN
        # ----------------------------
        else:
            return []
    # ---------------------------------------------------
    # DOWNSTREAM IMPACT
    # ---------------------------------------------------
    def _fetch_downstream(self, table):

        query = """
        MATCH (t:Table {full_name:$table})
        MATCH (t)-[:DERIVED_FROM*]->(downstream)
        RETURN DISTINCT downstream.full_name AS table
        """

        results = self.neo4j.execute_read(query, {"table": table})
        return [r["table"] for r in results]


    # ---------------------------------------------------
    # COMPLETE DAG BUILDER
    # ---------------------------------------------------
    def build(self, table, depth=3):

        lineage = self._fetch_lineage(table, depth)

        nodes = {}
        edges = []

        if lineage:

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

        # ensure target present
        table_type = self._get_table_type(table)
        nodes[table] = {
            "id": table,
            "type": table_type
        }

        dimensions = self.get_dimensions(table)
        downstream = self._fetch_downstream(table)

        return {
            "target_table": table,
            "table_type": table_type,
            "lineage_depth": depth,
            "dag": {
                "nodes": list(nodes.values()),
                "edges": edges
            },
            "dimensions": dimensions,   # <-- flat list maintained
            "downstream_impact": downstream
        }


    # ---------------------------------------------------
    # TABLE LISTING
    # ---------------------------------------------------
    def get_tables(self):
        query = """
        MATCH (t:Table)
        WITH coalesce(t.full_name, t.name) AS table_name
        WHERE table_name IS NOT NULL
          AND size(split(table_name, '.')) = 3
        RETURN DISTINCT table_name AS table
        ORDER BY table
        """

        results = self.neo4j.execute_read(query)
        return [row["table"] for row in results]


    def get_catalog_hierarchy(self):
        hierarchy_map = {}

        for full_name in self.get_tables():
            parts = full_name.split(".", 2)
            if len(parts) != 3 or not all(parts):
                continue

            catalog_name, schema_name, table_name = parts
            schema_tables = hierarchy_map.setdefault(catalog_name, {}).setdefault(schema_name, [])
            schema_tables.append(
                {
                    "name": table_name,
                    "full_name": full_name,
                }
            )

        catalogs = []
        for catalog_name in sorted(hierarchy_map):
            schemas = []
            for schema_name in sorted(hierarchy_map[catalog_name]):
                tables = sorted(
                    hierarchy_map[catalog_name][schema_name],
                    key=lambda table: (table["name"], table["full_name"]),
                )
                schemas.append(
                    {
                        "name": schema_name,
                        "tables": tables,
                    }
                )

            catalogs.append(
                {
                    "name": catalog_name,
                    "schemas": schemas,
                }
            )

        return {"catalogs": catalogs}


    # ---------------------------------------------------
    # TABLE SEARCH
    # ---------------------------------------------------
    def search_tables(self, query_text, limit=20):

        if not query_text:
            return []

        query_text = query_text.strip()
        if not query_text:
            return []

        query = """
        MATCH (t:Table)
        WITH DISTINCT coalesce(t.full_name, t.name) AS table_name
        WHERE table_name IS NOT NULL
          AND size(split(table_name, '.')) = 3
          AND toLower(table_name) CONTAINS toLower($query_text)
        RETURN table_name AS table
        ORDER BY
          CASE
            WHEN toLower(table_name) STARTS WITH toLower($query_text) THEN 0
            ELSE 1
          END,
          table_name
        LIMIT $limit
        """

        results = self.neo4j.execute_read(
            query,
            {"query_text": query_text, "limit": int(limit)},
        )

        return [row["table"] for row in results]

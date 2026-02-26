class Neo4jLineage:

    def __init__(self, neo4j):
        self.neo4j = neo4j

    def get_family(self, table):

        query = """
        MATCH (t:Table {full_name:$table})
    
        // Ancestors (upstream)
        OPTIONAL MATCH (ancestor)-[:DERIVED_FROM*]->(t)
    
        // Descendants (downstream)
        OPTIONAL MATCH (t)-[:DERIVED_FROM*]->(descendant)
    
        // Parents
        OPTIONAL MATCH (parent)-[:DERIVED_FROM]->(t)
    
        // Siblings
        OPTIONAL MATCH (parent)-[:DERIVED_FROM]->(sibling)
    
        WITH
            collect(DISTINCT ancestor.full_name) +
            collect(DISTINCT descendant.full_name) +
            collect(DISTINCT sibling.full_name) AS fam
    
        RETURN fam
        """
    
        result = self.neo4j.execute_read(query, {"table": table})
    
        family = result[0]["fam"] if result else []
    
        family.append(table)
    
        return list(set([f for f in family if f]))
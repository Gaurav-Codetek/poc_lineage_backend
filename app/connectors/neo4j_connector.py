from neo4j import GraphDatabase

class Neo4jConnector:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def execute_write(self, query, params=None):
        with self.driver.session() as session:
            session.execute_write(
                lambda tx: tx.run(query, params or {})
            )

    def execute_read(self, query, params=None):

        def run_query(tx):
            result = tx.run(query, params or {})
            return [r.data() for r in result]
    
        with self.driver.session() as session:
            return session.execute_read(run_query)

    def close(self):
        self.driver.close()
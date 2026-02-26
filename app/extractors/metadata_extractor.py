class MetadataExtractor:

    def __init__(self, connector):
        self.connector = connector

    def get_table_lineage(self):
        query = """
        SELECT DISTINCT
        source_table_full_name,
        target_table_full_name
    FROM system.access.table_lineage
    WHERE source_table_full_name IS NOT NULL
      AND target_table_full_name IS NOT NULL
        """
        return self.connector.execute_query(query)

    def get_column_lineage(self):
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
        return self.connector.execute_query(query)

    def get_data_profile(self, catalog):
        query = f"""
        SELECT *
        FROM {catalog}.silver_pharma_sales.data_profile
        """
        return self.connector.execute_query(query)
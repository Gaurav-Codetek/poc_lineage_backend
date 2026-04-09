class CatalogMetadataExtractor:

    def __init__(self, connector):
        self.connector = connector

    def get_all_tables(self):
        query = """
        SELECT
            table_catalog AS catalog_name,
            table_schema AS schema_name,
            table_name,
            table_type,
            comment,
            data_source_format,
            storage_path
        FROM system.information_schema.tables
        WHERE table_schema <> 'information_schema'
          AND table_catalog <> 'system'
        ORDER BY table_catalog, table_schema, table_name
        """
        return self.connector.execute_query(query)

    def get_all_columns(self):
        query = """
        SELECT
            table_catalog AS catalog_name,
            table_schema AS schema_name,
            table_name,
            column_name,
            ordinal_position,
            data_type,
            is_nullable,
            comment
        FROM system.information_schema.columns
        WHERE table_schema <> 'information_schema'
          AND table_catalog <> 'system'
        ORDER BY table_catalog, table_schema, table_name, ordinal_position
        """
        return self.connector.execute_query(query)

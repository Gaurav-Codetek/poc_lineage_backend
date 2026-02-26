from rapidfuzz import fuzz
import networkx as nx
import pandas as pd
import numpy as np

class EnhancedTableClassifier:

    def __init__(self, neo4j, profile_df):
        self.neo4j = neo4j
        self.profile = profile_df

    # -------- Numeric density --------

    def _numeric_density(self):

        numeric_types = [
            "int", "bigint", "double",
            "float", "decimal", "numeric"
        ]

        df = self.profile.copy()

        df["is_numeric"] = df["data_type"].str.lower().apply(
            lambda x: any(t in x for t in numeric_types)
        )

        grouped = df.groupby(
            ["catalog_name", "schema_name", "table_name"]
        ).agg(
            total_cols=("column_name", "count"),
            numeric_cols=("is_numeric", "sum")
        ).reset_index()

        # -------- Solution 3 --------
        grouped["numeric_density"] = (
            grouped["numeric_cols"] /
            np.sqrt(grouped["total_cols"])
        )

        grouped["full_name"] = (
            grouped["catalog_name"] + "." +
            grouped["schema_name"] + "." +
            grouped["table_name"]
        )

        return grouped[
            ["full_name", "numeric_density",
             "numeric_cols", "total_cols"]
        ]
    # -------- Improved FK connectivity --------
    def _fk_connectivity(self):

        query = """
        MATCH (t:Table)-[:HAS_COLUMN]->(c)-[r:FK_RELATION]->(pk)
        WHERE r.confidence > 0.5
        MATCH (pk)<-[:HAS_COLUMN]-(dim:Table)
        RETURN t.full_name AS full_name,
               count(DISTINCT c) AS fk_columns,
               count(DISTINCT dim) AS dim_connections
        """

        results = self.neo4j.execute_read(query)

        df = pd.DataFrame(results)

        if df.empty:
            return pd.DataFrame(
                columns=["full_name", "fk_columns", "dim_connections"]
            )

        return df

    # -------- Classification --------
    def classify(self):

        numeric = self._numeric_density()
        fk = self._fk_connectivity()

        df = pd.merge(
            numeric,
            fk,
            on="full_name",
            how="left"
        )

        df["fk_columns"] = df["fk_columns"].fillna(0)
        df["dim_connections"] = df["dim_connections"].fillna(0)

        # ---------- fact score ----------
        df["fact_score"] = (
            df["fk_columns"] * 0.5 +
            df["dim_connections"] * 0.3 +
            df["numeric_density"] * 0.2
            )

        # adaptive threshold
        threshold = df["fact_score"].median()

        df["type"] = df["fact_score"].apply(
            lambda x: "FACT" if x > threshold else "DIM"
        )

        return df
class KeyInferenceEngine:

    def __init__(self, profile_df):
        self.profile_df = profile_df

    def detect_primary_keys(self):
        return self.profile_df[
            (self.profile_df["uniqueness_ratio"] > 0.95) &
            (self.profile_df["null_ratio"] < 0.05)
        ]


def containment_test(connector, fact_table, fact_col, dim_table, dim_col):

    # sample based fast check
    query = f"""
    SELECT COUNT(*) AS unmatched
    FROM (
        SELECT DISTINCT {fact_col}
        FROM {fact_table}
        LIMIT 1000
    ) f
    LEFT JOIN {dim_table} d
    ON f.{fact_col} = d.{dim_col}
    WHERE d.{dim_col} IS NULL
    """

    result = connector.execute_query(query)
    return result["unmatched"].iloc[0] == 0


def detect_fk_for_target(
        connector,
        target_table,
        pk_df,
        family_profile):

    relationships = []

    target_columns = family_profile[
        family_profile["full_name"] == target_table
    ]

    for _, pk in pk_df.iterrows():

        dim_table = (
            f"{pk['catalog_name']}."
            f"{pk['schema_name']}."
            f"{pk['table_name']}"
        )

        if dim_table == target_table:
            continue

        for _, col in target_columns.iterrows():

            # datatype filter
            if col["data_type"] != pk["data_type"]:
                continue

            # cardinality filter
            if col["distinct_count"] > pk["distinct_count"]:
                continue

            # null filter
            if col["null_ratio"] > 0.5:
                continue

            # naming similarity
            score = fuzz.partial_ratio(
                col["column_name"],
                pk["column_name"]
            )

            if score < 70:
                continue

            try:
                if containment_test(
                        connector,
                        target_table,
                        col["column_name"],
                        dim_table,
                        pk["column_name"]
                ):
                    relationships.append({
                        "target_table": target_table,
                        "fk_column": col["column_name"],
                        "dimension_table": dim_table,
                        "pk_column": pk["column_name"]
                    })
            except Exception:
                continue

    return relationships


def lineage_distance(graph, source, target):
    try:
        return nx.shortest_path_length(graph.graph, source, target)
    except Exception:
        return 5  # large distance

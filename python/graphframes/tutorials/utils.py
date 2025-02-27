"""Utilities for Network Moitif Finding Tutorial"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from graphframes import GraphFrame


def three_edge_count(paths: DataFrame) -> DataFrame:
    """three_edge_count View the counts of the different types of 3-node graphlets in the graph.

    Parameters
    ----------
    paths : pyspark.sql.DataFrame
        A DataFrame of 3-paths in the graph.

    Returns
    -------
    DataFrame
        A DataFrame of the counts of the different types of 3-node graphlets in the graph.
    """  # noqa: E501
    graphlet_type_df = paths.select(
        F.col("a.Type").alias("A_Type"),
        F.col("e1.relationship").alias("E_relationship"),
        F.col("b.Type").alias("B_Type"),
        F.col("e2.relationship").alias("E2_relationship"),
        F.col("c.Type").alias("C_Type"),
        F.col("e3.relationship").alias("E3_relationship"),
        F.when(F.col("d").isNotNull(), F.col("d.Type")).alias("D_Type"),
    )
    graphlet_count_df = (
        graphlet_type_df.groupby(
            "A_Type", "E_relationship", "B_Type", "E2_relationship", "C_Type", "E3_relationship"
        )
        .count()
        .orderBy(F.col("count").desc())
        # Add a comma formatted column for display
        .withColumn("count", F.format_number(F.col("count"), 0))
    )
    return graphlet_count_df


def four_edge_count(paths: DataFrame) -> DataFrame:
    """four_edge_count View the counts of the different types of 4-node graphlets in the graph.

    Parameters
    ----------
    paths : DataFrame
        A DataFrame of 4-paths in the graph.

    Returns
    -------
    DataFrame
        A DataFrame of the counts of the different types of 4-node graphlets in the graph.
    """

    graphlet_type_df = paths.select(
        F.col("a.Type").alias("A_Type"),
        F.col("e1.relationship").alias("E_relationship"),
        F.col("b.Type").alias("B_Type"),
        F.col("e2.relationship").alias("E2_relationship"),
        F.col("c.Type").alias("C_Type"),
        F.col("e3.relationship").alias("E3_relationship"),
        F.col("d.Type").alias("D_Type"),
        F.col("e4.relationship").alias("E4_relationship"),
        F.when(F.col("e").isNotNull(), F.col("e.Type")).alias("E_Type"),
    )
    graphlet_count_df = (
        graphlet_type_df.groupby(
            "A_Type",
            "E_relationship",
            "B_Type",
            "E2_relationship",
            "C_Type",
            "E3_relationship",
            "D_Type",
            "E4_relationship",
        )
        .count()
        .orderBy(F.col("count").desc())
        # Add a comma formatted column for display
        .withColumn("count", F.format_number(F.col("count"), 0))
    )
    return graphlet_count_df


def add_degree(g: GraphFrame) -> GraphFrame:
    """add_degree compute the degree, adding it as a property of the nodes in the GraphFrame.

    Parameters
    ----------
    g : GraphFrame
        Any valid GraphFrame

    Returns
    -------
    GraphFrame
        Same GraphFrame with a 'degree' property added
    """
    degree_vertices: DataFrame = g.vertices.join(g.degrees, on="id")
    return GraphFrame(degree_vertices, g.edges)


def add_type_degree(g: GraphFrame) -> DataFrame:
    """add_type_degree add a map property to the vertices with the degree by each type of relationship.

    Parameters
    ----------
    g : GraphFrame
        Any valid GraphFrame

    Returns
    -------
    DataFrame - I am broke, next line is wrong
        A GraphFrame with a map[type:degree] 'type_degree' field added to the vertices
    """  # noqa: E501
    type_degree: DataFrame = (
        g.edges.select(F.col("src").alias("id"), "relationship")
        .filter(F.col("id").isNotNull())
        .groupby("id", "relationship")
        .count()
    )
    type_degree = type_degree.withColumn("type_degree", F.create_map(type_degree.columns))
    type_degree = type_degree.select("src", "type_degree")
    return g.vertices.join(type_degree, on="src")

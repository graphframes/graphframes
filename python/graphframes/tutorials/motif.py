# Demonstrate GraphFrames network motif finding capabilities

#
# Interactive Usage: pyspark --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12
#
# Batch Usage: spark-submit --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12 python/graphframes/tutorials/motif.py
#

import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

from graphframes import GraphFrame

# Initialize a SparkSession
spark: SparkSession = (
    SparkSession.builder.appName("Stack Overflow Motif Analysis")
    # Lets the Id:(Stack Overflow int) and id:(GraphFrames ULID) coexist
    .config("spark.sql.caseSensitive", True)
    .getOrCreate()
)
sc: SparkContext = spark.sparkContext
sc.setCheckpointDir("/tmp/graphframes-checkpoints")

# Change me if you download a different stackexchange site
STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
BASE_PATH = f"python/graphframes/tutorials/data/{STACKEXCHANGE_SITE}"


#
# Load the nodes and edges from disk, repartition, checkpoint [plan got long for some reason] and cache. 
#

# We created these in stackexchange.py from Stack Exchange data dump XML files
NODES_PATH: str = f"{BASE_PATH}/Nodes.parquet"
nodes_df: DataFrame = spark.read.parquet(NODES_PATH)

# Repartition the nodes to give our motif searches parallelism
nodes_df = nodes_df.repartition(50).checkpoint().cache()

# We created these in stackexchange.py from Stack Exchange data dump XML files
EDGES_PATH: str = f"{BASE_PATH}/Edges.parquet"
edges_df: DataFrame = spark.read.parquet(EDGES_PATH)

# Repartition the edges to give our motif searches parallelism
edges_df = edges_df.repartition(50).checkpoint().cache()

# What kind of nodes we do we have to work with?
node_counts = (
    nodes_df
    .select("id", F.col("Type").alias("Node Type"))
    .groupBy("Node Type")
    .count()
    .orderBy(F.col("count").desc())
    # Add a comma formatted column for display
    .withColumn("count", F.format_number(F.col("count"), 0))
)
node_counts.show()

# What kind of edges do we have to work with?
edge_counts = (
    edges_df
    .select("src", "dst", F.col("relationship").alias("Edge Type"))
    .groupBy("Edge Type")
    .count()
    .orderBy(F.col("count").desc())
    # Add a comma formatted column for display
    .withColumn("count", F.format_number(F.col("count"), 0))
)
edge_counts.show()

g = GraphFrame(nodes_df, edges_df)  

g.vertices.show(10)
print(f"Node columns: {g.vertices.columns}")

g.edges.sample(0.0001).show(10)

# Sanity test that all edges have valid ids
edge_count = g.edges.count()
valid_edge_count = (
    g.edges.join(g.vertices, on=g.edges.src == g.vertices.id)
    .select("src", "dst", "relationship")
    .join(g.vertices, on=g.edges.dst == g.vertices.id)
    .count()
)

# Just up and die if we have edges that point to non-existent nodes
assert (
    edge_count == valid_edge_count
), f"Edge count {edge_count} != valid edge count {valid_edge_count}"
print(f"Edge count: {edge_count:,} == Valid edge count: {valid_edge_count:,}")

# G4: Continuous Triangles
paths = g.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")

# Show the first path
paths.show(3)

graphlet_type_df = paths.select(
    F.col("a.Type").alias("A_Type"),
    F.col("e1.relationship").alias("(a)-[e1]->(b)"),
    F.col("b.Type").alias("B_Type"),
    F.col("e2.relationship").alias("(b)-[e2]->(c)"),
    F.col("c.Type").alias("C_Type"),
    F.col("e3.relationship").alias("(c)-[e3]->(a)"),
)

graphlet_count_df = (
    graphlet_type_df.groupby(
        "A_Type", "(a)-[e1]->(b)", "B_Type", "(b)-[e2]->(c)", "C_Type", "(c)-[e3]->(a)"
    )
    .count()
    .orderBy(F.col("count").desc())
    # Add a comma formatted column for display
    .withColumn("count", F.format_number(F.col("count"), 0))
)
graphlet_count_df.show()

# G5: Divergent Triangles
paths = g.find("(a)-[e1]->(b); (a)-[e2]->(c); (c)-[e3]->(b)")

graphlet_type_df = paths.select(
    F.col("a.Type").alias("A_Type"),
    F.col("e1.relationship").alias("(a)-[e1]->(b)"),
    F.col("b.Type").alias("B_Type"),
    F.col("e2.relationship").alias("(a)-[e2]->(c)"),
    F.col("c.Type").alias("C_Type"),
    F.col("e3.relationship").alias("(c)-[e3]->(b)"),
)

graphlet_count_df = (
    graphlet_type_df.groupby(
        "A_Type", "(a)-[e1]->(b)", "B_Type", "(a)-[e2]->(c)", "C_Type", "(c)-[e3]->(b)"
    )
    .count()
    .orderBy(F.col("count").desc())
    # Add a comma formatted column for display
    .withColumn("count", F.format_number(F.col("count"), 0))
)
graphlet_count_df.show()

# G17: A directed 3-path is a surprisingly diverse graphlet
paths = g.find("(a)-[e1]->(b); (b)-[e2]->(c); (d)-[e3]->(c)")

# Visualize the four-path by counting instances of paths by node / edge type
graphlet_type_df = paths.select(
    F.col("a.Type").alias("A_Type"),
    F.col("e1.relationship").alias("(a)-[e1]->(b)"),
    F.col("b.Type").alias("B_Type"),
    F.col("e2.relationship").alias("(b)-[e2]->(c)"),
    F.col("c.Type").alias("C_Type"),
    F.col("e3.relationship").alias("(d)-[e3]->(c)"),
    F.col("d.Type").alias("D_Type"),
)
graphlet_count_df = (
    graphlet_type_df.groupby(
        "A_Type",
        "(a)-[e1]->(b)",
        "B_Type",
        "(b)-[e2]->(c)",
        "C_Type",
        "(d)-[e3]->(c)",
        "D_Type",
    )
    .count()
    .orderBy(F.col("count").desc())
    # Add a comma formatted column for display
    .withColumn("count", F.format_number(F.col("count"), 0))
)
graphlet_count_df.show()

graphlet_count_df.orderBy([
    "A_Type",
    "(a)-[e1]->(b)",
    "B_Type",
    "(b)-[e2]->(c)",
    "C_Type",
    "(d)-[e3]->(c)",
    "D_Type",
], ascending=False).show(104)

# A user answers an answer that answers a question that links to an answer.
linked_vote_paths = paths.filter(
    (F.col("a.Type") == "Vote") &
    (F.col("e1.relationship") == "CastFor") &
    (F.col("b.Type") == "Question") &
    (F.col("e2.relationship") == "Links") &
    (F.col("c.Type") == "Question") &
    (F.col("e3.relationship") == "CastFor") &
    (F.col("d.Type") == "Vote")
)

# Sanity check the count - it should match the table above
linked_vote_paths.count()

b_vote_counts = linked_vote_paths.select("a", "b").distinct().groupBy("b").count()
c_vote_counts = linked_vote_paths.select("c", "d").distinct().groupBy("c").count()

linked_vote_counts = (
    linked_vote_paths
    .filter((F.col("a.VoteTypeId") == 2) & (F.col("d.VoteTypeId") == 2))
    .select("b", "c")
    .join(b_vote_counts, on="b", how="inner")
    .withColumnRenamed("count", "b_count")
    .join(c_vote_counts, on="c", how="inner")
    .withColumnRenamed("count", "c_count")
)
linked_vote_counts.stat.corr("b_count", "c_count")

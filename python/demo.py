import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from graphframes import GraphFrame

# Setup our SparkSession in case we are here through regular Python
spark: SparkSession = SparkSession.builder.appName("GraphGeeks Demo").getOrCreate()

# Change me if you download a different stackexchange site
STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
BASE_PATH = f"python/graphframes/tutorials/data/{STACKEXCHANGE_SITE}"
NODE_PATH = f"{BASE_PATH}/Nodes.parquet"
EDGE_PATH = f"{BASE_PATH}/Edges.parquet"

# Load our nodes and edges... in GraphFrames we call nodes vertices
vertex_df = spark.read.parquet(NODE_PATH)
edge_df = spark.read.parquet(EDGE_PATH)

# Instantiate our GraphFrame
g = GraphFrame(vertex_df, edge_df)

# Count the nodes and edges
print(f"Vertex count: {g.vertices.count():,}")
print(f"Edge count: {edge_df.count():,}")

# What kind of nodes we do we have to work with?
node_counts = (
    vertex_df
    .select("id", F.col("Type").alias("Node Type"))
    .groupBy("Node Type")
    .count()
    .orderBy(F.col("count").desc())
    # Add a comma formatted column for display
    .withColumn("count", F.format_number(F.col("count"), 0))
)
node_counts.show()

# Saving a GraphFrame as a Delta Table
g.vertices.write.mode("overwrite").saveAsTable("vertices")
g.edges.write.mode("overwrite").saveAsTable("edges")

# Loading a GraphFrame back from a Delta Table
g = GraphFrame(
    spark.read.table("vertices"),
    spark.read.table("edges")
)

#
# Touring some algorithms
#

g.pageRank(resetProbability=0.15, maxIter=10)
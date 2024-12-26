# Demonstrate GraphFrames network motif finding capabilities

#
# Interactive Usage: pyspark --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12
#
# Batch Usage: spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 python/graphframes/examples/motif.py
#

import os
import sys

# This is needed for the utils import... need to set this project up as a proper package!
os.chdir("python/graphframes/examples")
sys.path.append(os.getcwd())

import pyspark.sql.functions as F
from graphframes import GraphFrame
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from utils import three_edge_count, four_edge_count, add_degree, add_type_degree

#
# Initialize a SparkSession. You can configre SparkSession via: .config("spark.some.config.option", "some-value")
#

spark: SparkSession = (
    SparkSession.builder.appName("Stack Overflow Motif Analysis")
    # Lets the Id:(Stack Overflow int) and id:(GraphFrames UUID) coexist
    .config("spark.sql.caseSensitive", True)
    .getOrCreate()
)
sc: SparkContext = spark.sparkContext

# Change STACKEXCHANGE_SITE if you download a different stackexchange site
STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
BASE_PATH = f"python/graphframes/examples/data/{STACKEXCHANGE_SITE}"


#
# Load the nodes from disk and cache. GraphFrames likes nodes/vertices and edges/relatonships to be cached.
#

# We created these in stackexchange.py from Stack Exchange data dump XML files
NODES_PATH: str = f"{BASE_PATH}/Nodes.parquet"
nodes_df: DataFrame = spark.read.parquet(NODES_PATH).cache()

# What kind of nodes we do we have to work with?
node_counts = (
    nodes_df
    .select("id", F.col("Type").alias("Node Type"))
    .groupBy("Node Type")
    .count()
    .orderBy(F.col("count").desc())
)
node_counts.show()

# We created these in stackexchange.py from Stack Exchange data dump XML files
EDGES_PATH: str = f"{BASE_PATH}/Edges.parquet"
edges_df: DataFrame = spark.read.parquet(EDGES_PATH).cache()

# What kind of edges do we have to work with?
edge_counts = (
    edges_df
    .select("src", "dst", F.col("relationship").alias("Edge Type"))
    .groupBy("Edge Type")
    .count()
    .orderBy(F.col("count").desc())
)
edge_counts.show()

#
# Create the GraphFrame
#
g = GraphFrame(nodes_df, edges_df)

# Add the degree to use as a property in the motifs
g = add_degree(g).cache()

print(f"Node columns: {g.vertices.columns}")

g.vertices.show()
g.edges.show()

# You can find a list of directed motifs here: https://www.nature.com/articles/srep3509

# G4: Continuous Triangles
paths = g.find("(a)-[e]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")
three_edge_count(paths).show()

# G5: Divergent Triangles
paths = g.find("(a)-[e]->(b); (a)-[e2]->(c); (c)-[e3]->(b)")
three_edge_count(paths).show()

# G6: Continuous Path
paths = g.find("(a)-[e]->(b); (b)-[e2]->(c); (c)-[e3]->(d)")
three_edge_count(paths).show()

# G6: Continuous Path Votes with VoteTypes
graphlet_type_df = paths.select(
    F.col("a.Type").alias("A_Type"),
    F.col("a.VoteType").alias("A_VoteType"),
    F.col("e.relationship").alias("E_relationship"),
    F.col("b.Type").alias("B_Type"),
    F.col("e2.relationship").alias("E2_relationship"),
    F.col("c.Type").alias("C_Type"),
    F.col("e3.relationship").alias("E3_relationship"),
)
graphlet_count_df = (
    graphlet_type_df.groupby(
        "A_Type",
        "A_VoteType",
        "E_relationship",
        "B_Type",
        "E2_relationship",
        "C_Type",
        "E3_relationship",
    )
    .count()
    .filter(F.col("A_Type") == "Vote")
    .orderBy(F.col("count").desc())
)
graphlet_count_df.show()

# +------+----------+--------------+------+---------------+------+---------------+-----+
# |A_Type|A_VoteType|E_relationship|B_Type|E2_relationship|C_Type|E3_relationship|count|
# +------+----------+--------------+------+---------------+------+---------------+-----+
# |  Vote|    UpVote|       CastFor|  Post|        Answers|  Post|          Links|27197|
# |  Vote|    UpVote|       CastFor|  Post|          Links|  Post|          Links|22947|
# |  Vote|  DownVote|       CastFor|  Post|        Answers|  Post|          Links| 2129|
# |  Vote|  DownVote|       CastFor|  Post|          Links|  Post|          Links| 1503|
# |  Vote|   Unknown|       CastFor|  Post|        Answers|  Post|          Links|  523|
# |  Vote|   Unknown|       CastFor|  Post|          Links|  Post|          Links|  165|
# |  Vote|      Spam|       CastFor|  Post|          Links|  Post|          Links|   18|
# |  Vote|    UpVote|       CastFor|  Post|          Links|  Post|        Answers|   17|
# |  Vote|      Spam|       CastFor|  Post|        Answers|  Post|          Links|   16|
# |  Vote|Undeletion|       CastFor|  Post|        Answers|  Post|          Links|    1|
# |  Vote|   Unknown|       CastFor|  Post|          Links|  Post|        Answers|    1|
# |  Vote|    Reopen|       CastFor|  Post|          Links|  Post|          Links|    1|
# +------+----------+--------------+------+---------------+------+---------------+-----+

# G10: Convergent Triangle
paths = g.find("(a)-[e]->(b); (c)-[e2]->(a); (d)-[e3]->(a)")
three_edge_count(paths).show()

# G10: Convergent Triangle with VoteTypes
paths = g.find("(a)-[e]->(b); (c)-[e2]->(a); (d)-[e3]->(a)")
graphlet_type_df = paths.select(
    F.col("a.Type").alias("A_Type"),
    F.col("e.relationship").alias("E_relationship"),
    F.col("b.Type").alias("B_Type"),
    F.col("e2.relationship").alias("E2_relationship"),
    F.col("c.Type").alias("C_Type"),
    F.col("c.VoteType").alias("C_VoteType"),
    F.col("e3.relationship").alias("E3_relationship"),
    F.col("d.Type").alias("D_Type"),
    F.col("d.VoteType").alias("D_VoteType"),
)
graphlet_count_df = (
    graphlet_type_df.groupby(
        "A_Type",
        "E_relationship",
        "B_Type",
        "E2_relationship",
        "C_Type",
        "C_VoteType",
        "E3_relationship",
        "D_Type",
        "D_VoteType",
    )
    .count()
    .filter(F.col("C_Type") == "Vote")
    .filter(F.col("E3_relationship") == "CastFor")
    .orderBy(F.col("count").desc())
)
graphlet_count_df.show()

# +------+--------------+------+---------------+------+----------+---------------+------+----------+------+
# |A_Type|E_relationship|B_Type|E2_relationship|C_Type|C_VoteType|E3_relationship|D_Type|D_VoteType| count|
# +------+--------------+------+---------------+------+----------+---------------+------+----------+------+
# |  Post|       Answers|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|    UpVote|313807|
# |  Post|         Links|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|    UpVote|271944|
# |  Post|       Answers|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|   Unknown|  8159|
# |  Post|       Answers|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|    UpVote|  8159|
# |  Post|         Links|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|  DownVote|  6749|
# |  Post|         Links|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|    UpVote|  6749|
# |  Post|       Answers|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|    UpVote|  6586|
# |  Post|       Answers|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|  DownVote|  6586|
# |  Post|         Links|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|  DownVote|  4266|
# |  Post|       Answers|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|  DownVote|  4190|
# |  Post|         Links|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|   Unknown|  1617|
# |  Post|         Links|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|    UpVote|  1617|
# |  Post|       Answers|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|   Unknown|   919|
# |  Post|         Links|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|  DownVote|   317|
# |  Post|         Links|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|   Unknown|   317|
# |  Post|         Links|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|   Unknown|   239|
# |  Post|       Answers|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|  DownVote|   113|
# |  Post|       Answers|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|   Unknown|   113|
# |  Post|       Answers|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|      Spam|    92|
# |  Post|       Answers|  Post|        CastFor|  Vote|      Spam|        CastFor|  Vote|    UpVote|    92|
# +------+--------------+------+---------------+------+----------+---------------+------+----------+------+
# only showing top 20 rows

# G14: Cyclic Quadrilaterals
paths = g.find("(a)-[e]->(b); (b)-[e2]->(c); (c)-[e3]->(d); (d)-[e4]->(a)")
four_edge_count(paths).show()

# Shows two matches:
# - (Post)-[Answers]->(Post)<--[Posted]-(User)
# - (Post)-[Answers]->(Post)<--[VotedFor]-(User)
paths = g.find("(a)-[e]->(b); (c)-[e2]->(a)")
paths.select("a.Type", "e.*", "b.Type", "e2.*", "c.Type").show()

# Figure out how often questions are answered and the question poster votes for the answer. Neat!
# Shows (User A)-[Posted]->(Post)<-[Answers]-(Post)<-[VotedFor]-(User)
paths = g.find("(a)-[e]->(b); (c)-[e2]->(b); (d)-[e3]->(c)")

# If the node types are right...
paths = paths.filter(F.col("a.Type") == "User")
paths = paths.filter(F.col("b.Type") == "Post")
paths = paths.filter(F.col("c.Type") == "Post")
paths = paths.filter(F.col("d.Type") == "User")

# If the edge types are right...
# paths = paths.filter(F.col("e.relationship") == "Posted")
# paths = paths.filter(F.col("e2.relationship") == "Answers")
paths = paths.filter(F.col("e3.relationship") == "VotedFor")

paths.select(
    "a.Type",
    "e.relationship",
    "b.Type",
    "e2.relationship",
    "c.Type",
    "e3.relationship",
    "d.Type",
).show()

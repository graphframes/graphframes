"""Demonstrate GraphFrames Pregel API capabilities. Code from the Pregel Tutorial.

This script contains 7 progressive examples showing how to use GraphFrames'
Pregel and AggregateMessages APIs for scalable graph algorithms.

Interactive Usage:
    pyspark --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12

Batch Usage (Spark 4.x):
    spark-submit \\
        --packages io.graphframes:graphframes-spark4_2.13:0.10.1 \\
        python/graphframes/tutorials/pregel.py

Batch Usage (Spark 3.5.x):
    spark-submit \\
        --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12 \\
        python/graphframes/tutorials/pregel.py
"""

import click
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from graphframes.lib import Pregel

# ──────────────────────────────────────────────────────────────────────
# SparkSession
# ──────────────────────────────────────────────────────────────────────

spark: SparkSession = (
    SparkSession.builder.appName("Pregel Tutorial")
    .config("spark.sql.caseSensitive", True)
    .getOrCreate()
)
spark.sparkContext.setCheckpointDir("/tmp/graphframes-checkpoints/pregel")
spark.sparkContext.setLogLevel("WARN")

# ──────────────────────────────────────────────────────────────────────
# Load Stack Exchange Data
# ──────────────────────────────────────────────────────────────────────

STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
BASE_PATH = f"python/graphframes/tutorials/data/{STACKEXCHANGE_SITE}"
NODES_PATH: str = f"{BASE_PATH}/Nodes.parquet"
EDGES_PATH: str = f"{BASE_PATH}/Edges.parquet"

click.echo("\n" + "=" * 70)
click.echo("Loading Stack Exchange data...")
click.echo("=" * 70)

nodes_df: DataFrame = spark.read.parquet(NODES_PATH)
nodes_df = nodes_df.repartition(50).checkpoint().cache()

edges_df: DataFrame = spark.read.parquet(EDGES_PATH)
edges_df = edges_df.repartition(50).checkpoint().cache()

g = GraphFrame(nodes_df, edges_df)

click.echo(f"Nodes: {nodes_df.count():,}")
click.echo(f"Edges: {edges_df.count():,}")


# ======================================================================
# Example 1: In-Degree with AggregateMessages
# ======================================================================

click.echo("\n" + "=" * 70)
click.echo("EXAMPLE 1: In-Degree with AggregateMessages")
click.echo("=" * 70)

# Each source node sends 1 to its destination
msg_to_dst = AM.src["id"]  # We just need any non-null value; we'll count
am_in_degrees = g.aggregateMessages(
    F.count(AM.msg).alias("in_degree"),
    sendToDst=F.lit(1),
)

# Left join to include zero-degree nodes
complete_in_deg = (
    g.vertices.select("id", "Type")
    .join(am_in_degrees, on="id", how="left")
    .na.fill(0, ["in_degree"])
)

click.echo("\nIn-degree distribution (AggregateMessages):")
complete_in_deg.groupBy("in_degree").count().orderBy("in_degree").show(10)

click.echo("Top 10 nodes by in-degree:")
complete_in_deg.orderBy(F.desc("in_degree")).show(10)


# ======================================================================
# Example 2: In-Degree with Pregel
# ======================================================================

click.echo("\n" + "=" * 70)
click.echo("EXAMPLE 2: In-Degree with Pregel")
click.echo("=" * 70)

pregel_in_degree = (
    g.pregel.setMaxIter(1)
    .withVertexColumn(
        "in_degree",
        F.lit(0),  # Initial value: 0
        F.coalesce(Pregel.msg(), F.lit(0)),  # Update: use message or keep 0
    )
    .sendMsgToDst(F.lit(1))  # Send 1 to each destination
    .aggMsgs(F.sum(Pregel.msg()))  # Sum all received messages
    .run()
)

click.echo("\nIn-degree distribution (Pregel):")
pregel_in_degree.select("in_degree").groupBy("in_degree").count().orderBy("in_degree").show(10)

click.echo("Top 10 nodes by in-degree (Pregel):")
pregel_in_degree.select("id", "Type", "in_degree").orderBy(F.desc("in_degree")).show(10)


# ======================================================================
# Example 3: PageRank with Pregel
# ======================================================================

click.echo("\n" + "=" * 70)
click.echo("EXAMPLE 3: PageRank with Pregel")
click.echo("=" * 70)

# Compute out-degrees
out_degrees = g.outDegrees.withColumnRenamed("outDegree", "out_degree")
pr_vertices = (
    nodes_df.join(out_degrees, on="id", how="left").na.fill(1, ["out_degree"])
)
g_pr = GraphFrame(pr_vertices, edges_df)

# PageRank parameters
num_vertices = g_pr.vertices.count()
damping = 0.85
max_iter = 10

click.echo(f"Running PageRank: {num_vertices:,} vertices, damping={damping}, max_iter={max_iter}")

pr_results = (
    g_pr.pregel.setMaxIter(max_iter)
    .withVertexColumn(
        "pagerank",
        F.lit(1.0 / num_vertices),
        F.coalesce(Pregel.msg(), F.lit(0.0)) * F.lit(damping)
        + F.lit((1.0 - damping) / num_vertices),
    )
    .sendMsgToDst(Pregel.src("pagerank") / Pregel.src("out_degree"))
    .aggMsgs(F.sum(Pregel.msg()))
    .run()
)

click.echo("\nTop 20 nodes by PageRank:")
pr_results.select("id", "Type", "pagerank").orderBy(F.desc("pagerank")).show(20)

# Show by type
click.echo("Top 10 Questions by PageRank:")
(
    pr_results.filter(F.col("Type") == "Question")
    .select("id", "Title", "pagerank")
    .orderBy(F.desc("pagerank"))
    .show(10, truncate=50)
)

click.echo("Top 10 Users by PageRank:")
(
    pr_results.filter(F.col("Type") == "User")
    .select("id", "DisplayName", "Reputation", "pagerank")
    .orderBy(F.desc("pagerank"))
    .show(10, truncate=50)
)

# Compare rankings with built-in PageRank
# Note: the built-in PageRank may use different normalization so absolute values differ,
# but the relative rankings should be very similar.
click.echo("\nComparing rankings with built-in PageRank...")
builtin_pr = g.pageRank(resetProbability=1 - damping, maxIter=max_iter)

pregel_ranked = (
    pr_results.select("id", F.col("pagerank").alias("pregel_pr"))
    .withColumn("pregel_rank", F.dense_rank().over(
        Window.orderBy(F.desc("pregel_pr"))
    ))
)
builtin_ranked = (
    builtin_pr.vertices.select("id", F.col("pagerank").alias("builtin_pr"))
    .withColumn("builtin_rank", F.dense_rank().over(
        Window.orderBy(F.desc("builtin_pr"))
    ))
)
comparison = pregel_ranked.join(builtin_ranked, on="id")

click.echo("Top 10 comparison (Pregel rank vs Built-in rank):")
(
    comparison.filter(F.col("pregel_rank") <= 10)
    .select("id", "pregel_rank", "builtin_rank", "pregel_pr", "builtin_pr")
    .orderBy("pregel_rank")
    .show(10)
)

# Rank correlation
rank_corr = comparison.stat.corr("pregel_rank", "builtin_rank")
click.echo(f"Rank correlation (Spearman-like): {rank_corr:.4f}")

# Unpersist built-in PR result
builtin_pr.vertices.unpersist()


# ======================================================================
# Example 4: Connected Components with Pregel
# ======================================================================

click.echo("\n" + "=" * 70)
click.echo("EXAMPLE 4: Connected Components with Pregel")
click.echo("=" * 70)

# Use a simplified graph for connected components (just id and edges)
cc_vertices = g.vertices.select("id")
cc_graph = GraphFrame(cc_vertices, g.edges.select("src", "dst"))

cc_results = (
    cc_graph.pregel.setMaxIter(20)
    .setEarlyStopping(True)
    .withVertexColumn(
        "component",
        F.col("id"),  # Each vertex starts as its own component (label = own id)
        F.least(F.col("component"), F.coalesce(Pregel.msg(), F.col("component"))),
    )
    .sendMsgToDst(Pregel.src("component"))  # Send label to neighbors
    .sendMsgToSrc(Pregel.dst("component"))  # Bidirectional for undirected CC
    .aggMsgs(F.min(Pregel.msg()))  # Take the minimum label
    .run()
)

num_components = cc_results.select("component").distinct().count()
click.echo(f"\nNumber of connected components: {num_components:,}")

click.echo("Component size distribution:")
(
    cc_results.groupBy("component")
    .count()
    .orderBy(F.desc("count"))
    .withColumn("count", F.format_number(F.col("count"), 0))
    .show(10)
)

# Unpersist
cc_results.unpersist()


# ======================================================================
# Example 5: Single-Source Shortest Paths with Pregel
# ======================================================================

click.echo("\n" + "=" * 70)
click.echo("EXAMPLE 5: Shortest Paths with Pregel")
click.echo("=" * 70)

# Pick a popular question as the source
popular_question = (
    nodes_df.filter(F.col("Type") == "Question")
    .orderBy(F.desc("ViewCount"))
    .select("id", "Title")
    .first()
)
source_id = popular_question["id"]
click.echo(f"Source: {popular_question['Title']}")
click.echo(f"Source ID: {source_id}")

# Simplified graph for shortest paths
sp_vertices = g.vertices.select("id")
sp_graph = GraphFrame(sp_vertices, g.edges.select("src", "dst"))

# Use a large int as infinity
INF = 999999

sp_results = (
    sp_graph.pregel.setMaxIter(10)
    .setEarlyStopping(True)
    .withVertexColumn(
        "distance",
        F.when(F.col("id") == source_id, F.lit(0)).otherwise(F.lit(INF)),
        F.least(
            F.col("distance"),
            F.coalesce(Pregel.msg(), F.lit(INF)),
        ),
    )
    .sendMsgToDst(
        F.when(
            Pregel.src("distance") < F.lit(INF),
            Pregel.src("distance") + F.lit(1),
        )
    )
    .sendMsgToSrc(
        F.when(
            Pregel.dst("distance") < F.lit(INF),
            Pregel.dst("distance") + F.lit(1),
        )
    )
    .aggMsgs(F.min(Pregel.msg()))
    .run()
)

click.echo("\nDistance distribution from source question:")
sp_results.filter(F.col("distance") < INF).groupBy("distance").count().orderBy(
    "distance"
).show(20)

reachable = sp_results.filter(F.col("distance") < INF).count()
total = sp_results.count()
click.echo(f"Reachable vertices: {reachable:,} / {total:,} ({reachable/total:.1%})")

# Unpersist
sp_results.unpersist()


# ======================================================================
# Example 6: Reputation Propagation with Pregel
# ======================================================================

click.echo("\n" + "=" * 70)
click.echo("EXAMPLE 6: Reputation Propagation with Pregel")
click.echo("=" * 70)

# Build a subgraph: Users -> (Posts) -> Answers -> (Answers) -> Questions
# We want to propagate user reputation through the answer graph

# Get user nodes with Reputation
user_nodes = nodes_df.filter(F.col("Type") == "User").select(
    "id", F.col("Reputation").cast("double").alias("reputation"), F.lit("User").alias("Type")
)

# Get answer nodes
answer_nodes = nodes_df.filter(F.col("Type") == "Answer").select(
    "id", F.col("Score").cast("double").alias("score"), F.lit("Answer").alias("Type")
)

# Get question nodes
question_nodes = nodes_df.filter(F.col("Type") == "Question").select(
    "id", F.col("ViewCount").cast("double").alias("views"), F.lit("Question").alias("Type")
)

# Build unified vertices for the reputation subgraph
rep_vertices = (
    user_nodes.withColumn("score", F.lit(0.0))
    .withColumn("views", F.lit(0.0))
    .unionByName(
        answer_nodes.withColumn("reputation", F.lit(0.0)).withColumn("views", F.lit(0.0))
    )
    .unionByName(
        question_nodes.withColumn("reputation", F.lit(0.0)).withColumn("score", F.lit(0.0))
    )
    .na.fill(0.0)
)

# Get edges: User->Answer (Posts) and Answer->Question (Answers)
posts_edges = edges_df.filter(F.col("relationship") == "Posts").select("src", "dst")
answers_edges = edges_df.filter(F.col("relationship") == "Answers").select("src", "dst")
rep_edges = posts_edges.unionByName(answers_edges)

rep_graph = GraphFrame(rep_vertices, rep_edges)

click.echo(f"Reputation subgraph: {rep_vertices.count():,} vertices, {rep_edges.count():,} edges")

# Propagate reputation: User rep flows through Answers to Questions
# Iteration 1: Users send reputation to their Answers
# Iteration 2: Answers (now carrying user rep) send to Questions
rep_results = (
    rep_graph.pregel.setMaxIter(2)
    .withVertexColumn(
        "authority",
        F.col("reputation"),  # Users start with their reputation; others start at 0
        F.coalesce(Pregel.msg(), F.lit(0.0)) + F.col("authority"),
    )
    .sendMsgToDst(
        # Send reputation weighted by answer score
        F.when(
            Pregel.src("authority") > F.lit(0),
            Pregel.src("authority"),
        )
    )
    .aggMsgs(F.sum(Pregel.msg()))
    .run()
)

click.echo("\nTop 20 Questions by propagated authority (reputation from answerers):")
(
    rep_results.filter(F.col("Type") == "Question")
    .select("id", "authority")
    .orderBy(F.desc("authority"))
    .show(20)
)

# Join with original question data for interpretability
top_questions = (
    rep_results.filter(F.col("Type") == "Question")
    .select(F.col("id"), F.col("authority"))
    .join(
        nodes_df.filter(F.col("Type") == "Question").select("id", "Title", "ViewCount"),
        on="id",
    )
    .orderBy(F.desc("authority"))
)

click.echo("Top 10 Questions with titles and authority scores:")
top_questions.show(10, truncate=60)

# Unpersist
rep_results.unpersist()


# ======================================================================
# Example 7: Debug Trace - Message Path Tracking
# ======================================================================

click.echo("\n" + "=" * 70)
click.echo("EXAMPLE 7: Debug Trace - Message Path Tracking")
click.echo("=" * 70)

# Use a small test graph for clarity
test_vertices = spark.createDataFrame(
    [("A", "Alice"), ("B", "Bob"), ("C", "Charlie"), ("D", "David")],
    ["id", "name"],
)
test_edges = spark.createDataFrame(
    [("A", "B"), ("A", "C"), ("B", "C"), ("C", "D")],
    ["src", "dst"],
)
test_graph = GraphFrame(test_vertices, test_edges)

click.echo("Test graph: A->B, A->C, B->C, C->D")
click.echo("\nTracking message paths through 3 iterations of Pregel...")

# Track paths: each vertex accumulates the path of messages it receives
trace_results = (
    test_graph.pregel.setMaxIter(3)
    .withVertexColumn(
        "trace",
        F.col("id"),  # Start with own id
        F.concat_ws(
            " <- ",
            F.coalesce(Pregel.msg(), F.lit("")),
            F.col("id"),
        ),
    )
    .sendMsgToDst(Pregel.src("trace"))
    .aggMsgs(
        # Collect all incoming traces and join them
        F.concat_ws(" | ", F.collect_list(Pregel.msg()))
    )
    .run()
)

click.echo("\nVertex traces after 3 iterations:")
trace_results.select("id", "name", "trace").orderBy("id").show(truncate=False)

click.echo(
    "\nEach vertex shows who influenced it. Reading right-to-left:\n"
    "  'X <- Y <- Z' means Z's state flowed through Y to reach X.\n"
    "  '|' separates independent paths arriving at the same vertex."
)

# Unpersist
trace_results.unpersist()


# ──────────────────────────────────────────────────────────────────────
# Cleanup
# ──────────────────────────────────────────────────────────────────────
click.echo("\n" + "=" * 70)
click.echo("All 7 examples complete!")
click.echo("=" * 70)

spark.stop()

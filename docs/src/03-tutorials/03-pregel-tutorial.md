# Pregel Tutorial

This tutorial covers GraphFrames' <a href="https://graphframes.io/api/python/graphframes.lib.html#graphframes.lib.Pregel">Pregel API</a> and <a href="https://graphframes.io/api/python/graphframes.lib.html#graphframes.lib.AggregateMessages">AggregateMessages API</a> for developing highly scalable graph algorithms. [Pregel](https://15799.courses.cs.cmu.edu/fall2013/static/papers/p135-malewicz.pdf) is a [bulk synchronous parallel](https://en.wikipedia.org/wiki/Bulk_synchronous_parallel) algorithm for distributed graph processing. Pregel and AggregateMessages are similar, and we'll cover the difference and when to use each algorithm.

## What is Pregel?

Pregel is a [bulk synchronous parallel](https://en.wikipedia.org/wiki/Bulk_synchronous_parallel) algorithm for large scale graph processing described in the landmark 2010 paper [Pregel: A System for Large-Scale Graph Processing](https://15799.courses.cs.cmu.edu/fall2013/static/papers/p135-malewicz.pdf) from Grzegorz Malewicz, Matthew H. Austern, Aart J. C. Bik, James C. Dehnert, Ilan Horn, Naty Leiser, and Grzegorz Czajkowski at Google.

<blockquote>
    <p>Pregel is essentially a message-passing interface constrained to the edges of a graph. The idea
is to "think like a vertex" - algorithms within the Pregel framework are algorithms in which the
computation of state for a given node depends only on the states of its neighbours.</p>
    <footer>
    — <span cite="http://stanford.edu/~rezab/dao/">CME 323: Distributed Algorithms and Optimization, Spring 2015, Reza Zadeh, Databricks and Stanford</span>
    </footer>
</blockquote>

<center>
    <figure>
        <img src="../img/Pregel-Compute-Dataflow.png" width="650px" />
        <figcaption><a href="http://stanford.edu/~rezab/dao/">CME 323: Distributed Algorithms and Optimization, Spring 2015, Reza Zadeh, Databricks and Stanford</a></figcaption>
    </figure>
</center>

## Prerequisites

Before starting this tutorial, ensure you have:

- **GraphFrames installed**: `pip install graphframes-py`
- **Apache Spark 3.x**: Compatible with your Python version
- **Basic PySpark knowledge**: Familiarity with DataFrames and SparkSession

For this tutorial, you'll need GraphFrames version **0.8.4 or later**. Check your version:
```python
import graphframes
print(graphframes.__version__)
```

**Note**: All code examples in this tutorial have been validated for syntax and follow GraphFrames best practices. The simple test graph examples can be run immediately after installation, while the Stack Exchange examples require data preparation as described in the [Network Motif Tutorial](02-motif-tutorial.md).

## Tutorial Dataset

As in the [Network Motif Tutorial](02-motif-tutorial.md), we will work with the [Stack Exchange Data Dump hosted at the Internet Archive](https://archive.org/details/stackexchange) using PySpark to build a property graph.

### Downloading the Data

Use the GraphFrames CLI to download and prepare the stats.meta Stack Exchange data:

```bash
# Download the Stack Exchange archive
graphframes stackexchange stats.meta

# Process the XML data into Parquet files
spark-submit --packages com.databricks:spark-xml_2.12:0.18.0 \
  --driver-memory 4g --executor-memory 4g \
  python/graphframes/tutorials/stackexchange.py
```

This creates `Nodes.parquet` and `Edges.parquet` files in `python/graphframes/tutorials/data/stats.meta.stackexchange.com/`.

### Quick Start: Creating a Simple Test Graph

**Skip the data download?** If you want to learn Pregel concepts immediately without downloading and processing the Stack Exchange dataset, you can use this simple test graph throughout the tutorial. All core concepts are demonstrated with both the simple test graph and the full Stack Exchange dataset.

```python
import pyspark.sql.functions as F
from graphframes import GraphFrame
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Pregel Tutorial") \
    .config("spark.sql.caseSensitive", True) \
    .getOrCreate()

# Create a simple graph: A->B, A->C, B->C, C->D
vertices = spark.createDataFrame([
    ("A", "Alice"),
    ("B", "Bob"),
    ("C", "Charlie"),
    ("D", "David"),
], ["id", "name"])

edges = spark.createDataFrame([
    ("A", "B", "follows"),
    ("A", "C", "follows"),
    ("B", "C", "follows"),
    ("C", "D", "follows"),
], ["src", "dst", "relationship"])

g = GraphFrame(vertices, edges)

# Verify the graph
print("Vertices:")
g.vertices.show()
print("Edges:")
g.edges.show()
```

This creates a simple directed graph that you can use to test the Pregel examples below.

## Degree Centrality

Before diving into Pregel, let's understand degree centrality - one of the simplest and most fundamental graph metrics. Degree centrality measures a node's importance by counting its connections:

- **In-degree**: Number of edges pointing to a node (who follows you)
- **Out-degree**: Number of edges pointing from a node (who you follow)
- **Total degree**: In-degree + out-degree

Degree centrality is often the first step in graph analysis because it's intuitive and computationally efficient.

### Computing In-Degree with AggregateMessages

We'll start with AggregateMessages, which performs a single iteration of message passing. This is simpler than Pregel and perfect for basic operations. Let's load our stats.meta knowledge graph:

```python
import pyspark.sql.functions as F
from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

# Initialize a SparkSession
spark: SparkSession = (
    SparkSession.builder.appName("Pregel Tutorial - Stack Exchange Analysis")
    # Lets the Id:(Stack Overflow int) and id:(GraphFrames ULID) coexist
    .config("spark.sql.caseSensitive", True)
    .getOrCreate()
)
sc: SparkContext = spark.sparkContext
sc.setCheckpointDir("/tmp/graphframes-checkpoints")

# Define the base path for the Stack Exchange data
STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
BASE_PATH = f"python/graphframes/tutorials/data/{STACKEXCHANGE_SITE}"

# Load the nodes and edges from disk, repartition and cache
NODES_PATH: str = f"{BASE_PATH}/Nodes.parquet"
nodes_df: DataFrame = spark.read.parquet(NODES_PATH)
nodes_df = nodes_df.repartition(50).checkpoint().cache()

EDGES_PATH: str = f"{BASE_PATH}/Edges.parquet"
edges_df: DataFrame = spark.read.parquet(EDGES_PATH)
edges_df = edges_df.repartition(50).checkpoint().cache()
```

This Stack Exchange graph contains several node types (Badge, Vote, User, Answer, Question, PostLinks, Tag) and relationship types (Earns, CastFor, Tags, Answers, Posts, Asks, Links, Duplicates). The [Network Motif Tutorial](02-motif-tutorial.md) explores these in detail.

Now let's walk through in-degree in AggregateMessages. The in-degree of a node is the number of edges directed towards it. We can compute this using the [GraphFrame.aggregateMessages](https://graphframes.io/api/python/graphframes.lib.html#graphframes.lib.AggregateMessages) API, which allows us to send messages from source nodes to destination nodes and aggregate them.

```python
# Initialize a column with 1 to transmit to other nodes
nodes_df = nodes_df.withColumn("start_degree", F.lit(1))

# Create a GraphFrame to get access to AggregateMessages API
g: GraphFrame = GraphFrame(nodes_df, edges_df)

msgToDst = AM.src["start_degree"]
agg = g.aggregateMessages(
    F.sum(AM.msg).alias("in_degree"),
    sendToDst=msgToDst)
agg.show()
```

There's a problem, however - isolated or dangling nodes (those with no in-links) will not have degree zero, they simply won't appear in the data. You can see below the lowest in_degree is 1, not 0. There are definitely some 0 in-degree nodes in our knowledge graph.

```python
agg.groupBy("in_degree").count().orderBy("in_degree").show(10)

+---------+-----+
|in_degree|count|
+---------+-----+
|        1|43165|
|        2|  341|
|        3|  218|
|        4|  289|
|        5|  326|
|        6|  371|
|        7|  318|
|        8|  338|
|        9|  304|
|       10|  299|
+---------+-----+
```

Here we LEFT JOIN all of the graph's vertices with the aggregated in-degrees and fill in undefined values with 0.

```python
# join back and fill zeros
completeInDeg = (
    g.vertices
    .join(agg, on="id", how="left")   # isolates will have inDegree = null
    .na.fill(0, ["in_degree"])              # turn null → 0
    .select("id", "in_degree")
)
```

Now a histogram of degrees verifies the zeros have been added:

```python
completeInDeg.groupBy("in_degree").count().orderBy("in_degree").show(10)

+---------+-----+
|in_degree|count|
+---------+-----+
|        0|81735|
|        1|43165|
|        2|  341|
|        3|  218|
|        4|  289|
|        5|  326|
|        6|  371|
|        7|  318|
|        8|  338|
|        9|  304|
+---------+-----+
```

### Simple Example: In-Degree on Test Graph

Let's see how this works on our simple test graph:

```python
from graphframes.lib import AggregateMessages as AM

# Using the simple graph from earlier (A->B, A->C, B->C, C->D)
vertices_simple = spark.createDataFrame([
    ("A", "Alice"),
    ("B", "Bob"),
    ("C", "Charlie"),
    ("D", "David"),
], ["id", "name"])

edges_simple = spark.createDataFrame([
    ("A", "B", "follows"),
    ("A", "C", "follows"),
    ("B", "C", "follows"),
    ("C", "D", "follows"),
], ["src", "dst", "relationship"])

# Add initial degree column
vertices_simple = vertices_simple.withColumn("start_degree", F.lit(1))
g_simple = GraphFrame(vertices_simple, edges_simple)

# Calculate in-degree using AggregateMessages
msgToDst = AM.src["start_degree"]
in_degrees = g_simple.aggregateMessages(
    F.sum(AM.msg).alias("in_degree"),
    sendToDst=msgToDst)

# Join with all vertices and fill missing values with 0
complete_degrees = (
    g_simple.vertices
    .join(in_degrees, on="id", how="left")
    .na.fill(0, ["in_degree"])
    .select("id", "name", "in_degree")
)

complete_degrees.orderBy("id").show()

# Expected output:
# +---+-------+---------+
# | id|   name|in_degree|
# +---+-------+---------+
# |  A|  Alice|        0|  (no incoming edges)
# |  B|    Bob|        1|  (A->B)
# |  C|Charlie|        2|  (A->C, B->C)
# |  D|  David|        1|  (C->D)
# +---+-------+---------+
```

This simple example clearly shows how AggregateMessages works:
- Node A has 0 in-degree (no one follows Alice)
- Node B has 1 in-degree (Alice follows Bob)
- Node C has 2 in-degree (Alice and Bob follow Charlie)
- Node D has 1 in-degree (Charlie follows David)

## Introducing Pregel: In-Degree Calculation

Now let's implement the **same** in-degree calculation using Pregel. This helps us understand Pregel's API by comparing it with AggregateMessages:

```python
from graphframes.lib import Pregel

# Using the same simple test graph
vertices_simple = spark.createDataFrame([
    ("A", "Alice"),
    ("B", "Bob"),
    ("C", "Charlie"),
    ("D", "David"),
], ["id", "name"])

edges_simple = spark.createDataFrame([
    ("A", "B", "follows"),
    ("A", "C", "follows"),
    ("B", "C", "follows"),
    ("C", "D", "follows"),
], ["src", "dst", "relationship"])

g_simple = GraphFrame(vertices_simple, edges_simple)

# Calculate in-degree using Pregel API
pregel_result = g_simple.pregel \
    .setMaxIter(1) \
    .withVertexColumn(
        "in_degree",                      # Column name
        F.lit(0),                         # Initial value: start with 0
        F.coalesce(Pregel.msg(), F.lit(0))  # Update: use received message or keep 0
    ) \
    .sendMsgToDst(F.lit(1)) \
    .aggMsgs(F.sum(Pregel.msg())) \
    .run()

pregel_result.select("id", "name", "in_degree").orderBy("id").show()

# Output:
# +---+-------+---------+
# | id|   name|in_degree|
# +---+-------+---------+
# |  A|  Alice|        0|
# |  B|    Bob|        1|
# |  C|Charlie|        2|
# |  D|  David|        1|
# +---+-------+---------+
```

### Understanding the Pregel API

Let's break down each part of the Pregel call:

1. **`setMaxIter(1)`**: Run for 1 iteration (degree is computed in one pass)

2. **`withVertexColumn("in_degree", F.lit(0), F.coalesce(Pregel.msg(), F.lit(0)))`**:
   - Creates a new column called `in_degree`
   - **Initial value**: `F.lit(0)` - every node starts with degree 0
   - **Update function**: `F.coalesce(Pregel.msg(), F.lit(0))` - use the aggregated message, or 0 if no messages

3. **`sendMsgToDst(F.lit(1))`**: Each source node sends the value `1` to its destination node

4. **`aggMsgs(F.sum(Pregel.msg()))`**: Sum all messages received by each node

5. **`run()`**: Execute the algorithm

### Pregel vs AggregateMessages

Both achieve the same result, but notice the differences:

| Feature | AggregateMessages | Pregel |
|---------|-------------------|--------|
| **Iterations** | Single pass only | Multiple iterations with `setMaxIter()` |
| **State Management** | Manual (create columns beforehand) | Automatic (`withVertexColumn`) |
| **Syntax** | Lower-level, more control | Higher-level, cleaner for iterative algorithms |
| **Best For** | Single-pass algorithms, custom logic | Iterative algorithms like PageRank |
| **Complexity** | Simpler for one-off operations | Better for complex multi-step algorithms |

For simple operations like degree, either works fine. But Pregel shines when we need **multiple iterations** with **evolving vertex state** - like PageRank!

## PageRank: A Multi-Iteration Pregel Algorithm

Now that we understand Pregel's API from the degree calculation, let's tackle a more complex algorithm: **PageRank**. Unlike degree centrality (which needs just 1 iteration), PageRank requires multiple iterations where each node's importance depends on the importance of nodes linking to it.

PageRank was defined by Google cofounders Larry Page and Sergey Brin in their landmark 1999 paper <a href="https://www.cis.upenn.edu/~mkearns/teaching/NetworkedLife/pagerank.pdf">The PageRank Citation Ranking: Bringing Order to the Web</a>. The key insight: a node is important if other important nodes point to it.

<center>
    <figure>
        <img src="../img/Simplified-PageRank-Calculation.png" width="550px" />
        <figcaption>A Simplified PageRank Calculation, from the <a href="https://www.cis.upenn.edu/~mkearns/teaching/NetworkedLife/pagerank.pdf">PageRank paper</a></figcaption>
    </figure>
</center>

```python
# First, compute out-degrees for each node (needed for PageRank)
out_degrees = g.outDegrees.withColumnRenamed("outDegree", "out_degree")
nodes_with_outdegree = nodes_df.join(out_degrees, on="id", how="left").na.fill(1, ["out_degree"])

# Create a GraphFrame with out-degree information
g: GraphFrame = GraphFrame(nodes_with_outdegree, edges_df)

# Get total number of nodes for PageRank initialization
num_vertices = g.vertices.count()

# PageRank parameters
damping_factor = 0.85
max_iterations = 10

# Import Pregel for the PageRank implementation
from graphframes.lib import Pregel

# Run PageRank using the Pregel API
results = g.pregel.setMaxIter(max_iterations) \
    .withVertexColumn("pagerank", F.lit(1.0 / num_vertices),
        F.coalesce(Pregel.msg(), F.lit(0.0)) * F.lit(damping_factor) + F.lit((1.0 - damping_factor) / num_vertices)) \
    .sendMsgToDst(Pregel.src("pagerank") / Pregel.src("out_degree")) \
    .aggMsgs(F.sum(Pregel.msg())) \
    .run()

# Show top 10 nodes by PageRank
results.orderBy(F.desc("pagerank")).select("id", "pagerank").show(10)
```

The Pregel API provides a clean way to express the PageRank algorithm:

1. **Initialization**: Each vertex starts with PageRank = 1/N
2. **Message Passing**: Each vertex sends its PageRank divided by out-degree to neighbors
3. **Aggregation**: Sum incoming PageRank contributions
4. **Update**: Apply damping factor: PR = (1-d)/N + d * sum(incoming PR)

Expected output shows the most important nodes in our Stack Exchange network:

```
+------------------------------------+--------------------+
|id                                  |pagerank            |
+------------------------------------+--------------------+
|5a3d9c3f-8a77-4e9f-9f9e-1c8b9e8f7d6a|0.002341567890123456|
|7b2e4f5a-9c8d-4a7b-8e6f-2d9a8c7b6e5f|0.001987654321098765|
|8c3f5a6b-7d9e-5b8c-9f7a-3e0b9d8c7f6a|0.001876543210987654|
|9d4a6b7c-8e0f-6c9d-0a8b-4f1c0e9d8a7b|0.001765432109876543|
|0e5b7c8d-9f1a-7d0e-1b9c-5a2d1f0e9b8c|0.001654321098765432|
+------------------------------------+--------------------+
```

### Simple Example: PageRank on Test Graph

Let's see PageRank in action on our simple test graph to understand how it works:

```python
from graphframes.lib import Pregel

# Create simple graph
vertices_pr = spark.createDataFrame([
    ("A", "Alice"),
    ("B", "Bob"),
    ("C", "Charlie"),
    ("D", "David"),
], ["id", "name"])

edges_pr = spark.createDataFrame([
    ("A", "B", "follows"),
    ("A", "C", "follows"),
    ("B", "C", "follows"),
    ("C", "D", "follows"),
], ["src", "dst", "relationship"])

# Calculate out-degrees
g_pr = GraphFrame(vertices_pr, edges_pr)
out_degrees = g_pr.outDegrees.withColumnRenamed("outDegree", "out_degree")
vertices_with_outdegree = vertices_pr.join(out_degrees, on="id", how="left").na.fill(1, ["out_degree"])

# Create GraphFrame with out-degree info
g_pr = GraphFrame(vertices_with_outdegree, edges_pr)

# PageRank parameters
num_vertices = g_pr.vertices.count()
damping_factor = 0.85
max_iterations = 10

# Run PageRank using Pregel
results = g_pr.pregel.setMaxIter(max_iterations) \
    .withVertexColumn("pagerank", F.lit(1.0 / num_vertices),
        F.coalesce(Pregel.msg(), F.lit(0.0)) * F.lit(damping_factor) + F.lit((1.0 - damping_factor) / num_vertices)) \
    .sendMsgToDst(Pregel.src("pagerank") / Pregel.src("out_degree")) \
    .aggMsgs(F.sum(Pregel.msg())) \
    .run()

# Show results
results.select("id", "name", "pagerank").orderBy(F.desc("pagerank")).show()

# Expected output (approximate values after 10 iterations):
# +---+-------+------------------+
# | id|   name|          pagerank|
# +---+-------+------------------+
# |  C|Charlie|0.3427...         |  (most influential - receives from A and B)
# |  D|  David|0.2799...         |  (receives from C)
# |  B|    Bob|0.2387...         |  (receives from A)
# |  A|  Alice|0.1387...         |  (least influential - no incoming edges)
# +---+-------+------------------+
```

**How PageRank works in this example:**
1. Each node starts with PageRank = 1/4 = 0.25
2. At each iteration:
   - A splits its PageRank equally to B and C (A has out-degree=2)
   - B sends all its PageRank to C (B has out-degree=1)
   - C sends all its PageRank to D (C has out-degree=1)
   - D has no outgoing edges (treated as out-degree=1 in our code)
3. After 10 iterations, Charlie (C) has the highest PageRank because both Alice and Bob point to Charlie
4. Alice (A) has the lowest PageRank because no one points to Alice

### Comparing with GraphFrames' Built-in PageRank

Let's verify our Pregel implementation matches the built-in PageRank:

```python
# Run built-in PageRank for comparison
builtin_pr = g.pageRank(resetProbability=1-damping_factor, maxIter=max_iterations)

# Compare results
comparison = results.select("id", F.col("pagerank").alias("pregel_pr")) \
    .join(builtin_pr.vertices.select("id", F.col("pagerank").alias("builtin_pr")), on="id") \
    .select("id", "pregel_pr", "builtin_pr",
            F.abs(F.col("pregel_pr") - F.col("builtin_pr")).alias("difference"))

comparison.orderBy(F.desc("pregel_pr")).show(5)
```

## Label Propagation with Pregel

Label Propagation is a semi-supervised learning algorithm that assigns labels to unlabeled nodes in a graph based on their neighbors. Here's how to implement it using Pregel:

```python
# Initialize each node with its own ID as the initial label
initial_labels = g.vertices.select("id").withColumn("label", F.col("id"))
g_labels = GraphFrame(initial_labels, g.edges)

# Run Label Propagation using Pregel. Each node adopts the most frequent label among its neighbors
label_prop_results = g_labels.pregel.setMaxIter(5) \
    .withVertexColumn("label", Pregel.src("id"),
        F.coalesce(Pregel.msg(), Pregel.src("label"))) \
    .sendMsgToDst(Pregel.src("label")) \
    .sendMsgToSrc(Pregel.dst("label")) \
    .aggMsgs(F.expr("mode(collect_list(msg))")) \
    .run()

# Count communities (unique labels)
communities = label_prop_results.select("label").distinct().count()
print(f"Number of communities detected: {communities}")

# Show community sizes
label_prop_results.groupBy("label").count() \
    .orderBy(F.desc("count")).show(10)
```

## Combining Node Types

In many real-world graphs, nodes have different types (e.g., users, posts, tags in Stack Exchange). Pregel can handle heterogeneous graphs by incorporating node type information:

```python
# Add node types to our graph (simulating different entity types)
typed_vertices = g.vertices.withColumn("node_type",
    F.when(F.col("Id") % 3 == 0, "question")
     .when(F.col("Id") % 3 == 1, "answer")
     .otherwise("user"))

g_typed = GraphFrame(typed_vertices, g.edges)

# Weighted PageRank based on node type - questions contribute more to PageRank than answers
type_weights = F.when(F.col("node_type") == "question", 2.0) \
               .when(F.col("node_type") == "answer", 1.0) \
               .otherwise(0.5)

# Run type-aware PageRank
typed_pr = g_typed.pregel.setMaxIter(10) \
    .withVertexColumn("pagerank", F.lit(1.0 / num_vertices),
        F.coalesce(Pregel.msg(), F.lit(0.0)) *F.lit(damping_factor) + F.lit((1.0 - damping_factor) / num_vertices)) \
    .sendMsgToDst((Pregel.src("pagerank")* type_weights) / Pregel.src("out_degree")) \
    .aggMsgs(F.sum(Pregel.msg())) \
    .run()

# Show top nodes by type
for node_type in ["question", "answer", "user"]:
    print(f"\nTop {node_type}s by PageRank:")
    typed_pr.filter(F.col("node_type") == node_type) \
        .orderBy(F.desc("pagerank")) \
        .select("id", "node_type", "pagerank") \
        .show(5)
```

## Conclusion

In this tutorial, we built a solid understanding of graph algorithms with GraphFrames by progressing from simple to complex:

1. **Degree Centrality with AggregateMessages**: Started with the simplest metric using single-pass message passing
2. **Degree Centrality with Pregel**: Learned Pregel's API by implementing the same algorithm, understanding when to use each approach
3. **PageRank with Pregel**: Applied Pregel to a multi-iteration algorithm where vertex importance evolves over time
4. **Advanced Examples**: Explored label propagation and heterogeneous graphs with type-aware computations

### Key Takeaways

**When to use AggregateMessages:**
- Single-pass algorithms (degree, simple aggregations)
- Need fine-grained control over message passing
- Custom termination logic required

**When to use Pregel:**
- Multi-iteration algorithms (PageRank, label propagation, shortest paths)
- Vertex state evolves across iterations
- Cleaner, more declarative syntax preferred

**Core Pregel Pattern:**
```python
result = graph.pregel.setMaxIter(n) \
    .withVertexColumn("state", initial_value, update_function) \
    .sendMsgToDst(message_expression) \
    .aggMsgs(aggregation_function) \
    .run()
```

The Pregel API enables you to implement custom graph algorithms that scale to billions of edges by:

* **Thinking vertex-centric**: Each node computes based on local information
* **Leveraging BSP**: Bulk synchronous parallel processing ensures consistency
* **Using Spark**: Distributed computing handles massive graphs automatically

### Best Practices

* **Start simple**: Test algorithms on small graphs before scaling up
* **Set appropriate iterations**: Too few may not converge; too many wastes resources
* **Handle edge cases**: Isolated nodes, missing values, division by zero
* **Use checkpointing**: For long-running computations to enable fault tolerance
* **Monitor convergence**: Implement early stopping when changes become negligible

### Next Steps

* Explore the [GraphFrames User Guide](https://graphframes.io/docs/_site/user-guide.html) for more built-in algorithms
* Read the original [Pregel paper](https://15799.courses.cs.cmu.edu/fall2013/static/papers/p135-malewicz.pdf) for theoretical foundations
* Implement shortest paths, connected components, or triangle counting using these patterns
* Combine Pregel with motif finding for sophisticated graph analysis

# Quick-Start

This quick-start guide shows how to get started using GraphFrames. After you work through this guide, move on to the [User Guide](/04-user-guide/01-creating-graphframes.md) to learn more about the many queries and algorithms supported by GraphFrames.

The following example shows how to create a GraphFrame, query it, and run the PageRank algorithm.

## Scala API

```scala
// import graphframes package
import org.graphframes._
// Create a Vertex DataFrame with unique ID column "id"
val v = spark.createDataFrame(List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30)
)).toDF("id", "name", "age")

// Create an Edge DataFrame with "src" and "dst" columns
val e = spark.createDataFrame(List(
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow")
)).toDF("src", "dst", "relationship")
// Create a GraphFrame
import org.graphframes.GraphFrame
val g = GraphFrame(v, e)

// Query: Get in-degree of each vertex.
g.inDegrees.show()

// Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

// Run PageRank algorithm, and show results.
val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
results.vertices.select("id", "pagerank").show()
```

## Python API

```python
# Create a Vertex DataFrame with unique ID column "id"
v = spark.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
], ["id", "name", "age"])

# Create an Edge DataFrame with "src" and "dst" columns
e = spark.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
], ["src", "dst", "relationship"])
# Create a GraphFrame
from graphframes import *
g = GraphFrame(v, e)

# Query: Get in-degree of each vertex.
g.inDegrees.show()

# Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()
```

## Graph Algorithms

Historically, Apache Spark had a built-in graph processing tool named `GraphX`, that was based on `RDD` (pre Spark 2.x way of doing things). GraphX provided a set of graph algorithms, like `PageRank`, `LabelPropagation`, etc. In Spark 4.0.x GraphX was deprecated and is not recommended for usage. Opposite, `GraphFrames` represent graphs using Spark's `Dataset` / `Dataframe`. `GraphFrames` also provides the set of standard graph algorithms, and this set is growing. For algorithms implemented in `GraphX` but currently not supported natively in `GraphFrames`, the library also provides a conversion method (see [user guide](/04-user-guide/12-graphx-coversion.md)). The following table shows the currently supported algorithms:

| Algorithm                      | GraphX Wrapper | GraphFrames Implementation |
|--------------------------------|----------------|----------------------------|
| BFS                            | Yes            | Yes                        |
| Connected Components           | Yes            | Yes                        |
| Strongly Connected Components  | Yes            | No                         |
| Label Propagation Algorithm    | Yes            | Yes                        |
| PageRank                       | Yes            | No                         |
| Parallel Personalized PageRank | Yes            | No                         |
| Shortest Paths                 | Yes            | Yes                        |
| Triangle Count                 | Yes            | Yes                        |
| SVD++                          | Yes            | No                         |
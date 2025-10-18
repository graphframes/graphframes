# Centrality Metrics

## Degrees

GraphFrames provides three main APIs for computing degrees:

- `inDegrees`
- `outDegrees`
- `degrees`

### Python API

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()
in_degrees = g.inDegrees()
out_degrees = g.outDegrees()
degrees = g.degrees()
```

### Scala API

```scala
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends

val inDegrees: DataFrame = g.inDegrees
val outDegrees: DataFrame = g.outDegrees
val degrees: DataFrame = g.degrees
```

## PageRank

There are two implementations of PageRank.

* The first one uses the `org.apache.spark.graphx.graph` interface with `aggregateMessages` and runs PageRank for a fixed number of iterations. This can be executed by setting `maxIter`.
* The second implementation uses the `org.apache.spark.graphx.Pregel` interface and runs PageRank until convergence and this can be run by setting `tol`.

Both implementations support non-personalized and personalized PageRank, where setting a `sourceId` personalizes the results for that vertex.

See [Wikipedia](https://en.wikipedia.org/wiki/PageRank) for a background.

**NOTE:** The `pageRank` API at the moment is the only API in GraphFrames that returns a `GraphFrame` object instead of a `DataFrame`. Most probably, this behavior will change in the nearest major release for the API consistency. It is strongly recommended do not rely on the returned `edges` at all.

### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.pageRank).

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

# Run PageRank until convergence to tolerance "tol"

results = g.pageRank(resetProbability=0.15, tol=0.01)

# Display resulting pageranks and final edge weights

# Note that the displayed pagerank may be truncated, e.g., missing the E notation

# In Spark 1.5+, you can use show(truncate=False) to avoid truncation

results.vertices.select("id", "pagerank").show()
results.edges.select("src", "dst", "weight").show()

# Run PageRank for a fixed number of iterations

results2 = g.pageRank(resetProbability=0.15, maxIter=10)

# Run PageRank personalized for vertex "a"

results3 = g.pageRank(resetProbability=0.15, maxIter=10, sourceId="a")

# Run PageRank personalized for vertex ["a", "b", "c", "d"] in parallel

results4 = g.parallelPersonalizedPageRank(resetProbability=0.15, sourceIds=["a", "b", "c", "d"], maxIter=10)
```

### Scala API

For API details, refer to the @:scaladoc(org.graphframes.lib.PageRank).

```scala
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

// Run PageRank until convergence to tolerance "tol".
val results: GraphFrame = g.pageRank.resetProbability(0.15).tol(0.01).run()

// Display resulting pageranks and final edge weights
// Note that the displayed pagerank may be truncated, e.g., missing the E notation.
// In Spark 1.5+, you can use show(truncate=false) to avoid truncation.
results.vertices.select("id", "pagerank").show()
results.edges.select("src", "dst", "weight").show()

// Run PageRank for a fixed number of iterations.
val results2 = g.pageRank.resetProbability(0.15).maxIter(10).run()

// Run PageRank personalized for vertex "a"
val results3 = g.pageRank.resetProbability(0.15).maxIter(10).sourceId("a").run()

// Run PageRank personalized for vertex ["a", "b", "c", "d"] in parallel
val results4 = g.parallelPersonalizedPageRank.resetProbability(0.15).maxIter(10).sourceIds(Array("a", "b", "c", "d"))
.run()
results4.vertices.show()
results4.edges.show()
```

### Parallel personalized PageRank

GraphFrames also supports parallel personalized PageRank that allows users to compute ranks "from the subset of source vertices".

For the API details refer to:

* Scala API: @:scaladoc(org.graphframes.lib.ParallelPersonalizedPageRank)
* Python API: @:pydoc(graphframes.GraphFrame.parallelPersonalizedPageRank)

## K-Core

K-Core decomposition is a method used to identify the most tightly connected subgraphs within a network. A k-core is a maximal subgraph where every vertex has at least degree k. This metric helps in understanding the inner structure of networks by filtering out less connected nodes, revealing cores of highly interconnected entities. K-Core centrality can be applied in various domains such as social network analysis to find influential users, in biology to detect stable protein complexes, or in infrastructure networks to assess robustness and vulnerability.

The provided implementation of K-Core decomposition in GraphFrames is based on the research described in the paper available at [IEEE Xplore](https://ieeexplore.ieee.org/abstract/document/8258018). Using think-like-a-vertex paradigm, the proposed method utilizes a message passing paradigm for solving k-core decomposition, thus reducing the I/O cost substantially.

For more information, see:

> A. Farajollahi, S. G. Khaki, and L. Wang, "Efficient distributed k-core decomposition for large-scale graphs," *2017 IEEE International Conference on Big Data (Big Data)*, Boston, MA, USA, 2017, pp. 1430-1435.

### Arguments

- `checkpoint_interval`

For `graphframes` only. To avoid exponential growing of the Spark' Logical Plan, DataFrame lineage and query optimization time, it is required to do checkpointing periodically. While checkpoint itself is not free, it is still recommended to set this value to something less than `5`.

- `use_local_checkpoints`

For `graphframes` only. By default, GraphFrames uses persistent checkpoints. They are realiable and reduce the errors rate. The downside of the persistent checkpoints is that they are requiride to set up a `checkpointDir` in persistent storage like `S3` or `HDFS`. By providing `use_local_checkpoints=True`, user can say GraphFrames to use local disks of Spark' executurs for checkpointing. Local checkpoints are faster, but they are less reliable: if the executur lost, for example, is taking by the higher priority job, checkpoints will be lost and the whole job fails.

- `storage_level`

The level of storage for intermediate results and the output `DataFrame` with components. By default it is memory and disk deserialized as a good balance between performance and reliability. For very big graphs and out-of-core scenarious, using `DISK_ONLY` may be faster.

### Python API

```python
import org.graphframes.GraphFrame

v = spark.createDataFrame([(i, f"v{i}") for i in range(30)], ["id", "name"])

# Build edges to create a hierarchical structure:
# Core (k=5): vertices 0-4 - fully connected
core_edges = [(i, j) for i in range(5) for j in range(i + 1, 5)]

# Next layer (k=3): vertices 5-14 - each connects to multiple core vertices
mid_layer_edges = [
    (5, 0),
    (5, 1),
    (5, 2),  # Connect to core
    (6, 0),
    (6, 1),
    (6, 3),
    (7, 1),
    (7, 2),
    (7, 4),
    (8, 0),
    (8, 3),
    (8, 4),
    (9, 1),
    (9, 2),
    (9, 3),
    (10, 0),
    (10, 4),
    (11, 2),
    (11, 3),
    (12, 1),
    (12, 4),
    (13, 0),
    (13, 2),
    (14, 3),
    (14, 4),
]

# Outer layer (k=1): vertices 15-29 - sparse connections
outer_edges = [
    (15, 5),
    (16, 6),
    (17, 7),
    (18, 8),
    (19, 9),
    (20, 10),
    (21, 11),
    (22, 12),
    (23, 13),
    (24, 14),
    (25, 15),
    (26, 16),
    (27, 17),
    (28, 18),
    (29, 19),
]

all_edges = core_edges + mid_layer_edges + outer_edges
e = spark.createDataFrame(all_edges, ["src", "dst"])
g = GraphFrame(v, e)
result = g.k_core(
    checkpoint_interval=args.checkpoint_interval,
    use_local_checkpoints=args.use_local_checkpoints,
    storage_level=args.storage_level,
)
```

### Scala API

```scala
import org.graphframes.GraphFrame

val v = spark.createDataFrame((0L until 30L).map(id => (id, s"v$id"))).toDF("id", "name")

// Build edges to create a hierarchical structure:
// Core (k=5): vertices 0-4 - fully connected
// Next layer (k=3): vertices 5-14 - each connects to multiple core vertices
// Outer layer (k=1): vertices 15-29 - sparse connections
val coreEdges = for {
  i <- 0 until 5
  j <- (i + 1) until 5
} yield (i.toLong, j.toLong)

val midLayerEdges = Seq(
  (5L, 0L),
  (5L, 1L),
  (5L, 2L), // Connect to core
  (6L, 0L),
  (6L, 1L),
  (6L, 3L),
  (7L, 1L),
  (7L, 2L),
  (7L, 4L),
  (8L, 0L),
  (8L, 3L),
  (8L, 4L),
  (9L, 1L),
  (9L, 2L),
  (9L, 3L),
  (10L, 0L),
  (10L, 4L),
  (11L, 2L),
  (11L, 3L),
  (12L, 1L),
  (12L, 4L),
  (13L, 0L),
  (13L, 2L),
  (14L, 3L),
  (14L, 4L))

val outerEdges = Seq(
  (15L, 5L),
  (16L, 6L),
  (17L, 7L),
  (18L, 8L),
  (19L, 9L),
  (20L, 10L),
  (21L, 11L),
  (22L, 12L),
  (23L, 13L),
  (24L, 14L),
  (25L, 15L),
  (26L, 16L),
  (27L, 17L),
  (28L, 18L),
  (29L, 19L))

val allEdges = coreEdges ++ midLayerEdges ++ outerEdges
val e = spark.createDataFrame(allEdges).toDF("src", "dst")
val g = GraphFrame(v, e)
val result = g.kCore.run()
```

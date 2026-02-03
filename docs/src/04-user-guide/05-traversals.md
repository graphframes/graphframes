# Traversals and Connectivity

## Shortest paths

Computes shortest paths from each vertex to the given set of landmark vertices, where landmarks are specified by the
vertex ID. Note that this takes an edge direction into account.

See [Wikipedia](https://en.wikipedia.org/wiki/Shortest_path_problem) for a background.

---

**NOTE**

*Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!*

---

### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.shortestPaths).

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

results = g.shortestPaths(landmarks=["a", "d"])
results.select("id", "distances").show()
```

### Scala API

For API details, refer to the @:scaladoc(org.graphframes.lib.ShortestPaths).

```scala
import org.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val results = g.shortestPaths.landmarks(Seq("a", "d")).run()
results.select("id", "distances").show()
```

### Arguments

- `landmarks`

The list (`Seq`) of vertices that are used as landmarks to compute shortest paths from them to all other vertices.

- `algorithm`

Possible values are `graphx` and `graphframes`. Both implementations are based on the same logic. GraphX is faster for small-medium sized graphs but requires more memory due to less efficient RDD serialization and it's triplets-based nature. GraphFrames requires much less memory due to efficient Thungsten serialization and because the core structures are edges and messages, not triplets.

- `checkpoint_interval`

For `graphframes` only. To avoid exponential growing of the Spark' Logical Plan, DataFrame lineage and query optimization time, it is required to do checkpointing periodically. While checkpoint itself is not free, it is still recommended to set this value to something less than `5`.

- `use_local_checkpoints`

For `graphframes` only. By default, GraphFrames uses persistent checkpoints. They are realiable and reduce the errors rate. The downside of the persistent checkpoints is that they are requiride to set up a `checkpointDir` in persistent storage like `S3` or `HDFS`. By providing `use_local_checkpoints=True`, user can say GraphFrames to use local disks of Spark' executurs for checkpointing. Local checkpoints are faster, but they are less reliable: if the executur lost, for example, is taking by the higher priority job, checkpoints will be lost and the whole job fails.

- `storage_level`

The level of storage for intermediate results and the output `DataFrame` with components. By default it is memory and disk deserialized as a good balance between performance and reliability. For very big graphs and out-of-core scenarious, using `DISK_ONLY` may be faster.

- `is_direted`

By default this is true and algorithm will look for only directed paths. By passing false, graph will be considered as undirected and algorithm will look for any shortest path.

## Breadth-first search (BFS)

Breadth-first search (BFS) finds the shortest path(s) from one vertex (or a set of vertices) to another vertex (or a set
of vertices). The beginning and end vertices are specified as Spark DataFrame expressions.

See [Wikipedia on BFS](https://en.wikipedia.org/wiki/Breadth-first_search) for more background.

The following code snippets use BFS to find the path between vertex with name "Esther" to a vertex with `age < 32`.

### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.bfs).

```python
g = Graphs(spark).friends()  # Get example graph

# Search from "Esther" for users of age < 32

paths = g.bfs("name = 'Esther'", "age < 32")
paths.show()

# Specify edge filters or max path lengths

g.bfs("name = 'Esther'", "age < 32",
      edgeFilter="relationship != 'friend'", maxPathLength=3)
```

### Scala API

For API details, refer to @:scaladoc(org.graphframes.lib.BFS).

```scala
import org.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

// Search from "Esther" for users of age < 32.
val paths = g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32").run()
paths.show()

// Specify edge filters or max path lengths.
val paths = {
  g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32")
    .edgeFilter("relationship != 'friend'")
    .maxPathLength(3).run()
}
paths.show()
```

## Connected components

Computes the connected component membership of each vertex and returns a graph with each vertex assigned a component ID.

See [Wikipedia](https://en.wikipedia.org/wiki/Connected_component_(graph_theory)) for the background.

---

**NOTE:**

*With GraphFrames 0.3.0 and later releases, the default Connected Components algorithm requires setting a Spark checkpoint directory. Users can revert to the old algorithm using `connectedComponents.setAlgorithm("graphx")`. Starting from GraphFrames 0.9.3 release, users can also use `localCheckpoints` that does not require setting a Spark checkpoint directory. To use `localCheckpoints` users can set the config `spark.graphframes.useLocalCheckpoints` to `true` or use the API `connectedComponents.setUseLocalCheckpoints(true)`. While `localCheckpoints` provides better performance they are not as reliable as the persistent checkpointing.*

**NOTE**

*Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!*

---

### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.connectedComponents).

```python
from graphframes.examples import Graphs

sc.setCheckpointDir("/tmp/spark-checkpoints")

g = Graphs(spark).friends()  # Get example graph

result = g.connectedComponents()
result.select("id", "component").orderBy("component").show()
```

### Scala API

For API details, refer to the @:scaladoc(org.graphframes.lib.ConnectedComponents).

```scala
import org.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val result = g.connectedComponents.setUseLocalCheckpoints(true).run()
result.select("id", "component").orderBy("component").show()
```

### Arguments

- `algorithm`

Possible values are `graphx` and `graphframes`. GraphX-based implementation is a naive Pregel one-by-one. While it may be slightly faster on small-medium sized graphs, it has a much bigger convergence complexity and requires much more memory due to less efficient RDD serialization. GraphFrame-based implementation is based on the ideas from the [Kiveris, Raimondas, et al. "Connected components in mapreduce and beyond." Proceedings of the ACM Symposium on Cloud Computing. 2014.](https://dl.acm.org/doi/abs/10.1145/2670979.2670997). This implementation has much better convergence complexity as well as requires less amount of memory.

- `maxIter`

For `graphx` only. Limit the maximal amount of Pregel iterations. By default it is infinity (`Integer.maxValue`). It is recommended do not change this value. If the algorithm stucks, it is a problem of the graph, not algorithm.

- `checkpoint_interval`

For `graphframes` only. To avoid exponential growing of the Spark' Logical Plan, DataFrame lineage and query optimization time, it is required to do checkpointing periodically. While checkpoint itself is not free, it is still recommended to set this value to something less than `5`.

- `broadcast_threshold`

For `graphframes` only. See [this section](05-traversals.md#aqe-broadcast-mode) for details.

- `use_labels_as_components`

For `graphframes` only. In the case, when the type of the input graph vertices is not one of `Long`, `Int`, `Short`, `Byte`, output labels (components) are a random `Long` numbers by default. By providing `use_labels_as_components=True` user can ask GraphFrames to use original vertex labels for output components. In that case, the minimal value of all original IDs will be used for each of found components. This operation is not free and require an additional `groupBy` + `agg` + `join`.

- `use_local_checkpoints`

For `graphframes` only. By default, GraphFrames uses persistent checkpoints. They are realiable and reduce the errors rate. The downside of the persistent checkpoints is that they are requiride to set up a `checkpointDir` in persistent storage like `S3` or `HDFS`. By providing `use_local_checkpoints=True`, user can say GraphFrames to use local disks of Spark' executurs for checkpointing. Local checkpoints are faster, but they are less reliable: if the executur lost, for example, is taking by the higher priority job, checkpoints will be lost and the whole job fails.

- `storage_level`

The level of storage for intermediate results and the output `DataFrame` with components. By default it is memory and disk deserialized as a good balance between performance and reliability. For very big graphs and out-of-core scenarious, using `DISK_ONLY` may be faster.

### AQE-broadcast mode

*Starting from 0.10.0*

For `graphframes` algorithm only. During iterations, this algorithm can generate new edges that may tend to high skewness in joins and aggregates, because some vertices are having a very-high degree. In previous versions of GraphFrames this issue was addressed by manual broadcasting very high-degree nodes. Unfortunately, Apache Spark Adaptive Quey Execution optimization fails on such a case and that was the reason shy AQE was disabled for Connected Components.

In the new versions of GraphFrames (0.10+) there is a way to disable manual broadcasting, enable AQE and allow it to handle skewnewss. To enable this mode, pass `-1` to the `setBroadcastThreshold`. Based on benchmarks, this mode provides about 5x speed-up. It is possible, that in the future releases, the default value of the `broadcastThreshold` will be changed to `-1`.

### Strongly connected components

Compute the strongly connected component (SCC) of each vertex and return a graph with each vertex assigned to the SCC
containing that vertex. At the moment, SCC in GraphFrames is a wrapper around GraphX implementation.

See [Wikipedia](https://en.wikipedia.org/wiki/Strongly_connected_component) for the background.

---

**NOTE**

*Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!*

---

#### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.stronglyConnectedComponents).

```python
from graphframes.examples import Graphs

sc.setCheckpointDir("/tmp/spark-checkpoints")

g = Graphs(spark).friends()  # Get example graph

result = g.stronglyConnectedComponents(maxIter=10)
result.select("id", "component").orderBy("component").show()
```

#### Scala API

For API details, refer to the @:scaladoc(org.graphframes.lib.StronglyConnectedComponents).

```scala
import org.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val result = g.stronglyConnectedComponents.maxIter(10).run()
result.select("id", "component").orderBy("component").show()
```

## Triangle count

Triangle count computes the number of triangles passing through each vertex. A triangle is a set of three vertices where each pair is connected by an edge (A is connected to B, B to C, and C to A).

### Performance and Use Cases

Counting triangles is a fundamental task in network analysis:
- **Clustering Coefficient**: It is used to compute the local and global clustering coefficients, which measure the degree to which nodes in a graph tend to cluster together.
- **Community Detection**: A high density of triangles often indicates the presence of a tightly knit community or "clique."
- **Spam and Fraud Detection**: In social networks and financial transactions, unusual triangle patterns can help identify botnets or money-laundering rings.

### How it works

The core logic of the algorithm is based on **neighborhood intersection**. For every edge (u, v) in the graph, the algorithm finds the intersection of the neighbor sets of u and v. Every common neighbor w completes a triangle with u and v.

### Algorithms and Trade-offs

GraphFrames provides two implementations with different performance characteristics:

- **Exact**: This is the default algorithm. It computes the precise intersection of adjacency lists.
    - **Pros**: 100% accuracy.
    - **Cons**: Extremely memory-intensive. For high-degree nodes (hubs), collecting and intersecting large neighbor sets can lead to Out-of-Memory (OOM) errors or severe skew.
- **Approximate** (Starting from Spark 4.1): This version uses **DataSketches (Theta sketches)** to estimate the size of the intersection.
    - **Pros**: Highly scalable. It uses a fixed-size probabilistic structure to represent neighborhoods, dramatically reducing memory overhead and execution time.
    - **Cons**: Provides an estimate rather than an exact count.

### Selection Guide

- **Sparse/Regular Graphs**: For graphs like grids, spatial meshes, or infrastructure networks where the maximum degree is relatively low and there is no power-law distribution, the **Exact** algorithm is recommended.
- **Power-Law/Scale-Free Networks**: For social networks, web graphs, or biological networks containing "hubs" (vertices with thousands or millions of connections), the **Approximate** algorithm is often the only viable choice to avoid job failures.

### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.triangleCount).

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

results = g.triangleCount()
results.select("id", "count").show()
```

### Scala API

For API details, refer to the @:scaladoc(org.graphframes.lib.TriangleCount).

```scala
import org.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val results = g.triangleCount.run()
results.select("id", "count").show()
```

## Cycles Detection

GraphFrames provides an implementation of
the [Rocha–Thatte cycle detection algorithm](https://en.wikipedia.org/wiki/Rocha%E2%80%93Thatte_cycle_detection_algorithm).

---

**NOTE**

*Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!*

**WARNING:**

- *This algorithm collects the full sequences and may require a lot of cluste memory for power-law graphs*

---

### Python API

```python
from graphframes import GraphFrame

vertices = spark.createDataFrame(
    [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")], ["id", "attr"]
)
edges = spark.createDataFrame(
    [(1, 2), (2, 3), (3, 1), (1, 4), (2, 5)], ["src", "dst"]
)
graph = GraphFrame(vertices, edges)
res = graph.detectingCycles(
    checkpoint_interval=3,
    use_local_checkpoints=True,
)
res.show(False)

# Output:
# +----+--------------+
# | id | found_cycles |
# +----+--------------+
# |1   |[1, 3, 1]     |
# |1   |[1, 2, 1]     |
# |1   |[1, 2, 5, 1]  |
# |2   |[2, 1, 2]     |
# |2   |[2, 5, 1, 2]  |
# |3   |[3, 1, 3]     |
# |5   |[5, 1, 2, 5]  |
# +----+--------------+
```

### Scala API

```scala
import org.graphframes.GraphFrame

val graph = GraphFrame(
  spark
    .createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "d"), (5L, "e")))
    .toDF("id", "attr"),
  spark
    .createDataFrame(Seq((1L, 2L), (2L, 1L), (1L, 3L), (3L, 1L), (2L, 5L), (5L, 1L)))
    .toDF("src", "dst"))
val res = graph.detectingCycles.setUseLocalCheckpoints(true).run()
res.show(false)

// Output:
// +--------------+
// | found_cycles |
// +--------------+
// |[1, 3, 1]     |
// |[1, 2, 1]     |
// |[1, 2, 5, 1]  |
```

### Arguments

- `checkpoint_interval`

For `graphframes` only. To avoid exponential growing of the Spark' Logical Plan, DataFrame lineage and query optimization time, it is required to do checkpointing periodically. While checkpoint itself is not free, it is still recommended to set this value to something less than `5`.

- `use_local_checkpoints`

For `graphframes` only. By default, GraphFrames uses persistent checkpoints. They are realiable and reduce the errors rate. The downside of the persistent checkpoints is that they are requiride to set up a `checkpointDir` in persistent storage like `S3` or `HDFS`. By providing `use_local_checkpoints=True`, user can say GraphFrames to use local disks of Spark' executurs for checkpointing. Local checkpoints are faster, but they are less reliable: if the executur lost, for example, is taking by the higher priority job, checkpoints will be lost and the whole job fails.

- `storage_level`

The level of storage for intermediate results and the output `DataFrame` with components. By default it is memory and disk deserialized as a good balance between performance and reliability. For very big graphs and out-of-core scenarious, using `DISK_ONLY` may be faster.

# Graph algorithms

GraphFrames provides the same suite of standard graph algorithms as GraphX, plus some new ones. We provide the brief descriptions and code snippets below.

Some algorithms are currently wrappers around GraphX implementations, so they may not be more scalable than GraphX. More algorithms will be migrated to native GraphFrames implementations in the future.

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

## Breadth-first search (BFS)

Breadth-first search (BFS) finds the shortest path(s) from one vertex (or a set of vertices) to another vertex (or a set of vertices). The beginning and end vertices are specified as Spark DataFrame expressions.

See [Wikipedia on BFS](https://en.wikipedia.org/wiki/Breadth-first_search) for more background.

The following code snippets use BFS to find the path between vertex with name "Esther" to a vertex with `age < 32`.

### Scala API

For API details, refer to @:scaladoc(org.graphframes.lib.BFS).

```scala
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

// Search from "Esther" for users of age < 32.
val paths = g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32").run()
paths.show()

// Specify edge filters or max path lengths.
val paths = { g.bfs.fromExpr("name = 'Esther'").toExpr("age < 32")
.edgeFilter("relationship != 'friend'")
.maxPathLength(3).run() }
paths.show()
```

### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.bfs).

```python
g = Graphs(spark).friends()  # Get example graph

# Search from "Esther" for users of age < 32

paths = g.bfs("name = 'Esther'", "age < 32")
paths.show()

# Specify edge filters or max path lengths

g.bfs("name = 'Esther'", "age < 32",\
edgeFilter="relationship != 'friend'", maxPathLength=3)
```

## Connected components

Computes the connected component membership of each vertex and returns a graph with each vertex assigned a component ID.

See [Wikipedia](https://en.wikipedia.org/wiki/Connected_component_(graph_theory)) for the background.

**NOTE:** With GraphFrames 0.3.0 and later releases, the default Connected Components algorithm requires setting a Spark checkpoint directory. Users can revert to the old algorithm using `connectedComponents.setAlgorithm("graphx")`. Starting from GraphFrames 0.9.3 release, users can also use `localCheckpoints` that does not require setting a Spark checkpoint directory. To use `localCheckpoints` users can set the config `spark.graphframes.useLocalCheckpoints` to `true` or use the API `connectedComponents.setUseLocalCheckpoints(true)`. While `localCheckpoints` provides better performance they are not as reliable as the persistent checkpointing.

### Scala API

For API details, refer to the @:scaladoc(org.graphframes.lib.ConnectedComponents).

```scala
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val result = g.connectedComponents.setUseLocalCheckpoints(true).run()
result.select("id", "component").orderBy("component").show()
```

### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.connectedComponents).

```python
from graphframes.examples import Graphs

sc.setCheckpointDir("/tmp/spark-checkpoints")

g = Graphs(spark).friends()  # Get example graph

result = g.connectedComponents()
result.select("id", "component").orderBy("component").show()
```

### Strongly connected components

Compute the strongly connected component (SCC) of each vertex and return a graph with each vertex assigned to the SCC containing that vertex. At the moment, SCC in GraphFrames is a wrapper around GraphX implementation.

See [Wikipedia](https://en.wikipedia.org/wiki/Strongly_connected_component) for the background.

#### Scala API

For API details, refer to the @:scaladoc(org.graphframes.lib.StronglyConnectedComponents).

```scala
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val result = g.stronglyConnectedComponents.maxIter(10).run()
result.select("id", "component").orderBy("component").show()
```

#### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.stronglyConnectedComponents).

```python
from graphframes.examples import Graphs

sc.setCheckpointDir("/tmp/spark-checkpoints")

g = Graphs(spark).friends()  # Get example graph

result = g.stronglyConnectedComponents(maxIter=10)
result.select("id", "component").orderBy("component").show()
```

## Label Propagation (LPA)

Run a static Label Propagation Algorithm for detecting communities in networks.

Each node in the network is initially assigned to its own community. At every superstep, nodes send their community affiliation to all neighbors and update their state to the mode community affiliation of incoming messages.

LPA is a standard community detection algorithm for graphs. It is very inexpensive computationally, although (1) convergence is not guaranteed and (2) one can end up with trivial solutions (all nodes are identified into a single community).

See [Wikipedia](https://en.wikipedia.org/wiki/Label_Propagation_Algorithm) for the background.

### Scala API

For API details, refer to the @:scaladoc(org.graphframes.lib.LabelPropagation).

```scala
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val result = g.labelPropagation.maxIter(5).run()
result.select("id", "label").show()
```

### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.labelPropagation).

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

result = g.labelPropagation(maxIter=5)
result.select("id", "label").show()
```

## PageRank

There are two implementations of PageRank.

* The first one uses the `org.apache.spark.graphx.graph` interface with `aggregateMessages` and runs PageRank for a fixed number of iterations. This can be executed by setting `maxIter`.
* The second implementation uses the `org.apache.spark.graphx.Pregel` interface and runs PageRank until convergence and this can be run by setting `tol`.

Both implementations support non-personalized and personalized PageRank, where setting a `sourceId` personalizes the results for that vertex.

See [Wikipedia](https://en.wikipedia.org/wiki/PageRank) for a background.

**NOTE:** The `pageRank` API at the moment is the only API in GraphFrames that returns a `GraphFrame` object instead of a `DataFrame`. Most probably, this behavior will change in the nearest major release for the API consistency. It is strongly recommended do not rely on the returned `edges` at all.

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

### Parallel personalized PageRank

GraphFrames also supports parallel personalized PageRank that allows users to compute ranks "from the subset of source vertices".

For the API details refer to:

* Scala API: @:scaladoc(org.graphframes.lib.ParallelPersonalizedPageRank)
* Python API: @:pydoc(graphframes.GraphFrame.parallelPersonalizedPageRank)

## Shortest paths

Computes shortest paths from each vertex to the given set of landmark vertices, where landmarks are specified by the vertex ID. Note that this takes an edge direction into account.

See [Wikipedia](https://en.wikipedia.org/wiki/Shortest_path_problem) for a background.

### Scala API

For API details, refer to the @:scaladoc(org.graphframes.lib.ShortestPaths).

```scala
import org.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val results = g.shortestPaths.landmarks(Seq("a", "d")).run()
results.select("id", "distances").show()
```

### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.shortestPaths).

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

results = g.shortestPaths(landmarks=["a", "d"])
results.select("id", "distances").show()
```

## Triangle count

Computes the number of triangles passing through each vertex.

### Scala API

For API details, refer to the @:scaladoc(org.graphframes.lib.TriangleCount).

```scala
import org.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val results = g.triangleCount.run()
results.select("id", "count").show()
```

### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.triangleCount).

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

results = g.triangleCount()
results.select("id", "count").show()
```

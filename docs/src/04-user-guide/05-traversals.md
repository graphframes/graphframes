# Traversals and Connectivity

## Shortest paths

Computes shortest paths from each vertex to the given set of landmark vertices, where landmarks are specified by the
vertex ID. Note that this takes an edge direction into account.

See [Wikipedia](https://en.wikipedia.org/wiki/Shortest_path_problem) for a background.

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

**NOTE:** With GraphFrames 0.3.0 and later releases, the default Connected Components algorithm requires setting a Spark
checkpoint directory. Users can revert to the old algorithm using `connectedComponents.setAlgorithm("graphx")`. Starting
from GraphFrames 0.9.3 release, users can also use `localCheckpoints` that does not require setting a Spark checkpoint
directory. To use `localCheckpoints` users can set the config `spark.graphframes.useLocalCheckpoints` to `true` or use
the API `connectedComponents.setUseLocalCheckpoints(true)`. While `localCheckpoints` provides better performance they
are not as reliable as the persistent checkpointing.

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

### Strongly connected components

Compute the strongly connected component (SCC) of each vertex and return a graph with each vertex assigned to the SCC
containing that vertex. At the moment, SCC in GraphFrames is a wrapper around GraphX implementation.

See [Wikipedia](https://en.wikipedia.org/wiki/Strongly_connected_component) for the background.

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

Computes the number of triangles passing through each vertex.

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
// +----+--------------+
// | id | found_cycles |
// +----+--------------+
// |1   |[1, 3, 1]     |
// |1   |[1, 2, 1]     |
// |1   |[1, 2, 5, 1]  |
// |2   |[2, 1, 2]     |
// |2   |[2, 5, 1, 2]  |
// |3   |[3, 1, 3]     |
// |5   |[5, 1, 2, 5]  |
// +----+--------------+
```

**WARNING:** This algorithm returns all the cycles, and users should handle deduplication of \[1, 2, 1\] and \[2, 1, 2\] (
that is the same cycle)!

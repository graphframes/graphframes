# Centrality Metrics

## Degrees

GraphFrames provides three main APIs for computing degrees:

- `inDegrees`
- `outDegrees`
- `degrees`

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
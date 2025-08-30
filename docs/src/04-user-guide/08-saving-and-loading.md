# Saving and loading

At the moment, GraphFrames does not support out-of-the-box saving and loading, and one needs to use Spark SQL to save and load `vertices` and `edges` as DataFrames. Refer to the [Spark SQL User Guide on datasources](http://spark.apache.org/docs/latest/sql-programming-guide.html#data-sources) for more details.

**NOTE:** Maintainers of `GraphFrames` are working at the moment on adding support for saving and loading GraphFrames into various formats, like GraphML, Apache GraphAr (incubating), and others.

The below example shows how to save and then load a graph as parquet files for vertices and edges.

## Scala API

```scala
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends  // get example graph

// Save vertices and edges as Parquet to some location.
g.vertices.write.parquet("hdfs://myLocation/vertices")
g.edges.write.parquet("hdfs://myLocation/edges")

// Load the vertices and edges back.
val sameV = spark.read.parquet("hdfs://myLocation/vertices")
val sameE = spark.read.parquet("hdfs://myLocation/edges")

// Create an identical GraphFrame.
val sameG = GraphFrame(sameV, sameE)
```

## Python API

```python
from graphframes.examples import Graphs


g = Graphs(spark).friends()  # Get example graph

# Save vertices and edges as Parquet to some location
g.vertices.write.parquet("hdfs://myLocation/vertices")
g.edges.write.parquet("hdfs://myLocation/edges")

# Load the vertices and edges back
sameV = spark.read.parquet("hdfs://myLocation/vertices")
sameE = spark.read.parquet("hdfs://myLocation/edges")

# Create an identical GraphFrame
sameG = GraphFrame(sameV, sameE)
```
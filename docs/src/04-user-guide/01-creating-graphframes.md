# Creating GraphFrames

Users can create GraphFrames from vertex and edge DataFrames.

* *Vertex DataFrame*: A vertex DataFrame should contain a special column named "id" which specifies unique IDs for each vertex in the graph.
* *Edge DataFrame*: An edge DataFrame should contain two special columns: "src" (source vertex ID of edge) and "dst" (destination vertex ID of edge).

Both DataFrames can have arbitrary other columns. Those columns can represent vertex and edge attributes.

A GraphFrame can also be constructed from a single DataFrame containing edge information. The vertices will be inferred from the sources and destinations of the edges.

The following example demonstrates how to create a GraphFrame from vertex and edge DataFrames.

## Python API

```python
# Vertex DataFrame
v = spark.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
    ("d", "David", 29),
    ("e", "Esther", 32),
    ("f", "Fanny", 36),
    ("g", "Gabby", 60)
], ["id", "name", "age"])

# Edge DataFrame
e = spark.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
    ("f", "c", "follow"),
    ("e", "f", "follow"),
    ("e", "d", "friend"),
    ("d", "a", "friend"),
    ("a", "e", "friend")
], ["src", "dst", "relationship"])
# Create a GraphFrame
g = GraphFrame(v, e)
```

The GraphFrame constructed above is available in the GraphFrames package (not available in the Spark-Connect mode:

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph
```

## Scala API

```scala
import org.graphframes.GraphFrame

// Vertex DataFrame
val v = spark.createDataFrame(List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
)).toDF("id", "name", "age")

// Edge DataFrame
val e = spark.createDataFrame(List(
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
)).toDF("src", "dst", "relationship")
// Create a GraphFrame
val g = GraphFrame(v, e)
```

The GraphFrame constructed above is available in the GraphFrames package:

```scala
import org.graphframes.{examples, GraphFrame}

val g: GraphFrame = examples.Graphs.friends
```
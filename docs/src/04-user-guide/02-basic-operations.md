# Basic Graph Operations

## Basics

GraphFrames provide several simple graph queries, such as node degree. Also, since GraphFrames represent graphs as pairs of vertex and edge DataFrames, it is easy to make powerful queries directly on the vertex and edge DataFrames.  Those DataFrames are made available as `vertices` and `edges` fields in the GraphFrame.

### Python API

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

# Display the vertex DataFrame
g.vertices.show()

# +--+-------+---+
# |id|   name|age|
# +--+-------+---+
# | a|  Alice| 34|
# | b|    Bob| 36|
# | c|Charlie| 30|
# | d|  David| 29|
# | e| Esther| 32|
# | f|  Fanny| 36|
# | g|  Gabby| 60|
# +--+-------+---+

# Display the edge DataFrame
g.edges.show()

# +---+---+------------+
# |src|dst|relationship|
# +---+---+------------+
# |  a|  b|      friend|
# |  b|  c|      follow|
# |  c|  b|      follow|
# |  f|  c|      follow|
# |  e|  f|      follow|
# |  e|  d|      friend|
# |  d|  a|      friend|
# |  a|  e|      friend|
# +---+---+------------+

# Get a DataFrame with columns "id" and "inDegree" (in-degree)
vertexInDegrees = g.inDegrees

# Find the youngest user's age in the graph
# This queries the vertex DataFrame
g.vertices.groupBy().min("age").show()

# Count the number of "follows" in the graph
# This queries the edge DataFrame
numFollows = g.edges.filter("relationship = 'follow'").count()
```

### Scala API

```scala
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends  // get example graph

// Display the vertex and edge DataFrames
g.vertices.show()
// +--+-------+---+
// |id|   name|age|
// +--+-------+---+
// | a|  Alice| 34|
// | b|    Bob| 36|
// | c|Charlie| 30|
// | d|  David| 29|
// | e| Esther| 32|
// | f|  Fanny| 36|
// | g|  Gabby| 60|
// +--+-------+---+

g.edges.show()
// +---+---+------------+
// |src|dst|relationship|
// +---+---+------------+
// |  a|  b|      friend|
// |  b|  c|      follow|
// |  c|  b|      follow|
// |  f|  c|      follow|
// |  e|  f|      follow|
// |  e|  d|      friend|
// |  d|  a|      friend|
// |  a|  e|      friend|
// +---+---+------------+

// import Spark SQL package
import org.apache.spark.sql.DataFrame

// Get a DataFrame with columns "id" and "inDeg" (in-degree)
val vertexInDegrees: DataFrame = g.inDegrees
vertexInDegrees.show()

// Find the youngest user's age in the graph.
// This queries the vertex DataFrame.
g.vertices.groupBy().min("age").show()

// Count the number of "follows" in the graph.
// This queries the edge DataFrame.
val numFollows = g.edges.filter("relationship = 'follow'").count()
```

## Filtering edges or vertices

GraphFrames provides an API for filtering edges and vertices based on their attributes.

**NOTE:** *This API is for simple filtering. For the more complex use cases, it is recommended to use [`PropertyGraphFrame` model](/04-user-guide/11-property-graphs.md). `PropertyGraphFrame` handles the logical schema of the whole graph and provides a more powerful API for selecting any subgraph based on required properties and filters.*

### Python API

```python
from pyspark.sql import functions as F
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph
g.filterVertices(F.col("name") == F.lit("Alice"))
g.filterEdges(F.col("relationship") == F.lit("follow"))
```

### Scala API

```scala
import org.apache.spark.sql.functions._
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends
g.filterVertices(col("name") === lit("Alice"))
g.filterEdges(col("relationship") === lit("follow"))
```

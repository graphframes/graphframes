# Subgraphs

In GraphX, the `subgraph()` method takes an edge triplet (edge, src vertex, and dst vertex, plus attributes) and allows the user to select a subgraph based on triplet and vertex filters.

GraphFrames provide an even more powerful way to select subgraphs based on a combination of motif finding and DataFrame filters. We provide three helper methods for subgraph selection. `filterVertices(condition)`, `filterEdges(condition)`, and `dropIsolatedVertices()`.

## Simple subgraph
The following example shows how to select a subgraph based upon vertex and edge filters.

### Scala API

```scala
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends

// Select subgraph of users older than 30, and relationships of type "friend".
// Drop isolated vertices (users) which are not contained in any edges (relationships).
val g1 = g.filterVertices("age > 30").filterEdges("relationship = 'friend'").dropIsolatedVertices()
```

### Python API

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

# Select subgraph of users older than 30, and relationships of type "friend"
# Drop isolated vertices (users) which are not contained in any edges (relationships)
g1 = g.filterVertices("age > 30").filterEdges("relationship = 'friend'").dropIsolatedVertices()
```

## Complex subgraph: triplet filters

The following example shows how to select a subgraph based upon triplet filters which operate on an edge and its src and dst vertices.  This example could be extended to go beyond triplets by using more complex motifs.

### Scala API

```scala
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends  // get example graph

// Select subgraph based on edges "e" of type "follow"
// pointing from a younger user "a" to an older user "b".
val paths = { g.find("(a)-[e]->(b)")
  .filter("e.relationship = 'follow'")
  .filter("a.age < b.age") }
// "paths" contains vertex info. Extract the edges.
val e2 = paths.select("e.*")

// Construct the subgraph
val g2 = GraphFrame(g.vertices, e2)
```

### Python API

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

# Select subgraph based on edges "e" of type "follow"
# pointing from a younger user "a" to an older user "b"
paths = g.find("(a)-[e]->(b)")\
  .filter("e.relationship = 'follow'")\
  .filter("a.age < b.age")

# "paths" contains vertex info. Extract the edges
e2 = paths.select("e.src", "e.dst", "e.relationship")

# In Spark 1.5+, the user may simplify this call
# val e2 = paths.select("e.*")
# Construct the subgraph
g2 = GraphFrame(g.vertices, e2)
```

## Property Graphs

For more advanced subgraph selection, see the [Property Graphs](/04-user-guide/11-property-graphs.md) section.

---
layout: global
displayTitle: GraphFrames User Guide
title: User Guide
description: GraphFrames GRAPHFRAMES_VERSION user guide
---

This page gives examples of how to use GraphFrames for basic queries, motif finding, and
general graph algorithms.  This includes code examples in Scala and Python.

* Table of contents (This text will be scraped.)
{:toc}

*Note: Most examples use the GraphFrame from the first subsection:
[Creating GraphFrames](user-guide.html#creating-graphframes).*

# Creating GraphFrames

Users can create GraphFrames from vertex and edge DataFrames.

* *Vertex DataFrame*: A vertex DataFrame should contain a special column named "id" which specifies
  unique IDs for each vertex in the graph.
* *Edge DataFrame*: An edge DataFrame should contain two special columns: "src" (source vertex ID
  of edge) and "dst" (destination vertex ID of edge).

Both DataFrames can have arbitrary other columns.  Those columns can represent vertex and edge
attributes.

A GraphFrame can also be constructed from a single DataFrame containing edge information.
The vertices will be inferred from the sources and destinations of the edges.

<div class="codetabs">

The following example demonstrates how to create a GraphFrame from vertex and edge DataFrames.

<div data-lang="scala"  markdown="1">
{% highlight scala %}
// Vertex DataFrame
val v = sqlContext.createDataFrame(List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36)
)).toDF("id", "name", "age")
// Edge DataFrame
val e = sqlContext.createDataFrame(List(
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend")
)).toDF("src", "dst", "relationship")
// Create a GraphFrame
val g = GraphFrame(v, e)
{% endhighlight %}

The GraphFrame constructed above is available in the GraphFrames package:
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
# Vertex DataFrame
v = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36)
], ["id", "name", "age"])
# Edge DataFrame
e = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend")
], ["src", "dst", "relationship"])
# Create a GraphFrame
g = GraphFrame(v, e)
{% endhighlight %}

The GraphFrame constructed above is available in the GraphFrames package:
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()
{% endhighlight %}
</div>

</div>

# Basic graph and DataFrame queries

GraphFrames provide several simple graph queries, such as node degree.

Also, since GraphFrames represent graphs as pairs of vertex and edge DataFrames, it is easy to make
powerful queries directly on the vertex and edge DataFrames.  Those DataFrames are made available
as `vertices` and `edges` fields in the GraphFrame.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
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
// +---+---+------------+

// Get a DataFrame with columns "id" and "inDeg" (in-degree)
val vertexInDegrees: DataFrame = g.inDegrees

// Find the youngest user's age in the graph.
// This queries the vertex DataFrame.
g.vertices.groupBy().min("age").show()

// Count the number of "follows" in the graph.
// This queries the edge DataFrame.
val numFollows = g.edges.filter("relationship = 'follow'").count()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

# Display the vertex and edge DataFrames
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
# +--+-------+---+

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
# +---+---+------------+

# Get a DataFrame with columns "id" and "inDeg" (in-degree)
# TODO: vertexInDegrees = g.inDegrees

# Find the youngest user's age in the graph.
# This queries the vertex DataFrame.
g.vertices.groupBy().min("age").show()

# Count the number of "follows" in the graph.
# This queries the edge DataFrame.
numFollows = g.edges.filter("relationship = 'follow'").count()
{% endhighlight %}
</div>

</div>

# Motif finding

Motif finding refers to searching for structural patterns in a graph.

GraphFrame motif finding uses a simple Domain-Specific Language (DSL) for expressing structural
queries. For example, `graph.find("(a)-[e]->(b); (b)-[e2]->(a)")` will search for pairs of vertices
`a,b` connected by edges in both directions.  It will return a `DataFrame` of all such
structures in the graph, with columns for each of the named elements (vertices or edges)
in the motif.  In this case, the returned columns will be "a, b, e, e2."

DSL for expressing structural patterns:

* The basic unit of a pattern is an edge.
   For example, `"(a)-[e]->(b)"` expresses an edge `e` from vertex `a` to vertex `b`.
   Note that vertices are denoted by parentheses `(a)`, while edges are denoted by
   square brackets `[e]`.
* A pattern is expressed as a union of edges. Edge patterns can be joined with semicolons.
   Motif `"(a)-[e]->(b); (b)-[e2]->(c)"` specifies two edges from `a` to `b` to `c`.
* Within a pattern, names can be assigned to vertices and edges.  For example,
   `"(a)-[e]->(b)"` has three named elements: vertices `a,b` and edge `e`.
   These names serve two purposes:
  * The names can identify common elements among edges.  For example,
      `"(a)-[e]->(b); (b)-[e2]->(c)"` specifies that the same vertex `b` is the destination
      of edge `e` and source of edge `e2`.
  * The names are used as column names in the result `DataFrame`.  If a motif contains
      named vertex `a`, then the result `DataFrame` will contain a column "a" which is a
      `StructType` with sub-fields equivalent to the schema (columns) of
      [[GraphFrame.vertices]]. Similarly, an edge `e` in a motif will produce a column "e"
      in the result `DataFrame` with sub-fields equivalent to the schema (columns) of
      [[GraphFrame.edges]].
* It is acceptable to omit names for vertices or edges in motifs when not needed.
   E.g., `"(a)-[]->(b)"` expresses an edge between vertices `a,b` but does not assign a name
   to the edge.  There will be no column for the anonymous edge in the result `DataFrame`.
   Similarly, `"(a)-[e]->()"` indicates an out-edge of vertex `a` but does not name
   the destination vertex.

More complex queries, such as queries which operate on vertex or edge attributes,
can be expressed by applying filters to the result `DataFrame`.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

// Search for pairs of vertices with edges in both directions between them.
val motifs: DataFrame = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()

// More complex queries can be expressed by applying filters.
motifs.filter("e.relationship = 'follow' AND e2.relationship = 'follow'").show()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

# Search for pairs of vertices with edges in both directions between them.
motifs = g.find("(a)-[e]->(b); (b)-[e2]->(a)")
motifs.show()

# More complex queries can be expressed by applying filters.
motifs.filter("e.relationship = 'follow' AND e2.relationship = 'follow'").show()
{% endhighlight %}
</div>

</div>

Many motif queries are stateless and simple to express, as in the examples above.
The next examples demonstrate more complex queries which carry state along a path in the motif.
These queries can be expressed by combining GraphFrame motif finding with filters on the result,
where the filters use sequence operations to construct a series of `DataFrame` `Column`s.

For example, suppose one wishes to identify a chain of 4 vertices with some property defined
by a sequence of functions.  That is, among chains of 4 vertices `a->b->c->d`, identify the subset
of chains matching this complex filter:

* Initialize state on path.
* Update state based on vertex `a`.
* Update state based on vertex `b`.
* Etc. for `c` and `d`.
* If final state matches some condition, then the chain is accepted by the filter.

The below code snippets demonstrate this process, where we identify chains of 4 vertices
such that at least 2 of the 3 edges are "friend" relationships.
In this example, the state is the current count of "friend" edges; in general, it could be any
`DataFrame Column`.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}
import org.graphframes.examples

val g = examples.Graphs.friends  // get example graph

// Query on sequence, with state (cnt)
//  (a) Define method for updating state given the next element of the motif.
def sumFriends(cnt: Column, relationship: Column): Column = {
  when(relationship === "friend", cnt + 1).otherwise(cnt)
}
//  (b) Use sequence operation to apply method to sequence of elements in motif.
//      In this case, the elements are the 3 edges.
val condition = Seq("ab", "bc", "cd").
  foldLeft(lit(0))((cnt, e) => sumFriends(cnt, col(e)("relationship")))
//  (c) Apply filter to DataFrame.
val chainWith2Friends2 = chain4.where(condition >= 2)
chainWith2Friends2.show()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from pyspark.sql.functions import col, lit, udf, when
from pyspark.sql.types import IntegerType
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

# Query on sequence, with state (cnt)
#  (a) Define method for updating state given the next element of the motif.
sumFriends =\
  lambda cnt,relationship: when(relationship == "friend", cnt+1).otherwise(cnt)
#  (b) Use sequence operation to apply method to sequence of elements in motif.
#      In this case, the elements are the 3 edges.
condition =\
  reduce(lambda cnt,e: sumFriends(cnt, col(e).relationship), ["ab", "bc", "cd"], lit(0))
#  (c) Apply filter to DataFrame.
chainWith2Friends2 = chain4.where(condition >= 2)
chainWith2Friends2.show()
{% endhighlight %}
</div>

</div>

The above example demonstrated a stateful motif for a fixed-length chain.  Currently, in order to
search for variable-length motifs, users need to run one query for each possible length.
However, the above query patterns allow users to re-use the same code for each length, with the
only change being to update the sequence of motif elements ("ab", "bc", "cd" above).

# Subgraphs

In GraphX, the `subgraph()` method takes an edge triplet (edge, src vertex, and dst vertex, plus
attributes) and allows the user to select a subgraph based on triplet and vertex filters.

GraphFrames provide an even more powerful way to select subgraphs based on a combination of
motif finding and DataFrame filters.

**Simple subgraph: vertex and edge filters**:
The following example shows how to select a subgraph based upon vertex and edge filters.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends

// Select subgraph of users older than 30, and edges of type "friend"
val v2 = g.vertices.filter("age > 30")
val e2 = g.edges.filter("relationship = 'friend'")
val g2 = GraphFrame(v2, e2)
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

# Select subgraph of users older than 30, and edges of type "friend"
v2 = g.vertices.filter("age > 30")
e2 = g.edges.filter("relationship = 'friend'")
g2 = GraphFrame(v2, e2)
{% endhighlight %}
</div>

</div>

**Complex subgraph: triplet filters**:
The following example shows how to select a subgraph based upon triplet filters which
operate on an edge and its src and dst vertices.  This example could be extended to go beyond
triplets by using more complex motifs.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

// Select subgraph based on edges "e" of type "follow"
// pointing from a younger user "a" to an older user "b".
val paths = g.find("(a)-[e]->(b)")
  .filter("e.relationship = 'follow'")
  .filter("a.age < b.age")
// "paths" contains vertex info. Extract the edges.
val e2 = paths.select("e.src", "e.dst", "e.relationship")
// In Spark 1.5+, the user may simplify this call:
//  val e2 = paths.select("e.*")

// Construct the subgraph
val g2 = GraphFrame(g.vertices, e2)
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

# Select subgraph based on edges "e" of type "follow"
# pointing from a younger user "a" to an older user "b".
paths = g.find("(a)-[e]->(b)")\
  .filter("e.relationship = 'follow'")\
  .filter("a.age < b.age")
# "paths" contains vertex info. Extract the edges.
e2 = paths.select("e.src", "e.dst", "e.relationship")
# In Spark 1.5+, the user may simplify this call:
#  val e2 = paths.select("e.*")

# Construct the subgraph
g2 = GraphFrame(g.vertices, e2)
{% endhighlight %}
</div>

</div>

# Graph algorithms

GraphFrames provides the same suite of standard graph algorithms as GraphX, plus some new ones.
We provide brief descriptions and code snippets below.  See the API docs for more details.

## Breadth-first search (BFS)

Breadth-first search (BFS) finds the shortest path(s) from one vertex (or a set of vertices)
to another vertex (or a set of vertices).  The beginning and end vertices are specified as
Spark DataFrame expressions.

<div class="codetabs">

The following code snippets search for people connected to the user "Bob."

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

// Search from "Bob" for users of age <= 30.
val paths: DataFrame = g.bfs("name = 'Bob'", "age <= 30").run()
paths.show()

// Specify edge filters or max path lengths.
g.bfs("name = 'Bob'", "age <= 30")
  .setEdgeFilter("relationship != 'follow'")
  .setMaxPathLength(3)
  .run()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

# Search from "Bob" for users of age <= 30.
paths = g.bfs("name = 'Bob'", "age <= 30")
paths.show()

# Specify edge filters or max path lengths.
g.bfs("name = 'Bob'", "age <= 30",\
  edgeFilter="relationship != 'follow'", maxPathLength=3)
{% endhighlight %}
</div>

</div>

## Connected components

Computes the connected component membership of each vertex and returns a graph with each vertex
assigned a component ID.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

val result = g.connectedComponents.run()
result.vertices.select("id", "component").show()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

result = g.connectedComponents()
result.vertices.select("id", "component").show()
{% endhighlight %}
</div>

</div>

### Strongly connected components

Compute the strongly connected component (SCC) of each vertex and return a graph with each vertex
assigned to the SCC containing that vertex.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

val result = g.stronglyConnectedComponents.numIter(10).run()
result.vertices.select("id", "component").show()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

result = g.stronglyConnectedComponents(numIter=10)
result.vertices.select("id", "component").show()
{% endhighlight %}
</div>

</div>

## Label propagation

Run static Label Propagation for detecting communities in networks.

Each node in the network is initially assigned to its own community. At every superstep, nodes
send their community affiliation to all neighbors and update their state to the mode community
affiliation of incoming messages.

LPA is a standard community detection algorithm for graphs. It is very inexpensive
computationally, although (1) convergence is not guaranteed and (2) one can end up with
trivial solutions (all nodes are identified into a single community).

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

val result = g.labelPropagation.maxSteps(5).run()
result.vertices.select("id", "label").show()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

result = g.labelPropagation(maxSteps=5)
result.vertices.select("id", "label").show()
{% endhighlight %}
</div>

</div>

## PageRank

There are two implementations of PageRank.

* The first implementation uses the standalone [[GraphFrame]] interface and runs PageRank
 for a fixed number of iterations.  This can be run by setting `numIter`.
* The second implementation uses the `org.apache.spark.graphx.Pregel` interface and runs PageRank
  until convergence.  This can be run by setting `tol`.

Both implementations support non-personalized and personalized PageRank, where setting a `sourceId`
personalizes the results for that vertex.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

// Run PageRank until convergence to tolerance "tol".
val results = g.pageRank.resetProbability(0.15).tol(0.01).run()
// Display resulting pageranks and final edge weights
results.vertices.select("id", "pagerank").show()
results.edges.select("src", "dst", "weight").show()

// Run PageRank for a fixed number of iterations.
val results2 = g.pageRank.resetProbability(0.15).numIter(10).run()

// Run PageRank personalized for vertex "a"
val results3 = g.pageRank.resetProbability(0.15).numIter(10).sourceId("a").run()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

# Run PageRank until convergence to tolerance "tol".
results = g.pageRank(resetProbability=0.15, tol=0.01)
# Display resulting pageranks and final edge weights
results.vertices.select("id", "pagerank").show()
results.edges.select("src", "dst", "weight").show()

# Run PageRank for a fixed number of iterations.
results2 = g.pageRank(resetProbability=0.15, numIter=10)

# Run PageRank personalized for vertex "a"
results3 = g.pageRank(resetProbability=0.15, numIter=10, sourceId="a")
{% endhighlight %}
</div>

</div>

## Shortest paths

Computes shortest paths to the given set of landmark vertices.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

val results = g.shortestPaths.landmarks(Seq("a", "d")).run()
results.vertices.select("id", "distances").show()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

results = g.shortestPaths(landmarks=["a", "d"])
results.vertices.select("id", "distances").show()
{% endhighlight %}
</div>

</div>

## Triangle count

Computes the number of triangles passing through each vertex.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

val results = g.triangleCount.run()
results.vertices.select("id", "count").show()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

results = g.triangleCount()
results.vertices.select("id", "count").show()
{% endhighlight %}
</div>

</div>

# Saving and loading GraphFrames

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

TODO
{% endhighlight %}
</div>

</div>

# GraphX compatibility

## GraphX-GraphFrame conversions

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

TODO
{% endhighlight %}
</div>

</div>

## GraphX APIs

algorithms

aggregateMessages

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.graphframes.examples
val g: GraphFrame = examples.Graphs.friends  // get example graph

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
from graphframes.examples import Graphs
g = Graphs(sqlContext).friends()  # Get example graph

TODO
{% endhighlight %}
</div>

</div>

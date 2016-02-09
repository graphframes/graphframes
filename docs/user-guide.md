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
]).toDF("id", "name", "age")
# Edge DataFrame
e = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend")
]).toDF("src", "dst", "relationship")
# Create a GraphFrame
g = GraphFrame(v, e)
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
import org.apache.spark.sql.functions._
val g: GraphFrame = ...  // Create a GraphFrame

// Get a DataFrame with columns "id" and "inDeg" (in-degree)
val vertexInDegrees: DataFrame = g.inDegrees

// Find the youngest user in the graph.
// This queries the vertex DataFrame.
val youngest = g.vertices.select(min("age"), "name")
youngest.show()

// Count the number of "follows" in the graph.
// This queries the edge DataFrame.
val numFollows = g.edges.filter("relationship = 'follow'").count()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
{% endhighlight %}
</div>

</div>

# Motif finding

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
{% endhighlight %}
</div>

</div>

# Subgraphs

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
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
val g: GraphFrame = ...  // Create a GraphFrame

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
g = ...  # Create a GraphFrame

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

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
{% endhighlight %}
</div>

</div>

### Strongly connected components

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
{% endhighlight %}
</div>

</div>

## Label propagation

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
{% endhighlight %}
</div>

</div>

## PageRank

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
{% endhighlight %}
</div>

</div>

## Shortest paths

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
{% endhighlight %}
</div>

</div>

## SVD++

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
{% endhighlight %}
</div>

</div>

## Triangle count

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
{% endhighlight %}
</div>

</div>

# Saving and loading GraphFrames

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
{% endhighlight %}
</div>

</div>

# GraphX compatibility

## GraphX-GraphFrame conversions

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

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
val g: GraphFrame = ...  // Create a GraphFrame

TODO
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
g = ...  # Create a GraphFrame

TODO
{% endhighlight %}
</div>

</div>

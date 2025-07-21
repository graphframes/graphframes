---
layout: global
displayTitle: GraphFrames Quick-Start Guide
title: Quick-Start Guide
description: GraphFrames GRAPHFRAMES_VERSION guide for getting started quickly
---

This quick-start guide shows how to get started using GraphFrames.
After you work through this guide, move on to the [User Guide](user-guide.html)
to learn more about the many queries and algorithms supported by GraphFrames.

* Table of contents
{:toc}

# Getting started with Apache Spark and Spark packages

If you are new to using Apache Spark, refer to the
[Apache Spark Documentation](http://spark.apache.org/docs/latest/index.html) and its
[Quick-Start Guide](http://spark.apache.org/docs/latest/quick-start.html) for more information.

The following example shows how to run the Spark shell with the GraphFrames package.
We use the `--packages` argument to download the graphframes package and any dependencies automatically.

<div class="codetabs">

<div data-lang="scala"  markdown="1">

{% highlight bash %}
$ ./bin/spark-shell --packages io.graphframes:graphframes-spark3_2.12:0.9.2
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

{% highlight bash %}
$ ./bin/pyspark --packages io.graphframes:graphframes-spark3_2.12:0.9.2
{% endhighlight %}

</div>

</div>

# Start using GraphFrames

The following example shows how to create a GraphFrame, query it, and run the PageRank algorithm.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
// import graphframes package
import org.graphframes._
// Create a Vertex DataFrame with unique ID column "id"
val v = spark.createDataFrame(List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30)
)).toDF("id", "name", "age")

// Create an Edge DataFrame with "src" and "dst" columns
val e = spark.createDataFrame(List(
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow")
)).toDF("src", "dst", "relationship")
// Create a GraphFrame
import org.graphframes.GraphFrame
val g = GraphFrame(v, e)

// Query: Get in-degree of each vertex.
g.inDegrees.show()

// Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

// Run PageRank algorithm, and show results.
val results = g.pageRank.resetProbability(0.01).maxIter(20).run()
results.vertices.select("id", "pagerank").show()
{% endhighlight %}
</div>

<div data-lang="python"  markdown="1">
{% highlight python %}
# Create a Vertex DataFrame with unique ID column "id"
v = spark.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
], ["id", "name", "age"])

# Create an Edge DataFrame with "src" and "dst" columns
e = spark.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
], ["src", "dst", "relationship"])
# Create a GraphFrame
from graphframes import *
g = GraphFrame(v, e)

# Query: Get in-degree of each vertex.
g.inDegrees.show()

# Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()
{% endhighlight %}
</div>

</div>

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

If you are new to using [Spark packages](http://spark-packages.org/package/graphframes/graphframes), you can find more information
in the [Spark User Guide on using the interactive shell](http://spark.apache.org/docs/latest/programming-guide.html#using-the-shell).
You just need to make sure your Spark shell session has the package as a dependency.

The following example shows how to run the Spark shell with the GraphFrames package.
We use the `--packages` argument to download the graphframes package and any dependencies automatically.

<div class="codetabs">

<div data-lang="scala"  markdown="1">

{% highlight bash %}
$ ./bin/spark-shell --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

{% highlight bash %}
$ ./bin/pyspark --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11
{% endhighlight %}

</div>

</div>

The above examples of running the Spark shell with GraphFrames use a specific version of the GraphFrames
package.  To use a different version, just change the last part of the `--packages` argument;
for example, to run with version `0.1.0-spark1.6`, pass the argument
`--packages graphframes:graphframes:0.1.0-spark1.6`.

# Start using GraphFrames

The following example shows how to create a GraphFrame, query it, and run the PageRank algorithm.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
// import graphframes package
import org.graphframes._
// Create a Vertex DataFrame with unique ID column "id"
val v = sqlContext.createDataFrame(List(
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30)
)).toDF("id", "name", "age")
// Create an Edge DataFrame with "src" and "dst" columns
val e = sqlContext.createDataFrame(List(
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
v = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
], ["id", "name", "age"])
# Create an Edge DataFrame with "src" and "dst" columns
e = sqlContext.createDataFrame([
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

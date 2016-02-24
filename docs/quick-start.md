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

# Getting started with Spark and Spark packages

If you are new to using Spark, refer to the
[Spark Documentation](http://spark.apache.org/docs/latest/index.html) and its
[Quick-Start Guide](http://spark.apache.org/docs/latest/quick-start.html) for more information.

If you are new to using [Spark packages](http://spark-packages.org/), you can find more information
in the [Spark User Guide on using the interactive shell](http://spark.apache.org/docs/latest/programming-guide.html#using-the-shell).
You just need to make sure your Spark shell session has the package as a dependency.

The following example shows how to run the Spark shell with the GraphFrames package.

<div class="codetabs">

<div data-lang="scala"  markdown="1">

If you have GraphFrames available as a JAR `graphframes.jar`, you can add the JAR to the shell
classpath:

{% highlight bash %}
$ ./bin/spark-shell --master local[4] --jars graphframes.jar
{% endhighlight %}

</div>

<div data-lang="python"  markdown="1">

If you have GraphFrames available as a JAR `graphframes.jar`, you can make GraphFrames available
by passing the JAR to the pyspark shell script as follows:

{% highlight bash %}
$ ./bin/pyspark --master local[4] --py-files graphframes.jar --jars graphframes.jar
{% endhighlight %}

</div>

</div>

# Start using GraphFrames

The following example shows how to create a GraphFrame, query it, and run the PageRank algorithm.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% highlight scala %}
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
val g = GraphFrame(v, e)

// Query: Get in-degree of each vertex.
g.inDegrees.show()

// Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

// Run PageRank algorithm, and show results.
val results = g.pageRank.resetProbability(0.01).numIter(20).run()
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
g = GraphFrame(v, e)

# Query: Get in-degree of each vertex.
g.inDegrees().show()

# Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.01, numIter=20)
results.vertices.select("id", "pagerank").show()
{% endhighlight %}
</div>

</div>

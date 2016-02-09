---
layout: global
displayTitle: GraphFrames Overview
title: Overview
description: GraphFrames GRAPHFRAMES_VERSION documentation homepage
---

GraphFrames is a package for Apache Spark which provides DataFrame-based Graphs.
It provides high-level APIs in Scala, Java, and Python.
It aims to provide both the functionality of GraphX and extended functionality taking advantage
of Spark DataFrames.  This extended functionality includes motif finding, DataFrame-based
serialization, and highly expressive graph queries.

# What are GraphFrames?

*GraphX is to RDDs as GraphFrames are to DataFrames.*

GraphFrames represent graphs: vertices (e.g., users) and edges (e.g., relationships between users).
If you are familiar with [GraphX](http://spark.apache.org/docs/latest/graphx-programming-guide.html),
then GraphFrames will be easy to learn.  The key difference is that GraphFrames are based upon
[Spark DataFrames](http://spark.apache.org/docs/latest/sql-programming-guide.html), rather than RDDs.

GraphFrames also provide powerful tools for running queries and standard graph algorithms.
With GraphFrames, you can easily search for patterns within graphs, find important vertices, and more.
Refer to the [User Guide](user-guide.html) for a full list of queries and algorithms.

# Downloading

Get GraphFrames from the [Spark Packages website](http://spark-packages.org).
This documentation is for GraphFrames version {{site.GRAPHFRAMES_VERSION}}.
GraphFrames depends on Apache Spark, which is available for download from the
[Apache Spark website](http://spark.apache.org).

GraphFrames should be compatible with any platform which runs Spark.
Refer to the [Spark documentation](http://spark.apache.org/docs/latest) for more information.

GraphFrames is compatible with Spark 1.4+.  However, later versions of Spark include major improvements
to DataFrames, so GraphFrames may be more efficient when running on more recent Spark versions.

# Running the Examples and Shell

**TODO: UPDATE THIS SECTION**

GraphFrames comes with several sample programs.  Scala, Java, and Python examples are in the
`examples/src/main` directory. To run one of the Java or Scala sample programs, use
`bin/run-example <class> [params]` in the top-level GraphFrames directory. (Behind the scenes, this
invokes the more general
[`spark-submit` script](submitting-applications.html) for
launching applications). For example,

    ./bin/run-example SparkPi 10

You can also run Spark interactively through a modified version of the Scala shell. This is a
great way to learn the framework.

    ./bin/spark-shell --master local[2]

The `--master` option specifies the
[master URL for a distributed cluster](submitting-applications.html#master-urls), or `local` to run
locally with one thread, or `local[N]` to run locally with N threads. You should start by using
`local` for testing. For a full list of options, run Spark shell with the `--help` option.

Spark also provides a Python API. To run Spark interactively in a Python interpreter, use
`bin/pyspark`:

    ./bin/pyspark --master local[2]

Example applications are also provided in Python. For example,

    ./bin/spark-submit examples/src/main/python/pi.py 10

Spark also provides an experimental [R API](sparkr.html) since 1.4 (only DataFrames APIs included).
To run Spark interactively in a R interpreter, use `bin/sparkR`:

    ./bin/sparkR --master local[2]

Example applications are also provided in R. For example,
    
    ./bin/spark-submit examples/src/main/r/dataframe.R

# Launching on a Cluster

**TODO: UPDATE THIS SECTION**


# Where to Go from Here

**TODO: UPDATE THIS SECTION**

**User Guides:**

* [Quick Start](quick-start.html): a quick introduction to the GraphFrames API; start here!
* [GraphFrames User Guide](user-guide.html): detailed overview of GraphFrames
  in all supported languages (Scala, Python)

**API Docs:**

* [GraphFrames Scala API (Scaladoc)](api/scala/index.html#org.graphframes.package)
* [GraphFrames Python API (Sphinx)](api/python/index.html)

**Other Documents:**

**TODO: PULL INFO FROM GRAPHX DOCS?**

* [Configuration](configuration.html): customize Spark via its configuration system
* [Tuning Guide](tuning.html): best practices to optimize performance and memory use
* [Building Spark](building-spark.html): build Spark using the Maven system
* [Contributing to Spark](https://cwiki.apache.org/confluence/display/SPARK/Contributing+to+Spark)

**External Resources:**

* [Spark Homepage](http://spark.apache.org)
* [Spark Wiki](https://cwiki.apache.org/confluence/display/SPARK)
* [Mailing Lists](http://spark.apache.org/mailing-lists.html): ask questions about Spark here
* [Code Examples](http://spark.apache.org/examples.html): more are also available in the `examples` subfolder of Spark ([Scala]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/scala/org/apache/spark/examples),
 [Java]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/java/org/apache/spark/examples),
 [Python]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/python))

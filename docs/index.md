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
[Spark DataFrames](http://spark.apache.org/docs/latest/sql-programming-guide.html),
rather than [RDDs](http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds).

GraphFrames also provide powerful tools for running queries and standard graph algorithms.
With GraphFrames, you can easily search for patterns within graphs, find important vertices, and more.
Refer to the [User Guide](user-guide.html) for a full list of queries and algorithms.

# Downloading

Get GraphFrames from the [Spark Packages website](http://spark-packages.org).
This documentation is for GraphFrames version {{site.GRAPHFRAMES_VERSION}}.
GraphFrames depends on Apache Spark, which is available for download from the
[Apache Spark website](http://spark.apache.org).

GraphFrames should be compatible with any platform which runs Spark.
Refer to the [Apache Spark documentation](http://spark.apache.org/docs/latest) for more information.

GraphFrames is compatible with Spark 1.4+.  However, later versions of Spark include major improvements
to DataFrames, so GraphFrames may be more efficient when running on more recent Spark versions.

# Applications, the Apache Spark shell, and clusters

See the [Apache Spark User Guide](http://spark.apache.org/docs/latest/) for more information about
submitting Spark jobs to clusters, running the Spark shell, and launching Spark clusters.
The [GraphFrame Quick-Start guide](quick-start.html) also shows how to run the Spark shell with
GraphFrames supplied as a package.

# Where to Go from Here

**User Guides:**

* [Quick Start](quick-start.html): a quick introduction to the GraphFrames API; start here!
* [GraphFrames User Guide](user-guide.html): detailed overview of GraphFrames
  in all supported languages (Scala, Java, Python)

**API Docs:**

* [GraphFrames Scala API (Scaladoc)](api/scala/index.html#org.graphframes.package)
* [GraphFrames Python API (Sphinx)](api/python/index.html)

**External Resources:**

* [Apache Spark Homepage](http://spark.apache.org)
* [Apache Spark Wiki](https://cwiki.apache.org/confluence/display/SPARK)
* [Mailing Lists](http://spark.apache.org/mailing-lists.html): Ask questions about Spark here

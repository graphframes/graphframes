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

*GraphFrames are to DataFrames as GraphX is to RDDs.*

GraphFrames represent graphs: vertices (e.g., users) and edges (e.g., relationships between users).
If you are familiar with [GraphX](http://spark.apache.org/docs/latest/graphx-programming-guide.html),
then GraphFrames will be easy to learn.  The key difference is that GraphFrames are based upon
[Spark DataFrames](http://spark.apache.org/docs/latest/sql-programming-guide.html),
rather than [RDDs](http://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds).

GraphFrames also provide powerful tools for running queries and standard graph algorithms.
With GraphFrames, you can easily search for patterns within graphs, find important vertices, and more.
Refer to the [User Guide](user-guide.html) for a full list of queries and algorithms.

__Will GraphFrames be part of Apache Spark?__

The GraphX component of Apache Spark has no DataFrames - or Dataset-based equivalent, so it is
natural to ask this question.

**The new plan is to merge GraphFrames with Apache Spark to make it a component of core Spark.
[Spark committer Holden Karau](https://spark.apache.org/committers.html#:~:text=Holden%20Karau,Netflix)
is willing to support this effort by reviewing the relevant pull requests.**

# Downloading

Get GraphFrames from the [Maven Central](https://central.sonatype.com/namespace/io.graphframes).
This documentation is for GraphFrames version {{site.GRAPHFRAMES_VERSION}}.
GraphFrames depends on Apache Spark, which is available for download from the
[Apache Spark website](http://spark.apache.org).

GraphFrames should be compatible with any platform which runs Spark.
Refer to the [Apache Spark documentation](http://spark.apache.org/docs/latest) for more information.

GraphFrames is compatible with Spark 3.4+. However, later versions of Spark include major improvements
to DataFrames, so GraphFrames may be more efficient when running on more recent Spark versions.

GraphFrames is tested with Java 8, 11 and 17, Python 3, Spark 3.5 and Spark 4.0 (Scala 2.12 / Scala 2.13).

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
* [Motif Finding Tutorial](motif-tutorial.html): learn to perform pattern recognition with GraphFrames using a technique called network motif finding over the knowledge graph for the `stackexchange.com` subdomain [data dump](https://archive.org/details/stackexchange)
* [GraphFrames Configurations](configurations.html): detailed information about GraphFrames configurations, their descriptions, and usage examples

**API Docs:**

* [GraphFrames Scala API (Scaladoc)](api/scala/index.html#org.graphframes.package)
* [GraphFrames Python API (Sphinx)](api/python/index.html)

**Community Forums:**

* [GraphFrames Mailing List](https://groups.google.com/g/graphframes/): ask questions about GraphFrames here
* [#graphframes Discord Channel on GraphGeeks](https://discord.com/channels/1162999022819225631/1326257052368113674)

**External Resources:**

* [Apache Spark Homepage](http://spark.apache.org)
* [Apache Spark Wiki](https://cwiki.apache.org/confluence/display/SPARK)
* [Apache Spark Mailing Lists](http://spark.apache.org/mailing-lists.html)
* [GraphFrames on Stack Overflow](https://stackoverflow.com/questions/tagged/graphframes)

# graphframes
[![Build Status](https://travis-ci.org/graphframes/graphframes.svg?branch=master)](https://travis-ci.org/graphframes/graphframes)
[![codecov.io](http://codecov.io/github/graphframes/graphframes/coverage.svg?branch=master)](http://codecov.io/github/graphframes/graphframes?branch=master)


# GraphFrames: DataFrame-based Graphs

This is a package for DataFrame-based graphs on top of Apache Spark. Users can write highly expressive queries by leveraging the DataFrame API, combined with a new API for network motif finding. The user also benefits from DataFrame performance optimizations within the Spark SQL engine. GraphFrames works in Java, Scala, and Python.

You can find user guide and API docs at https://graphframes.github.io/graphframes

## Installation and Quick-Start

The easiest way to start using GraphFrames is through the Spark Packages system. Just run the following command:

```bash
# Interactive Scala/Java
$ spark-shell --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12

# Interactive Python
$ pyspark --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12

# Submit a script in Scala/Java/Python
$ spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 script.py
```

Now you can create a GraphFrame as follows.

In Python:

```python
from pyspark.sql import SparkSession
from graphframes import GraphFrame

spark = SparkSession.builder.getOrCreate()

nodes = [
    (1, "Alice", 30),
    (2, "Bob", 25),
    (3, "Charlie", 35)
]
nodes_df = spark.createDataFrame(nodes, ["id", "name", "age"])

edges = [
    (1, 2, "friend"),
    (2, 1, "friend"),
    (2, 3, "friend"),
    (3, 2, "enemy")  # eek!
]
edges_df = spark.createDataFrame(edges, ["src", "dst", "relationship"])

g = GraphFrame(nodes_df, edges_df)
```

Now let's run some graph algorithms at scale!

```python
g.inDegrees.show()

# +---+--------+
# | id|inDegree|
# +---+--------+
# |  2|       2|
# |  1|       1|
# |  3|       1|
# +---+--------+

g.outDegrees.show()

# +---+---------+
# | id|outDegree|
# +---+---------+
# |  1|        1|
# |  2|        2|
# |  3|        1|
# +---+---------+

g.degrees.show()

# +---+------+
# | id|degree|
# +---+------+
# |  1|     2|
# |  2|     4|
# |  3|     2|
# +---+------+

g2 = g.pageRank(resetProbability=0.15, tol=0.01)
g2.vertices.show()

# +---+-----+---+------------------+
# | id| name|age|          pagerank|
# +---+-----+---+------------------+
# |  1| John| 30|0.7758750474847483|
# |  2|Alice| 25|1.4482499050305027|
# |  3|  Bob| 35|0.7758750474847483|
# +---+-----+---+------------------+

# GraphFrames' most used feature...
# Connected components can do big data entity resolution on billions or even trillions of records!
# First connect records with a similarity metric, then run connectedComponents. That gives you groups of identical records, which you then link by edges or merge into list-based master records.
sc.setCheckpointDir("/tmp/graphframes-example-connected-components")  # required by GraphFrames.connectedComponents
g.connectedComponents().show()

# +---+-----+---+---------+
# | id| name|age|component|
# +---+-----+---+---------+
# |  1| John| 30|        1|
# |  2|Alice| 25|        1|
# |  3|  Bob| 35|        1|
# +---+-----+---+---------+

# Find frenemies with network motif finding! Isn't it cool how graph and relational queries are combined?
g.find("(a)-[e]->(b); (b)-[e2]->(a)").filter("e.relationship = 'friend' and e2.relationship = 'enemy'").show()

# These are paths, which you can aggregate and count to find complex patterns.
# +------------+--------------+----------------+-------------+
# |           a|             e|               b|           e2|
# +------------+--------------+----------------+-------------+
# |{2, Bob, 25}|{2, 3, friend}|{3, Charlie, 35}|{3, 2, enemy}|
# +------------+--------------+----------------+-------------+
```

## Learn GraphFrames

To learn more about GraphFrames, check out these resources:

* [GraphFrames Network Motif Finding Tutorial](https://graphframes.github.io/graphframes/docs/_site/motif-tutorial.html)
* [Introducing GraphFrames](https://databricks.com/blog/2016/03/03/introducing-graphframes.html)
* [On-Time Flight Performance with GraphFrames for Apache Spark](https://databricks.com/blog/2016/03/16/on-time-flight-performance-with-graphframes-for-apache-spark.html)

## GraphFrames on PyPI is Unofficial

The project is not in ownership or control of the [graphframes PyPI package](https://pypi.org/project/graphframes/) (installs 0.6.0) or [graphframes-latest PyPI package](https://pypi.org/project/graphframes-latest/) (installs 0.8.3). We recommend using the Spark Packages system to install the latest version of GraphFrames. The PyPI packages are not maintained by the GraphFrames project.

If you are in control of one of these packages, please reach out to us to discuss how we can work together to keep them up to date. Hopefully this situation will be addressed in the near future.

See [Installation and Quick-Start](#installation-and-quick-start) for the best way to install and use GraphFrames.

## GraphFrames Internals

To learn how GraphFrames works internally to combine graph and relational queries, check out the paper [GraphFrames: An Integrated API for Mixing Graph and
Relational Queries, Dave et al. 2016](https://people.eecs.berkeley.edu/~matei/papers/2016/grades_graphframes.pdf).

## Building and running unit tests

To compile this project, run `build/sbt assembly` from the project home directory. This will also run the Scala unit tests.

To run the Python unit tests, run the `run-tests.sh` script from the `python/` directory. You will need to set `SPARK_HOME` to your local Spark installation directory.

## Release new version

Please see guide `dev/release_guide.md`.

## Spark version compatibility

This project is compatible with Spark 2.4+.  However, significant speed improvements have been made to DataFrames in more recent versions of Spark, so you may see speedups from using the latest Spark version.

## Contributing

GraphFrames is collaborative effort among UC Berkeley, MIT, Databricks and the open source community. We welcome open source contributions as well!

## Releases:

See [release notes](https://github.com/graphframes/graphframes/releases).

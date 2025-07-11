<img src="docs/img/GraphFrames-Logo-Large.png" alt="GraphFrames Logo" width="400"/>

[![Scala CI](https://github.com/graphframes/graphframes/actions/workflows/scala-ci.yml/badge.svg)](https://github.com/graphframes/graphframes/actions/workflows/scala-ci.yml)
[![Python CI](https://github.com/graphframes/graphframes/actions/workflows/python-ci.yml/badge.svg)](https://github.com/graphframes/graphframes/actions/workflows/python-ci.yml)
[![pages-build-deployment](https://github.com/graphframes/graphframes/actions/workflows/pages/pages-build-deployment/badge.svg)](https://github.com/graphframes/graphframes/actions/workflows/pages/pages-build-deployment)
[![scala-central-publish](https://github.com/graphframes/graphframes/actions/workflows/scala-publish.yml/badge.svg)](https://github.com/graphframes/graphframes/actions/workflows/scala-publish.yml)

# GraphFrames: DataFrame-based Graphs

This is a package for graphs processing and analytics on scale. It is built on top of Apache Spark and relies on DataFrame abstraction. Users can write highly expressive queries by leveraging the DataFrame API, combined with a new API for network motif finding. The user also benefits from DataFrame performance optimizations within the Spark SQL engine. GraphFrames works in Java, Scala, and Python.

You can find user guide and API docs at https://graphframes.io

## GraphFrames is Back!

This projects was in maintenance mode for some time, but we are happy to announce that it is now back in active development! We are working on a new release with many bug fixes and improvements. We are also working on a new website and documentation.

## Installation and Quick-Start

The easiest way to start using GraphFrames is through the [Spark Packages system](https://spark-packages.org/package/graphframes/graphframes). Just run the following command:

```bash
# Interactive Scala/Java
$ spark-shell --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12

# Interactive Python
$ pyspark --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12

# Submit a script in Scala/Java/Python
$ spark-submit --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12 script.py
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
# First connect records with a similarity metric, then run connectedComponents.
# This gives you groups of identical records, which you then link by same_as edges or merge into list-based master records.
sc.setCheckpointDir("/tmp/graphframes-example-connected-components")  # required by GraphFrames.connectedComponents
g.connectedComponents().show()

# +---+-----+---+---------+
# | id| name|age|component|
# +---+-----+---+---------+
# |  1| John| 30|        1|
# |  2|Alice| 25|        1|
# |  3|  Bob| 35|        1|
# +---+-----+---+---------+

# Find frenemies with network motif finding! See how graph and relational queries are combined?
(
    g.find("(a)-[e]->(b); (b)-[e2]->(a)")
    .filter("e.relationship = 'friend' and e2.relationship = 'enemy'")
    .show()
)

# These are paths, which you can aggregate and count to find complex patterns.
# +------------+--------------+----------------+-------------+
# |           a|             e|               b|           e2|
# +------------+--------------+----------------+-------------+
# |{2, Bob, 25}|{2, 3, friend}|{3, Charlie, 35}|{3, 2, enemy}|
# +------------+--------------+----------------+-------------+
```

## Learn GraphFrames

To learn more about GraphFrames, check out these resources:
* [GraphFrames Documentation](https://graphframes.github.io/graphframes)
* [GraphFrames Network Motif Finding Tutorial](https://graphframes.github.io/graphframes/docs/_site/motif-tutorial.html)
* [Introducing GraphFrames](https://databricks.com/blog/2016/03/03/introducing-graphframes.html)
* [On-Time Flight Performance with GraphFrames for Apache Spark](https://databricks.com/blog/2016/03/16/on-time-flight-performance-with-graphframes-for-apache-spark.html)

## Community Resources

* [GraphFrames Google Group](https://groups.google.com/forum/#!forum/graphframes)
* [#graphframes Discord Channel on GraphGeeks](https://discord.com/channels/1162999022819225631/1326257052368113674)

## `graphframes-py` is our Official PyPi Package

We recommend using the Spark Packages system to install the latest version of GraphFrames, but now publish a build of our Python package to PyPi in the [graphframes-py](https://pypi.org/project/graphframes-py/) package. It can be used to provide type hints in IDEs, but does not load the java-side of GraphFrames so will not work without loading the GraphFrames package. See [Installation and Quick-Start](#installation-and-quick-start).

```bash
pip install graphframes-py
```

This project does not own or control the [graphframes PyPI package](https://pypi.org/project/graphframes/) (installs 0.6.0) or [graphframes-latest PyPI package](https://pypi.org/project/graphframes-latest/) (installs 0.8.4). 

## GraphFrames and sbt

If you use the sbt-spark-package plugin, in your sbt build file, add the following, pulled from [GraphFrames on Spark Packages](https://spark-packages.org/package/graphframes/graphframes):

```
spDependencies += "graphframes/graphframes:0.8.4-spark3.5-s_2.12"
```

Otherwise,

```
resolvers += "Spark Packages Repo" at "https://repos.spark-packages.org/"

libraryDependencies += "graphframes" % "graphframes" % "0.8.4-spark3.5-s_2.12"
```

## GraphFrames and Maven

Please see the section about nightly builds!

**WARNING!**

**=========================**

Due to governance problems and limitations, all the new releases of `GraphFrames` will be published to the Maven Central under the namespace `io.graphframes` (not `org.graphframes`)!

**=========================**

## GraphFrames Internals

To learn how GraphFrames works internally to combine graph and relational queries, check out the paper [GraphFrames: An Integrated API for Mixing Graph and
Relational Queries, Dave et al. 2016](https://people.eecs.berkeley.edu/~matei/papers/2016/grades_graphframes.pdf).

## Building and running unit tests

To compile the core project, run `build/sbt package` from the project home directory.
To compile the Spark Connect Plugin, run `build/sbt connect/package`

## Spark version compatibility

This project is compatible with Spark 3.4+. Significant speed improvements have been made to DataFrames in recent versions of Spark, so you may see speedups from using the latest Spark version.

Nightly builds of GraphFrames:

| Component           | Spark 3.x (Scala 2.12) | Spark 3.x (Scala 2.13) | Spark 4.x (Scala 2.13) |
|---------------------|------------------------|------------------------|------------------------|
| graphframes         | ✓                      | ✓                      | ✓                      |
| graphframes-connect | ✓                      | ✓                      | ✓                      |

## Contributing

GraphFrames was made as collaborative effort among UC Berkeley, MIT, Databricks and the open source community. At the moment GraphFrames is maintained by the group of individual contributors.

See [contribution guide](./CONTRIBUTING.md)

## Releases:

See [release notes](https://github.com/graphframes/graphframes/releases).

## Nightly builds

GraphFrames project is publishing SNAPSHOTS (nightly builds) to the "Central Portal Snapshots."
Please read [this section](https://central.sonatype.org/publish/publish-portal-snapshots/#consuming-snapshot-releases-for-your-project) of the Sonatype documentation to check how can you use snapshots in your project.

GroupId: `io.graphframes`
ArtifactIds:
- `graphframes-spark3_2.12`
- `graphframes-spark3_2.13`
- `graphframes-connect-spark3_2.12`
- `graphframes-connect-spark3_2.13`
- `graphframes-spark4_2.13`
- `graphframes-connect-spark4_2.13`
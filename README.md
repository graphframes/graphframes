<p align="center">
    <img src="docs/src/img/GraphFrames-Logo-Large.png" alt="GraphFrames Logo" width="500"/>
</p>

<p align="center">
    <a href="https://github.com/graphframes/graphframes/actions/workflows/scala-ci.yml"><img src="https://github.com/graphframes/graphframes/actions/workflows/scala-ci.yml/badge.svg" alt="Scala CI"></a> <a href="https://github.com/graphframes/graphframes/actions/workflows/python-ci.yml"><img src="https://github.com/graphframes/graphframes/actions/workflows/python-ci.yml/badge.svg" alt="Python CI"></a> <a href="https://github.com/graphframes/graphframes/actions/workflows/pages/pages-build-deployment"><img src="https://github.com/graphframes/graphframes/actions/workflows/pages/pages-build-deployment/badge.svg" alt="pages-build-deployment"></a> <a href="https://github.com/graphframes/graphframes/actions/workflows/scala-publish.yml"><img src="https://github.com/graphframes/graphframes/actions/workflows/scala-publish.yml/badge.svg" alt="scala-central-publish"></a> <a href="https://github.com/graphframes/graphframes/actions/workflows/python-publish.yml"><img src="https://github.com/graphframes/graphframes/actions/workflows/python-publish.yml/badge.svg" alt="python-pypi-publish"></a> <img src="https://img.shields.io/github/v/release/graphframes/graphframes" alt="GitHub Release"> <img src="https://img.shields.io/github/license/graphframes/graphframes" alt="GitHub License"> <img src="https://img.shields.io/pypi/dm/graphframes-py" alt="PyPI - Downloads">
</p>

# GraphFrames: graph algorithms at scale

This is a package for graphs processing and analytics on scale. It is built on top of Apache Spark and relies on DataFrame abstraction. It provides built-in and easy to use distributed graph algorithms as well as a flexible APIs like `Pregel` or `AggregateMessages` to make custom graph processing. Users can write highly expressive queries by leveraging the DataFrame API, combined with a new API for network motif finding. The user also benefits from DataFrame performance optimizations within the Spark SQL engine. GraphFrames works in Java, Scala, and Python.

## GraphFrames usecases

There are some popular use cases when GraphFrames is almost irreplaceable, including, but not limited to:

- Compliance analytics with a scalable shortest paths algorithm and motif analysis;
- Anti-fraud with scalable cycles detection in large networks;
- Identity resolution on the scale of billions with highly efficient connected components;
- Search result ranking with a distributed, Pregel-based PageRank;
- Clustering huge graphs with Label Propagation and Power Iteration Clustering;
- Building a knowledge graph systems with Property Graph Model.

## Documentation

- [Installation](https://graphframes.io/02-quick-start/01-installation.html)
- [Creating Graphs](https://graphframes.io/04-user-guide/01-creating-graphframes.html)
- [Basic Graph Manipulations](https://graphframes.io/04-user-guide/02-basic-operations.html)
- [Centrality Metrics](https://graphframes.io/04-user-guide/03-centralities.html)
- [Motif finding](https://graphframes.io/04-user-guide/04-motif-finding.html)
- [Traversals and Connectivity](https://graphframes.io/04-user-guide/05-traversals.html)
- [Community Detection](https://graphframes.io/04-user-guide/06-graph-clustering.html)
- [Scala API](https://graphframes.io/api/scaladoc/)
- [Python API](https://graphframes.io/api/python/)
- [Apache Spark compatibility](https://graphframes.io/02-quick-start/01-installation.html#spark-versions-compatibility)

## Quick Start

Now you can create a GraphFrame as follows.

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

### GraphFrames tutorials

- [GraphFrames Network Motif Finding Tutorial](https://graphframes.github.io/graphframes/docs/_site/motif-tutorial.html)

### Community Resources

This resources are provided by the community:

- [Introducing GraphFrames](https://databricks.com/blog/2016/03/03/introducing-graphframes.html)
- [GraphFrames Google Group](https://groups.google.com/forum/#!forum/graphframes)
- [#graphframes Discord Channel on GraphGeeks](https://discord.com/channels/1162999022819225631/1326257052368113674)
- [Graph Operations in Apache Spark Using GraphFrames](https://www.pluralsight.com/courses/apache-spark-graphframes-graph-operations)
- [Executing Graph Algorithms with GraphFrames on Databricks](https://www.pluralsight.com/courses/executing-graph-algorithms-graphframes-databricks)
- [On-Time Flight Performance with GraphFrames for Apache Spark](https://databricks.com/blog/2016/03/16/on-time-flight-performance-with-graphframes-for-apache-spark.html)

## GraphFrames Internals

- [A top level overview of GraphFrames internals](https://graphframes.io/01-about/02-architecture.html)
- [GraphFrames: An Integrated API for Mixing Graph and Relational Queries, Dave et al. 2016](https://people.eecs.berkeley.edu/~matei/papers/2016/grades_graphframes.pdf).

## Contributing

GraphFrames was made as collaborative effort among UC Berkeley, MIT, Databricks and the open source community. At the moment GraphFrames is maintained by the group of individual contributors.

See [contribution guide](./CONTRIBUTING.md) and the [local development setup walkthrough](https://graphframes.io/06-contributing/01-contributing-guide.html) for step-by-step instructions on preparing your environment, running tests, and submitting changes.

## Releases

See [release notes](https://github.com/graphframes/graphframes/releases).

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=graphframes/graphframes&type=Date)](https://www.star-history.com/#graphframes/graphframes&Date)

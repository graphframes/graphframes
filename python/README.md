# GraphFrames Python Package

<p align="center">
    <img src="https://raw.githubusercontent.com/graphframes/graphframes/refs/heads/main/docs/src/img/GraphFrames-Logo-Large.png" alt="GraphFrames Logo" width="500"/>
</p>

<p align="center">
<a href="https://pypi.org/project/graphframes-py/"><img src="https://img.shields.io/pypi/dm/graphframes-py" alt="PyPI - Downloads"></a> <a href="https://pypi.org/project/graphframes-py/"><img src="https://img.shields.io/pypi/l/graphframes-py" alt="PyPI - License"></a> <a href="https://pypi.org/project/graphframes-py/"><img src="https://img.shields.io/pypi/v/graphframes-py" alt="PyPI - Version"></a>
</p>

The is the officila [graphframes-py PyPI package](https://pypi.org/project/graphframes-py/), which is a Python wrapper for the Scala GraphFrames library.
This package is maintained by the GraphFrames project and is available on PyPI.

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

**NOTE!** *Python distribution does not include JVM-core. You need to add it to your cluster or Spark-Connect server!*

**NOTE!** *GraphFrames depends on its own version of GraphX. While in case of installation from the Maven Central repository, all the runtime dependencies will be resolved automatically, you may need to add them manually in case of installation from the local repository.*

## Spark-Connect Note

GraphFrames PySpark is choosing connect or classic implementation implicitly based on the result of `is_remote()`.
To enforce usage of connect-based implementation, you may export this variable `SPARK_CONNECT_MODE_ENABLED=1`

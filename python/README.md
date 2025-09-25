# GraphFrames Python Package

![PyPI - Downloads](https://img.shields.io/pypi/dm/graphframes-py) ![PyPI - License](https://img.shields.io/pypi/l/graphframes-py) ![PyPI - Version](https://img.shields.io/pypi/v/graphframes-py)

<img src=https://raw.githubusercontent.com/graphframes/graphframes/refs/heads/master/docs/src/img/GraphFrames-Logo-Large.png width=500>

https://graphframes.io/

The is the officila [graphframes-py PyPI package](https://pypi.org/project/graphframes-py/), which is a Python wrapper for the Scala GraphFrames library.
This package is maintained by the GraphFrames project and is available on PyPI.

For instructions on GraphFrames, check the project [README.md](https://github.com/graphframes/graphframes?tab=readme-ov-file#graphframes-dataframe-based-graphs).

See [Installation and Quick-Start](https://github.com/graphframes/graphframes?tab=readme-ov-file#installation-and-quick-start) for the best way to install and use GraphFrames.

## Installation

```bash
pip install graphframes-py
```

**NOTE!** *Python distribution does not include JVM-core. You need to add it to your cluster or Spark-Connect server!*

**NOTE!** *GraphFrames depends on its own version of GraphX. While in case of installation from the Maven Central repository, all the runtime dependencies will be resolved automatically, you may need to add them manually in case of installation from the local repository.*

## Running `graphframes-py`

You should use GraphFrames via the `--packages` argument to `pyspark` or `spark-submit`, but this package is helpful in development environments.

```bash
# Interactive Python, Spark 3.5.x
$ pyspark --packages io.graphframes:graphframes-spark3_2.12:0.9.2

# Interactive Python, Spark 4.0.x
$ pyspark --packages io.graphframes:graphframes-spark4_2.13:0.9.2
```

## Documentation

- [API Reference](https://graphframes.io/api/python/index.html)

## Spark-Connect Note

GraphFrames PySpark is choosing connect or classic implementation implicitly based on the result of `is_remote()`.
To enforce usage of connect-based implementation, you may export this variable `SPARK_CONNECT_MODE_ENABLED=1`

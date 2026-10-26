# Stack Exchange Data Setup

This tutorial walks through setting up the Stack Exchange dataset used by the [Motif Finding Tutorial](02-motif-tutorial.md) and [Pregel Tutorial](04-pregel-tutorial.md). You only need to complete this setup once — both tutorials use the same dataset.

## What We're Building

We will download the [Stack Exchange Data Dump](https://archive.org/details/stackexchange) for `stats.meta.stackexchange.com` and convert the raw XML files into Apache Parquet files that PySpark and GraphFrames can load efficiently. The result is a property graph with ~130K nodes (Users, Questions, Answers, Votes, Badges, Tags, PostLinks) and ~97K edges (Asks, Answers, CastFor, Earns, Tags, Links, Duplicates, Posts).

## Prerequisites

- **Python 3.11+** (3.14 recommended)
- **Java 21** (OpenJDK)
- **Apache Spark 4.x** (installed via PySpark) — all tutorials in this series were written in Spark 4.1.3

## Installing GraphFrames

The official GraphFrames Python package is [graphframes-py](https://pypi.org/project/graphframes-py/) on PyPI. Install it with the tutorials extra:

```bash
pip install "graphframes-py[docs,tutorials]>=0.12.1"
```

This installs `graphframes-py` along with dependencies needed for the tutorials: `py7zr`, `requests`, and `click`.

**Important**: Do **not** install the `graphframes` package on PyPI — that is an old, unmaintained third-party package. The official package is `graphframes-py`.

### JVM Core Dependency

The Python package is a thin wrapper around the Scala/JVM implementation. If you install `graphframes-py` from PyPI, the JVM core is loaded automatically at runtime. Alternatively, you can use the `--packages` flag with `pyspark` or `spark-submit` to fetch the JVM core from Maven Central:

```bash
# Spark 4.x with Scala 2.13
pyspark --packages io.graphframes:graphframes-spark4_2.13:0.12.1

# Spark 3.5.x with Scala 2.13
pyspark --packages io.graphframes:graphframes-spark3_2.13:0.12.1
```

## Download the Stack Exchange Archive

The `graphframes-py` package includes a CLI utility for downloading Stack Exchange data dumps from the Internet Archive.

```bash
Usage: graphframes [OPTIONS] COMMAND [ARGS]...

  GraphFrames CLI: a collection of commands for graphframes.

Options:
  --help  Show this message and exit.

Commands:
  stackexchange  Download Stack Exchange archive for a given SUBDOMAIN.
```

Download the `stats.meta` archive (the default `--data-dir` is the package data directory):

```bash
graphframes stackexchange stats.meta
```

To download to a custom directory, use the `--data-dir` option:

```bash
graphframes stackexchange --data-dir /path/to/data stats.meta
```

You should see output like:

```
Downloading archive from https://archive.org/download/stackexchange/stats.meta.stackexchange.com.7z
Downloading  [####################################]  100%
Download complete: python/graphframes/tutorials/data/stats.meta.stackexchange.com.7z
Extracting archive...
Extraction complete: stats.meta.stackexchange.com
```

This downloads and extracts the 7zip archive into `python/graphframes/tutorials/data/stats.meta.stackexchange.com/`, containing XML files for Posts, Users, Votes, Badges, Tags, PostLinks, PostHistory, and Comments.

## Convert XML to Parquet

The @:srcLink(python/graphframes/tutorials/stackexchange.py) script reads the raw XML files, builds a unified property graph schema, and writes the result as Parquet files. This tutorial uses **Spark 4.0**, which includes built-in XML support — no additional packages are needed. The XML processing requires extra driver memory:

```bash
spark-submit \
  --driver-memory 4g \
  --executor-memory 4g \
  python/graphframes/tutorials/stackexchange.py
```

**Spark 3.5.x users**: Spark 3.5 does not include built-in XML support. Add the [spark-xml](https://github.com/databricks/spark-xml) package: `--packages com.databricks:spark-xml_2.13:0.18.0`. This package was merged into Spark 4.0 and is no longer needed for Spark 4.0+.

The script:

1. Loads each XML file (Posts, Users, Votes, Badges, Tags, PostLinks, Comments)
2. Splits Posts into Questions and Answers
3. Adds a `Type` column to each entity
4. Merges all entity types into a unified `nodes_df` DataFrame with a consistent schema
5. Generates a UUID `id` column for each node (GraphFrames requires a lowercase `id`)
6. Builds edge DataFrames for each relationship type (CastFor, Asks, Posts, Answers, Tags, Earns, Links, Duplicates)
7. Writes `Nodes.parquet` and `Edges.parquet` to disk

After processing, the data directory contains:

```
python/graphframes/tutorials/data/stats.meta.stackexchange.com/
├── Nodes.parquet
├── Edges.parquet
└── (original XML files)
```

## Load the Graph

The Motif Finding and Pregel tutorials both start from the same loaded graph. Use this block once you have `Nodes.parquet` and `Edges.parquet`. If you gave `graphframes stackexchange` a different subdomain, change `STACKEXCHANGE_SITE` to match.

```python
from pathlib import Path

import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

import graphframes
from graphframes import GraphFrame

# In the pyspark shell a SparkSession named `spark` already exists, so set
# configuration on the live session rather than through the builder
spark: SparkSession = SparkSession.builder.appName("Stack Exchange Graph").getOrCreate()
# Lets the Id:(Stack Overflow int) and id:(GraphFrames ULID) coexist
spark.conf.set("spark.sql.caseSensitive", True)
sc: SparkContext = spark.sparkContext
sc.setCheckpointDir("/tmp/graphframes-checkpoints")

# Change me if you download a different stackexchange site
STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
# Package data directory — the default download location for `graphframes stackexchange`.
# If you downloaded with --data-dir, point DATA_DIR at that path instead.
DATA_DIR = str(Path(graphframes.__file__).parent / "tutorials" / "data")
BASE_PATH = f"{DATA_DIR}/{STACKEXCHANGE_SITE}"
```

Load the nodes and edges, then repartition, checkpoint, and cache them. GraphFrames motif and Pregel workloads benefit from cached vertices and edges, and repartitioning gives later searches parallelism.

```python
#
# Load the nodes and edges from disk, repartition, checkpoint, and cache.
#

NODES_PATH: str = f"{BASE_PATH}/Nodes.parquet"
nodes_df: DataFrame = spark.read.parquet(NODES_PATH)
nodes_df = nodes_df.repartition(50).checkpoint().cache()

EDGES_PATH: str = f"{BASE_PATH}/Edges.parquet"
edges_df: DataFrame = spark.read.parquet(EDGES_PATH)
edges_df = edges_df.repartition(50).checkpoint().cache()

print(f"Nodes: {nodes_df.count():,}")
print(f"Edges: {edges_df.count():,}")
```

Check the node types you have to work with:

```python
node_counts = (
    nodes_df
    .select("id", F.col("Type").alias("Node Type"))
    .groupBy("Node Type")
    .count()
    .orderBy(F.col("count").desc())
    .withColumn("count", F.format_number(F.col("count"), 0))
)
node_counts.show()
```

```
+---------+------+
|Node Type| count|
+---------+------+
|    Badge|43,029|
|     Vote|42,593|
|     User|37,709|
|   Answer| 2,978|
| Question| 2,025|
|PostLinks| 1,274|
|      Tag|   143|
+---------+------+
```

Check the edge types:

```python
edge_counts = (
    edges_df
    .select("src", "dst", F.col("relationship").alias("Edge Type"))
    .groupBy("Edge Type")
    .count()
    .orderBy(F.col("count").desc())
    .withColumn("count", F.format_number(F.col("count"), 0))
)
edge_counts.show()
```

```
+----------+------+
| Edge Type| count|
+----------+------+
|     Earns|43,029|
|   CastFor|40,701|
|      Tags| 4,427|
|   Answers| 2,978|
|     Posts| 2,767|
|      Asks| 1,934|
|     Links| 1,180|
|Duplicates|    88|
+----------+------+
```

Create a @:pydoc(graphframes.GraphFrame) from the loaded DataFrames. Both follow-on tutorials continue from this `g` object (or recreate it the same way).

```python
g = GraphFrame(nodes_df, edges_df)
```

You should see about **129,751** nodes and **97,104** edges.

## Next Steps

With the data loaded into `nodes_df`, `edges_df`, and `g`, you are ready to proceed to:

- **[Motif Finding Tutorial](02-motif-tutorial.md)**: Pattern matching and network motif discovery
- **[Pregel Tutorial](04-pregel-tutorial.md)**: Iterative graph algorithms with the Pregel API

Return to the tutorial you came from and continue from where you left off — skip any duplicate load steps and start from motif finding or Pregel examples.

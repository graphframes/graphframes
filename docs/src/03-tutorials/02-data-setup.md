# Stack Exchange Data Setup

This tutorial walks through setting up the Stack Exchange dataset used by the [Motif Finding Tutorial](03-motif-tutorial.md) and [Pregel Tutorial](04-pregel-tutorial.md). You only need to complete this setup once — both tutorials use the same dataset.

## What We're Building

We will download the [Stack Exchange Data Dump](https://archive.org/details/stackexchange) for `stats.meta.stackexchange.com` and convert the raw XML files into Apache Parquet files that PySpark and GraphFrames can load efficiently. The result is a property graph with ~130K nodes (Users, Questions, Answers, Votes, Badges, Tags, PostLinks) and ~97K edges (Asks, Answers, CastFor, Earns, Tags, Links, Duplicates, Posts).

## Prerequisites

- **Python 3.10+** (3.12 recommended)
- **Java 17** (OpenJDK)
- **Apache Spark 4.0** (installed via PySpark) — all tutorials in this series use Spark 4.0

## Installing GraphFrames

The official GraphFrames Python package is [graphframes-py](https://pypi.org/project/graphframes-py/) on PyPI. Install it with the tutorials extra:

```bash
pip install "graphframes-py[tutorials]>=0.10.1"
```

This installs `graphframes-py` along with dependencies needed for the tutorials: `py7zr`, `requests`, and `click`.

**Important**: Do **not** install the `graphframes` package on PyPI — that is an old, unmaintained third-party package. The official package is `graphframes-py`.

### JVM Core Dependency

The Python package is a thin wrapper around the Scala/JVM implementation. If you install `graphframes-py` from PyPI, the JVM core is loaded automatically at runtime. Alternatively, you can use the `--packages` flag with `pyspark` or `spark-submit` to fetch the JVM core from Maven Central:

```bash
# Spark 4.x with Scala 2.13
pyspark --packages io.graphframes:graphframes-spark4_2.13:0.10.1

# Spark 3.5.x with Scala 2.13
pyspark --packages io.graphframes:graphframes-spark3_2.13:0.10.1
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

**Spark 3.5.x users**: Spark 3.5 does not include built-in XML support. Add the [spark-xml](https://github.com/databricks/spark-xml) package: `--packages com.databricks:spark-xml_2.12:0.18.0`. This package was merged into Spark 4.0 and is no longer needed for Spark 4.0+.

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

## Verify the Data

A quick sanity check to make sure everything loaded correctly:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Data Verification").getOrCreate()

STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
BASE_PATH = f"python/graphframes/tutorials/data/{STACKEXCHANGE_SITE}"

nodes_df = spark.read.parquet(f"{BASE_PATH}/Nodes.parquet")
edges_df = spark.read.parquet(f"{BASE_PATH}/Edges.parquet")

print(f"Nodes: {nodes_df.count():,}")
print(f"Edges: {edges_df.count():,}")

# Check node types
nodes_df.groupBy("Type").count().orderBy("count", ascending=False).show()

# Check edge types
edges_df.groupBy("relationship").count().orderBy("count", ascending=False).show()
```

Expected output:

```
Nodes: 129,751
Edges: 97,104

+---------+-----+
|     Type|count|
+---------+-----+
|    Badge|43029|
|     Vote|42593|
|     User|37709|
|   Answer| 2978|
| Question| 2025|
|PostLinks| 1274|
|      Tag|  143|
+---------+-----+

+------------+-----+
|relationship|count|
+------------+-----+
|       Earns|43029|
|     CastFor|40701|
|        Tags| 4427|
|     Answers| 2978|
|       Posts| 2767|
|        Asks| 1934|
|       Links| 1180|
|  Duplicates|   88|
+------------+-----+
```

## Next Steps

With the data prepared, you are ready to proceed to:

- **[Motif Finding Tutorial](03-motif-tutorial.md)**: Pattern matching and network motif discovery
- **[Pregel Tutorial](04-pregel-tutorial.md)**: Iterative graph algorithms with the Pregel API

Return to the tutorial you came from and continue from where you left off.

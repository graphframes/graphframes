# Neo4j Integration Tutorial

This tutorial demonstrates how to integrate GraphFrames with Neo4j, enabling you to offload an expensive connected components calculation onto Spark where it can scale linearly taking advantage of a distributed system. This provides graph database persistence with distributed graph analytics to scale expensive algorithms.

In this tutorial we will:

1. Set up Neo4j using Docker
2. Load Stack Exchange data into Neo4j
3. Read graph data from Neo4j into GraphFrames
4. Run distributed graph algorithms (Connected Components)
5. Write enriched results back to Neo4j

This is a complete pipeline for bidirectional data flow between Neo4j and GraphFrames you can use as the basis for many different workflows.

## Why Integrate Neo4j with GraphFrames?

**Neo4j** excels at:

- ACID transactions
- Real-time graph queries
- Complex path traversals
- Interactive graph exploration

**GraphFrames** excels at:

- Distributed graph analytics at scale
- Batch processing of billions of edges
- Integration with Spark ML pipelines
- Complex pattern matching with motif finding

One is a natural complement of the other.

## Prerequisites

Before starting, ensure you have:

- **Docker**: For running Neo4j (install from [docker.com](https://www.docker.com/))
- **GraphFrames 0.12+**: `pip install graphframes-py`
- **Apache Spark 4.x+**: Compatible with your Python version
- **Stack Exchange Dataset**: Follow the [Tutorial Data Setup](03-data-setup.md) to download

Check your versions:

```bash
docker --version
```

## Architecture Overview

Our data pipeline:

```
Stack Exchange Data (Parquet)
         ↓
    [Load into Neo4j]
         ↓
    Neo4j Database ← Docker Container
         ↓
[Read via Neo4j Connector]
         ↓
    PySpark DataFrames
         ↓
    GraphFrames Graph
         ↓
  [Connected Components]
         ↓
    Enriched DataFrames
         ↓
[Write via Neo4j Connector]
         ↓
    Neo4j Database (updated with component IDs)
```

## Step 1: Set Up Neo4j with Docker

First, let's start a Neo4j instance using Docker. We'll use Neo4j Community Edition with APOC plugins for data import capabilities.

Create a directory for Neo4j data:

```bash
mkdir -p /tmp/neo4j-data/data /tmp/neo4j-data/logs /tmp/neo4j-data/import /tmp/neo4j-data/plugins
```

Start Neo4j container:

```bash
docker run -d --name neo4j-graphframes -p 7474:7474 -p 7687:7687 -v /tmp/neo4j-data/data:/data -v /tmp/neo4j-data/logs:/logs -v /tmp/neo4j-data/import:/var/lib/neo4j/import -v /tmp/neo4j-data/plugins:/plugins -e NEO4J_apoc_import_file_enabled=true -e NEO4J_apoc_export_file_enabled=true -e NEO4J_AUTH=neo4j/graphframes123 -e NEO4J_PLUGINS='["apoc"]' -e NEO4J_dbms_memory_heap_initial__size=1G -e NEO4J_dbms_memory_heap_max__size=2G neo4j:community
```

**Connection Details:**

- **Browser UI**: <http://localhost:7474>
- **Bolt Protocol**: neo4j://localhost:7687

Verify Neo4j is running:

```bash
docker logs neo4j-graphframes
```

Wait for the message: `Started.`

You can also visit <http://localhost:7474> and log in with the credentials above.

## Step 2: Prepare the Stack Exchange Data

If you haven't already, download and process the Stack Exchange data:

```bash
graphframes stackexchange stats.meta
```

Then convert the XML dumps to Parquet. Spark 4 has [native XML support](https://spark.apache.org/docs/4.0.0/sql-data-sources-xml.html):

```bash
spark-submit --driver-memory 4g --executor-memory 4g python/graphframes/tutorials/stackexchange.py
```

On Spark 3, add [spark-xml](https://github.com/databricks/spark-xml):

```bash
spark-submit --packages com.databricks:spark-xml_2.13:0.18.0 --driver-memory 4g --executor-memory 4g python/graphframes/tutorials/stackexchange.py
```

This creates two Parquet datasets in the standard GraphFrames shape:

```
python/graphframes/tutorials/data/stats.meta.stackexchange.com/
├── Nodes.parquet          # All node types, one unified schema, with a UUID 'id'
└── Edges.parquet          # All edges: src, dst, relationship
```

Both use a **unified schema**. `Nodes.parquet` carries every column of every node type — a `Badge` row has a `Title` column, it is just null — with a `Type` column naming the type and a UUID `id`. `Edges.parquet` has exactly three columns: `src`, `dst` and `relationship`, where `src`/`dst` are the UUID `id`s from `Nodes.parquet`.

For `stats.meta.stackexchange.com` that is 129,751 nodes:

| Type | Count |
|---|---|
| Badge | 43,029 |
| Vote | 42,593 |
| User | 37,709 |
| Answer | 2,978 |
| Question | 2,025 |
| PostLinks | 1,274 |
| Tag | 143 |

and 97,104 edges:

| relationship | Count | Shape |
|---|---|---|
| Earns | 43,029 | User → Badge |
| CastFor | 40,701 | Vote → Question *or* Answer |
| Tags | 4,427 | Tag → Question *or* Answer |
| Answers | 2,978 | Answer → Question |
| Posts | 2,767 | User → Answer |
| Asks | 1,934 | User → Question |
| Links | 1,180 | Post → Post |
| Duplicates | 88 | Post → Post |

Note the third column. Half of these relationship types have **heterogeneous endpoints** — a `Vote` is `CastFor` a `Question` *or* an `Answer`. That single fact drives the graph model in the next step.

## Step 3: Model the Graph

Before writing any data, decide what the Neo4j model looks like. The Neo4j Spark Connector attaches a relationship by *matching* its two endpoints against **one fixed label set per write**. So the shape of your labels decides whether the relationship load works at all.

The obvious model — label each node with its Stack Exchange type and nothing else — does not survive contact with heterogeneous endpoints. To write `CastFor` you would have to name the target label, and there is no single answer: some targets are `:Question`, some are `:Answer`. You cannot name both, because multiple labels in a match are **ANDed** — `MATCH (n:Question:Answer)` asks for nodes that are simultaneously a question and an answer, which is nothing. You would have to split every relationship type into one write per endpoint-type combination and derive those combinations from the data first.

Instead, give every node **two labels**: a shared `:Node` label plus its type.

```
(:Node:User)  (:Node:Badge)  (:Node:Question)  (:Node:Answer)  ...
```

Then put a uniqueness constraint on `:Node(id)`. This gives you:

- **One indexed lookup key for the entire graph.** Endpoint matching and node MERGEs are both index-backed. Without the constraint they degrade into full label scans, and a load that takes minutes takes all night.
- **One write per relationship type**, matching both endpoints on `:Node`, regardless of what types that relationship actually connects.
- **Natural Cypher.** `MATCH (u:User)-[:Earns]->(b:Badge)` still reads exactly as you would expect, because the type labels are still there.

## Step 4: Load Stack Exchange Data into Neo4j

The full, runnable version of everything below is `python/graphframes/tutorials/neo4j.py`. Run it with:

```bash
spark-submit --packages io.graphframes:graphframes-spark4_2.13:0.12.1,org.neo4j:neo4j-connector-apache-spark_2.13:6.0.0_for_spark_4 python/graphframes/tutorials/neo4j.py
```

To follow along interactively instead:

```bash
pyspark --packages io.graphframes:graphframes-spark4_2.13:0.12.1,org.neo4j:neo4j-connector-apache-spark_2.13:6.0.0_for_spark_4
```

On Spark 3.5, use `graphframes-spark3_2.13:0.12.1` and `6.0.0_for_spark_3`.

### Connect

Pass the Neo4j connection settings as **DataFrame options**, not as `spark.conf` entries. As options they drop the `neo4j.` prefix, and `spark-submit` will not print a `Ignoring non-Spark config property: neo4j.url` warning for every one of them.

```python
from pathlib import Path

import graphframes
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from graphframes import GraphFrame

spark = (
    SparkSession.builder.appName("Neo4j + GraphFrames: Stack Exchange")
    .config(
        "spark.jars.packages",
        "io.graphframes:graphframes-spark4_2.13:0.12.1,"
        "org.neo4j:neo4j-connector-apache-spark_2.13:6.0.0_for_spark_4",
    )
    .getOrCreate()
)
spark.sparkContext.setCheckpointDir("/tmp/graphframes-checkpoints/neo4j")

NEO4J_FORMAT = "org.neo4j.spark.DataSource"

# Passed to every Neo4j read and write below.
NEO4J = {
    "url": "neo4j://localhost:7687",
    "authentication.basic.username": "neo4j",
    "authentication.basic.password": "graphframes123",
    "database": "neo4j",
}

# Every node carries this label in addition to its Stack Exchange type. See Step 3.
SHARED_LABEL = "Node"

# Change me if you downloaded a different stackexchange site
STACKEXCHANGE_SITE = "stats.meta.stackexchange.com"
# Package data directory — the default download location for `graphframes stackexchange`.
# If you downloaded with --data-dir, point DATA_DIR at that path instead.
DATA_DIR = str(Path(graphframes.__file__).parent / "tutorials" / "data")
BASE_PATH = f"{DATA_DIR}/{STACKEXCHANGE_SITE}"

nodes_df: DataFrame = spark.read.parquet(f"{BASE_PATH}/Nodes.parquet").cache()
edges_df: DataFrame = spark.read.parquet(f"{BASE_PATH}/Edges.parquet").cache()

print(f"Total nodes: {nodes_df.count():,}")
print(f"Total edges: {edges_df.count():,}")

nodes_df.groupBy("Type").count().orderBy(F.desc("count")).show()
edges_df.groupBy("relationship").count().orderBy(F.desc("count")).show()
```

### Create the constraint

Everything downstream depends on the `:Node(id)` uniqueness constraint, so create it first.

The connector can only create constraints as a side effect of a write, via `schema.optimization.node.keys`. We ask for it on a write of an **empty** DataFrame. That creates the constraint and no nodes — and it sidesteps a limitation in connector 6.0.0, whose schema-optimization code has no mapping for `ArrayType`. Passing `schema.optimization.node.keys` on a write that includes an array column — `Question.Tags`, here — fails with `key not found: ArrayType(StringType,true)`. Creating the constraint up front lets every later write omit the option entirely.

```python
empty = spark.createDataFrame([], StructType([StructField("id", StringType(), False)]))
(
    empty.write.format(NEO4J_FORMAT)
    .options(**NEO4J)
    .mode("Overwrite")
    .option("labels", f":{SHARED_LABEL}")
    .option("node.keys", "id")
    .option("schema.optimization.node.keys", "UNIQUE")
    .save()
)
print(f"✓ uniqueness constraint on :{SHARED_LABEL}(id)")
```

### Load the nodes

Two details matter here.

**Use `Overwrite`, not `Append`.** The connector maps Spark's save modes onto Cypher: `Overwrite` becomes a `MERGE` on `node.keys`, and `Append` becomes a bare `CREATE`. `Append` **ignores `node.keys` entirely**, so it cannot deduplicate — running an `Append` load twice gives you two copies of every node. `Overwrite` is idempotent: re-run it as often as you like.

**Project away the empty columns.** In the unified schema every type carries every other type's columns as nulls. Keeping only the columns a type actually populates makes the Neo4j model readable and cuts what goes over the wire — a `Badge` needs 8 properties, not 53.

```python
def populated_columns(df: DataFrame) -> list[str]:
    """Return the columns of df that hold at least one non-null value."""
    non_null_counts = df.select([F.count(F.col(c)).alias(c) for c in df.columns]).first()
    return [c for c in df.columns if non_null_counts[c] > 0]


node_types = sorted(row["Type"] for row in nodes_df.select("Type").distinct().collect())

for node_type in node_types:
    type_nodes = nodes_df.filter(F.col("Type") == node_type)
    type_nodes = type_nodes.select(*populated_columns(type_nodes)).cache()

    count = type_nodes.count()
    print(f"Loading {count:,} {node_type} nodes ({len(type_nodes.columns)} properties)...")

    (
        type_nodes.write.format(NEO4J_FORMAT)
        .options(**NEO4J)
        .mode("Overwrite")  # MERGE on node.keys — idempotent
        .option("labels", f":{SHARED_LABEL}:{node_type}")
        .option("node.keys", "id")
        .save()
    )
    type_nodes.unpersist()
    print(f"  ✓ {node_type}")
```

### Load the relationships

Nodes must already exist: the endpoints are **matched**, not created.

```python
rel_types = sorted(
    row["relationship"] for row in edges_df.select("relationship").distinct().collect()
)

for rel_type in rel_types:
    # 'relationship' becomes the Neo4j relationship type, so drop it from the payload;
    # any column left over would be written as a relationship property.
    type_edges = edges_df.filter(F.col("relationship") == rel_type).drop("relationship")

    count = type_edges.count()
    print(f"Loading {count:,} {rel_type} relationships...")

    (
        type_edges.coalesce(1)  # see "Deadlocks" below
        .write.format(NEO4J_FORMAT)
        .options(**NEO4J)
        .mode("Overwrite")
        .option("relationship", rel_type)
        # Match each endpoint by key. Both ends match on the shared :Node label, which is
        # what lets one write cover every source/target type this relationship connects.
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", f":{SHARED_LABEL}")
        .option("relationship.source.save.mode", "Match")
        .option("relationship.source.node.keys", "src:id")
        .option("relationship.target.labels", f":{SHARED_LABEL}")
        .option("relationship.target.save.mode", "Match")
        .option("relationship.target.node.keys", "dst:id")
        .save()
    )
    print(f"  ✓ {rel_type}")
```

Two things deserve a note.

**Endpoints must match a label that actually exists.** `relationship.source.labels` and `relationship.target.labels` name the labels the connector matches against. With `save.mode = Match`, an endpoint that matches nothing is **silently skipped** — no exception, no warning, just a relationship that never appears. If you finish a load and find zero relationships, this is almost always why: check that the label you named is the label your nodes carry.

**Deadlocks.** `coalesce(1)` writes each relationship type from a single partition. Stack Exchange has dense nodes — one popular question collects thousands of `CastFor` votes — and concurrent Spark partitions attaching relationships to the same node contend for its relationship-group lock. When separate transactions take those locks in different orders, that is a deadlock: Neo4j kills one transaction with a `TransientException` and Spark fails the whole job. One writer cannot deadlock against itself.

That single partition is the throughput ceiling of this load. To go faster on a real graph, keep the parallelism but make sure no two partitions touch the same dense node — repartition by whichever endpoint is the dense one, per relationship type — and raise `transaction.retries` so the occasional loser is retried instead of fatal.

### Verify

Read the counts back with Cypher and compare them against the Parquet:

```python
def cypher(query: str) -> DataFrame:
    """Run a read-only Cypher query and return the result as a DataFrame."""
    return spark.read.format(NEO4J_FORMAT).options(**NEO4J).option("query", query).load()


cypher(
    f"MATCH (n:{SHARED_LABEL}) RETURN n.Type AS Type, count(*) AS count ORDER BY count DESC"
).show()

cypher(
    f"MATCH (:{SHARED_LABEL})-[r]->(:{SHARED_LABEL}) "
    "RETURN type(r) AS relationship, count(*) AS count ORDER BY count DESC"
).show()
```

You should see 129,751 nodes and all 97,104 relationships, matching the tables in Step 2 exactly:

```
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

Because both loads MERGE, running the whole thing a second time produces these same numbers rather than doubling them.

> **Note:** `cypher()` uses the connector's `query` option, which rewrites your query to partition it. That means it rejects a trailing `SKIP`/`LIMIT` — take the top N with `DataFrame.limit()` instead of in Cypher.

## Step 5: Read the Graph Back into GraphFrames

Reading with Cypher gives you exactly the column names GraphFrames wants — `id`, `src`, `dst`, `relationship` — with no post-processing. A `query` read is single-partition unless you tell the connector how many rows to expect, so pass a `query.count` and a `partitions` count.

```python
PARTITIONS = 8

vertices = (
    spark.read.format(NEO4J_FORMAT)
    .options(**NEO4J)
    .option("query", f"MATCH (n:{SHARED_LABEL}) RETURN n.id AS id, n.Type AS Type")
    .option("query.count", f"MATCH (n:{SHARED_LABEL}) RETURN count(n) AS count")
    .option("partitions", str(PARTITIONS))
    .load()
)

edges = (
    spark.read.format(NEO4J_FORMAT)
    .options(**NEO4J)
    .option(
        "query",
        f"MATCH (s:{SHARED_LABEL})-[r]->(t:{SHARED_LABEL}) "
        "RETURN s.id AS src, t.id AS dst, type(r) AS relationship",
    )
    .option(
        "query.count",
        f"MATCH (:{SHARED_LABEL})-[r]->(:{SHARED_LABEL}) RETURN count(r) AS count",
    )
    .option("partitions", str(PARTITIONS))
    .load()
)

graph = GraphFrame(vertices, edges)
graph.vertices.cache()
graph.edges.cache()

print(f"Vertices read from Neo4j: {graph.vertices.count():,}")
print(f"Edges read from Neo4j:    {graph.edges.count():,}")
```

Now run Connected Components — the expensive, iterative job we came to Spark for. GraphFrames treats edges as undirected here, so a component is a set of nodes reachable from one another by any path.

```python
components = graph.connectedComponents().cache()

print(f"Connected components found: {components.select('component').distinct().count():,}")

components.groupBy("component").count().orderBy(F.desc("count")).limit(10).show()

# What kinds of node make up the largest component?
largest = components.groupBy("component").count().orderBy(F.desc("count")).first()["component"]
components.filter(F.col("component") == largest).groupBy("Type").count().orderBy(
    F.desc("count")
).show()
```

On `stats.meta.stackexchange.com` this finds **40,115 components**, and the distribution is the interesting part:

```
+------------+------+
|component   |count |
+------------+------+
|6           |56,442|
|103079215534|18    |
|128849019129|18    |
|85899346025 |18    |
...
```

One giant component holds 56,442 nodes — the connected core of the site — and everything else is tiny. Breaking that core down by type shows what it is made of:

```
+--------+------+
|    Type| count|
+--------+------+
|    Vote|40,700|
|   Badge| 9,836|
|  Answer| 2,978|
|Question| 2,024|
|    User|   771|
|     Tag|   133|
+--------+------+
```

Only 771 of 37,709 users are in it. The long tail is accounts that registered, earned an automatic badge, and never posted or voted — each one its own little island. That is a real finding about the site, and it is exactly the kind of question that is painful to answer with a traversal query and natural to answer with a distributed algorithm.

## Step 6: Write the Results Back to Neo4j

Send only the key and the new property. The connector writes every column it is given, so a wider DataFrame means rewriting properties Neo4j already has.

```python
(
    components.select("id", F.col("component").alias("componentId"))
    .write.format(NEO4J_FORMAT)
    .options(**NEO4J)
    .mode("Overwrite")
    .option("labels", f":{SHARED_LABEL}")
    .option("node.keys", "id")
    .save()
)
print("✓ componentId written")
```

Matching on the shared label alone — `:Node`, not `:Node:Question` — means this one write updates every node type at once. Because `Overwrite` MERGEs on `:Node(id)` and every id already exists, it sets the new property **in place**: it does not create nodes, drop the per-type labels, or disturb the properties loaded in Step 4.

Verify, and then ask a question that mixes the property graph with the result Spark just computed:

```python
cypher(
    f"MATCH (n:{SHARED_LABEL}) WHERE n.componentId IS NOT NULL "
    "RETURN count(n) AS nodesWithComponentId"
).show()

cypher(
    f"MATCH (u:User)-[:Earns]->(b:Badge) WHERE u.componentId = {largest} "
    "RETURN u.DisplayName AS user, count(b) AS badges ORDER BY badges DESC"
).limit(10).show(truncate=False)
```

That last query returns the most decorated users in the site's connected core:

```
+---------------------------+------+
|user                       |badges|
+---------------------------+------+
|whuber                     |249   |
|gung - Reinstate Monica    |220   |
|Glen_b                     |217   |
|amoeba                     |129   |
|Tim                        |119   |
+---------------------------+------+
```

`u.componentId` came from Spark; `[:Earns]->(b:Badge)` came from Neo4j. All 129,751 nodes now carry a `componentId`, queryable alongside everything else — in the browser, from Cypher, or from an application. That is the round trip: Neo4j stored the graph, Spark did the work that does not fit on one machine, and Neo4j got the answer back.

```python
spark.stop()
```

## Troubleshooting

**The load finished but there are no relationships.** The endpoint labels in `relationship.source.labels` / `relationship.target.labels` do not match any node. With `save.mode = Match` the connector skips unmatched endpoints silently. Confirm what your nodes actually carry:

```bash
docker exec neo4j-graphframes cypher-shell -u neo4j -p graphframes123 "CALL db.labels();"
```

**Node counts are an exact multiple of what you expect.** A previous run used `.mode("Append")`, which is `CREATE` and ignores `node.keys`. `MERGE` cannot repair this afterwards — the duplicates have distinct internal ids and none of them carry the label the fixed script MERGEs on, so re-running adds another copy rather than collapsing them. Reset and reload:

```bash
docker exec neo4j-graphframes cypher-shell -u neo4j -p graphframes123 "MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS;"
```

**`key not found: ArrayType(StringType,true)`.** Connector 6.0.0's schema-optimization code cannot map array columns. Do not pass `schema.optimization.node.keys` on a write whose DataFrame contains one; create the constraint up front on an empty DataFrame, as in Step 4.

**`TransientException` mentioning a deadlock wait cycle.** Concurrent partitions are contending for the relationship-group lock of a dense node. Write relationships from a single partition, or repartition so no two partitions touch the same dense node.

**`SKIP/LIMIT are not allowed at the end of the query`.** The connector rewrites `query` reads to partition them. Use `DataFrame.limit()` instead.

## Next Steps

- Swap `connectedComponents()` for [PageRank](../04-user-guide/03-centralities.md) or [Label Propagation](../04-user-guide/06-graph-clustering.md) and write those results back the same way
- Use [motif finding](02-motif-tutorial.md) to search for patterns that Cypher expresses awkwardly, then persist what you find
- Point `STACKEXCHANGE_SITE` at a larger site and raise the relationship-load parallelism as described in Step 4

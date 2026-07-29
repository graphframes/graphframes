# Configurations

GraphFrames provides several configuration options that can be used to tune the behavior of algorithms and operations. This page documents all available configurations, their descriptions, default values, and usage examples.

## Configuration Table

The following table lists all available GraphFrames configurations:

| Configuration | Description | Default Value | Since Version |
|---------------|-------------|---------------|---------------|
| `spark.graphframes.useLocalCheckpoints` | Tells the connected components algorithm to use local checkpoints. If set to "true", iterative algorithm will use the checkpointing mechanism to the persistent storage. Local checkpoints are faster but can make the whole job less prone to errors. | `false` | 0.9.3 |
| `spark.graphframes.useLabelsAsComponents` | Tells the connected components algorithm to use labels as components in the output DataFrame. If set to "false", randomly generated labels with the data type LONG will returned. | Optional (default: `true`) | 0.9.0 |
| `spark.graphframes.connectedComponents.algorithm` | Sets the connected components algorithm to use. Supported algorithms: <br>- "graphframes": Uses alternating large star and small star iterations proposed in [Connected Components in MapReduce and Beyond](http://dx.doi.org/10.1145/2670979.2670997) with skewed join optimization. <br>- "graphx": Converts the graph to a GraphX graph and then uses the connected components implementation in GraphX. | Optional (default: `graphframes`) | 0.9.0 |
| `spark.graphframes.connectedComponents.broadcastthreshold` | Sets broadcast threshold in propagating component assignments. If a node degree is greater than this threshold at some iteration, its component assignment will be collected and then broadcasted back to propagate the assignment to its neighbors. Otherwise, the assignment propagation is done by a normal Spark join. This parameter is only used when the algorithm is set to "graphframes". | Optional (default: `1000000`) | 0.9.0 |
| `spark.graphframes.connectedComponents.checkpointinterval` | Sets checkpoint interval in terms of number of iterations. Checkpointing regularly helps recover from failures, clean shuffle files, shorten the lineage of the computation graph, and reduce the complexity of plan optimization. As of Spark 2.0, the complexity of plan optimization would grow exponentially without checkpointing. Hence, disabling or setting longer-than-default checkpoint intervals are not recommended. Checkpoint data is saved under `org.apache.spark.SparkContext.getCheckpointDir` with prefix "connected-components". If the checkpoint directory is not set, this throws a `java.io.IOException`. Set a nonpositive value to disable checkpointing. This parameter is only used when the algorithm is set to "graphframes". | Optional (default: `2`) | 0.9.0 |
| `spark.graphframes.connectedComponents.intermediatestoragelevel` | Sets storage level for intermediate datasets that require multiple passes. | Optional (default: `MEMORY_AND_DISK`) | 0.9.0 |

## Managing DataFrame Lineage Before GraphFrame Creation

Spark keeps the transformation history (the *lineage*) of each DataFrame in
its logical plan. A long or highly branched plan can make analysis and query
optimization increasingly expensive. This often appears when `vertices` or
`edges` have been assembled through many joins, unions, filters, or projections
before they are passed to `GraphFrame`.

Checkpoint the prepared DataFrames at a stable boundary before constructing the
graph. The checkpoint must preserve the required graph columns: `vertices`
must contain `id`, and `edges` must contain `src` and `dst`.

### Scala API

```scala
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

val spark = SparkSession.builder().getOrCreate()

// Persistent checkpoints are recoverable if an executor is lost.
spark.sparkContext.setCheckpointDir("/path/to/spark-checkpoints")

val preparedVertices = rawVertices
  .filter("active = true")
  .select("id", "name")
  .checkpoint(eager = true)

val preparedEdges = rawEdges
  .filter("weight > 0")
  .select("src", "dst", "weight")
  .checkpoint(eager = true)

val graph = GraphFrame(preparedVertices, preparedEdges)
```

`checkpoint(eager = true)` materializes the files immediately and truncates the
lineage that precedes the checkpoint. Set the checkpoint directory before
materializing either DataFrame. If the workload can tolerate recomputation
after executor loss, `localCheckpoint(eager = true)` avoids the checkpoint
directory and is usually faster, but local checkpoint data is not reliable and
should not be used as a recovery boundary.

### Python API

```python
from graphframes import GraphFrame

# Persistent checkpoints are recoverable if an executor is lost.
spark.sparkContext.setCheckpointDir("/path/to/spark-checkpoints")

prepared_vertices = (
    raw_vertices.filter("active = true")
    .select("id", "name")
    .checkpoint(eager=True)
)

prepared_edges = (
    raw_edges.filter("weight > 0")
    .select("src", "dst", "weight")
    .checkpoint(eager=True)
)

graph = GraphFrame(prepared_vertices, prepared_edges)
```

Use `localCheckpoint(eager=True)` instead when losing executor-local data is
acceptable. The `spark.graphframes.useLocalCheckpoints` setting controls local
checkpointing used by supported GraphFrames algorithms; it does not replace
checkpointing complex input DataFrames before `GraphFrame` construction.

Checkpointing is a deliberate materialization boundary, so use it after a
meaningful preparation stage rather than after every transformation. It can
add I/O and storage overhead, but avoids repeatedly analyzing an unnecessarily
large plan when the graph is created or queried.

## Setting Configurations

GraphFrames configurations can be set in several ways:

### Spark Configuration

You can set configurations when creating a SparkSession:

#### Scala API

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("GraphFrames Example")
  .config("spark.graphframes.connectedComponents.algorithm", "graphframes")
  .config("spark.graphframes.connectedComponents.checkpointinterval", 3)
  .getOrCreate()
```

#### Python API

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName("GraphFrames Example") \
  .config("spark.graphframes.connectedComponents.algorithm", "graphframes") \
  .config("spark.graphframes.connectedComponents.checkpointinterval", 3) \
  .getOrCreate()
```

### Runtime Configuration

You can also set configurations at runtime:

#### Scala API

```scala
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "graphframes")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
```

#### Python API
```python
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "graphframes")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
```

## Example: Connected Components with Custom Configurations

This example shows how to run the Connected Components algorithm with custom configurations:

### Scala API

```scala
import org.graphframes.GraphFrame
import org.graphframes.examples

// Get example graph
val g = examples.Graphs.friends

// Set configurations
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "graphframes")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
spark.conf.set("spark.graphframes.useLocalCheckpoints", true)

// Run connected components with custom configurations
val result = g.connectedComponents.run()
result.show()
```

### Python API

```python
from graphframes.examples import Graphs

# Get example graph
g = Graphs(spark).friends()

# Set configurations
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "graphframes")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
spark.conf.set("spark.graphframes.useLocalCheckpoints", "true")

# Run connected components with custom configurations
result = g.connectedComponents()
result.show()
```

## Notes on Configuration Usage

- **Checkpoint Directory**: For configurations related to checkpointing, make sure to set a checkpoint directory using `spark.sparkContext.setCheckpointDir("path/to/checkpoint/dir")` before running algorithms that use checkpointing.
- **Storage Levels**: When setting the `spark.graphframes.connectedComponents.intermediatestoragelevel` configuration, use one of the following values: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`, `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`, `MEMORY_AND_DISK_2`, etc.
- **Algorithm Selection**: The choice of algorithm for connected components can significantly impact performance. The "graphframes" algorithm is generally more scalable for large graphs, while the "graphx" algorithm may be faster for smaller graphs.
- **Local Checkpoints**: Local checkpoints are faster and less error prone, but can put strain on the local disk if insufficiently large. Because local checkpoints do not require to set `checkpointDir` it is a recommended option.

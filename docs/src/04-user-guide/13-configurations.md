# Configurations

GraphFrames provides several configuration options that can be used to tune the behavior of algorithms and operations. This page documents all available configurations, their descriptions, default values, and usage examples.

## Configuration Table

The following table lists all available GraphFrames configurations:

| Configuration | Description | Default Value | Since Version |
|---------------|-------------|---------------|---------------|
| `spark.graphframes.useLocalCheckpoints` | Tells the connected components algorithm to use local checkpoints. If set to "true", the algorithm checkpoints to the local disks of the executors instead of to persistent storage. For "two_phase" this removes the need to configure a checkpoint directory. "randomized_contraction" always needs one, because it stores its intermediate tables there. Local checkpoints are faster but less reliable: they do not survive the loss of an executor. Used by the "two_phase" and "randomized_contraction" algorithms. | Optional (default: `false`) | 0.9.3 |
| `spark.graphframes.useLabelsAsComponents` | Tells the connected components algorithm to use the minimum original vertex label as the component ID. When "false", component IDs have the data type LONG. The effect depends on the algorithm and on the vertex ID type, see [Traversals](/04-user-guide/05-traversals.md) for the details. Used by the "two_phase" and "randomized_contraction" algorithms. | Optional (default: `false`) | 0.9.0 |
| `spark.graphframes.connectedComponents.algorithm` | Sets the connected components algorithm to use. Supported algorithms: <br>- "two_phase": Uses alternating large star and small star iterations proposed in [Connected Components in MapReduce and Beyond](http://dx.doi.org/10.1145/2670979.2670997). <br>- "randomized_contraction": Uses the randomized algorithm proposed in [In-database connected component analysis](https://arxiv.org/pdf/1802.09478). <br>- "graphx": Converts the graph to a GraphX graph and then uses the connected components implementation in GraphX. <br>- "graphframes": Deprecated alias for "two_phase". | Optional (default: `two_phase`) | 0.9.0 |
| `spark.graphframes.connectedComponents.broadcastthreshold` | Sets broadcast threshold in propagating component assignments. If a node degree is greater than this threshold at some iteration, its component assignment will be collected and then broadcasted back to propagate the assignment to its neighbors. Otherwise, the assignment propagation is done by a normal Spark join. Set it to `-1` to disable manual broadcasting and let Adaptive Query Execution handle the skew instead; that mode is considerably faster on most graphs. This parameter is only used by the "two_phase" algorithm. | Optional (default: `1000000`) | 0.9.0 |
| `spark.graphframes.connectedComponents.checkpointinterval` | Sets checkpoint interval in terms of number of iterations. Checkpointing regularly helps recover from failures, clean shuffle files, shorten the lineage of the computation graph, and reduce the complexity of plan optimization. As of Spark 2.0, the complexity of plan optimization would grow exponentially without checkpointing. Hence, disabling or setting longer-than-default checkpoint intervals are not recommended. Checkpoint data is saved under the directory set by `org.apache.spark.SparkContext.setCheckpointDir`, or, when that is not set, under the `spark.checkpoint.dir` configuration; the algorithm writes into a sub-directory prefixed with its own name ("connected-components" or "randomized-contraction"). If neither is set, "randomized_contraction" throws a `java.io.IOException`, and so does "two_phase" unless local checkpoints are enabled. Set a nonpositive value to disable checkpointing. Used by the "two_phase" and "randomized_contraction" algorithms; note that "two_phase" in AQE mode (`broadcastthreshold` set to `-1`) relies on Spark's own DataFrame checkpointing instead of its own sub-directory. | Optional (default: `2`) | 0.9.0 |
| `spark.graphframes.connectedComponents.intermediatestoragelevel` | Sets storage level for intermediate datasets that require multiple passes. Used by all the connected components algorithms. | Optional (default: `MEMORY_AND_DISK`) | 0.9.0 |

## Setting Configurations

GraphFrames configurations can be set in several ways:

### Spark Configuration

You can set configurations when creating a SparkSession:

#### Scala API

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("GraphFrames Example")
  .config("spark.graphframes.connectedComponents.algorithm", "two_phase")
  .config("spark.graphframes.connectedComponents.checkpointinterval", 3)
  .getOrCreate()
```

#### Python API

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName("GraphFrames Example") \
  .config("spark.graphframes.connectedComponents.algorithm", "two_phase") \
  .config("spark.graphframes.connectedComponents.checkpointinterval", 3) \
  .getOrCreate()
```

### Runtime Configuration

You can also set configurations at runtime:

#### Scala API

```scala
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "two_phase")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
```

#### Python API
```python
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "two_phase")
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
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "two_phase")
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
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "two_phase")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
spark.conf.set("spark.graphframes.useLocalCheckpoints", "true")

# Run connected components with custom configurations
result = g.connectedComponents()
result.show()
```

## Notes on Configuration Usage

- **Checkpoint Directory**: For configurations related to checkpointing, make sure to set a checkpoint directory using `spark.sparkContext.setCheckpointDir("path/to/checkpoint/dir")` before running algorithms that use checkpointing. Where `sparkContext` is not reachable, for example from a Spark Connect client, set the `spark.checkpoint.dir` configuration instead; GraphFrames falls back to it. For "two_phase" you can instead enable `spark.graphframes.useLocalCheckpoints`, which needs no checkpoint directory. "randomized_contraction" always needs one.
- **Storage Levels**: When setting the `spark.graphframes.connectedComponents.intermediatestoragelevel` configuration, use one of the following values: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`, `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`, `MEMORY_AND_DISK_2`, etc.
- **Algorithm Selection**: The choice of algorithm for connected components can significantly impact performance. The DataFrame-based algorithms, "two_phase" (the default) and "randomized_contraction", are generally more scalable for large graphs, while the RDD-based "graphx" algorithm may be faster for smaller graphs. See [Traversals](/04-user-guide/05-traversals.md) for a comparison of the algorithms.
- **Local Checkpoints**: Local checkpoints are faster and can put strain on the local disk if insufficiently large. They are also less reliable than checkpoints in persistent storage, because they do not survive the loss of an executor. Because local checkpoints do not require setting `checkpointDir` for "two_phase", they are a convenient option for shorter runs.
- **Input DataFrames**: The checkpointing configured above only shortens the lineage of the algorithms' own intermediate results. If the vertices or edges come out of a long chain of transformations, see [Preparing Input DataFrames](#preparing-input-dataframes).

## Preparing Input DataFrames

### The Growing Lineage Problem

A `GraphFrame` does not copy its vertices and edges. It keeps the DataFrames you pass to `GraphFrame(vertices, edges)` together with their logical plan, that is, the full description of how they were computed (their lineage). Algorithms build every query on top of these DataFrames, often refer to them several times in one query (for example, joining the edges on both `src` and `dst`), and iterative algorithms join them again in every iteration.

As a result, the whole plan that produced the inputs becomes part of every query the algorithm runs. When the inputs are the result of a long pipeline (many joins, unions, window functions, UDFs, or the output of another graph algorithm), Spark has to analyze and optimize that large plan again and again, and to recompute it unless it is cached. Typical symptoms are:

- long pauses on the driver before each job starts, while Spark is planning the query;
- very large plans in `explain()` and in the SQL tab of the Spark UI;
- driver out-of-memory errors or a `StackOverflowError` during query planning;
- the stages that compute the inputs running again in every iteration.

The `checkpointInterval` parameters of the algorithms do not fully solve this: they truncate the lineage of the intermediate results that an algorithm produces, but algorithms built on Pregel and message passing keep joining the input DataFrames in every iteration, and their plan is still analyzed before the first checkpoint and in every query that reads them.

### Recommendation: Materialize Complex Inputs

If the vertices or edges are built by complex transformations, materialize them before you create the graph, so that the graph starts from a short plan:

#### Scala API

```scala
import org.graphframes.GraphFrame

spark.sparkContext.setCheckpointDir("/path/to/checkpoint/dir")

// vertices and edges are the results of a long ETL pipeline
val g = GraphFrame(vertices.checkpoint(), edges.checkpoint())
```

#### Python API

```python
from graphframes import GraphFrame

spark.sparkContext.setCheckpointDir("/path/to/checkpoint/dir")

# vertices and edges are the results of a long ETL pipeline
g = GraphFrame(vertices.checkpoint(), edges.checkpoint())
```

There are several ways to materialize a DataFrame:

| Method | Truncates the lineage | Notes |
|--------|-----------------------|-------|
| `df.checkpoint()` | Yes | Writes the data to the checkpoint directory and survives the loss of an executor. Needs a checkpoint directory, see the note on the checkpoint directory above. The files are not deleted automatically unless `spark.cleaner.referenceTracking.cleanCheckpoints` is set to `true`. |
| `df.localCheckpoint()` | Yes | Stores the data on the executors and needs no checkpoint directory. Faster, but the data is lost, and the job fails, if an executor is lost. |
| Writing to a table or to files (for example Parquet) and reading them back | Yes | The data outlives the Spark application, and you control when it is deleted. Useful when the same graph is analyzed several times. |
| `df.persist()` / `df.cache()` | No | Avoids recomputing the inputs, but their full plan still has to be analyzed in every query. Not enough on its own for very long pipelines. |

With Spark Connect, `checkpoint()` and `localCheckpoint()` are available from Spark 4.0 on. Because `sparkContext` is not reachable from a Spark Connect client, configure the checkpoint directory with `spark.checkpoint.dir` on the server.

Materializing writes the data once more, so it is not worth it for inputs that are cheap to compute, such as a table that is read with a few column selections and filters.

---
layout: global
displayTitle: GraphFrames Configurations
title: Configurations
description: GraphFrames GRAPHFRAMES_VERSION configurations documentation
---

* Table of contents
{:toc}

# GraphFrames Configurations

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

## Setting Configurations

GraphFrames configurations can be set in several ways:

### Spark Configuration

You can set configurations when creating a SparkSession:

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("GraphFrames Example")
  .config("spark.graphframes.connectedComponents.algorithm", "graphframes")
  .config("spark.graphframes.connectedComponents.checkpointinterval", 3)
  .getOrCreate()
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.sql import SparkSession

spark = SparkSession.builder \
  .appName("GraphFrames Example") \
  .config("spark.graphframes.connectedComponents.algorithm", "graphframes") \
  .config("spark.graphframes.connectedComponents.checkpointinterval", 3) \
  .getOrCreate()
{% endhighlight %}
</div>

</div>

### Runtime Configuration

You can also set configurations at runtime:

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% highlight scala %}
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "graphframes")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
spark.conf.set("spark.graphframes.connectedComponents.algorithm", "graphframes")
spark.conf.set("spark.graphframes.connectedComponents.checkpointinterval", 3)
{% endhighlight %}
</div>

</div>

## Example: Connected Components with Custom Configurations

This example shows how to run the Connected Components algorithm with custom configurations:

<div class="codetabs">

<div data-lang="scala" markdown="1">
{% highlight scala %}
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
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
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
{% endhighlight %}
</div>

</div>

## Notes on Configuration Usage

- **Checkpoint Directory**: For configurations related to checkpointing, make sure to set a checkpoint directory using `spark.sparkContext.setCheckpointDir("path/to/checkpoint/dir")` before running algorithms that use checkpointing.

- **Storage Levels**: When setting the `spark.graphframes.connectedComponents.intermediatestoragelevel` configuration, use one of the following values: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`, `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`, `MEMORY_AND_DISK_2`, etc.

- **Algorithm Selection**: The choice of algorithm for connected components can significantly impact performance. The "graphframes" algorithm is generally more scalable for large graphs, while the "graphx" algorithm may be faster for smaller graphs.
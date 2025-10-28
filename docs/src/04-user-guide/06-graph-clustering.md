# Community Detection

## Label Propagation (LPA)

Run a static Label Propagation Algorithm for detecting communities in networks. Each node in the network is initially assigned to its own community. At every superstep, nodes send their community affiliation to all neighbors and update their state to the mode community affiliation of incoming messages. LPA is a standard community detection algorithm for graphs. It is very inexpensive computationally, although (1) convergence is not guaranteed and (2) one can end up with trivial solutions (all nodes are identified into a single community).

See [Wikipedia](https://en.wikipedia.org/wiki/Label_Propagation_Algorithm) for the background.

---

**NOTE**

*Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!*

---

### Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.labelPropagation).

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

result = g.labelPropagation(maxIter=5)
result.select("id", "label").show()
```

### Scala API

For API details, refer to the @:scaladoc(org.grapimport org.graphframes.lib.LabelPropagation).

```scala
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends // get example graph

val result = g.labelPropagation.maxIter(5).run()
result.select("id", "label").show()
```

### Arguments

- `maxIter`

An amount of Pregel iterations. While in theory, Label Propagation algorithm should converge sooner or later to some stable state, there are a lot of problems with it on a real-world graphs. The first one is oscillations: even if the algorithm is almost converged, on a big graphs some vertices at the border between detected communities may contibue oscilate from one iteration to another. The biggest problme, however, is that algorithm may easily converge to the state when all vertices has the same label. It is strongly recommended to set `maxIter` to some reasonable value from `5` to `10` and do some experiments depends of the task and the goal.

- `algorithm`

Possible values are `graphx` and `graphframes`. Both implementations are based on the same logic. GraphX is faster for small-medium sized graphs but requires more memory due to less efficient RDD serialization and it's triplets-based nature. GraphFrames requires much less memory due to efficient Thungsten serialization and because the core structures are edges and messages, not triplets.

- `checkpoint_interval`

For `graphframes` only. To avoid exponential growing of the Spark' Logical Plan, DataFrame lineage and query optimization time, it is required to do checkpointing periodically. While checkpoint itself is not free, it is still recommended to set this value to something less than `5`.

- `use_local_checkpoints`

For `graphframes` only. By default, GraphFrames uses persistent checkpoints. They are realiable and reduce the errors rate. The downside of the persistent checkpoints is that they are requiride to set up a `checkpointDir` in persistent storage like `S3` or `HDFS`. By providing `use_local_checkpoints=True`, user can say GraphFrames to use local disks of Spark' executurs for checkpointing. Local checkpoints are faster, but they are less reliable: if the executur lost, for example, is taking by the higher priority job, checkpoints will be lost and the whole job fails.

- `storage_level`

The level of storage for intermediate results and the output `DataFrame` with components. By default it is memory and disk deserialized as a good balance between performance and reliability. For very big graphs and out-of-core scenarious, using `DISK_ONLY` may be faster.

## Power Iteration Clustering (PIC)

GraphFrames provides a wrapper for the [Power Iteration Clustering](https://www.cs.cmu.edu/~frank/papers/icml2010-pic-final.pdf) algorithm from the SparkML library.

---

**NOTE**

*Be aware, that returned `DataFrame` is persistent and should be unpersisted manually after processing to avoid memory leaks!*

---

### Python API

```python
g = GraphFrame(vertices, edges)
g.powerIterationClustering(k=2, maxIter=40, weightCol="weight")
```

### Scala API

```scala
val gf = GraphFrame(vertices, edges)
val clusters = gf
  .powerIterationClustering(k = 2, maxIter = 40, weightCol = Some("weight"))
```

## Maximal Independent Set

An MIS is a set of vertices such that no two vertices in the set are adjacent (i.e., there is no edge between any two vertices in the set), and the set is maximal, meaning that adding any other vertex to the set would violate the independence property. Note that maximal independent set is not necessarily maximum independent set; that is, it ensures no more vertices can be added to the set, but does not guarantee that the set has the largest possible number of vertices among all possible independent sets in the graph. Finding a maximum independent set is NP-hard problem.

The algorithm implemented in GraphFrames is based on the paper: Ghaffari, Mohsen. "An improved distributed algorithm for maximal independent set." Proceedings of the twenty-seventh annual ACM-SIAM symposium on Discrete algorithms. Society for Industrial and Applied Mathematics, 2016. ([https://doi.org/10.1137/1.9781611974331.ch20](https://doi.org/10.1137/1.9781611974331.ch20))

Algorithm is useful, for example, if you are planning a marketing campaign and want to cover at most of users of the social network by minimizing communications. In that case, you need to find a big subset of nodes does not connected to each other but connected to othe nodes of the network. It is exactly what Maximal Independent Set do. It returns a set of not connected vertices so no more vertices can be added to the set without violation of the independence.

Note: This is a randomized, non-deterministic algorithm. The result may vary between runs even if a fixed random seed is provided because how Apache Spark works.

### Python API

```python
vertices = spark.createDataFrame(
    [(0, "a"), (1, "b"), (2, "c"), (3, "d")], ["id", "name"]
)

edges = spark.createDataFrame([(0, 1, "edge1")], ["src", "dst", "name"])

graph = GraphFrame(vertices, edges)
mis = graph.maximal_independent_set(storage_level=storage_level, seed=12345)
```

### Scala API

```scala
val vertices =
  spark.createDataFrame(Seq((0L, "a"), (1L, "b"), (2L, "c"), (3L, "d"))).toDF("id", "name")

val edges = spark.createDataFrame(Seq((0L, 1L, "edge1"))).toDF("src", "dst", "name")

val graph = GraphFrame(vertices, edges)
val mis = graph.maximalIndependentSet.run(seed = 12345L)
```

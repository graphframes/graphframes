# Community Detection

## Label Propagation (LPA)

Run a static Label Propagation Algorithm for detecting communities in networks. Each node in the network is initially assigned to its own community. At every superstep, nodes send their community affiliation to all neighbors and update their state to the mode community affiliation of incoming messages. LPA is a standard community detection algorithm for graphs. It is very inexpensive computationally, although (1) convergence is not guaranteed and (2) one can end up with trivial solutions (all nodes are identified into a single community).

See [Wikipedia](https://en.wikipedia.org/wiki/Label_Propagation_Algorithm) for the background.

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

## Power Iteration Clustering (PIC)

GraphFrames provides a wrapper for the [Power Iteration Clustering](https://www.cs.cmu.edu/~frank/papers/icml2010-pic-final.pdf) algorithm from the SparkML library.

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

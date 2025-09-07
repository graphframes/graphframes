# Message passing

Like GraphX, GraphFrames provides primitives for developing graph algorithms. The two key components are:

* `aggregateMessages`: Send messages between vertices, and aggregate messages for each vertex. GraphFrames provides a native `aggregateMessages` method implemented using DataFrame operations. This may be used analogously to the GraphX API.
* joins: Join message aggregates with the original graph. GraphFrames rely on `DataFrame` joins, which provide the full functionality of GraphX joins.

The below code snippets show how to use `aggregateMessages` to compute the sum of the ages of adjacent users.

## Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.aggregateMessages).

```python
from graphframes.lib import AggregateMessages as AM
from graphframes.examples import Graphs
from pyspark.sql.functions import sum as sqlsum


g = Graphs(spark).friends()  # Get example graph

# For each user, sum the ages of the adjacent users
msgToSrc = AM.dst["age"]
msgToDst = AM.src["age"]
agg = g.aggregateMessages(
    sqlsum(AM.msg).alias("summedAges"),
    sendToSrc=msgToSrc,
    sendToDst=msgToDst)
agg.show()
```

## Scala API

For API details, refer to the @:scaladoc(org.graphframes.lib.AggregateMessages).

```scala
import org.graphframes.{examples,GraphFrame}
import org.graphframes.lib.AggregateMessages

val g: GraphFrame = examples.Graphs.friends  // get example graph

// We will use AggregateMessages utilities later, so name it "AM" for short.
val AM = AggregateMessages

// For each user, sum the ages of the adjacent users.
val msgToSrc = AM.dst("age")
val msgToDst = AM.src("age")
val agg = { g.aggregateMessages
  .sendToSrc(msgToSrc)  // send destination user's age to source
  .sendToDst(msgToDst)  // send source user's age to destination
  .agg(sum(AM.msg).as("summedAges")) } // sum up ages, stored in AM.msg column
agg.show()
```

For a more complex example, look at the code used to implement the @:pydoc(graphframes.examples.BeliefPropagation).
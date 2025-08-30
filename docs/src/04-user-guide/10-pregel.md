# Pregel API

Pregel API is one of the core backbones of GraphFrames. It is based on the implementation of the [Pregel algorithm](https://blog.lavaplanets.com/wp-content/uploads/2023/12/p135-malewicz.pdf) in terms of relational operations using the Apache Spark `DataFrame` / `Dataset` API.

## API

For the API details, please refer to:

- Scala API: @:scaladoc(org.graphframes.lib.Pregel)
- Python API: @:pydoc(graphframes.lib.Pregel)

## Arguments

### Message Columns

Internally, GraphFrames handles all the messages via a specific column:

```scala
val pregelMessageColumn = org.graphframes.lib.Pregel.msg
val pregelMessageColumnName = org.graphframes.lib.Pregel.MSG_COL_NAME
```

This name is applied to both raw and aggregated messages and should be used in conditions and aggregations. For example, in the simplest implementation of the PageRank algorithm it may look like this:

```scala
val aggregateMessagesExpression = sum(Pregel.msg)
val updateExpression = coalesce(Pregel.msg, lit(0.0)) * (1 - alpha) + alpha / numVertices
```

### Triplet Column

At the stage of message generation, GraphFrames internally creates triplets, that contains `StructType` columns for source vertex, edge and destination vertex. These structs contain all the columns from the original source, destination and edge, including not only initial attributes, but also Pregel columns, like activity column, all the vertex columns, etc. To access these data, user should rely on the following API:

```scala
val sourceVertexColumn1 = Pregel.src("vertexColumn1")
val sourceVertexColumn2 = Pregel.src("vertexColumn2")
val destinationVertexColumn1 = Pregel.dst("vertexColumn1")
val edgeWeight = Pregel.edge("weight")
```

Under the hood, the passed name of the column will be resolved to get the corresponding element of the triplet structs.

### Sending Messages

GraphFrames Pregel API support arbitrary number of messages per vertex. Inside the Pregel API **graphs are always considered directed**. This means that if a vertex has an outgoing edge to another vertex, then the message will be sent to the destination vertex. To emulate the behavior of the undirected graph, the user can send the same message to both the source and the destination vertex.

```scala
graph
  .pregel
  .sendMsgToDst(Pregel.src("vertexColumn")) // Sending the vertex column of the destination vertex to the source vertex.
  .sendMsgToSrc(Pregel.dst("vertexColumn")) // Sending the vertex column of the source vertex to the destination vertex.
  .run()
```

### Termination Conditions

GraphFrames Pregel API provides the following termination conditions:

- By a number of iterations. Users can specify the maximum number of iterations with `setMaxIter(value: Int)`.
- In case of no new messages are sent. User can say GF to terminate the computations if all the messages sent on the iteration are empty (`null`). To do this, user should specify `setEarlyStopping(value: Boolean)`. Be careful, because the checking of nullity is a not free operation, but Apache Spark action. So, for example, if messages cannot be empty, this condition should be set to `false`. For example, in algorithms like `ShortestPaths`, this condition should be set to `true`, but for algorithms like `PageRank`, this condition should be set to `false`  because the messages cannot be empty.
- By vertex voting. Users can specify the participation condition per vertex with `setInitialActiveVertexExpression(expression: Column` and `setUpdateActiveVertexExpression(expression: Column)`. In the case if `stopIfAllNonActiveVertices(value: Boolean)` is set to `true`, the computation will stop if all the vertices are inactive. This is useful for algorithms like `LabelPropagation`, when messages are always not `null`, but if no vertex changed a label on the last iteration, the computation should stop.
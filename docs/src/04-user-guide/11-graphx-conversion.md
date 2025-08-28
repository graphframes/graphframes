# GraphX conversions

We provide utilities for converting between GraphFrame and GraphX graphs. See the [GraphX User Guide](http://spark.apache.org/docs/latest/graphx-programming-guide.html) for details on GraphX.

## GraphFrame to GraphX

Conversion to GraphX creates a GraphX `Graph` which has `Long` vertex IDs and attributes of type `Row`.

Vertex and edge attributes are the original rows in `vertices` and `edges`, respectively.

Note that vertex (and edge) attributes include vertex IDs (and source, destination IDs) to support non-Long vertex IDs.  If the vertex IDs are not convertible to Long values, then the values are indexed to generate corresponding Long vertex IDs (which is an
expensive operation).

The column ordering of the returned `Graph` vertex and edge attributes are specified by `GraphFrame.vertexColumns` and `GraphFrame.edgeColumns`, respectively.

## GraphX to GraphFrame

GraphFrame provides two conversion methods. The first takes any GraphX graph and converts the vertex and edge `RDD`s into `DataFrame`s using schema inference. Those DataFrames are then used to create a GraphFrame.

The second conversion method is more complex and is useful for users with existing GraphX code. Its main purpose is to support workflows of the following form: (1) convert a GraphFrame to GraphX, (2) run GraphX code to augment the GraphX graph with new vertex or edge attributes, and (3) merge the new attributes back into the original GraphFrame.

For example, given:

* GraphFrame `originalGraph`
* GraphX `Graph[String, Int]` `graph` with a String vertex attribute we want to call "category" and an Int edge attribute we want to call "count"

We can call `fromGraphX(originalGraph, graph, Seq("category"), Seq("count"))` to produce a new GraphFrame. The new GraphFrame will be an augmented version of `originalGraph`, with new `GraphFrame.vertices` column "category" and new `GraphFrame.edges` column "count" added.

For example usage, look at the code used to implement the @:scaladoc(org.graphframes.examples.BeliefPropagation).

## Example conversions

The below example demonstrates simple GraphFrame-GraphX conversions.

For API details, refer to the API docs for @:scaladoc(org.graphframes.GraphFrame).

```scala
import org.apache.spark.graphx.Graph
import org.apache.spark.sql.Row
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends  // get example graph

// Convert to GraphX
val gx: Graph[Row, Row] = g.toGraphX

// Convert back to GraphFrame.
// Note that the schema is changed because of constraints in the GraphX API.
val g2: GraphFrame = GraphFrame.fromGraphX(gx)
```

These conversions are only supported in Scala since GraphX does not have a Python API.
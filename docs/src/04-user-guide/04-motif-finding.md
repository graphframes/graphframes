# Motif finding

Motif finding refers to searching for structural patterns in a graph. For an example of real-world use, check out the [Motif Finding Tutorial](/03-tutorials/02-motif-tutorial.md).

GraphFrame motif finding uses a simple Domain-Specific Language (DSL) for expressing structural queries. For example, `graph.find("(a)-[e1]->(b); (b)-[e2]->(a)")` or `graph.find("(a)<-[e]->(b)")` will search for pairs of vertices `a`,`b` connected by edges in both directions.  It will return a `DataFrame` of all such structures in the graph, with columns for each of the named elements (vertices or edges) in the motif.  In this case, the returned columns will be "a, b, e1, e2."

DSL for expressing structural patterns:

* The basic unit of a pattern is an edge.
   For example, `"(a)-[e]->(b)"` expresses an edge `e` from vertex `a` to vertex `b`.
   Note that vertices are denoted by parentheses `(a)`, while edges are denoted by
   square brackets `[e]`.
* A pattern is expressed as a join of edges. Edge patterns can be joined with semicolons.
   Motif `"(a)-[e1]->(b); (b)-[e2]->(c)"` specifies two edges from `a` to `b` to `c`.
* Simply, you can also quantify the fixed length like `"(a)-[e*2]->(c)"`. The motif parser decompose it into multiple patterns `"(a)-[e1]->(_v1);(_v1)-[e1]->(c)"` by inserting interim vertexes arbitrarily. It specifies two edges from `a` to `_v1` to `c`.
* In order to search for variable-length motifs, you can specify the range `"(a)-[e*1..3]->(c)"`. It unions the results from each possible length `"(a)-[e*1]->(c)"`, `"(a)-[e*2]->(c)"`, and `"(a)-[e*3]->(c)"` into a DataFrame.
* If the direction is omitted `"(a)-[e]-(b)"`, it represents an undirected pattern — that is, either `"(a)-[e]->(b)"` or `"(a)<-[e]-(b)"`, which includes edges that are incoming or outgoing.
* Within a pattern, names can be assigned to vertices and edges.  For example,
   `"(a)-[e]->(b)"` has three named elements: vertices `a,b` and edge `e`.
   These names serve two purposes:
  * The names can identify common elements among edges.  For example,
      `"(a)-[e1]->(b); (b)-[e2]->(c)"` specifies that the same vertex `b` is the destination
      of edge `e1` and source of edge `e2`.
  * The names are used as column names in the result `DataFrame`.  If a motif contains
      named vertex `a`, then the result `DataFrame` will contain a column "a" which is a
      `StructType` with sub-fields equivalent to the schema (columns) of
      `GraphFrame.vertices`. Similarly, an edge `e` in a motif will produce a column "e"
      in the result `DataFrame` with sub-fields equivalent to the schema (columns) of
      `GraphFrame.edges`.
  * Be aware that names do *not* identify *distinct* elements: two elements with different
      names may refer to the same graph element.  For example, in the motif
      `"(a)-[e1]->(b); (b)-[e2]->(c)"`, the names `a` and `c` could refer to the same vertex.
      To restrict named elements to be distinct vertices or edges, use post-hoc filters
      such as `resultDataframe.filter("a.id != c.id")`.
* It is acceptable to omit names for vertices or edges in motifs when not needed.
   E.g., `"(a)-[]->(b)"` expresses an edge between vertices `a,b` but does not assign a name
   to the edge.  There will be no column for the anonymous edge in the result `DataFrame`.
   Similarly, `"(a)-[e]->()"` indicates an out-edge of vertex `a` but does not name
   the destination vertex.  These are called *anonymous* vertices and edges.
* An edge can be negated to indicate that the edge should *not* be present in the graph.
  E.g., `"(a)-[]->(b); !(b)-[]->(a)"` finds edges from `a` to `b` for which there is *no*
  edge from `b` to `a`.

Restrictions:

* Motifs are not allowed to contain edges without any named elements: `"()-[]->()"` and `"!()-[]->()"` are prohibited terms.
* Motifs are not allowed to contain named edges within negated terms (since these named edges would never appear within results).  E.g., `"!(a)-[ab]->(b)"` is invalid, but `"!(a)-[]->(b)"` is valid.
* Negation is not supported for the variable-length pattern, bidirectional pattern and undirected pattern: `"!(a)-[*1..3]->(b)"`, `"!(a)<-[]->(b)"` and `"!(a)-[]-(b)"` are not allowed.
* Unbounded length patten is not supported: `"(a)-[*..3]->(b)"` and `"(a)-[*1..]->(b)"` are not allowed.
* You cannot join additional edges with the variable length pattern: `"(a)-[*1..3]-(b);(b)-[]-(c)"`is not valid.

More complex queries, such as queries which operate on vertex or edge attributes,
can be expressed by applying filters to the result `DataFrame`.

This can return duplicate rows.  E.g., a query `"(u)-[]->()"` will return a result for each
matching edge, even if those edges share the same vertex `u`.

## Python API

For API details, refer to the @:pydoc(graphframes.GraphFrame.find).

```python
from graphframes.examples import Graphs

g = Graphs(spark).friends()  # Get example graph

# Search for pairs of vertices with edges in both directions between them
motifs = g.find("(a)-[e1]->(b); (b)-[e2]->(a)")
motifs.show()

# More complex queries can be expressed by applying filters
motifs.filter("b.age > 30").show()
```

## Scala API

For API details, refer to the @:scaladoc(org.graphframes.GraphFrame).

```scala
import org.apache.spark.sql.DataFrame
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends  // get example graph

// Search for pairs of vertices with edges in both directions between them.
val motifs: DataFrame = g.find("(a)-[e1]->(b); (b)-[e2]->(a)")
motifs.show()

// More complex queries can be expressed by applying filters.
motifs.filter("b.age > 30").show()
```

## Examples

Many motif queries are stateless and simple to express, as in the examples above. The next examples demonstrate more complex queries which carry state along a path in the motif. These queries can be expressed by combining GraphFrame motif finding with filters on the result, where the filters use sequence operations to construct a series of `DataFrame` `Column`s.

For example, suppose one wishes to identify a chain of 4 vertices with some property defined by a sequence of functions.  That is, among chains of 4 vertices `a->b->c->d`, identify the subset of chains matching this complex filter:

* Initialize state on path.
* Update state based on vertex `a`.
* Update state based on vertex `b`.
* Etc. for `c` and `d`.
* If final state matches some condition, then the chain is accepted by the filter.

The below code snippets demonstrate this process, where we identify chains of 4 vertices
such that at least 2 of the 3 edges are "friend" relationships.
In this example, the state is the current count of "friend" edges; in general, it could be any
`DataFrame Column`.

### Python API

```python
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import IntegerType
from graphframes.examples import Graphs


g = Graphs(spark).friends()  # Get example graph

chain4 = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")

# Query on sequence, with state (cnt)
# (a) Define method for updating state given the next element of the motif
sumFriends =\
  lambda cnt,relationship: when(relationship == "friend", cnt+1).otherwise(cnt)

# (b) Use sequence operation to apply method to sequence of elements in motif
# In this case, the elements are the 3 edges
condition =\
  reduce(lambda cnt,e: sumFriends(cnt, col(e).relationship), ["ab", "bc", "cd"], lit(0))

# (c) Apply filter to DataFrame
chainWith2Friends2 = chain4.where(condition >= 2)
chainWith2Friends2.show()
```

### Scala API

```scala
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, when}
import org.graphframes.{examples,GraphFrame}

val g: GraphFrame = examples.Graphs.friends  // get example graph

// Find chains of 4 vertices.
val chain4: DataFrame = g.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")
chain4.show()

// Query on sequence, with state (cnt)
//  (a) Define method for updating state given the next element of the motif.
def sumFriends(cnt: Column, relationship: Column): Column = {
  when(relationship === "friend", cnt + 1).otherwise(cnt)
}
//  (b) Use sequence operation to apply method to sequence of elements in motif.
//      In this case, the elements are the 3 edges.
val condition = { Seq("ab", "bc", "cd")
  .foldLeft(lit(0))((cnt, e) => sumFriends(cnt, col(e)("relationship"))) }
//  (c) Apply filter to DataFrame.
val chainWith2Friends2 = chain4.where(condition >= 2)
chainWith2Friends2.show()
```

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.graphframes

import java.util.Random

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{array, broadcast, col, count, explode, struct, udf, monotonically_increasing_id, expr}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import org.graphframes.lib._
import org.graphframes.pattern._

/**
 * A representation of a graph using `DataFrame`s.
 *
 * @groupname structure Structure information
 * @groupname conversions Conversions
 * @groupname stdlib Standard graph algorithms
 * @groupname subgraph Subgraph selection
 * @groupname degree Graph topology
 * @groupname motif Motif finding
 */
class GraphFrame private(
    @transient private val _vertices: DataFrame,
    @transient private val _edges: DataFrame) extends Logging with Serializable {

  import GraphFrame._

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null)

  override def toString: String = {
    // We call select on the vertices and edges to ensure that ID, SRC, DST always come first
    // in the printed schema.
    val v = vertices.select(ID, vertices.columns.filter(_ != ID) :_ *).toString
    val e = edges.select(SRC, DST +: edges.columns.filter(c => c != SRC && c != DST) :_ *).toString
    "GraphFrame(v:" + v + ", e:" + e + ")"
  }

  /**
   * Persist the dataframe representation of vertices and edges of the graph with the default
   * storage level.
   */
  def cache(): this.type = {
    persist()
  }

  /**
   * Persist the dataframe representation of vertices and edges of the graph with the default
   * storage level.
   */
  def persist(): this.type = {
    vertices.persist()
    edges.persist()
    this
  }

  /**
   * Persist the dataframe representation of vertices and edges of the graph with the given
   * storage level.
   * @param newLevel  One of: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`,
   * `MEMORY_AND_DISK_SER`, `DISK_ONLY`, `MEMORY_ONLY_2`, `MEMORY_AND_DISK_2`, etc..
   */
  def persist(newLevel: StorageLevel): this.type = {
    vertices.persist(newLevel)
    edges.persist(newLevel)
    this
  }

  /**
   * Mark the dataframe representation of vertices and edges of the graph as non-persistent, and
   * remove all blocks for it from memory and disk.
   */
  def unpersist(): this.type = {
    vertices.unpersist()
    edges.unpersist()
    this
  }

  /**
   * Mark the dataframe representation of vertices and edges of the graph as non-persistent, and
   * remove all blocks for it from memory and disk.
   * @param blocking  Whether to block until all blocks are deleted.
   */
  def unpersist(blocking: Boolean): this.type = {
    vertices.unpersist(blocking)
    edges.unpersist(blocking)
    this
  }

  // ============== Basic structural methods ============

  /**
   * The dataframe representation of the vertices of the graph.
   *
   * It contains a column called [[GraphFrame.ID]] with the id of the vertex,
   * and various other user-defined attributes with other attributes.
   *
   * The order of the columns is available in [[vertexColumns]].
   *
   * @group structure
   */
  def vertices: DataFrame = {
    if (_vertices == null) {
      throw new Exception("You cannot use GraphFrame objects within a Spark closure")
    }
    _vertices
  }

  /**
   * The dataframe representation of the edges of the graph.
   *
   * It contains two columns called [[GraphFrame.SRC]] and [[GraphFrame.DST]] that contain
   * the ids of the source vertex and the destination vertex of each edge, respectively.
   * It may also contain various other columns with user-defined attributes for each edge.
   *
   * For symmetric graphs, both pairs src -> dst and dst -> src are present with the same
   * attributes for each pair.
   *
   * The order of the columns is available in [[edgeColumns]].
   *
   * @group structure
   */
  // TODO(tjhunter) eventually clarify the treatment of duplicate edges
  def edges: DataFrame = {
    if (_edges == null) {
      throw new Exception("You cannot use GraphFrame objects within a Spark closure")
    }
    _edges
  }

  /**
   * Returns triplets: (source vertex)-[edge]->(destination vertex) for all edges in the graph.
   * The DataFrame returned has 3 columns, with names: [[GraphFrame.SRC]], [[GraphFrame.EDGE]],
   * and [[GraphFrame.DST]].  The 2 vertex columns have schema matching [[GraphFrame.vertices]],
   * and the edge column has a schema matching [[GraphFrame.edges]].
   *
   * @group structure
   */
  lazy val triplets: DataFrame = find(s"($SRC)-[$EDGE]->($DST)")

  // ============================ Conversions ========================================

  /**
   * Converts this [[GraphFrame]] instance to a GraphX `Graph`.
   * Vertex and edge attributes are the original rows in [[vertices]] and [[edges]], respectively.
   *
   * Note that vertex (and edge) attributes include vertex IDs (and source, destination IDs)
   * in order to support non-Long vertex IDs.  If the vertex IDs are not convertible to Long values,
   * then the values are indexed in order to generate corresponding Long vertex IDs (which is an
   * expensive operation).
   *
   * The column ordering of the returned `Graph` vertex and edge attributes are specified by
   * [[vertexColumns]] and [[edgeColumns]], respectively.
   *
   * @group conversions
   */
  def toGraphX: Graph[Row, Row] = {
    if (hasIntegralIdType) {
      val vv = vertices.select(col(ID).cast(LongType), nestAsCol(vertices, ATTR))
        .rdd.map { case Row(id: Long, attr: Row) => (id, attr) }
      val ee = edges.select(col(SRC).cast(LongType), col(DST).cast(LongType), nestAsCol(edges, ATTR))
        .rdd.map { case Row(srcId: Long, dstId: Long, attr: Row) => Edge(srcId, dstId, attr) }
      Graph(vv, ee)
    } else {
      // Compute Long vertex IDs
      val vv = indexedVertices.select(LONG_ID, ATTR).rdd.map { case Row(long_id: Long, attr: Row) => (long_id, attr) }
      val ee = indexedEdges.select(LONG_SRC, LONG_DST, ATTR).rdd.map { case Row(long_src: Long, long_dst: Long, attr: Row) =>
        Edge(long_src, long_dst, attr)
      }
      Graph(vv, ee)
    }
  }

  /**
   * The column names in the [[vertices]] DataFrame, in order.
   *
   * Helper method for [[toGraphX]] which specifies the schema of vertex attributes.
   * The vertex attributes of the returned `Graph` are given as a `Row`,
   * and this method defines the column ordering in that `Row`.
   *
   * @group conversions
   */
  def vertexColumns: Array[String] = vertices.columns

  /**
   * Version of [[vertexColumns]] which maps column names to indices in the Rows.
   *
   * @group conversions
   */
  def vertexColumnMap: Map[String, Int] = vertexColumns.zipWithIndex.toMap

  /**
   * The vertex names in the [[vertices]] DataFrame, in order.
   *
   * Helper method for [[toGraphX]] which specifies the schema of edge attributes.
   * The edge attributes of the returned `edges` are given as a `Row`,
   * and this method defines the column ordering in that `Row`.
   *
   * @group conversions
   */
  def edgeColumns: Array[String] = edges.columns

  /**
   * Version of [[edgeColumns]] which maps column names to indices in the Rows.
   *
   * @group conversions
   */
  def edgeColumnMap: Map[String, Int] = edgeColumns.zipWithIndex.toMap

  // ============================ Degree metrics =======================================

  /**
   * The out-degree of each vertex in the graph, returned as a DataFrame with two columns:
   *  - [[GraphFrame.ID]] the ID of the vertex
   *  - "outDegree" (integer) storing the out-degree of the vertex
   * Note that vertices with 0 out-edges are not returned in the result.
   *
   * @group degree
   */
  @transient lazy val outDegrees: DataFrame = {
    edges.groupBy(edges(SRC).as(ID)).agg(count("*").cast("int").as("outDegree"))
  }

  /**
   * The in-degree of each vertex in the graph, returned as a DataFame with two columns:
   *  - [[GraphFrame.ID]] the ID of the vertex
   * "- "inDegree" (int) storing the in-degree of the vertex
   * Note that vertices with 0 in-edges are not returned in the result.
   *
   * @group degree
   */
  @transient lazy val inDegrees: DataFrame = {
    edges.groupBy(edges(DST).as(ID)).agg(count("*").cast("int").as("inDegree"))
  }

  /**
   * The degree of each vertex in the graph, returned as a DataFrame with two columns:
   *  - [[GraphFrame.ID]] the ID of the vertex
   *  - 'degree' (integer) the degree of the vertex
   * Note that vertices with 0 edges are not returned in the result.
   *
   * @group degree
   */
  @transient lazy val degrees: DataFrame = {
    edges.select(explode(array(SRC, DST)).as(ID)).groupBy(ID).agg(count("*").cast("int").as("degree"))
  }

  // ============================ Motif finding ========================================

  /**
   * Motif finding: Searching the graph for structural patterns
   *
   * Motif finding uses a simple Domain-Specific Language (DSL) for expressing structural queries.
   * For example, `graph.find("(a)-[e]->(b); (b)-[e2]->(a)")` will search for pairs of vertices
   * `a,b` connected by edges in both directions.  It will return a `DataFrame` of all such
   * structures in the graph, with columns for each of the named elements (vertices or edges)
   * in the motif.  In this case, the returned columns will be in order of the pattern:
   * "a, e, b, e2."
   *
   * DSL for expressing structural patterns:
   *  - The basic unit of a pattern is an edge.
   *    For example, `"(a)-[e]->(b)"` expresses an edge `e` from vertex `a` to vertex `b`.
   *    Note that vertices are denoted by parentheses `(a)`, while edges are denoted by
   *    square brackets `[e]`.
   *  - A pattern is expressed as a union of edges. Edge patterns can be joined with semicolons.
   *    Motif `"(a)-[e]->(b); (b)-[e2]->(c)"` specifies two edges from `a` to `b` to `c`.
   *  - Within a pattern, names can be assigned to vertices and edges.  For example,
   *    `"(a)-[e]->(b)"` has three named elements: vertices `a,b` and edge `e`.
   *    These names serve two purposes:
   *     - The names can identify common elements among edges.  For example,
   *       `"(a)-[e]->(b); (b)-[e2]->(c)"` specifies that the same vertex `b` is the destination
   *       of edge `e` and source of edge `e2`.
   *     - The names are used as column names in the result `DataFrame`.  If a motif contains
   *       named vertex `a`, then the result `DataFrame` will contain a column "a" which is a
   *       `StructType` with sub-fields equivalent to the schema (columns) of
   *       [[GraphFrame.vertices]]. Similarly, an edge `e` in a motif will produce a column "e"
   *       in the result `DataFrame` with sub-fields equivalent to the schema (columns) of
   *       [[GraphFrame.edges]].
   *     - Be aware that names do *not* identify *distinct* elements: two elements with different
   *       names may refer to the same graph element.  For example, in the motif
   *       `"(a)-[e]->(b); (b)-[e2]->(c)"`, the names `a` and `c` could refer to the same vertex.
   *       To restrict named elements to be distinct vertices or edges, use post-hoc filters
   *       such as `resultDataframe.filter("a.id != c.id")`.
   *  - It is acceptable to omit names for vertices or edges in motifs when not needed.
   *    E.g., `"(a)-[]->(b)"` expresses an edge between vertices `a,b` but does not assign a name
   *    to the edge.  There will be no column for the anonymous edge in the result `DataFrame`.
   *    Similarly, `"(a)-[e]->()"` indicates an out-edge of vertex `a` but does not name
   *    the destination vertex.  These are called *anonymous* vertices and edges.
   *  - An edge can be negated to indicate that the edge should *not* be present in the graph.
   *    E.g., `"(a)-[]->(b); !(b)-[]->(a)"` finds edges from `a` to `b` for which there is *no*
   *    edge from `b` to `a`.
   *
   * Restrictions:
   *  - Motifs are not allowed to contain edges without any named elements: `"()-[]->()"` and
   *    `"!()-[]->()"` are prohibited terms.
   *  - Motifs are not allowed to contain named edges within negated terms (since these named
   *    edges would never appear within results).  E.g., `"!(a)-[ab]->(b)"` is invalid, but
   *    `"!(a)-[]->(b)"` is valid.
   *
   * More complex queries, such as queries which operate on vertex or edge attributes,
   * can be expressed by applying filters to the result `DataFrame`.
   *
   * This can return duplicate rows.  E.g., a query `"(u)-[]->()"` will return a result for each
   * matching edge, even if those edges share the same vertex `u`.
   *
   * @param pattern  Pattern specifying a motif to search for.
   * @return  `DataFrame` containing all instances of the motif.
   * @group motif
   */
  def find(pattern: String): DataFrame = {
    val patterns = Pattern.parse(pattern)

    // For each named vertex appearing only in a negated term, we augment the positive terms
    // with the vertex as a standalone term `(v)`.
    // See https://github.com/graphframes/graphframes/issues/276
    val namedVerticesOnlyInNegatedTerms = Pattern.findNamedVerticesOnlyInNegatedTerms(patterns)
    val extraPositivePatterns = namedVerticesOnlyInNegatedTerms.map(v => NamedVertex(v))
    val augmentedPatterns = extraPositivePatterns ++ patterns
    val df = findSimple(augmentedPatterns)

    val names = Pattern.findNamedElementsInOrder(patterns, includeEdges = true)
    if (names.isEmpty) df else df.select(names.head, names.tail : _*)
  }

  // ======================== Other queries ===================================

  /**
   * Breadth-first search (BFS)
   *
   * Refer to the documentation of [[org.graphframes.lib.BFS]] for the description of the output.
   *
   * @group stdlib
   */
  def bfs: BFS = new BFS(this)

  /**
   * This is a primitive for implementing graph algorithms.
   * This method aggregates values from the neighboring edges and vertices of each vertex.
   * See [[org.graphframes.lib.AggregateMessages AggregateMessages]] for detailed documentation.
   */
  def aggregateMessages: AggregateMessages = new AggregateMessages(this)

  /**
   * Filter the vertices according to Column expression, remove edges containing any dropped
   * vertices.
   * @group subgraph
   */
  def filterVertices(condition: Column): GraphFrame = {
    val vv = vertices.filter(condition)
    val ee = edges.join(vv, vv(ID) === edges(SRC), "left_semi")
                  .join(vv, vv(ID) === edges(DST), "left_semi")
    GraphFrame(vv, ee)
  }

  /**
   * Filter the vertices according to String expression, remove edges containing any dropped
   * vertices.
   * @group subgraph
   */
  def filterVertices(conditionExpr: String): GraphFrame = filterVertices(expr(conditionExpr))

  /** 
   * Filter the edges according to Column expression, keep all vertices.
   * @group subgraph
   */
  def filterEdges(condition: Column): GraphFrame = {
    val vv = vertices
    val ee = edges.filter(condition)
    GraphFrame(vv, ee)
  }

  /** 
   * Filter the edges according to String expression.
   * @group subgraph
   */
  def filterEdges(conditionExpr: String): GraphFrame = filterEdges(expr(conditionExpr))

  /**
   * Drop isolated vertices, vertices not contained in any edges.
   * @group subgraph
   */
  def dropIsolatedVertices(): GraphFrame = {
    val ee = edges
    val e1 = ee.withColumn(ID, explode(array(col(SRC), col(DST))))
    val vv = vertices.join(e1, Seq(ID), "left_semi")
    GraphFrame(vv, ee)
  }

  // **** Standard library ****

  /**
   * Connected component algorithm.
   *
   * See [[org.graphframes.lib.ConnectedComponents]] for more details.
   *
   * @group stdlib
   */
  def connectedComponents: ConnectedComponents = new ConnectedComponents(this)

  /**
   * Label propagation algorithm.
   *
   * See [[org.graphframes.lib.LabelPropagation]] for more details.
   *
   * @group stdlib
   */
  def labelPropagation: LabelPropagation = new LabelPropagation(this)

  /**
   * PageRank algorithm.
   *
   * See [[org.graphframes.lib.PageRank]] for more details.
   *
   * @group stdlib
   */
  def pageRank: PageRank = new PageRank(this)

  /**
    * Parallel personalized PageRank algorithm.
    *
    * See [[org.graphframes.lib.ParallelPersonalizedPageRank]] for more details.
    *
    * @group stdlib
    */
  def parallelPersonalizedPageRank: ParallelPersonalizedPageRank =
    new ParallelPersonalizedPageRank(this)

  /**
   * Pregel algorithm.
   *
   * @see [[org.graphframes.lib.Pregel]]
   * @group stdlib
   */
  def pregel = new Pregel(this)

  /**
   * Shortest paths algorithm.
   *
   * See [[org.graphframes.lib.ShortestPaths]] for more details.
   *
   * @group stdlib
   */
  def shortestPaths: ShortestPaths = new ShortestPaths(this)

  /**
   * Strongly connected components algorithm.
   *
   * See [[org.graphframes.lib.StronglyConnectedComponents]] for more details.
   *
   * @group stdlib
   */
  def stronglyConnectedComponents: StronglyConnectedComponents =
    new StronglyConnectedComponents(this)

  /**
   * SVD++ algorithm.
   *
   * See [[org.graphframes.lib.SVDPlusPlus]] for more details.
   *
   * @group stdlib
   */
  def svdPlusPlus: SVDPlusPlus = new SVDPlusPlus(this)

  /**
   * Triangle count algorithm.
   *
   * See [[org.graphframes.lib.TriangleCount]] for more details.
   *
   * @group stdlib
   */
  def triangleCount: TriangleCount = new TriangleCount(this)

  // ========= Motif finding (private) =========

  /**
   * Primary method implementing motif finding.
   * This iterative method handles one pattern (via [[findIncremental()]] on each iteration,
   * augmenting the `DataFrame` in prevDF with each new pattern.
   *
   * @return `DataFrame` containing all instances of the motif specified by the given patterns
   */
  private def findSimple(patterns: Seq[Pattern]): DataFrame = {
    val (_, finalDFOpt, _) =
      patterns.foldLeft((Seq.empty[Pattern], Option.empty[DataFrame], Seq.empty[String])) {
        case ((handledPatterns, dfOpt, names), cur) =>
          val (nextDF, nextNames) = findIncremental(this, handledPatterns, dfOpt, names, cur)
          (handledPatterns :+ cur, nextDF, nextNames)
      }
    finalDFOpt.getOrElse(sqlContext.emptyDataFrame)
  }

  // ========= Other private methods ===========

  private[graphframes] def sqlContext: SQLContext = vertices.sqlContext

  /**
   * True if the id type can be cast to Long.
   *
   * This is important for performance reasons. The underlying graphx
   * implementation only deals with Long types.
   */
  private[graphframes] lazy val hasIntegralIdType: Boolean = {
    vertices.schema(ID).dataType match {
      case _ @ (ByteType | IntegerType | LongType | ShortType) => true
      case _ => false
    }
  }

  /**
   * Vertices with each vertex assigned a unique long ID.
   * If the vertex ID type is integral, this casts the original IDs to long.
   *
   * Columns:
   *  - $LONG_ID: the new ID of LongType
   *  - $ORIGINAL_ID: the ID provided by the user
   *  - $ATTR: all the original vertex attributes
   */
  private[graphframes] lazy val indexedVertices: DataFrame = {
    if (hasIntegralIdType) {
      val indexedVertices = vertices.select(nestAsCol(vertices, ATTR))
      indexedVertices.select(
        col(ATTR + "." + ID).cast("long").as(LONG_ID), col(ATTR + "." + ID).as(ID), col(ATTR))
    } else {
      val withLongIds = vertices.select(ID)
        .repartition(col(ID))
        .distinct()
        .sortWithinPartitions(ID)
        .withColumn(LONG_ID, monotonically_increasing_id())
        .persist(StorageLevel.MEMORY_AND_DISK)
      vertices.select(col(ID), nestAsCol(vertices, ATTR))
        .join(withLongIds, ID)
        .select(LONG_ID, ID, ATTR)
    }
  }

  /**
   * Columns:
   *  - $SRC
   *  - $LONG_SRC
   *  - $DST
   *  - $LONG_DST
   *  - $ATTR
   */
  private[graphframes] lazy val indexedEdges: DataFrame = {
    val packedEdges = edges.select(col(SRC), col(DST), nestAsCol(edges, ATTR))
    if (hasIntegralIdType) {
      packedEdges.select(
        col(SRC), col(SRC).cast("long").as(LONG_SRC),
        col(DST), col(DST).cast("long").as(LONG_DST),
        col(ATTR))
    } else {
      val threshold = broadcastThreshold
      val hubs: Set[Any] = degrees.filter(col("degree") >= threshold).select(ID)
        .collect().map(_.get(0)).toSet
      val indexedSourceEdges = GraphFrame.skewedJoin(
        packedEdges,
        indexedVertices.select(col(ID).as(SRC), col(LONG_ID).as(LONG_SRC)),
        SRC, hubs, "GraphFrame.indexedEdges:")
      val indexedEdges = GraphFrame.skewedJoin(
        indexedSourceEdges,
        indexedVertices.select(col(ID).as(DST), col(LONG_ID).as(LONG_DST)),
        DST, hubs, "GraphFrame.indexedEdges:")
      indexedEdges.select(SRC, LONG_SRC, DST, LONG_DST, ATTR)
    }
  }

  /**
   * A cached conversion of this graph to the GraphX structure. All the data is stripped away.
   */
  @transient lazy private[graphframes] val cachedTopologyGraphX: Graph[Unit, Unit] = {
    cachedGraphX.mapVertices((_, _) => ()).mapEdges(e => ())
  }

  /**
   * A cached conversion of this graph to the GraphX structure, with the data stored for each edge and vertex.
   */
  @transient private lazy val cachedGraphX: Graph[Row, Row] = { toGraphX }

}


object GraphFrame extends Serializable with Logging {

  /**
   * Implements `a.join(b, joinCol)`, handling skew in the join keys.
   * @param a  DataFrame which may have multiple rows with the same key in `joinCol`
   * @param b  DataFrame which has exactly 1 row for every key in `a.joinCol`.
   * @param joinCol  Name of column on which to do join
   * @param hubs  Set of join keys which are high-degree (skewed)
   * @param logPrefix  Prefix for logging, e.g., name of algorithm doing the join
   * @return  `a.join(b, joinCol)`
   * @tparam T  DataType for join key
   */
  private[graphframes] def skewedJoin[T : TypeTag](
      a: DataFrame,
      b: DataFrame,
      joinCol: String,
      hubs: Set[T],
      logPrefix: String): DataFrame = {
    val sqlContext = a.sqlContext
    import sqlContext.implicits._
    if (hubs.isEmpty) {
      // No skew.  Do regular join.
      a.join(b, joinCol)
    } else {
      logDebug(s"$logPrefix Skewed join with ${hubs.size} high-degree keys.")
      val isHub = udf { id: T =>
        hubs.contains(id)
      }
      val hashJoined = a.filter(!isHub(col(joinCol)))
        .join(b.filter(!isHub(col(joinCol))), joinCol)
      val broadcastJoined = a.filter(isHub(col(joinCol)))
        .join(broadcast(b.filter(isHub(col(joinCol)))), joinCol)
      hashJoined.unionAll(broadcastJoined)
    }
  }

  /** Column name for vertex IDs in [[GraphFrame.vertices]] */
  val ID: String = "id"

  /**
   * Column name for source vertices of edges.
   *  - In [[GraphFrame.edges]], this is a column of vertex IDs.
   *  - In [[GraphFrame.triplets]], this is a column of vertices with schema matching
   *    [[GraphFrame.vertices]].
   */
  val SRC: String = "src"

  /**
   * Column name for destination vertices of edges.
   *  - In [[GraphFrame.edges]], this is a column of vertex IDs.
   *  - In [[GraphFrame.triplets]], this is a column of vertices with schema matching
   *    [[GraphFrame.vertices]].
   */
  val DST: String = "dst"

  /**
   * Column name for edge in [[GraphFrame.triplets]].  In [[GraphFrame.triplets]],
   * this is a column of edges with schema matching [[GraphFrame.edges]].
   */
  val EDGE: String = "edge"

  // ============================ Constructors and converters =================================

  /**
   * Create a new [[GraphFrame]] from vertex and edge `DataFrame`s.
   *
   * @param vertices  Vertex DataFrame.  This must include a column "id" containing unique vertex IDs.
   *           All other columns are treated as vertex attributes.
   * @param edges  Edge DataFrame.  This must include columns "src" and "dst" containing source and
   *           destination vertex IDs.  All other columns are treated as edge attributes.
   * @return  New [[GraphFrame]] instance
   */
  def apply(vertices: DataFrame, edges: DataFrame): GraphFrame = {
    require(vertices.columns.contains(ID),
      s"Vertex ID column '$ID' missing from vertex DataFrame, which has columns: "
        + vertices.columns.mkString(","))
    require(edges.columns.contains(SRC),
      s"Source vertex ID column '$SRC' missing from edge DataFrame, which has columns: "
        + edges.columns.mkString(","))
    require(edges.columns.contains(DST),
      s"Destination vertex ID column '$DST' missing from edge DataFrame, which has columns: "
        + edges.columns.mkString(","))

    new GraphFrame(vertices, edges)
  }

  /**
   * Create a new [[GraphFrame]] from an edge `DataFrame`.
   * The resulting [[GraphFrame]] will have [[GraphFrame.vertices]] with a single "id" column.
   *
   * Note: The [[GraphFrame.vertices]] DataFrame will be persisted at level
   *       `StorageLevel.MEMORY_AND_DISK`.
   * @param e  Edge DataFrame.  This must include columns "src" and "dst" containing source and
   *           destination vertex IDs.  All other columns are treated as edge attributes.
   * @return  New [[GraphFrame]] instance
   *
   * @group conversions
   */
  def fromEdges(e: DataFrame): GraphFrame = {
    val srcs = e.select(e("src").as("id"))
    val dsts = e.select(e("dst").as("id"))
    val v = srcs.unionAll(dsts).distinct
    v.persist(StorageLevel.MEMORY_AND_DISK)
    apply(v, e)
  }

  /**
   * Converts a GraphX `Graph` instance into a [[GraphFrame]].
   *
   * This converts each `org.apache.spark.rdd.RDD` in the `Graph` to a `DataFrame` using
   * schema inference.
   *
   * Vertex ID column names will be converted to "id" for the vertex DataFrame,
   * and to "src" and "dst" for the edge DataFrame.
   *
   * @group conversions
   */
  // TODO: Add version which takes explicit schemas.
  def fromGraphX[VD : TypeTag, ED : TypeTag](graph: Graph[VD, ED]): GraphFrame = {
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    val vv = sqlContext.createDataFrame(graph.vertices).toDF(ID, ATTR)
    val ee = sqlContext.createDataFrame(graph.edges).toDF(SRC, DST, ATTR)
    GraphFrame(vv, ee)
  }


  /**
   * Given:
   *  - a GraphFrame `originalGraph`
   *  - a GraphX graph derived from the GraphFrame using [[GraphFrame.toGraphX]]
   * this method merges attributes from the GraphX graph into the original GraphFrame.
   *
   * This method is useful for doing computations using the GraphX API and then merging the results
   * with a GraphFrame.  For example, given:
   *  - GraphFrame `originalGraph`
   *  - GraphX Graph[String, Int] `graph` with a String vertex attribute we want to call "category"
   *    and an Int edge attribute we want to call "count"
   * We can call `fromGraphX(originalGraph, graph, Seq("category"), Seq("count"))` to produce
   * a new GraphFrame. The new GraphFrame will be an augmented version of `originalGraph`,
   * with new [[GraphFrame.vertices]] column "category" and new [[GraphFrame.edges]] column
   * "count" added.
   *
   * See [[org.graphframes.examples.BeliefPropagation]] for example usage.
   *
   * @param originalGraph  Original GraphFrame used to compute the GraphX graph.
   * @param graph  GraphX graph. Vertex and edge attributes, if any, will be merged into
   *               the original graph as new columns.  If the attributes are `Product` types
   *               such as tuples, then each element of the `Product` will be put in a separate
   *               column.  If the attributes are other types, then the entire GraphX attribute
   *               will become a single new column.
   * @param vertexNames  Column name(s) for vertex attributes in the GraphX graph.
   *                     If there is no vertex attribute, this should be empty.
   *                     If there is a singleton attribute, this should have a single column name.
   *                     If the attribute is a `Product` type, this should be a list of names
   *                     matching the order of the attribute elements.
   * @param edgeNames  Column name(s) for edge attributes in the GraphX graph.
   *                     If there is no edge attribute, this should be empty.
   *                     If there is a singleton attribute, this should have a single column name.
   *                     If the attribute is a `Product` type, this should be a list of names
   *                     matching the order of the attribute elements.
   * @tparam V the type of the vertex data
   * @tparam E the type of the edge data
   * @return original graph augmented with vertex and column attributes from the GraphX graph
   *
   * @group conversions
   */
  def fromGraphX[V : TypeTag, E : TypeTag](
      originalGraph: GraphFrame,
      graph: Graph[V, E],
      vertexNames: Seq[String] = Nil,
      edgeNames: Seq[String] = Nil): GraphFrame = {
    GraphXConversions.fromGraphX[V, E](originalGraph, graph, vertexNames, edgeNames)
  }


  // ============== Private constants ==============

  /** Default name for attribute columns when converting from GraphX [[Graph]] format */
  private[graphframes] val ATTR: String = "attr"

  /**
   * The integral id that is used as a surrogate id when using graphX implementation
   */
  private[graphframes] val LONG_ID: String = "new_id"

  private[graphframes] val LONG_SRC: String = "new_src"
  private[graphframes] val LONG_DST: String = "new_dst"
  private[graphframes] val GX_ATTR: String = "graphx_attr"



  /** Helper for using [col].* in Spark 1.4.  Returns sequence of [col].[field] for all fields */
  private[graphframes] def colStar(df: DataFrame, col: String): Seq[String] = {
    df.schema(col).dataType match {
      case s: StructType =>
        s.fieldNames.map(f => col + "." + f)
      case other =>
        throw new RuntimeException(s"Unknown error in GraphFrame. Expected column $col to be" +
          s" StructType, but found type: $other")
    }
  }

  /** Nest all columns within a single StructType column with the given name */
  private[graphframes] def nestAsCol(df: DataFrame, name: String): Column = {
    struct(df.columns.map(c => df(c)) :_*).as(name)
  }

  // ========== Motif finding ==========

  private val random: Random = new Random(classOf[GraphFrame].getName.##)

  private def prefixWithName(name: String, col: String): String = name + "." + col
  private def vId(name: String): String = prefixWithName(name, ID)
  private def eSrcId(name: String): String = prefixWithName(name, SRC)
  private def eDstId(name: String): String = prefixWithName(name, DST)


  private def maybeCrossJoin(aOpt: Option[DataFrame], b: DataFrame): DataFrame = {
    aOpt match {
      case Some(a) => a.crossJoin(b)
      case None => b
    }
  }

  private def maybeJoin(
      aOpt: Option[DataFrame],
      b: DataFrame,
      joinExprs: DataFrame => Column): DataFrame = {
    aOpt match {
      case Some(a) => a.join(b, joinExprs(a))
      case None => b
    }
  }


  /** Indicate whether a named vertex has been seen in any of the given patterns */
  private def seen(v: NamedVertex, patterns: Seq[Pattern]) = patterns.exists(p => seen1(v, p))

  /** Indicate whether a named vertex has been seen in the given pattern */
  private def seen1(v: NamedVertex, pattern: Pattern): Boolean = pattern match {
    case Negation(edge) =>
      seen1(v, edge)
    case AnonymousEdge(src, dst) =>
      seen1(v, src) || seen1(v, dst)
    case NamedEdge(_, src, dst) =>
      seen1(v, src) || seen1(v, dst)
    case v2 @ NamedVertex(_) =>
      v2 == v
    case AnonymousVertex =>
      false
  }


  /**
   * Augment the given DataFrame based on a pattern.
   *
   * @param prevPatterns  Patterns which have contributed to the given DataFrame
   * @param prev  Given DataFrame
   * @param pattern  Pattern to search for
   * @return  DataFrame augmented with the current search pattern
   */
  private def findIncremental(
      gf: GraphFrame,
      prevPatterns: Seq[Pattern],
      prev: Option[DataFrame],
      prevNames: Seq[String],
      pattern: Pattern): (Option[DataFrame], Seq[String]) = {
    def nestE(name: String): DataFrame = gf.edges.select(nestAsCol(gf.edges, name))
    def nestV(name: String): DataFrame = gf.vertices.select(nestAsCol(gf.vertices, name))

    pattern match {

      case AnonymousVertex =>
        (prev, prevNames)

      case v @ NamedVertex(name) =>
        if (seen(v, prevPatterns)) {
          for (prev <- prev) assert(prev.columns.toSet.contains(name))
          (prev, prevNames)
        } else {
          (Some(maybeCrossJoin(prev, nestV(name))), prevNames :+ name)
        }

      case NamedEdge(name, AnonymousVertex, AnonymousVertex) =>
        val eRen = nestE(name)
        (Some(maybeCrossJoin(prev, eRen)), prevNames :+ name)

      case NamedEdge(name, AnonymousVertex, dst @ NamedVertex(dstName)) =>
        if (seen(dst, prevPatterns)) {
          val eRen = nestE(name)
          (Some(maybeJoin(prev, eRen, prev => eRen(eDstId(name)) === prev(vId(dstName)))),
            prevNames :+ name)
        } else {
          val eRen = nestE(name)
          val dstV = nestV(dstName)
          (Some(maybeCrossJoin(prev, eRen)
            .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName)))),
            prevNames :+ name :+ dstName)
        }

      case NamedEdge(name, src @ NamedVertex(srcName), AnonymousVertex) =>
        if (seen(src, prevPatterns)) {
          val eRen = nestE(name)
          (Some(maybeJoin(prev, eRen, prev => eRen(eSrcId(name)) === prev(vId(srcName)))),
            prevNames :+ name)
        } else {
          val eRen = nestE(name)
          val srcV = nestV(srcName)
          (Some(maybeCrossJoin(prev, eRen)
            .join(srcV, eRen(eSrcId(name)) === srcV(vId(srcName)))),
             prevNames :+ srcName :+ name)
        }

      case NamedEdge(name, src @ NamedVertex(srcName), dst @ NamedVertex(dstName)) =>
        (seen(src, prevPatterns), seen(dst, prevPatterns)) match {
          case (true, true) =>
            val eRen = nestE(name)
            (Some(maybeJoin(prev, eRen, prev =>
              eRen(eSrcId(name)) === prev(vId(srcName)) && eRen(eDstId(name)) === prev(vId(dstName)))),
              prevNames :+ name)

          case (true, false) =>
            val eRen = nestE(name)
            val dstV = nestV(dstName)
            (Some(maybeJoin(prev, eRen, prev => eRen(eSrcId(name)) === prev(vId(srcName)))
              .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName)))),
              prevNames :+ name :+ dstName)

          case (false, true) =>
            val eRen = nestE(name)
            val srcV = nestV(srcName)
            (Some(maybeJoin(prev, eRen, prev => eRen(eDstId(name)) === prev(vId(dstName)))
              .join(srcV, eRen(eSrcId(name)) === srcV(vId(srcName)))),
              prevNames :+ srcName :+ name)

          case (false, false) if srcName != dstName =>
            val eRen = nestE(name)
            val srcV = nestV(srcName)
            val dstV = nestV(dstName)
            (Some(maybeCrossJoin(prev, eRen)
              .join(srcV, eRen(eSrcId(name)) === srcV(vId(srcName)))
              .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName)))),
              prevNames :+ srcName :+ name :+ dstName)
          // TODO: expose the plans from joining these in the opposite order

          case (false, false) if srcName == dstName =>
            val eRen = nestE(name)
            val srcV = nestV(srcName)
            (Some(maybeCrossJoin(prev, eRen)
              .join(srcV,
                eRen(eSrcId(name)) === srcV(vId(srcName)) &&
                  eRen(eDstId(name)) === srcV(vId(srcName)))),
              prevNames :+ srcName :+ name)
        }

      case AnonymousEdge(src, dst) =>
        val tmpName = "__tmp" + random.nextLong.toString
        val (df, names) = findIncremental(gf, prevPatterns, prev, prevNames, NamedEdge(tmpName, src, dst))
        (df.map(_.drop(tmpName)), names.filter(_ != tmpName))

      case Negation(edge) => prev match {
        case Some(p) =>
          val (df, names) = findIncremental(gf, prevPatterns, Some(p), prevNames, edge)
          (df.map(result => p.except(result)), names)
        case None =>
          throw new InvalidPatternException
      }
    }
  }

  /**
   * Controls broadcast threshold in skewed joins.
   * Use normal joins for vertices with degrees less than the threshold,
   * and broadcast joins otherwise.
   * The default value is 1000000.
   * If we have less than 100 billion edges, this would collect at most
   * 2e11 / 1000000 = 200000 hubs, which could be handled by the driver.
   */
  private[this] var _broadcastThreshold: Int = 1000000

  private[graphframes] def broadcastThreshold: Int = _broadcastThreshold

  // for unit testing only
  private[graphframes] def setBroadcastThreshold(value: Int): this.type = {
    require(value >= 0)
    _broadcastThreshold = value
    this
  }
}
